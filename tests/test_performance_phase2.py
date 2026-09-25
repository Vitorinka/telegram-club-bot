import asyncio
import hashlib
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.test_aiogram3_bootstrap import FakeMiniAppRequest, import_main


class PerformancePhase2Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.main = import_main()
        with self.main.MINIAPP_ADMIN_SESSION_CACHE_LOCK:
            self.main.MINIAPP_ADMIN_SESSION_CACHE.clear()

    async def asyncTearDown(self):
        with self.main.MINIAPP_ADMIN_SESSION_CACHE_LOCK:
            self.main.MINIAPP_ADMIN_SESSION_CACHE.clear()
        await self.main.bot.session.close()

    async def test_admin_session_cache_avoids_repeated_db_lookup_and_revoke_invalidates(self):
        session = SimpleNamespace(
            session_id="session-1", telegram_id=1,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        request = FakeMiniAppRequest(None, "Bearer token")
        with patch.object(
            self.main, "load_miniapp_admin_session", return_value=session
        ) as load:
            first = await self.main.run_sync_db(
                self.main.authenticate_miniapp_session, request
            )
            second = await self.main.run_sync_db(
                self.main.authenticate_miniapp_session, request
            )
            self.assertIs(first, session)
            self.assertIs(second, session)
            load.assert_called_once()
            self.main._invalidate_admin_session_cache(session_id="session-1")
            await self.main.run_sync_db(
                self.main.authenticate_miniapp_session, request
            )
            self.assertEqual(load.call_count, 2)

    async def test_cached_session_never_outlives_database_expiry(self):
        session = SimpleNamespace(
            session_id="session-expired", telegram_id=1,
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
        token_hash = hashlib.sha256(b"token").hexdigest()
        with self.main.MINIAPP_ADMIN_SESSION_CACHE_LOCK:
            self.main.MINIAPP_ADMIN_SESSION_CACHE[token_hash] = (
                time.monotonic() + 30, session,
            )
        fresh = SimpleNamespace(
            session_id="session-fresh", telegram_id=1,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        with patch.object(
            self.main, "load_miniapp_admin_session", return_value=fresh
        ) as load:
            result = self.main._cached_admin_session("token")
        self.assertIs(result, fresh)
        load.assert_called_once()

    async def test_slow_db_work_does_not_block_light_async_request(self):
        started = asyncio.Event()

        def slow_operation():
            started_loop.call_soon_threadsafe(started.set)
            time.sleep(0.2)
            return "slow"

        started_loop = asyncio.get_running_loop()
        slow_task = asyncio.create_task(
            self.main.run_sync_db(slow_operation)
        )
        await asyncio.wait_for(started.wait(), timeout=0.1)
        light_started = time.perf_counter()
        await asyncio.sleep(0)
        light_elapsed = time.perf_counter() - light_started
        self.assertLess(light_elapsed, 0.05)
        self.assertFalse(slow_task.done())
        self.assertEqual(await slow_task, "slow")

    async def test_db_telemetry_counts_queries_without_exposing_parameters(self):
        class Cursor:
            def execute(self, _sql, _params=None):
                return self

        metrics = {
            "pool_wait_ms": 0.0, "sql_ms": 0.0,
            "query_count": 0, "lock": threading.Lock(),
        }
        token = self.main.MINIAPP_DB_METRICS.set(metrics)
        try:
            cursor = self.main.TrackedDbCursor(Cursor())
            cursor.execute("SELECT 1 WHERE secret = %s", ("credential",))
        finally:
            self.main.MINIAPP_DB_METRICS.reset(token)
        self.assertEqual(metrics["query_count"], 1)
        self.assertGreaterEqual(metrics["sql_ms"], 0.0)

    async def test_admin_cover_proxy_accepts_safe_telegram_reencoding(self):
        request = FakeMiniAppRequest(
            None, path="/api/admin/content/cms/content/media/media",
            match_info={"content_id": "content", "media_id": "media"},
        )
        media = {
            "media_type": "cover", "mime_type": "image/png",
            "server_reference": "telegram-file-id",
        }
        telegram_file = SimpleNamespace(file_path="photos/file.jpg", file_size=8)

        async def download_file(_path, destination, **_kwargs):
            destination.write(b"\xff\xd8\xffsafe-jpeg")

        with patch.object(
            self.main, "get_media_reference", return_value=media
        ), patch.object(
            self.main.bot, "get_file", new=AsyncMock(return_value=telegram_file)
        ), patch.object(
            self.main.bot, "download_file", new=AsyncMock(side_effect=download_file)
        ):
            response = await self.main.miniapp_admin_content_media_proxy(request)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.content_type, "image/jpeg")
        self.assertEqual(response.body, b"\xff\xd8\xffsafe-jpeg")


if __name__ == "__main__":
    unittest.main()
