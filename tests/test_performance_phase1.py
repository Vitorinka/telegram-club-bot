import asyncio
import json
import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests.test_aiogram3_bootstrap import import_main
from bookable_classes import BookableClassError, list_admin_classes


ROOT = Path(__file__).resolve().parents[1]
APP_JS = ROOT / "miniapp" / "app.js"


class PerformancePhase1Tests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.main = import_main()

    async def test_versioned_assets_are_immutable_and_html_revalidates(self):
        html_response = await self.main.miniapp_index(SimpleNamespace(query={}))
        self.assertEqual(html_response.headers["Cache-Control"], "no-cache, must-revalidate")
        body = html_response.text
        js_version = self.main.miniapp_asset_version(self.main.MINIAPP_ASSET_DIR / "app.js")
        css_version = self.main.miniapp_asset_version(self.main.MINIAPP_ASSET_DIR / "styles.css")
        self.assertIn(f"/miniapp/app.js?v={js_version}", body)
        self.assertIn(f"/miniapp/styles.css?v={css_version}", body)
        js_response = await self.main.miniapp_javascript(SimpleNamespace(query={"v": js_version}))
        stale_response = await self.main.miniapp_javascript(SimpleNamespace(query={"v": "stale"}))
        self.assertEqual(js_response.headers["Cache-Control"], "public, max-age=31536000, immutable")
        self.assertEqual(stale_response.headers["Cache-Control"], "no-cache, must-revalidate")

    async def test_member_entitlement_is_request_scoped(self):
        request = {"miniapp_member": SimpleNamespace(telegram_id=42)}
        access = {"has_active_access": True, "expires_at": "future"}
        with patch.object(self.main, "member_access", return_value=access) as lookup:
            self.assertIs(self.main.member_request_access(request), access)
            self.assertIs(self.main.member_request_access(request), access)
        lookup.assert_called_once_with(self.main.get_db_conn, 42)

    def test_cover_queue_is_bounded_and_frontend_uses_lazy_cache(self):
        script = r"""
          const core=require('./miniapp/app.js');
          const queue=core.createBoundedTaskQueue(4);
          let active=0,peak=0;
          const tasks=Array.from({length:20},()=>queue.add(async()=>{
            active+=1; peak=Math.max(peak,active);
            await new Promise(resolve=>setTimeout(resolve,3));
            active-=1;
          }));
          Promise.all(tasks).then(async()=>{
            const cache=new Map(),pending=new Map(); let fetches=0;
            const args={cache,pending,key:'cover-1',load:async()=>{fetches+=1; return 'blob:cover-1';},store:(key,value)=>cache.set(key,value)};
            await core.getOrCreateCachedResource(args);
            await core.getOrCreateCachedResource(args);
            console.log(JSON.stringify({peak,stats:queue.stats(),fetches}));
          });
        """
        result = subprocess.run(
            ["node", "-e", script], cwd=ROOT, check=True,
            text=True, capture_output=True,
        )
        payload = json.loads(result.stdout.strip())
        self.assertLessEqual(payload["peak"], 4)
        self.assertEqual(payload["fetches"], 1)
        source = APP_JS.read_text(encoding="utf-8")
        self.assertIn('new IntersectionObserver', source)
        self.assertIn('rootMargin:"300px 0px"', source)
        self.assertIn('const cached=memberCoverUrls.get(key)', source)
        self.assertIn('MEMBER_COVER_CACHE_LIMIT = 64', source)
        self.assertNotIn('memberLibrarySearch.addEventListener("input", renderMemberLibrary)', source)

    def test_search_cancellation_and_overview_request_deduplication(self):
        source = APP_JS.read_text(encoding="utf-8")
        self.assertIn('adminSearchController.abort()', source)
        self.assertIn('if(error.name === "AbortError") return', source)
        dashboard = source[source.index("const loadDashboard ="):source.index("const addBadges =")]
        self.assertEqual(dashboard.count('/api/admin/gifts?'), 0)
        self.assertEqual(dashboard.count('/api/admin/failed-subscriptions?'), 0)
        self.assertNotIn('/api/admin/content/cms?', dashboard)
        self.assertNotIn('/api/admin/users?', dashboard)
        self.assertNotIn('/api/admin/schedule?', dashboard)
        bootstrap = source[source.index('fetch("/api/admin/session"'):]
        self.assertIn('return loadDashboard().then(() => {', bootstrap)

    def test_telemetry_contract_does_not_log_credentials(self):
        source = (ROOT / "main.py").read_text(encoding="utf-8")
        marker = source[source.index('"MINIAPP_API_PERF method='):source.index('return response', source.index('"MINIAPP_API_PERF method='))]
        for forbidden in ("Authorization", "initData", "file_path", "stripe_subscription_id", "BOT_TOKEN"):
            self.assertNotIn(forbidden, marker)
        self.assertIn("auth_ms=%.1f", marker)
        self.assertIn("handler_ms=%.1f", marker)
        self.assertIn("total_ms=%.1f", marker)
        self.assertIn("response_bytes=%s", marker)

    def test_admin_classes_reject_malformed_cursor_before_database_access(self):
        with self.assertRaisesRegex(BookableClassError, "invalid_cursor"):
            list_admin_classes(lambda: self.fail("database must not be opened"), cursor="%%%")


if __name__ == "__main__":
    unittest.main()
