import asyncio
import ast
import hashlib
import hmac
import importlib
import json
import os
from pathlib import Path
import sys
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from aiohttp import web


TEST_ENV = {
    "BOT_TOKEN": "123456:TEST_TOKEN_FOR_BOOTSTRAP_ONLY",
    "DATABASE_URL": "postgresql://user:pass@localhost/db",
    "GROUP_ID": "-100123",
    "ADMIN_IDS": "1,2",
    "STRIPE_API_KEY": "sk_test_dummy",
    "STRIPE_WEBHOOK_SECRET": "whsec" + "_bootstrap_secret",
    "WEBHOOK_SECRET": "telegram_secret",
    "YOUR_DOMAIN": "https://club.example",
    "PRICE_TRIAL": "price_trial",
    "PRICE_1M": "price_1m",
    "PRICE_6M": "price_6m",
    "PRICE_12M": "price_12m",
}


class FakeWebhookInfo:
    url = "https://club.example/webhook/telegram_secret"
    pending_update_count = 0
    last_error_message = None


class FakeScheduler:
    def __init__(self):
        self.jobs = []
        self.running = False
        self.shutdown_calls = 0
        self.start_calls = 0

    def add_job(self, *args, **kwargs):
        self.jobs.append((args, kwargs))

    def start(self):
        self.running = True
        self.start_calls += 1

    def shutdown(self, wait=True):
        self.running = False
        self.shutdown_calls += 1


def signed_header(payload, secret, timestamp=None):
    if timestamp is None:
        timestamp = int(time.time())
    signed_payload = f"{timestamp}.".encode("utf-8") + payload
    signature = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={signature}"


def import_main():
    os.environ.update(TEST_ENV)
    if "main" in sys.modules:
        return sys.modules["main"]
    return importlib.import_module("main")


class FakeTelegramRequest:
    headers = {}

    def __init__(self, payload):
        self.payload = payload

    async def json(self, loads=json.loads):
        return self.payload


class FakeStripeRequest:
    path = "/stripe-payment"
    host = "club.example"
    content_type = "application/json"

    def __init__(self, payload, headers):
        self.payload = payload
        self.headers = headers

    async def read(self):
        return self.payload


class FakeMessage:
    def __init__(self):
        self.answers = []

    async def answer(self, text, **kwargs):
        self.answers.append((text, kwargs))


class FakeCallback:
    def __init__(self, user_id=123):
        self.message = FakeMessage()
        self.from_user = SimpleNamespace(id=user_id)
        self.answers = []

    async def answer(self, text=None, **kwargs):
        self.answers.append((text, kwargs))


class FakeState:
    def __init__(self):
        self.clear_calls = 0
        self.states = []

    async def clear(self):
        self.clear_calls += 1

    async def set_state(self, state):
        self.states.append(state)


class FakeCursor:
    def __init__(self, fetches=None):
        self.queries = []
        self.fetches = list(fetches or [])

    def execute(self, query, params=None):
        self.queries.append((query, params))

    def fetchone(self):
        return self.fetches.pop(0) if self.fetches else None

    def fetchall(self):
        return self.fetches.pop(0) if self.fetches else []

    def close(self):
        pass


class FakeConnection:
    def __init__(self, fetches=None):
        self.cursor_obj = FakeCursor(fetches=fetches)
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class FakeIncomingMessage:
    def __init__(self, user_id=123):
        self.from_user = SimpleNamespace(id=user_id)
        self.chat = SimpleNamespace(type="private")
        self.answers = []
        self.replies = []

    async def answer(self, text, **kwargs):
        self.answers.append((text, kwargs))

    async def reply(self, text, **kwargs):
        self.replies.append((text, kwargs))


class Aiogram3BootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.main = import_main()

    async def asyncTearDown(self):
        await self.main.bot.session.close()

    def route_count(self, app, method, path):
        return sum(
            1
            for route in app.router.routes()
            if route.method == method and getattr(route.resource, "canonical", None) == path
        )

    def route_handler(self, app, method, path):
        for route in app.router.routes():
            if route.method == method and getattr(route.resource, "canonical", None) == path:
                return route.handler
        self.fail(f"route not found: {method} {path}")

    def invoice_payment_event(self, event_id, *, period_end=None, paid_out_of_band=False):
        period_end = period_end or int((datetime.utcnow() + timedelta(days=30)).timestamp())
        period_start = int((datetime.utcnow() - timedelta(days=1)).timestamp())
        payment = SimpleNamespace(
            status="paid",
            amount_paid=1000,
            payment=SimpleNamespace(type="payment_intent", payment_intent="pi_" + event_id[-6:]),
        )
        invoice = SimpleNamespace(
            id="in_" + event_id[-8:],
            subscription="sub_rejoin",
            customer="cus_rejoin",
            customer_email="paid@example.com",
            amount_paid=1000,
            amount_due=1000,
            currency="rub",
            billing_reason="subscription_cycle",
            status="paid",
            paid_out_of_band=paid_out_of_band,
            metadata={},
            payments=SimpleNamespace(data=[payment]),
            lines=SimpleNamespace(data=[
                SimpleNamespace(
                    subscription="sub_rejoin",
                    period=SimpleNamespace(start=period_start, end=period_end),
                    price=SimpleNamespace(id="price_1m"),
                )
            ]),
        )
        event = SimpleNamespace(
            id=event_id,
            type="invoice.payment_succeeded",
            created=1720000000,
            data=SimpleNamespace(object=invoice),
        )
        subscription = SimpleNamespace(
            id="sub_rejoin",
            customer="cus_rejoin",
            status="active",
            trial_end=None,
            current_period_end=period_end,
            metadata={},
        )
        payload = json.dumps(
            {
                "id": event_id,
                "object": "event",
                "type": "invoice.payment_succeeded",
                "created": 1720000000,
                "data": {"object": {"id": invoice.id, "object": "invoice"}},
            },
            separators=(",", ":"),
        ).encode("utf-8")
        return payload, event, subscription

    async def run_invoice_webhook_with_future_expiry(self, event_id, *, paid_out_of_band=False):
        payload, event, subscription = self.invoice_payment_event(event_id, paid_out_of_band=paid_out_of_band)
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[
            (123, datetime.utcnow() + timedelta(days=14), False),
            ("stripe:%s:rejoin_invite" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value="claimed")), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        self.assertTrue(conn.closed)
        enqueue_params = next(
            params for query, params in conn.cursor_obj.queries
            if "INSERT INTO message_delivery_events" in query
        )
        self.assertEqual(enqueue_params[0], "stripe:%s:rejoin_invite" % event_id)
        self.assertEqual(enqueue_params[2], "stripe_rejoin_check")
        return enqueue_params

    async def test_bot_dispatcher_storage_and_app_are_created(self):
        from aiogram import Bot, Dispatcher
        from postgres_fsm_storage import PostgresFSMStorage

        app = self.main.create_app()

        self.assertIsInstance(self.main.bot, Bot)
        self.assertIsInstance(self.main.dp, Dispatcher)
        self.assertIsInstance(self.main.storage, PostgresFSMStorage)
        self.assertIsInstance(app, web.Application)

    async def test_postgres_fsm_storage_module_imports_independently(self):
        import postgres_fsm_storage

        source = Path(self.main.__file__).read_text()
        self.assertNotIn("class PostgresFSMStorage", source)
        self.assertIs(self.main.PostgresFSMStorage, postgres_fsm_storage.PostgresFSMStorage)

    async def test_postgres_fsm_storage_round_trip_and_clear(self):
        from aiogram.fsm.storage.base import StorageKey
        from postgres_fsm_storage import PostgresFSMStorage

        records = {}

        class FsmCursor:
            def __init__(self):
                self.row = None

            def execute(self, query, params=None):
                params = tuple(params or ())
                normalized = " ".join(query.split())
                key = params[:6]
                if normalized.startswith("INSERT INTO aiogram_fsm_states") and "EXCLUDED.state" in normalized:
                    records.setdefault(key, {"state": None, "data_json": "{}"})["state"] = params[6]
                    return
                if normalized.startswith("INSERT INTO aiogram_fsm_states") and "EXCLUDED.data_json" in normalized:
                    records.setdefault(key, {"state": None, "data_json": "{}"})["data_json"] = params[6]
                    return
                if normalized.startswith("DELETE FROM aiogram_fsm_states"):
                    record = records.get(key)
                    if record and record["state"] is None and record["data_json"] == "{}":
                        records.pop(key)
                    return
                if normalized.startswith("SELECT state"):
                    record = records.get(key)
                    self.row = (record["state"],) if record else None
                    return
                if normalized.startswith("SELECT data_json"):
                    record = records.get(key)
                    self.row = (record["data_json"],) if record else None

            def fetchone(self):
                return self.row

            def close(self):
                pass

        class FsmConnection:
            def __init__(self):
                self.cursor_obj = FsmCursor()
                self.closed = False
                self.commits = 0
                self.rollbacks = 0

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                self.commits += 1

            def rollback(self):
                self.rollbacks += 1

            def close(self):
                self.closed = True

        storage = PostgresFSMStorage(FsmConnection)
        key = StorageKey(bot_id=10, chat_id=20, user_id=30)

        await storage.set_state(key, self.main.ContactState.waiting_for_message)
        await storage.set_data(key, {"reply_to_user": 42, "text": "hello"})

        self.assertEqual(await storage.get_state(key), self.main.ContactState.waiting_for_message.state)
        self.assertEqual(await storage.get_data(key), {"reply_to_user": 42, "text": "hello"})

        await storage.set_state(key, None)
        self.assertEqual(await storage.get_data(key), {"reply_to_user": 42, "text": "hello"})
        await storage.set_data(key, {})

        self.assertIsNone(await storage.get_state(key))
        self.assertEqual(await storage.get_data(key), {})
        self.assertEqual(records, {})

    async def test_postgres_fsm_storage_concurrent_update_data_keeps_all_fields(self):
        from aiogram.fsm.storage.base import StorageKey
        from postgres_fsm_storage import PostgresFSMStorage

        records = {}
        record_lock = threading.Lock()
        connections = []

        class FsmCursor:
            def __init__(self):
                self.row = None
                self.lock_held = False

            def execute(self, query, params=None):
                params = tuple(params or ())
                normalized = " ".join(query.split())
                if normalized.startswith("INSERT INTO aiogram_fsm_states"):
                    key = params[:6]
                    with record_lock:
                        records.setdefault(key, {"state": None, "data_json": "{}"})
                    return
                if normalized.startswith("SELECT data_json") and "FOR UPDATE" in normalized:
                    key = params[:6]
                    record_lock.acquire()
                    self.lock_held = True
                    record = records.get(key, {"data_json": "{}"})
                    self.row = (record["data_json"],)
                    return
                if normalized.startswith("SELECT data_json"):
                    key = params[:6]
                    record = records.get(key, {"data_json": "{}"})
                    self.row = (record["data_json"],)
                    return
                if normalized.startswith("UPDATE aiogram_fsm_states"):
                    data_json = params[0]
                    key = params[1:7]
                    records.setdefault(key, {"state": None, "data_json": "{}"})["data_json"] = data_json
                    return
                if normalized.startswith("DELETE FROM aiogram_fsm_states"):
                    key = params[:6]
                    record = records.get(key)
                    if record and record["state"] is None and record["data_json"] == "{}":
                        records.pop(key)

            def fetchone(self):
                return self.row

            def close(self):
                if self.lock_held:
                    self.lock_held = False
                    record_lock.release()

        class FsmConnection:
            def __init__(self):
                self.cursor_obj = FsmCursor()
                self.closed = False
                connections.append(self)

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                pass

            def rollback(self):
                pass

            def close(self):
                self.closed = True

        key = StorageKey(bot_id=10, chat_id=20, user_id=30)
        first = PostgresFSMStorage(FsmConnection)
        second = PostgresFSMStorage(FsmConnection)

        await asyncio.gather(
            first.update_data(key, {"first": 1}),
            second.update_data(key, {"second": 2}),
        )

        self.assertEqual(await first.get_data(key), {"first": 1, "second": 2})
        self.assertTrue(all(conn.closed for conn in connections))

    async def test_first_purchase_recovery_delivery_skips_when_no_longer_due(self):
        delivery = (
            "first_purchase_recovery:123:20260731T100000",
            123,
            "first_purchase_recovery_reminder",
            json.dumps({"text": "retry", "keyboard_kind": "retry_payment"}),
            1,
            None,
        )
        claim_conn = FakeConnection(fetches=[[delivery]])
        check_conn = FakeConnection(fetches=[(False, True, False)])
        conns = iter([claim_conn, check_conn])

        with patch.object(self.main, "get_db_conn", side_effect=lambda: next(conns)), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["retryable_failed"], 0)
        send_message.assert_not_awaited()
        self.assertTrue(claim_conn.closed)
        self.assertTrue(check_conn.closed)
        sql = "\n".join(query for query, _ in check_conn.cursor_obj.queries)
        self.assertIn("SELECT payment_failed, first_payment_done, blocked_bot", sql)
        self.assertIn("UPDATE message_delivery_events", sql)

    async def test_handlers_are_registered_on_native_aiogram3_router(self):
        self.assertEqual(len(self.main.router.message.handlers), 52)
        self.assertEqual(len(self.main.router.callback_query.handlers), 19)

    async def test_ast_handler_inventory_matches_expected_commands_and_callbacks(self):
        source = Path(self.main.__file__).read_text()
        tree = ast.parse(source)
        message_handlers = []
        callback_handlers = []
        commands = []
        callback_filters = []
        catch_all_messages = []

        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                text = ast.unparse(decorator)
                if text.startswith("router.message"):
                    message_handlers.append(node.name)
                    if "CommandStart()" in text:
                        commands.append("start")
                    elif "Command(" in text and isinstance(decorator, ast.Call):
                        command_call = decorator.args[0]
                        for arg in getattr(command_call, "args", []):
                            if isinstance(arg, ast.Constant):
                                commands.append(arg.value)
                    if "Command(" not in text and "CommandStart()" not in text and "F." not in text and "StateFilter(" not in text:
                        catch_all_messages.append((node.name, text))
                elif text.startswith("router.callback_query"):
                    callback_handlers.append(node.name)
                    callback_filters.append(text)

        self.assertEqual(len(message_handlers), 52)
        self.assertEqual(len(callback_handlers), 19)
        self.assertEqual(
            commands,
            [
                "promo_trial", "cancel", "menu", "ask", "start", "profile",
                "send_user", "broadcast", "give_access", "set_expiry", "sync_stripe_user",
                "expired_users", "user", "access_history", "recent_access_events",
                "outbox_status", "retry_delivery", "find_by_stripe", "bot_health", "admin", "admin_help", "expiring_users",
                "test_followup", "help", "stats", "weekly_report", "weekly_report_current",
                "weekly_report_send", "test_expiry", "test_grace", "test_auto_lesson",
                "test_backup", "unblock_user", "send_invite_link", "unlinked_stripe",
                "stripe_links", "duplicate_subscriptions", "revoke_invite_links",
                "resolve_checkout", "link_stripe_user", "unban_user",
            ],
        )
        self.assertEqual(len(commands), len(set(commands)))
        self.assertEqual(len(callback_handlers), len(set(callback_handlers)))
        self.assertEqual(catch_all_messages, [])
        self.assertTrue(any("F.data.startswith('sub_')" in item for item in callback_filters))
        self.assertTrue(any("F.data == 'retry_payment'" in item for item in callback_filters))

    async def test_keyboard_callback_data_are_routable(self):
        source = Path(self.main.__file__).read_text()
        tree = ast.parse(source)
        callback_values = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.keyword) and node.arg == "callback_data":
                if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                    callback_values.add(node.value.value)

        exact = {
            "confirm_promo", "cancel_promo", "feedback_join", "feedback_question",
            "feedback_think", "to_desc", "to_rules", "to_choice", "retry_payment",
            "back_to_tariffs", "cancel_subscription", "show_renew_options",
        }
        prefix_covered = {"sub_trial", "sub_1", "sub_6", "sub_12", "admin_menu:back"}
        self.assertTrue(exact.issubset(callback_values))
        self.assertTrue(prefix_covered.issubset(callback_values))
        for value in exact:
            self.assertIn(f'F.data == "{value}"', source)
        self.assertIn("F.data.startswith('sub_')", source)
        self.assertIn('F.data.startswith("admin_menu:")', source)

    async def test_production_code_no_longer_uses_aiogram2_handler_or_fsm_api(self):
        source = Path(self.main.__file__).read_text()
        self.assertNotIn("@dp.message_handler", source)
        self.assertNotIn("@dp.callback_query_handler", source)
        self.assertNotIn("_legacy_message_handler", source)
        self.assertNotIn("_legacy_callback_query_handler", source)
        self.assertNotIn("dp.message_handler =", source)
        self.assertNotIn("dp.callback_query_handler =", source)
        self.assertNotIn("get_new_configured_app", source)
        self.assertNotIn("Dispatcher(bot", source)
        self.assertNotIn("MemoryStorage", source)
        self.assertNotIn("BotBlocked", source)
        self.assertNotIn("aiogram.utils.exceptions", source)
        self.assertNotIn("await state.finish()", source)
        self.assertNotIn("InlineKeyboardMarkup(row_width", source)
        self.assertNotIn("ReplyKeyboardMarkup(row_width", source)
        self.assertNotIn("InlineKeyboardMarkup().add", source)

    async def test_telegram_exception_helpers_prioritize_aiogram3_classes(self):
        from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNetworkError, TelegramRetryAfter

        forbidden = TelegramForbiddenError(method=None, message="bot was blocked by the user")
        bad_request = TelegramBadRequest(method=None, message="chat not found")
        network = TelegramNetworkError(method=None, message="temporary network error")
        retry_after = TelegramRetryAfter(method=None, message="Too Many Requests", retry_after=125)

        self.assertTrue(self.main.is_undeliverable_user_error(forbidden))
        self.assertTrue(self.main.is_undeliverable_user_error(bad_request))
        self.assertFalse(self.main.is_undeliverable_user_error(network))
        self.assertFalse(self.main.is_undeliverable_user_error(retry_after))
        self.assertEqual(self.main.telegram_retry_delay_minutes(retry_after, attempt_count=1), 3)

    async def test_message_delivery_forbidden_marks_blocked_permanent(self):
        from aiogram.exceptions import TelegramForbiddenError

        connections = []
        failed_calls = []

        def fake_conn():
            conn = FakeConnection()
            connections.append(conn)
            return conn

        def fake_mark_failed(cur, delivery_key, error, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(error).__name__, retry_delay_minutes, permanently_failed))

        with patch.object(self.main, "get_db_conn", side_effect=fake_conn), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[("key1", 123, "notice", '{"text":"hi"}', 1, None)]), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="blocked"))), \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 1})
        self.assertEqual(failed_calls, [("key1", "TelegramForbiddenError", None, True)])
        self.assertTrue(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        mark_sent.assert_not_called()

    async def test_message_delivery_bad_request_and_network_errors_remain_retryable(self):
        from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter

        for error, expected_delay in (
            (TelegramBadRequest(method=None, message="message is not modified"), 5),
            (TelegramNetworkError(method=None, message="network down"), 5),
            (TelegramRetryAfter(method=None, message="retry later", retry_after=125), 3),
        ):
            with self.subTest(error=type(error).__name__):
                connections = []
                failed_calls = []

                def fake_conn():
                    conn = FakeConnection()
                    connections.append(conn)
                    return conn

                def fake_mark_failed(cur, delivery_key, exc, retry_delay_minutes=None, permanently_failed=False):
                    failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))

                with patch.object(self.main, "get_db_conn", side_effect=fake_conn), \
                     patch.object(self.main, "claim_pending_message_deliveries", return_value=[("key2", 123, "notice", '{"text":"hi"}', 1, None)]), \
                     patch.object(self.main.bot, "send_message", AsyncMock(side_effect=error)), \
                     patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
                     patch.object(self.main, "mark_delivery_sent") as mark_sent, \
                     patch.object(self.main, "notify_admins", AsyncMock()):
                    result = await self.main.process_pending_message_deliveries()

                self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
                self.assertEqual(failed_calls, [("key2", type(error).__name__, expected_delay, False)])
                self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
                mark_sent.assert_not_called()

    async def test_message_delivery_retry_keyboard_payload_renders_callback_data(self):
        send_message = AsyncMock()

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("key_retry", 123, "stripe_user_message", '{"text":"retry","keyboard_kind":"retry_payment"}', 1, None)
             ]), \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main, "mark_delivery_sent"), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        markup = send_message.await_args.kwargs["reply_markup"]
        self.assertEqual(markup.inline_keyboard[0][0].callback_data, "retry_payment")

    async def test_legacy_free_lesson_null_payload_sends_video_with_trial_button(self):
        send_video = AsyncMock()

        with patch.dict(os.environ, {"FREE_LESSON_VIDEO_ID": "video_free_1"}), \
             patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson:123", 123, "free_lesson", None, 1, None)
             ]), \
             patch.object(self.main.bot, "send_video", send_video), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        send_video.assert_awaited_once()
        self.assertEqual(send_video.await_args.kwargs["video"], "video_free_1")
        markup = send_video.await_args.kwargs["reply_markup"]
        self.assertEqual(markup.inline_keyboard[0][0].callback_data, "sub_trial")

    async def test_legacy_free_lesson_followup_null_payload_sends_feedback_keyboard(self):
        send_message = AsyncMock()

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson_followup:123", 123, "free_lesson_followup", None, 1, None)
             ]), \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        send_message.assert_awaited_once()
        self.assertEqual(send_message.await_args.args[1], self.main.get_free_lesson_followup_text())
        self.assertIs(send_message.await_args.kwargs["reply_markup"].__class__, self.main.get_free_lesson_feedback_keyboard().__class__)
        self.assertEqual(send_message.await_args.kwargs["reply_markup"].inline_keyboard[0][0].callback_data, "feedback_join")

    async def test_success_free_lesson_updates_user_and_delivery_in_one_final_transaction(self):
        connections = []

        def fake_conn():
            conn = FakeConnection()
            connections.append(conn)
            return conn

        async def send_video_side_effect(**kwargs):
            self.assertTrue(all(conn.closed for conn in connections))

        with patch.dict(os.environ, {"FREE_LESSON_VIDEO_ID": "video_free_1"}), \
             patch.object(self.main, "get_db_conn", side_effect=fake_conn), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson:123", 123, "free_lesson", "{}", 1, None)
             ]), \
             patch.object(self.main.bot, "send_video", AsyncMock(side_effect=send_video_side_effect)), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        final_queries = "\n".join(query for query, _ in connections[-1].cursor_obj.queries)
        self.assertIn("video_sent = TRUE", final_queries)
        self.assertIn("video_sent_at = NOW()", final_queries)
        self.assertIn("status = 'sent'", final_queries)
        self.assertEqual(connections[-1].commits, 1)

    async def test_success_followup_updates_user_and_delivery_in_one_final_transaction(self):
        connections = []

        def fake_conn():
            conn = FakeConnection()
            connections.append(conn)
            return conn

        async def send_message_side_effect(*args, **kwargs):
            self.assertTrue(all(conn.closed for conn in connections))

        with patch.object(self.main, "get_db_conn", side_effect=fake_conn), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson_followup:123", 123, "free_lesson_followup", "{}", 1, None)
             ]), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=send_message_side_effect)), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        final_queries = "\n".join(query for query, _ in connections[-1].cursor_obj.queries)
        self.assertIn("feedback_sent = TRUE", final_queries)
        self.assertIn("feedback_sent_at = NOW()", final_queries)
        self.assertIn("status = 'sent'", final_queries)
        self.assertEqual(connections[-1].commits, 1)

    async def test_manual_free_lesson_forbidden_uses_shared_classification(self):
        from aiogram.exceptions import TelegramForbiddenError

        connections = [
            FakeConnection(fetches=[(False, False, False)]),
            FakeConnection(fetches=[("free_lesson:123",), (1,)]),
            FakeConnection(),
        ]
        message = FakeIncomingMessage(123)

        with patch.dict(os.environ, {"FREE_LESSON_VIDEO_ID": "video_free_1"}), \
             patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main.bot, "send_video", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="blocked"))), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            await self.main.free_lesson_button(message, FakeState())

        fail_queries = "\n".join(query for query, _ in connections[-1].cursor_obj.queries)
        self.assertIn("blocked_bot = TRUE", fail_queries)
        self.assertIn("status = %s", fail_queries)
        self.assertIn("next_attempt_at = NULL", fail_queries)
        self.assertEqual(connections[-1].cursor_obj.queries[-1][1][0], "permanently_failed")

    async def test_automatic_free_lesson_forbidden_uses_shared_classification(self):
        from aiogram.exceptions import TelegramForbiddenError

        connections = [
            FakeConnection(fetches=[("free_lesson:123",), (1,)]),
            FakeConnection(),
        ]

        with patch.dict(os.environ, {"FREE_LESSON_VIDEO_ID": "video_free_1"}), \
             patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main.bot, "send_video", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="blocked"))), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            sent = await self.main.send_auto_free_lesson(123)

        self.assertFalse(sent)
        self.assertIn("next_attempt_at = NULL", "\n".join(query for query, _ in connections[-1].cursor_obj.queries))
        self.assertEqual(connections[-1].cursor_obj.queries[-1][1][0], "permanently_failed")

    async def test_automatic_followup_forbidden_uses_shared_classification(self):
        from aiogram.exceptions import TelegramForbiddenError

        connections = [
            FakeConnection(fetches=[("free_lesson_followup:123",), (1,)]),
            FakeConnection(),
        ]

        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="blocked"))), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            sent = await self.main.send_free_lesson_followup(123)

        self.assertFalse(sent)
        self.assertIn("next_attempt_at = NULL", "\n".join(query for query, _ in connections[-1].cursor_obj.queries))
        self.assertEqual(connections[-1].cursor_obj.queries[-1][1][0], "permanently_failed")

    async def test_message_delivery_retry_after_network_bad_request_and_unknown_classification(self):
        from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter

        cases = [
            (TelegramRetryAfter(method=None, message="retry later", retry_after=125), 1, "failed", 3, "retryable_failed"),
            (TelegramNetworkError(method=None, message="network down"), 1, "failed", 5, "retryable_failed"),
            (TelegramBadRequest(method=None, message="chat not found"), 1, "permanently_failed", None, "permanently_failed"),
            (TelegramBadRequest(method=None, message="message is not modified"), 1, "failed", 5, "retryable_failed"),
            (RuntimeError("boom"), self.main.OUTBOX_UNKNOWN_FAILURE_LIMIT, "permanently_failed", None, "permanently_failed"),
        ]
        for error, attempt_count, status, delay, counter in cases:
            with self.subTest(error=type(error).__name__, message=str(error)):
                connections = [FakeConnection(), FakeConnection()]
                with patch.object(self.main, "get_db_conn", side_effect=connections), \
                     patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                         ("free_lesson_followup:123", 123, "free_lesson_followup", "{}", attempt_count, None)
                     ]), \
                     patch.object(self.main.bot, "send_message", AsyncMock(side_effect=error)), \
                     patch.object(self.main, "notify_admins", AsyncMock()):
                    result = await self.main.process_pending_message_deliveries()

                expected = {"sent": 0, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0}
                expected[counter] += 1
                if status == "permanently_failed" and self.main.is_undeliverable_user_error(error):
                    expected["blocked"] += 1
                self.assertEqual(result, expected)
                params = connections[-1].cursor_obj.queries[-1][1]
                self.assertEqual(params[0], status)
                if status == "failed":
                    self.assertEqual(params[2], delay)
                else:
                    self.assertIsNone(delay)

    async def test_missing_free_lesson_video_retries_until_limit_then_alerts_once(self):
        notify_admins = AsyncMock()

        with patch.dict(os.environ, {}, clear=False), \
             patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson:123", 123, "free_lesson", "{}", 1, None)
             ]), \
             patch.object(self.main, "notify_admins", notify_admins):
            os.environ.pop("FREE_LESSON_VIDEO_ID", None)
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        notify_admins.assert_awaited_once()

        notify_admins.reset_mock()
        with patch.dict(os.environ, {}, clear=False), \
             patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson:123", 123, "free_lesson", "{}", self.main.OUTBOX_MISSING_FREE_LESSON_VIDEO_LIMIT, None)
             ]), \
             patch.object(self.main, "notify_admins", notify_admins):
            os.environ.pop("FREE_LESSON_VIDEO_ID", None)
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 0})
        self.assertGreaterEqual(notify_admins.await_count, 1)
        keys = [call.kwargs.get("alert_key") for call in notify_admins.await_args_list]
        self.assertIn(f"free_lesson_video_missing:{self.main.safe_delivery_hash('free_lesson:123')}", keys)

    async def test_two_replicas_do_not_send_same_claimed_free_lesson_twice(self):
        send_func = AsyncMock()
        connections = [
            FakeConnection(fetches=[("free_lesson:123",), (1,)]),
            FakeConnection(),
            FakeConnection(fetches=[None, ("processing",)]),
        ]

        first = await self.main.process_claimed_delivery(
            lambda: connections.pop(0),
            "free_lesson:123",
            123,
            "free_lesson",
            send_func,
            classify_error_func=self.main.classify_delivery_error,
        )
        second = await self.main.process_claimed_delivery(
            lambda: connections.pop(0),
            "free_lesson:123",
            123,
            "free_lesson",
            send_func,
            classify_error_func=self.main.classify_delivery_error,
        )

        self.assertEqual(first, "sent")
        self.assertEqual(second, "already_processing")
        send_func.assert_awaited_once()

    async def test_sent_delivery_is_not_sent_again(self):
        send_func = AsyncMock()
        connections = [FakeConnection(fetches=[None, ("sent",)])]

        result = await self.main.process_claimed_delivery(
            lambda: connections.pop(0),
            "free_lesson:123",
            123,
            "free_lesson",
            send_func,
            classify_error_func=self.main.classify_delivery_error,
        )

        self.assertEqual(result, "already_sent")
        send_func.assert_not_awaited()

    async def test_critical_alert_fingerprint_is_stable_and_full_text_sensitive(self):
        text = "CRITICAL: payment check failed abc123"
        self.assertEqual(
            self.main.critical_alert_fingerprint(text),
            self.main.critical_alert_fingerprint(text),
        )
        self.assertNotEqual(
            self.main.critical_alert_fingerprint("CRITICAL: first prefix abc123"),
            self.main.critical_alert_fingerprint("CRITICAL: second prefix abc123"),
        )

    async def test_critical_alert_race_two_replicas_sends_one_admin_alert(self):
        send_message = AsyncMock()
        connections = [
            FakeConnection(fetches=[(10,)]),
            FakeConnection(),
            FakeConnection(fetches=[None]),
        ]

        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main.asyncio, "sleep", AsyncMock()):
            first = await self.main.notify_admins("CRITICAL same alert abc123", severity="CRITICAL")
            second = await self.main.notify_admins("CRITICAL same alert abc123", severity="CRITICAL")

        self.assertEqual(first["delivered"], self.main.ADMIN_IDS)
        self.assertTrue(second["deduped"])
        self.assertEqual(send_message.await_count, len(self.main.ADMIN_IDS))
        first_dedupe_key = connections[0].cursor_obj.queries[0][1][0]
        self.assertTrue(first_dedupe_key.startswith("critical:"))
        self.assertNotIn("abc123", first_dedupe_key)

    async def test_critical_alert_cooldown_repeat_is_not_sent(self):
        send_message = AsyncMock()
        connections = [
            FakeConnection(fetches=[None]),
        ]

        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main.bot, "send_message", send_message):
            result = await self.main.notify_admins("CRITICAL repeat abc123", severity="CRITICAL")

        self.assertTrue(result["deduped"])
        send_message.assert_not_awaited()

    async def test_retry_delivery_closes_db_before_replies_and_confirmation(self):
        no_match_conn = FakeConnection(fetches=[[]])
        no_match_message = FakeIncomingMessage(user_id=1)

        async def reply_after_close(text, **kwargs):
            self.assertTrue(no_match_conn.closed)
            no_match_message.replies.append((text, kwargs))

        no_match_message.reply = reply_after_close
        with patch.object(self.main, "get_db_conn", return_value=no_match_conn):
            await self.main.retry_delivery_command(no_match_message, SimpleNamespace(args="missinghash"))

        self.assertEqual(no_match_message.replies[0][0], "❌ Delivery не найден или не подходит для retry.")

        delivery_key = "free_lesson:123"
        requested_hash = self.main.safe_delivery_hash(delivery_key)
        match_conn = FakeConnection(fetches=[[
            (delivery_key, 123, "free_lesson", "failed", 2, "network", None)
        ]])
        match_message = FakeIncomingMessage(user_id=1)
        confirmations = []

        async def confirmation_after_close(message, action_id, text):
            self.assertTrue(match_conn.closed)
            confirmations.append((action_id, text))

        with patch.object(self.main, "get_db_conn", return_value=match_conn), \
             patch.object(self.main, "send_admin_action_confirmation", AsyncMock(side_effect=confirmation_after_close)):
            await self.main.retry_delivery_command(match_message, SimpleNamespace(args=requested_hash))

        self.assertEqual(len(confirmations), 1)
        self.assertIn(f"delivery_hash: {requested_hash}", confirmations[0][1])

    async def run_checkout_days_webhook(self, days_marker, notify_side_effect=None):
        session_payload = {
            "id": f"cs_days_{days_marker if days_marker is not None else 'missing'}",
            "client_reference_id": "123",
            "mode": "payment",
            "payment_status": "paid",
            "customer": f"cus_days_{days_marker if days_marker is not None else 'missing'}",
            "subscription": None,
            "amount_total": 1000,
            "currency": "usd",
        }
        if days_marker != "missing":
            session_payload["metadata"] = {"days": days_marker}
        event_id = f"evt_days_{days_marker if days_marker is not None else 'none'}"
        event = SimpleNamespace(
            id=event_id,
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(**session_payload)),
        )
        payload = json.dumps({
            "id": event_id,
            "object": "event",
            "type": "checkout.session.completed",
            "data": {"object": session_payload},
        }).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        notify = AsyncMock(side_effect=notify_side_effect)
        release = AsyncMock()
        mark_processed = AsyncMock()
        get_db_conn = Mock(side_effect=AssertionError("invalid checkout days must not open DB"))

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value="claimed")), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "notify_admins", notify), \
             patch.object(self.main, "get_db_conn", get_db_conn):
            response = await self.main.stripe_webhook(request)

        return SimpleNamespace(
            response=response,
            notify=notify,
            release=release,
            mark_processed=mark_processed,
            get_db_conn=get_db_conn,
            event_id=event_id,
        )

    async def run_checkout_identity_webhook(self, client_reference_id="123", metadata_telegram_id="123", notify_side_effect=None):
        session_payload = {
            "id": f"cs_identity_{client_reference_id if client_reference_id not in (None, '') else 'empty'}_{metadata_telegram_id if metadata_telegram_id not in (None, '') else 'empty'}",
            "mode": "payment",
            "payment_status": "paid",
            "customer": "cus_identity",
            "subscription": None,
            "amount_total": 1000,
            "currency": "usd",
            "metadata": {"days": "7"},
        }
        if client_reference_id is not None:
            session_payload["client_reference_id"] = client_reference_id
        if metadata_telegram_id is not None:
            session_payload["metadata"]["telegram_id"] = metadata_telegram_id
        event_id = f"evt_identity_{client_reference_id if client_reference_id not in (None, '') else 'empty'}_{metadata_telegram_id if metadata_telegram_id not in (None, '') else 'empty'}"
        event = SimpleNamespace(
            id=event_id,
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(**session_payload)),
        )
        payload = json.dumps({
            "id": event_id,
            "object": "event",
            "type": "checkout.session.completed",
            "data": {"object": session_payload},
        }).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        notify = AsyncMock(side_effect=notify_side_effect)
        release = AsyncMock()
        mark_processed = AsyncMock()
        conn = FakeConnection(fetches=[(False, None, False)])
        get_db_conn = Mock(return_value=conn)

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value="claimed")), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "notify_admins", notify), \
             patch.object(self.main, "claim_trial_redemption", return_value=True), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main, "get_db_conn", get_db_conn):
            response = await self.main.stripe_webhook(request)

        return SimpleNamespace(
            response=response,
            notify=notify,
            release=release,
            mark_processed=mark_processed,
            conn=conn,
            get_db_conn=get_db_conn,
            event_id=event_id,
        )

    async def test_checkout_identity_accepts_client_reference_metadata_and_matching_sources(self):
        cases = (
            ("123", None),
            (None, "123"),
            ("123", "123"),
            ("", "123"),
        )
        for client_reference_id, metadata_telegram_id in cases:
            with self.subTest(client_reference_id=client_reference_id, metadata_telegram_id=metadata_telegram_id):
                result = await self.run_checkout_identity_webhook(client_reference_id, metadata_telegram_id)

                self.assertEqual(result.response.status, 200)
                result.mark_processed.assert_awaited_once_with(result.event_id)
                result.release.assert_not_awaited()
                result.notify.assert_not_awaited()
                sql = "\n".join(query for query, _ in result.conn.cursor_obj.queries)
                self.assertIn("INSERT INTO users", sql)
                self.assertIn("INSERT INTO payment_events", sql)
                self.assertIn("INSERT INTO access_events", sql)

    async def test_checkout_identity_missing_invalid_or_conflicting_sources_fail_closed(self):
        cases = (
            (None, None),
            ("abc", "-1"),
            ("123", "456"),
        )
        for client_reference_id, metadata_telegram_id in cases:
            with self.subTest(client_reference_id=client_reference_id, metadata_telegram_id=metadata_telegram_id):
                result = await self.run_checkout_identity_webhook(client_reference_id, metadata_telegram_id)

                self.assertEqual(result.response.status, 500)
                result.release.assert_awaited_once_with(result.event_id)
                result.mark_processed.assert_not_awaited()
                result.get_db_conn.assert_not_called()
                result.notify.assert_awaited_once()
                self.assertTrue(result.notify.await_args.kwargs["alert_key"].startswith("checkout_invalid_identity:"))
                self.assertEqual(result.notify.await_args.kwargs["severity"], "CRITICAL")
                alert_text = result.notify.await_args.args[0]
                self.assertIn("client_reference_id/metadata.telegram_id missing, invalid, or conflicting", alert_text)
                self.assertIn("access_granted: false", alert_text)
                self.assertNotIn(result.event_id, alert_text)
                self.assertNotIn("cs_identity", alert_text)

    async def test_checkout_identity_notify_failure_still_releases_claim_and_returns_500(self):
        result = await self.run_checkout_identity_webhook(None, None, notify_side_effect=RuntimeError("notify down"))

        self.assertEqual(result.response.status, 500)
        result.release.assert_awaited_once_with(result.event_id)
        result.mark_processed.assert_not_awaited()
        result.get_db_conn.assert_not_called()

    async def test_checkout_invalid_days_missing_empty_text_zero_and_negative_fail_closed(self):
        for days_marker in ("missing", "", "abc", "0", "-30"):
            with self.subTest(days_marker=days_marker):
                result = await self.run_checkout_days_webhook(days_marker)

                self.assertEqual(result.response.status, 500)
                result.release.assert_awaited_once_with(result.event_id)
                result.mark_processed.assert_not_awaited()
                result.get_db_conn.assert_not_called()
                result.notify.assert_awaited_once()
                self.assertTrue(result.notify.await_args.kwargs["alert_key"].startswith("checkout_invalid_days:"))
                self.assertEqual(result.notify.await_args.kwargs["severity"], "CRITICAL")
                alert_text = result.notify.await_args.args[0]
                self.assertIn("metadata.days missing or invalid", alert_text)
                self.assertIn("access_granted: false", alert_text)
                self.assertNotIn(result.event_id, alert_text)

    async def test_checkout_invalid_days_notify_failure_still_releases_claim_and_returns_500(self):
        result = await self.run_checkout_days_webhook("abc", notify_side_effect=RuntimeError("notify down"))

        self.assertEqual(result.response.status, 500)
        result.release.assert_awaited_once_with(result.event_id)
        result.mark_processed.assert_not_awaited()
        result.get_db_conn.assert_not_called()

    async def test_checkout_days_seven_success_path_still_processes(self):
        event = SimpleNamespace(
            id="evt_days_valid_7",
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(
                id="cs_days_valid_7",
                client_reference_id="123",
                metadata={"days": "7"},
                mode="payment",
                payment_status="paid",
                customer="cus_days_valid_7",
                subscription=None,
                amount_total=1000,
                currency="usd",
            )),
        )
        payload = json.dumps({
            "id": "evt_days_valid_7",
            "object": "event",
            "type": "checkout.session.completed",
            "data": {"object": {"id": "cs_days_valid_7", "client_reference_id": "123", "metadata": {"days": "7"}}},
        }).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[(False, None, False)])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value="claimed")), \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "claim_trial_redemption", return_value=True), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with("evt_days_valid_7")
        release.assert_not_awaited()
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("INSERT INTO users", sql)
        self.assertIn("INSERT INTO payment_events", sql)
        self.assertIn("INSERT INTO access_events", sql)

    async def test_outbox_status_duration_helpers_never_go_negative(self):
        now = datetime(2026, 7, 29, 13, 16)
        future = now + timedelta(minutes=9)
        past = now - timedelta(minutes=18)

        self.assertEqual(self.main.format_elapsed_duration(self.main.outbox_unresolved_age_seconds(now, future)), "0 мин.")
        self.assertEqual(self.main.format_elapsed_duration(self.main.outbox_unresolved_age_seconds(now, past)), "18 мин.")
        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, future)), "9 мин.")
        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, past)), "нет")

    async def test_outbox_status_duration_helpers_accept_aware_now_and_naive_timestamps(self):
        db_now = datetime(2026, 7, 29, 13, 16, tzinfo=timezone.utc)
        oldest_unresolved = datetime(2026, 7, 29, 13, 4)
        next_retry = datetime(2026, 7, 29, 13, 25)

        self.assertEqual(self.main.outbox_unresolved_age_seconds(db_now, oldest_unresolved), 12 * 60)
        self.assertEqual(self.main.outbox_next_retry_seconds(db_now, next_retry), 9 * 60)

    async def test_outbox_status_duration_helpers_accept_naive_now_and_aware_timestamps(self):
        db_now = datetime(2026, 7, 29, 13, 16)
        oldest_unresolved = datetime(2026, 7, 29, 13, 4, tzinfo=timezone.utc)
        next_retry = datetime(2026, 7, 29, 13, 25, tzinfo=timezone.utc)

        self.assertEqual(self.main.outbox_unresolved_age_seconds(db_now, oldest_unresolved), 12 * 60)
        self.assertEqual(self.main.outbox_next_retry_seconds(db_now, next_retry), 9 * 60)

    async def test_outbox_status_future_retry_rounds_up_minutes(self):
        now = datetime(2026, 7, 29, 13, 16)

        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, now + timedelta(seconds=20))), "1 мин.")
        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, now + timedelta(seconds=61))), "2 мин.")
        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, now)), "нет")
        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, now - timedelta(seconds=1))), "нет")

    async def test_outbox_status_uses_utc_naive_now_query(self):
        now = datetime(2026, 7, 29, 13, 16)
        conn = FakeConnection(fetches=[
            (now,),
            [],
            [],
            (0,),
            (None,),
            (None,),
            (None,),
            (0,),
            (0,),
            None,
            [],
        ])
        message = FakeIncomingMessage(user_id=1)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.outbox_status_command(message)

        self.assertEqual(conn.cursor_obj.queries[0][0], "SELECT NOW() AT TIME ZONE 'UTC'")

    async def test_feedback_join_closes_db_before_telegram_replies(self):
        conn = FakeConnection(fetches=[(False, False)])
        callback = FakeCallback(user_id=123)
        state = FakeState()
        answer_checks = []

        async def answer_after_close(*args, **kwargs):
            answer_checks.append(conn.closed)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            callback.message.answer = AsyncMock(side_effect=answer_after_close)
            callback.answer = AsyncMock(side_effect=lambda *args, **kwargs: answer_checks.append(conn.closed))
            await self.main.feedback_join(callback, state)

        self.assertEqual(answer_checks, [True, True])
        self.assertEqual(state.states, [self.main.RegistrationStates.choice])
        self.assertEqual(conn.commits, 1)

    async def test_feedback_think_closes_db_before_telegram_replies(self):
        conn = FakeConnection()
        callback = FakeCallback(user_id=123)
        state = FakeState()
        answer_checks = []

        async def answer_after_close(*args, **kwargs):
            answer_checks.append(conn.closed)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            callback.message.answer = AsyncMock(side_effect=answer_after_close)
            callback.answer = AsyncMock(side_effect=lambda *args, **kwargs: answer_checks.append(conn.closed))
            await self.main.feedback_think(callback, state)

        self.assertEqual(answer_checks, [True, True])
        self.assertEqual(state.clear_calls, 1)
        self.assertEqual(conn.commits, 1)

    def test_tracked_db_pool_concurrent_accounting_and_double_put_guard(self):
        class RawPool:
            def __init__(self, *args, **kwargs):
                self.lock = threading.Lock()
                self.next_id = 0
                self.puts = []

            def getconn(self):
                with self.lock:
                    self.next_id += 1
                    return SimpleNamespace(id=self.next_id)

            def putconn(self, conn, close=False):
                with self.lock:
                    self.puts.append((conn.id, close))

            def closeall(self):
                pass

        with patch.object(self.main.psycopg2_pool, "ThreadedConnectionPool", RawPool):
            pool = self.main.TrackedThreadedConnectionPool(1, 5, "postgresql://example")
            errors = []

            def worker():
                try:
                    conn = pool.getconn()
                    self.assertGreaterEqual(pool.health()["pool_used"], 1)
                    pool.putconn(conn)
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.assertEqual(errors, [])
            self.assertEqual(pool.health()["pool_used"], 0)
            self.assertEqual(pool.health()["pool_available"], 5)
            raw_conn = pool.getconn()
            pool.putconn(raw_conn)
            pool.putconn(raw_conn)
            self.assertEqual(pool.health()["pool_used"], 0)
            self.assertEqual(len(pool._pool.puts), 9)

    async def test_outbox_status_text_shows_retry_and_no_negative_age(self):
        now = datetime(2026, 7, 29, 13, 16)
        next_retry = now + timedelta(minutes=9)
        conn = FakeConnection(fetches=[
            (now,),
            [("pending", 1), ("failed", 1)],
            [("free_lesson", "failed", 1)],
            (0,),
            (next_retry,),
            (next_retry,),
            (next_retry,),
            (2,),
            (0,),
            None,
            [],
        ])
        message = FakeIncomingMessage(user_id=1)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.outbox_status_command(message)

        text = message.answers[0][0]
        self.assertIn("oldest unresolved age: 0 мин.", text)
        self.assertIn("next retry in: 9 мин.", text)
        self.assertIn("nearest next_attempt_at: 29.07.2026 13:25", text)
        self.assertNotIn("-", text)
        self.assertLess(len(text), 4096)

    async def test_outbox_status_due_or_empty_retry_text_is_none(self):
        now = datetime(2026, 7, 29, 13, 16)
        for oldest_unresolved, next_retry_at, expected_age in ((now - timedelta(minutes=3), None, "3 мин."), (None, None, "нет")):
            with self.subTest(oldest_unresolved=oldest_unresolved):
                conn = FakeConnection(fetches=[
                    (now,),
                    [],
                    [],
                    (0,),
                    (oldest_unresolved,),
                    (next_retry_at,),
                    (next_retry_at,),
                    (0,),
                    (0,),
                    None,
                    [],
                ])
                message = FakeIncomingMessage(user_id=1)

                with patch.object(self.main, "get_db_conn", return_value=conn):
                    await self.main.outbox_status_command(message)

                text = message.answers[0][0]
                self.assertIn(f"oldest unresolved age: {expected_age}", text)
                self.assertIn("next retry in: нет", text)
                self.assertIn("nearest next_attempt_at: нет", text)
                self.assertLess(len(text), 4096)

    async def test_message_delivery_invalid_payload_does_not_stop_batch(self):
        connections = [FakeConnection(), FakeConnection(), FakeConnection()]
        failed_calls = []
        send_message = AsyncMock()

        def fake_mark_failed(cur, delivery_key, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, permanently_failed))

        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("bad_payload", 123, "stripe_user_message", "{bad json", 1, None),
                 ("good_payload", 124, "stripe_user_message", '{"text":"ok"}', 1, None),
             ]), \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent"), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("bad_payload", "JSONDecodeError", False)])
        send_message.assert_awaited_once()

    async def test_rejoin_delivery_reuses_saved_invite_after_restart(self):
        send_message = AsyncMock()

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 (
                     "stripe:evt_1:rejoin_invite",
                     123,
                     "stripe_rejoin_invite",
                     '{"text":"link {invite_link}","source":"invoice.payment_succeeded","stripe_event_id":"evt_1"}',
                     1,
                     "https://t.me/+saved",
                 )
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main, "mark_delivery_sent"), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        unban.assert_awaited_once()
        create_link.assert_not_awaited()
        self.assertIn("https://t.me/+saved", send_message.await_args.args[1])

    async def test_rejoin_unban_network_failure_retries_without_send_or_sent(self):
        from aiogram.exceptions import TelegramNetworkError

        connections = [FakeConnection(), FakeConnection()]
        failed_calls = []

        def fake_mark_failed(cur, delivery_key, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))

        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_network_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramNetworkError(method=None, message="network down"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_network_secret:rejoin_invite", "TelegramNetworkError", 5, False)])
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()

    async def test_rejoin_unban_retry_after_sets_retry_delay(self):
        from aiogram.exceptions import TelegramRetryAfter

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_retry_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramRetryAfter(method=None, message="retry", retry_after=125))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_retry_secret:rejoin_invite", "TelegramRetryAfter", 3, False)])
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()

    async def test_rejoin_unban_group_permission_error_does_not_block_user(self):
        from aiogram.exceptions import TelegramForbiddenError

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, permanently_failed))

        connections = [FakeConnection(), FakeConnection()]
        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_group_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="not enough rights"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_group_secret:rejoin_invite", "TelegramForbiddenError", False)])
        self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()

    async def test_rejoin_unban_group_permission_error_at_limit_is_terminal(self):
        from aiogram.exceptions import TelegramForbiddenError

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, permanently_failed))

        connections = [FakeConnection(), FakeConnection()]
        notify_admins = AsyncMock()
        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_group_limit_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}","stripe_event_id":"evt_group_limit_secret"}', 3, None)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="not enough rights"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", notify_admins):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_group_limit_secret:rejoin_invite", "TelegramForbiddenError", True)])
        self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()
        self.assertGreaterEqual(notify_admins.await_count, 1)
        alert_text = notify_admins.await_args_list[0].args[0]
        self.assertNotIn("evt_group_limit_secret", alert_text)
        self.assertIn("delivery_hash:", alert_text)
        self.assertNotIn("evt_***secret", alert_text)

    async def test_rejoin_unban_bot_not_administrator_is_not_benign(self):
        from aiogram.exceptions import TelegramBadRequest

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, permanently_failed))

        connections = [FakeConnection(), FakeConnection()]
        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_bot_admin_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramBadRequest(method=None, message="bot is not an administrator"))), \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_bot_admin_secret:rejoin_invite", "TelegramBadRequest", False)])
        self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        create_link.assert_not_awaited()
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()

    async def test_rejoin_unban_benign_administrator_error_continues(self):
        from aiogram.exceptions import TelegramBadRequest

        invite = SimpleNamespace(invite_link="https://t.me/+new")
        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_admin_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramBadRequest(method=None, message="user is an administrator"))), \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock(return_value=invite)) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed") as mark_failed, \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        create_link.assert_awaited_once()
        send_message.assert_awaited_once()
        mark_sent.assert_called_once()
        mark_failed.assert_not_called()

    async def test_rejoin_check_membership_timeout_is_retryable(self):
        from aiogram.exceptions import TelegramNetworkError

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_member_timeout:rejoin_invite", 123, "stripe_rejoin_check", '{"text":"link {invite_link}"}', 1, None)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(side_effect=TelegramNetworkError(method=None, message="network down"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_member_timeout:rejoin_invite", "TelegramNetworkError", 5, False)])
        unban.assert_not_awaited()
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()

    async def test_rejoin_check_member_completes_without_link_or_message(self):
        member = SimpleNamespace(status="member", is_member=True)

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_member_ok:rejoin_invite", 123, "stripe_rejoin_check", '{"text":"link {invite_link}"}', 1, None)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=member)), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed") as mark_failed, \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        unban.assert_not_awaited()
        create_link.assert_not_awaited()
        send_message.assert_not_awaited()
        mark_sent.assert_called_once()
        mark_failed.assert_not_called()

    async def test_rejoin_check_kicked_unbans_and_sends_link(self):
        member = SimpleNamespace(status="kicked", is_member=False)
        invite = SimpleNamespace(invite_link="https://t.me/+new")

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_kicked:rejoin_invite", 123, "stripe_rejoin_check", '{"text":"link {invite_link}","stripe_event_id":"evt_kicked"}', 1, None)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=member)), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock(return_value=invite)) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed") as mark_failed, \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        unban.assert_awaited_once()
        create_link.assert_awaited_once()
        send_message.assert_awaited_once()
        self.assertIn("https://t.me/+new", send_message.await_args.args[1])
        mark_sent.assert_called_once()
        mark_failed.assert_not_called()

    async def test_rejoin_send_forbidden_blocks_and_permanently_fails(self):
        from aiogram.exceptions import TelegramForbiddenError

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, permanently_failed))

        connections = [FakeConnection(), FakeConnection()]
        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_send_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, "https://t.me/+saved")
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="bot was blocked"))), \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 1})
        self.assertEqual(failed_calls, [("stripe:evt_send_secret:rejoin_invite", "TelegramForbiddenError", True)])
        self.assertTrue(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        mark_sent.assert_not_called()

    async def test_rejoin_unban_logs_mask_delivery_key(self):
        from aiogram.exceptions import TelegramBadRequest

        full_key = "stripe:evt_full_secret_identifier_123456789:rejoin_invite"
        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 (full_key, 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramBadRequest(method=None, message="not enough rights"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()), \
             patch.object(self.main, "mark_delivery_failed"), \
             patch.object(self.main, "mark_delivery_sent"), \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             self.assertLogs(level="WARNING") as logs:
            await self.main.process_pending_message_deliveries()

        log_text = "\n".join(logs.output)
        self.assertNotIn(full_key, log_text)
        self.assertNotIn("evt_full_secret_identifier_123456789", log_text)
        self.assertIn("stripe:evt_***invite", log_text)

    async def test_checkout_webhook_enqueues_durable_rejoin_check_without_telegram_membership_call(self):
        payload = json.dumps(
            {
                "id": "evt_checkout_boundary",
                "object": "event",
                "type": "checkout.session.completed",
                "created": 1720000000,
                "data": {
                    "object": {
                        "id": "cs_boundary",
                        "object": "checkout.session",
                        "client_reference_id": "123",
                        "metadata": {"days": "30"},
                        "mode": "payment",
                        "payment_status": "paid",
                        "customer": "cus_boundary",
                        "subscription": None,
                        "amount_total": 1000,
                        "currency": "usd",
                    }
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[
            (True, datetime.utcnow() + timedelta(days=14), True),
            ("stripe:evt_checkout_boundary:rejoin_invite",),
        ])

        event = SimpleNamespace(
            id="evt_checkout_boundary",
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(
                id="cs_boundary",
                client_reference_id="123",
                metadata={"days": "30"},
                mode="payment",
                payment_status="paid",
                customer="cus_boundary",
                subscription=None,
                amount_total=1000,
                currency="usd",
            )),
        )

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value="claimed")), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as get_member, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        get_member.assert_not_awaited()
        self.assertTrue(conn.closed)
        queries = [query for query, _ in conn.cursor_obj.queries]
        enqueue_index = next(i for i, query in enumerate(queries) if "INSERT INTO message_delivery_events" in query)
        access_index = next(i for i, query in enumerate(queries) if "INSERT INTO users" in query)
        self.assertLess(access_index, enqueue_index)
        enqueue_params = next(params for query, params in conn.cursor_obj.queries if "INSERT INTO message_delivery_events" in query)
        self.assertEqual(enqueue_params[0], "stripe:evt_checkout_boundary:rejoin_invite")
        self.assertEqual(enqueue_params[2], "stripe_rejoin_check")
        self.assertEqual(conn.commits, 1)

    async def test_subscription_checkout_link_only_does_not_enqueue_rejoin_check(self):
        payload = json.dumps(
            {
                "id": "evt_checkout_link_only",
                "object": "event",
                "type": "checkout.session.completed",
                "created": 1720000000,
                "data": {
                    "object": {
                        "id": "cs_link_only",
                        "object": "checkout.session",
                        "client_reference_id": "123",
                        "metadata": {},
                        "mode": "subscription",
                        "payment_status": "paid",
                        "customer": "cus_link_only",
                        "subscription": "sub_link_only",
                    }
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection()
        event = SimpleNamespace(
            id="evt_checkout_link_only",
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(
                id="cs_link_only",
                client_reference_id="123",
                metadata={},
                mode="subscription",
                payment_status="paid",
                customer="cus_link_only",
                subscription="sub_link_only",
            )),
        )

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value="claimed")), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        self.assertTrue(conn.closed)
        queries = [query for query, _ in conn.cursor_obj.queries]
        self.assertFalse(any("INSERT INTO message_delivery_events" in query for query in queries))

    async def test_checkout_webhook_duplicate_does_not_create_second_rejoin_check(self):
        payload = json.dumps(
            {
                "id": "evt_checkout_duplicate",
                "object": "event",
                "type": "checkout.session.completed",
                "created": 1720000000,
                "data": {
                    "object": {
                        "id": "cs_duplicate",
                        "object": "checkout.session",
                        "client_reference_id": "123",
                        "metadata": {"days": "30"},
                        "mode": "payment",
                        "payment_status": "paid",
                        "customer": "cus_boundary",
                        "subscription": None,
                        "amount_total": 1000,
                        "currency": "usd",
                    }
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[
            (False, datetime.utcnow() - timedelta(days=1), False),
            ("stripe:evt_checkout_duplicate:rejoin_invite",),
        ])

        event = SimpleNamespace(
            id="evt_checkout_duplicate",
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(
                id="cs_duplicate",
                client_reference_id="123",
                metadata={"days": "30"},
                mode="payment",
                payment_status="paid",
                customer="cus_boundary",
                subscription=None,
                amount_total=1000,
                currency="usd",
            )),
        )

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(side_effect=["claimed", "duplicate"])), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as get_member, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response1 = await self.main.stripe_webhook(request)
            response2 = await self.main.stripe_webhook(request)

        self.assertEqual(response1.status, 200)
        self.assertEqual(response2.status, 200)
        get_member.assert_not_awaited()
        self.assertTrue(conn.closed)
        enqueue_queries = [query for query, _ in conn.cursor_obj.queries if "INSERT INTO message_delivery_events" in query]
        self.assertEqual(len(enqueue_queries), 1)
        self.assertEqual(conn.rollbacks, 0)

    async def test_invoice_future_expiry_kicked_user_gets_rejoin_task_and_link(self):
        enqueue_params = await self.run_invoice_webhook_with_future_expiry("evt_invoice_future_kicked")
        payload_json = enqueue_params[3]

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_invoice_future_kicked:rejoin_invite", 123, "stripe_rejoin_check", payload_json, 1, None)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="kicked"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock(return_value=SimpleNamespace(invite_link="https://t.me/+again"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result["sent"], 1)
        unban.assert_awaited_once()
        send_message.assert_awaited_once()
        self.assertIn("https://t.me/+again", send_message.await_args.args[1])

    async def test_invoice_future_expiry_member_gets_rejoin_task_completed_without_message(self):
        enqueue_params = await self.run_invoice_webhook_with_future_expiry("evt_invoice_future_member")
        payload_json = enqueue_params[3]

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_invoice_future_member:rejoin_invite", 123, "stripe_rejoin_check", payload_json, 1, None)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="member"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result["sent"], 1)
        unban.assert_not_awaited()
        create_link.assert_not_awaited()
        send_message.assert_not_awaited()

    async def test_out_of_band_invoice_future_expiry_kicked_user_gets_rejoin_task_and_link(self):
        enqueue_params = await self.run_invoice_webhook_with_future_expiry(
            "evt_invoice_oob_kicked",
            paid_out_of_band=True,
        )
        payload_json = enqueue_params[3]

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_invoice_oob_kicked:rejoin_invite", 123, "stripe_rejoin_check", payload_json, 1, None)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="kicked"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock(return_value=SimpleNamespace(invite_link="https://t.me/+oob"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result["sent"], 1)
        unban.assert_awaited_once()
        send_message.assert_awaited_once()
        self.assertIn("https://t.me/+oob", send_message.await_args.args[1])

    async def test_out_of_band_invoice_without_linked_user_does_not_enqueue_rejoin_task(self):
        payload, event, subscription = self.invoice_payment_event(
            "evt_invoice_oob_unlinked",
            paid_out_of_band=True,
        )
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[None, None, None, None, None, None])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value="claimed")), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        self.assertTrue(conn.closed)
        queries = [query for query, _ in conn.cursor_obj.queries]
        self.assertFalse(any("INSERT INTO message_delivery_events" in query for query in queries))
        self.assertTrue(any("INSERT INTO unlinked_stripe_events" in query for query in queries))

    async def test_webhook_and_stripe_routes_are_registered_once_and_do_not_conflict(self):
        app = self.main.create_app()
        telegram_path = self.main.get_telegram_webhook_path()

        self.assertEqual(self.route_count(app, "POST", telegram_path), 1)
        self.assertEqual(self.route_count(app, "POST", "/stripe-payment"), 1)
        self.assertNotEqual(telegram_path, "/stripe-payment")

    async def test_startup_sets_db_commands_webhook_verifies_url_and_registers_jobs_once(self):
        fake_scheduler = FakeScheduler()
        app = self.main.create_app()
        with patch.object(self.main, "scheduler", fake_scheduler), \
             patch.object(self.main, "SCHEDULER_JOBS_REGISTERED", False), \
             patch.object(self.main, "init_db") as init_db, \
             patch.object(self.main.bot, "set_my_commands", AsyncMock()) as set_commands, \
             patch.object(self.main.bot, "set_webhook", AsyncMock()) as set_webhook, \
             patch.object(self.main.bot, "get_webhook_info", AsyncMock(return_value=FakeWebhookInfo())) as get_info:
            await self.main.on_startup(app)
            await self.main.on_startup(app)

        self.assertEqual(init_db.call_count, 2)
        self.assertEqual(set_commands.await_count, 2)
        self.assertEqual(set_webhook.await_count, 2)
        self.assertEqual(get_info.await_count, 2)
        self.assertEqual(len(fake_scheduler.jobs), 7)
        self.assertEqual(fake_scheduler.start_calls, 1)

    async def test_shutdown_closes_bot_session_and_is_repeatable(self):
        fake_scheduler = FakeScheduler()
        fake_scheduler.running = True
        close_db_pool = Mock()
        close_session = AsyncMock()
        with patch.object(self.main, "scheduler", fake_scheduler), \
             patch.object(self.main.bot.session, "close", close_session), \
             patch.object(self.main, "close_db_pool", close_db_pool):
            await self.main.on_shutdown(self.main.create_app())
            await self.main.on_shutdown(self.main.create_app())

        self.assertEqual(close_session.await_count, 2)
        self.assertEqual(close_db_pool.call_count, 2)
        self.assertEqual(fake_scheduler.shutdown_calls, 1)

    async def test_main_and_question_fsm_handlers_use_aiogram3_state_api(self):
        state = FakeState()
        message = FakeMessage()

        await self.main.show_menu(message, state)
        self.assertEqual(state.clear_calls, 1)
        self.assertEqual(message.answers[-1][0], "Главное меню\n\nВыберите нужный раздел:")
        menu_keyboard = message.answers[-1][1]["reply_markup"]
        self.assertEqual(
            [[button.text for button in row] for row in menu_keyboard.keyboard],
            [
                ["🎁 Бесплатный урок"],
                ["💬 Задать вопрос", "🆘 Правила клуба"],
                ["👤 Профиль и подписка"],
            ],
        )

        await self.main.ask_question_button(message, state)
        self.assertEqual(state.clear_calls, 2)
        self.assertEqual(state.states[-1], self.main.ContactState.waiting_for_message)
        question_keyboard = message.answers[-1][1]["reply_markup"]
        self.assertEqual(question_keyboard.keyboard[0][0].text, "❌ Отмена")

    async def test_inline_keyboards_keep_expected_callback_data_and_urls(self):
        tariffs = self.main.get_tariffs_keyboard(show_trial=True)
        self.assertEqual(
            [row[0].callback_data for row in tariffs.inline_keyboard],
            ["sub_trial", "sub_1", "sub_6", "sub_12"],
        )

        no_trial = self.main.get_tariffs_keyboard(show_trial=False)
        self.assertEqual(
            [row[0].callback_data for row in no_trial.inline_keyboard],
            ["sub_1", "sub_6", "sub_12"],
        )

        feedback = self.main.get_free_lesson_feedback_keyboard()
        self.assertEqual(
            [row[0].callback_data for row in feedback.inline_keyboard],
            ["feedback_join", "feedback_question", "feedback_think"],
        )

        callback = FakeCallback()
        await self.main.send_checkout_open_instruction(
            callback,
            "https://checkout.example/session",
            42,
            "cs_test_123",
            "sub_1",
            "subscription",
        )
        payment_keyboard = callback.message.answers[-1][1]["reply_markup"]
        self.assertEqual(payment_keyboard.inline_keyboard[0][0].url, "https://checkout.example/session")
        self.assertEqual(payment_keyboard.inline_keyboard[1][0].callback_data, "back_to_tariffs")

    async def test_telegram_webhook_request_passes_through_aiogram3_handler(self):
        app = self.main.create_app()
        app.on_startup.clear()
        app.on_shutdown.clear()
        update = {
            "update_id": 1001,
            "message": {
                "message_id": 1,
                "date": int(time.time()),
                "chat": {"id": 2002, "type": "private"},
                "from": {"id": 2002, "is_bot": False, "first_name": "Test"},
                "text": "/not_registered_for_bootstrap",
            },
        }
        handler = self.route_handler(app, "POST", self.main.get_telegram_webhook_path())
        response = await handler(FakeTelegramRequest(update))

        self.assertEqual(response.status, 200)

    async def test_unknown_callback_passes_through_dispatcher_without_payment_or_admin_action(self):
        app = self.main.create_app()
        app.on_startup.clear()
        app.on_shutdown.clear()
        update = {
            "update_id": 1002,
            "callback_query": {
                "id": "unknown-callback",
                "from": {"id": 2002, "is_bot": False, "first_name": "Test"},
                "chat_instance": "ci",
                "data": "unknown_callback",
                "message": {
                    "message_id": 2,
                    "date": int(time.time()),
                    "chat": {"id": 2002, "type": "private"},
                },
            },
        }
        handler = self.route_handler(app, "POST", self.main.get_telegram_webhook_path())
        with patch.object(self.main, "process_payment", AsyncMock()) as process_payment, \
             patch.object(self.main, "admin_action_confirm_callback", AsyncMock()) as confirm_action:
            response = await handler(FakeTelegramRequest(update))

        self.assertEqual(response.status, 200)
        process_payment.assert_not_awaited()
        confirm_action.assert_not_awaited()

    async def test_signed_stripe_webhook_route_still_returns_expected_response(self):
        app = self.main.create_app()
        app.on_startup.clear()
        app.on_shutdown.clear()
        payload = json.dumps(
            {
                "id": "evt_bootstrap_duplicate",
                "object": "event",
                "type": "checkout.session.completed",
                "created": 1720000000,
                "data": {"object": {"id": "cs_bootstrap", "object": "checkout.session"}},
            },
            separators=(",", ":"),
        ).encode("utf-8")
        headers = {
            "Stripe-Signature": signed_header(payload, TEST_ENV["STRIPE_WEBHOOK_SECRET"]),
            "Content-Type": "application/json",
        }
        with patch.dict(os.environ, TEST_ENV, clear=False), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value="duplicate")):
            handler = self.route_handler(app, "POST", "/stripe-payment")
            response = await handler(FakeStripeRequest(payload, headers))

        self.assertEqual(response.status, 200)


if __name__ == "__main__":
    unittest.main()
