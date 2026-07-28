import asyncio
import ast
import hashlib
import hmac
import importlib
import json
import os
from pathlib import Path
import sys
import time
import unittest
from datetime import datetime, timedelta
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
    def __init__(self):
        self.message = FakeMessage()


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

    def invoice_payment_event(self, event_id, *, period_end=None):
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
            paid_out_of_band=False,
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

    async def run_invoice_webhook_with_future_expiry(self, event_id):
        payload, event, subscription = self.invoice_payment_event(event_id)
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
        from aiogram.fsm.storage.memory import MemoryStorage

        app = self.main.create_app()

        self.assertIsInstance(self.main.bot, Bot)
        self.assertIsInstance(self.main.dp, Dispatcher)
        self.assertIsInstance(self.main.storage, MemoryStorage)
        self.assertIsInstance(app, web.Application)

    async def test_handlers_are_registered_on_native_aiogram3_router(self):
        self.assertEqual(len(self.main.router.message.handlers), 50)
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

        self.assertEqual(len(message_handlers), 50)
        self.assertEqual(len(callback_handlers), 19)
        self.assertEqual(
            commands,
            [
                "promo_trial", "cancel", "menu", "ask", "start", "profile",
                "send_user", "broadcast", "give_access", "set_expiry", "sync_stripe_user",
                "expired_users", "user", "access_history", "recent_access_events",
                "find_by_stripe", "bot_health", "admin", "admin_help", "expiring_users",
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
        self.assertFalse(self.main.is_undeliverable_user_error(bad_request))
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

        self.assertEqual(result, {"sent": 0, "failed": 0, "blocked": 1})
        self.assertEqual(failed_calls, [("key1", "TelegramForbiddenError", None, True)])
        self.assertTrue(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        mark_sent.assert_not_called()

    async def test_message_delivery_bad_request_and_network_errors_remain_retryable(self):
        from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter

        for error, expected_delay in (
            (TelegramBadRequest(method=None, message="chat not found"), 5),
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

                self.assertEqual(result, {"sent": 0, "failed": 1, "blocked": 0})
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

        self.assertEqual(result, {"sent": 1, "failed": 0, "blocked": 0})
        markup = send_message.await_args.kwargs["reply_markup"]
        self.assertEqual(markup.inline_keyboard[0][0].callback_data, "retry_payment")

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

        self.assertEqual(result, {"sent": 1, "failed": 1, "blocked": 0})
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

        self.assertEqual(result, {"sent": 1, "failed": 0, "blocked": 0})
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

        self.assertEqual(result, {"sent": 0, "failed": 1, "blocked": 0})
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

        self.assertEqual(result, {"sent": 0, "failed": 1, "blocked": 0})
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

        self.assertEqual(result, {"sent": 0, "failed": 1, "blocked": 0})
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

        self.assertEqual(result, {"sent": 0, "failed": 1, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_group_limit_secret:rejoin_invite", "TelegramForbiddenError", True)])
        self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()
        self.assertGreaterEqual(notify_admins.await_count, 1)
        alert_text = notify_admins.await_args_list[0].args[0]
        self.assertNotIn("evt_group_limit_secret", alert_text)
        self.assertIn("evt_***secret", alert_text)

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

        self.assertEqual(result, {"sent": 0, "failed": 1, "blocked": 0})
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

        self.assertEqual(result, {"sent": 1, "failed": 0, "blocked": 0})
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

        self.assertEqual(result, {"sent": 0, "failed": 1, "blocked": 0})
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

        self.assertEqual(result, {"sent": 1, "failed": 0, "blocked": 0})
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

        self.assertEqual(result, {"sent": 1, "failed": 0, "blocked": 0})
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

        self.assertEqual(result, {"sent": 0, "failed": 0, "blocked": 1})
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
        self.assertEqual(len(fake_scheduler.jobs), 6)
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
