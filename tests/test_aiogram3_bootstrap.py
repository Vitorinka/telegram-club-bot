import asyncio
import hashlib
import hmac
import importlib
import json
import os
from pathlib import Path
import sys
import time
import unittest
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

    async def test_production_code_no_longer_uses_aiogram2_handler_or_fsm_api(self):
        source = Path(self.main.__file__).read_text()
        self.assertNotIn("@dp.message_handler", source)
        self.assertNotIn("@dp.callback_query_handler", source)
        self.assertNotIn("await state.finish()", source)
        self.assertNotIn("InlineKeyboardMarkup(row_width", source)
        self.assertNotIn("ReplyKeyboardMarkup(row_width", source)
        self.assertNotIn("InlineKeyboardMarkup().add", source)

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
