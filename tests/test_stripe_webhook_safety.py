import asyncio
import hashlib
import hmac
import importlib
import json
import os
import sys
import time
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from stripe_webhook_safety import (
    claim_normalized_stripe_event,
    construct_verified_stripe_event,
    normalize_stripe_event,
    require_normalized_stripe_event,
    stripe_signature_error_class,
    stripe_event_created_at,
    stripe_signature_timestamp,
    stripe_value,
    stripe_webhook_diagnostics,
    webhook_secret_diagnostics,
)


SECRET_PREFIX = "wh" + "sec_"
MAIN_IMPORT_LOOP = None
WEBHOOK_EVENT_TYPES = (
    "checkout.session.completed",
    "invoice.payment_succeeded",
    "invoice.payment_failed",
    "customer.subscription.updated",
    "customer.subscription.deleted",
    "checkout.session.expired",
)


def signed_header(payload, secret, timestamp=None):
    timestamp = int(time.time()) if timestamp is None else timestamp
    signature = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.".encode("utf-8") + payload,
        hashlib.sha256,
    ).hexdigest()
    return f"t={timestamp},v1={signature}"


class FakeRequest:
    path = "/stripe-payment"
    host = "club.example"
    headers = {"Content-Type": "application/json"}


class FakeWebhookRequest:
    path = "/stripe-payment"
    host = "club.example"

    def __init__(self, payload, signature):
        self._payload = payload
        self.headers = {
            "Content-Type": "application/json",
            "Stripe-Signature": signature,
        }

    async def read(self):
        return self._payload


class FakeStripeObject:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class NoItemEvent:
    def __init__(self, event_type, event_object):
        self.id = "evt_no_item"
        self.type = event_type
        self.created = 123
        self.data = FakeStripeObject(object=event_object)


def stripe_webhook_test_env(secret):
    return {
        "BOT_TOKEN": "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi",
        "DATABASE_URL": "postgres://bot:secret@localhost:5432/club",
        "GROUP_ID": "-1001234567890",
        "ADMIN_IDS": "123456",
        "STRIPE_API_KEY": "sk_test_unit",
        "STRIPE_WEBHOOK_SECRET": secret,
        "WEBHOOK_SECRET": "webhook-test-secret",
        "YOUR_DOMAIN": "https://club.example",
        "PRICE_TRIAL": "price_trial",
        "PRICE_1M": "price_1m",
        "PRICE_6M": "price_6m",
        "PRICE_12M": "price_12m",
    }


class _FakeBot:
    def __init__(self, *args, **kwargs):
        pass

    async def close(self):
        pass


class _FakeDispatcher:
    def __init__(self, *args, **kwargs):
        pass

    def message_handler(self, *args, **kwargs):
        return lambda func: func

    def callback_query_handler(self, *args, **kwargs):
        return lambda func: func


class _FakeMarkup:
    def __init__(self, *args, **kwargs):
        self.items = []

    def add(self, *items):
        self.items.extend(items)
        return self


class _FakeState:
    async def set(self):
        pass


class _FakeStatesGroup:
    pass


class _FakeBotBlocked(Exception):
    pass


class _FakeContentTypes:
    TEXT = "text"
    ANY = "any"


class _FakeContentType:
    NEW_CHAT_MEMBERS = "new_chat_members"
    LEFT_CHAT_MEMBER = "left_chat_member"


class _FakeScheduler:
    def __init__(self, *args, **kwargs):
        self.jobs = []

    def add_job(self, *args, **kwargs):
        self.jobs.append((args, kwargs))

    def get_jobs(self):
        return list(self.jobs)

    def start(self):
        pass


def install_aiogram_import_stubs():
    aiogram = types.ModuleType("aiogram")
    aiogram.Bot = _FakeBot
    aiogram.Dispatcher = _FakeDispatcher

    aiogram_types = types.ModuleType("aiogram.types")
    for name in (
        "InlineKeyboardButton",
        "KeyboardButton",
        "InputFile",
        "BotCommand",
    ):
        setattr(aiogram_types, name, type(name, (), {"__init__": lambda self, *args, **kwargs: None}))
    aiogram_types.InlineKeyboardMarkup = _FakeMarkup
    aiogram_types.ReplyKeyboardMarkup = _FakeMarkup
    aiogram_types.Message = type("Message", (), {})
    aiogram_types.CallbackQuery = type("CallbackQuery", (), {})
    aiogram_types.ContentTypes = _FakeContentTypes
    aiogram_types.ContentType = _FakeContentType
    aiogram.types = aiogram_types

    storage_module = types.ModuleType("aiogram.contrib.fsm_storage.memory")
    storage_module.MemoryStorage = type("MemoryStorage", (), {"__init__": lambda self, *args, **kwargs: None})

    exceptions_module = types.ModuleType("aiogram.utils.exceptions")
    exceptions_module.BotBlocked = _FakeBotBlocked

    dispatcher_module = types.ModuleType("aiogram.dispatcher")
    dispatcher_module.FSMContext = type("FSMContext", (), {})

    state_module = types.ModuleType("aiogram.dispatcher.filters.state")
    state_module.State = _FakeState
    state_module.StatesGroup = _FakeStatesGroup

    sys.modules.update({
        "aiogram": aiogram,
        "aiogram.types": aiogram_types,
        "aiogram.contrib": types.ModuleType("aiogram.contrib"),
        "aiogram.contrib.fsm_storage": types.ModuleType("aiogram.contrib.fsm_storage"),
        "aiogram.contrib.fsm_storage.memory": storage_module,
        "aiogram.utils": types.ModuleType("aiogram.utils"),
        "aiogram.utils.exceptions": exceptions_module,
        "aiogram.dispatcher": dispatcher_module,
        "aiogram.dispatcher.filters": types.ModuleType("aiogram.dispatcher.filters"),
        "aiogram.dispatcher.filters.state": state_module,
    })


def install_scheduler_import_stubs():
    scheduler_module = types.ModuleType("apscheduler.schedulers.asyncio")
    scheduler_module.AsyncIOScheduler = _FakeScheduler
    sys.modules.update({
        "apscheduler": types.ModuleType("apscheduler"),
        "apscheduler.schedulers": types.ModuleType("apscheduler.schedulers"),
        "apscheduler.schedulers.asyncio": scheduler_module,
    })


def import_main_for_stripe_webhook(secret):
    global MAIN_IMPORT_LOOP
    with patch.dict(os.environ, stripe_webhook_test_env(secret), clear=False):
        install_aiogram_import_stubs()
        install_scheduler_import_stubs()
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            MAIN_IMPORT_LOOP = asyncio.new_event_loop()
            asyncio.set_event_loop(MAIN_IMPORT_LOOP)
        if "main" in sys.modules:
            return sys.modules["main"]
        return importlib.import_module("main")


def stripe_payload(event_id, event_type="checkout.session.completed", created=1720000000, event_object=None):
    payload = {
        "id": event_id,
        "object": "event",
        "type": event_type,
        "created": created,
        "data": {"object": event_object or {"id": "cs_test_webhook", "object": "checkout.session"}},
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


class StripeWebhookSafetyTests(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        global MAIN_IMPORT_LOOP
        if MAIN_IMPORT_LOOP and not MAIN_IMPORT_LOOP.is_closed():
            MAIN_IMPORT_LOOP.close()
            MAIN_IMPORT_LOOP = None
        try:
            asyncio.set_event_loop(None)
        except RuntimeError:
            pass

    def test_construct_event_accepts_correct_signature(self):
        payload = json.dumps(
            {"id": "evt_test_ok", "object": "event", "type": "checkout.session.completed", "data": {"object": {}}},
            separators=(",", ":"),
        ).encode("utf-8")
        secret = SECRET_PREFIX + "test_secret"
        event = construct_verified_stripe_event(payload, signed_header(payload, secret), secret)
        self.assertEqual(event["id"], "evt_test_ok")

    def test_construct_event_rejects_wrong_signature(self):
        payload = b'{"id":"evt_bad","object":"event"}'
        header = signed_header(payload, SECRET_PREFIX + "right")
        with self.assertRaises(stripe_signature_error_class()):
            construct_verified_stripe_event(payload, header, SECRET_PREFIX + "wrong")

    def test_secret_with_whitespace_is_diagnosed_and_trimmed_for_verification(self):
        payload = b'{"id":"evt_trim","object":"event"}'
        raw_secret = " " + SECRET_PREFIX + "trimmed\n"
        header = signed_header(payload, raw_secret.strip())
        diagnostics = webhook_secret_diagnostics(raw_secret)
        self.assertTrue(diagnostics["secret_strip_differs"])
        self.assertFalse(diagnostics["secret_starts_whsec"])
        self.assertTrue(diagnostics["secret_stripped_starts_whsec"])
        event = construct_verified_stripe_event(payload, header, raw_secret)
        self.assertEqual(event["id"], "evt_trim")

    def test_diagnostics_include_safe_request_and_signature_metadata(self):
        payload = b'{"id":"evt_diag","object":"event"}'
        header = signed_header(payload, SECRET_PREFIX + "diag", timestamp=222)
        diagnostics = stripe_webhook_diagnostics(
            FakeRequest(),
            payload,
            header,
            SECRET_PREFIX + "diag",
            {"RAILWAY_ENVIRONMENT": "production", "RAILWAY_SERVICE_NAME": "bot"},
        )
        self.assertEqual(diagnostics["path"], "/stripe-payment")
        self.assertEqual(diagnostics["host"], "club.example")
        self.assertEqual(diagnostics["content_type"], "application/json")
        self.assertEqual(diagnostics["payload_bytes"], len(payload))
        self.assertTrue(diagnostics["signature_present"])
        self.assertEqual(stripe_signature_timestamp(header), "222")
        self.assertEqual(diagnostics["RAILWAY_ENVIRONMENT"], "production")
        self.assertEqual(diagnostics["RAILWAY_SERVICE_NAME"], "bot")
        self.assertNotIn(SECRET_PREFIX + "diag", str(diagnostics))

    def test_stripe_value_reads_stripe_object_without_get(self):
        event = FakeStripeObject(
            id="evt_object",
            created=123,
            data=FakeStripeObject(object=FakeStripeObject(id="sub_object")),
        )

        self.assertFalse(hasattr(event, "get"))
        self.assertEqual(stripe_value(event, "created"), 123)
        event_object = stripe_value(event, "data", "object")
        self.assertEqual(stripe_value(event_object, "id"), "sub_object")

    def test_stripe_value_keeps_dict_compatibility(self):
        event = {
            "id": "evt_dict",
            "created": 456,
            "data": {"object": {"id": "sub_dict"}},
        }

        self.assertEqual(stripe_value(event, "created"), 456)
        event_object = stripe_value(event, "data", "object")
        self.assertEqual(stripe_value(event_object, "id"), "sub_dict")

    def test_normalize_stripe_event_reads_dict_event(self):
        event = {
            "id": " evt_dict ",
            "type": "invoice.payment_succeeded",
            "created": 123,
            "data": {"object": {"id": "in_dict"}},
        }

        normalized = require_normalized_stripe_event(normalize_stripe_event(event))

        self.assertEqual(normalized["event_id"], "evt_dict")
        self.assertEqual(normalized["event_type"], "invoice.payment_succeeded")
        self.assertEqual(normalized["event_created_at"], datetime.utcfromtimestamp(123))
        self.assertEqual(stripe_value(normalized["event_object"], "id"), "in_dict")
        self.assertEqual(normalized["object_id"], "in_dict")

    def test_normalize_stripe_event_reads_object_without_get_or_item_access(self):
        event_object = FakeStripeObject(id="sub_object")
        event = NoItemEvent("customer.subscription.updated", event_object)

        self.assertFalse(hasattr(event, "get"))
        self.assertFalse(hasattr(event, "__getitem__"))
        normalized = require_normalized_stripe_event(normalize_stripe_event(event))

        self.assertEqual(normalized["event_id"], "evt_no_item")
        self.assertEqual(normalized["event_type"], "customer.subscription.updated")
        self.assertEqual(stripe_value(normalized["event_object"], "id"), "sub_object")
        self.assertEqual(normalized["object_id"], "sub_object")

    def test_stripe_event_created_at_tolerates_missing_and_invalid_values(self):
        for created in (None, "", "bad", 10**100):
            with self.subTest(created=created):
                self.assertIsNone(stripe_event_created_at(created))

    def test_normalize_stripe_event_tolerates_missing_created(self):
        event = FakeStripeObject(
            id="evt_missing_created",
            type="checkout.session.completed",
            data=FakeStripeObject(object=FakeStripeObject(id="cs_test")),
        )

        normalized = require_normalized_stripe_event(normalize_stripe_event(event))

        self.assertIsNone(normalized["event_created_at"])

    def test_require_normalized_stripe_event_rejects_missing_event_id(self):
        normalized = normalize_stripe_event(
            FakeStripeObject(type="invoice.payment_failed", data=FakeStripeObject(object=FakeStripeObject(id="in_test")))
        )

        with self.assertRaises(ValueError):
            require_normalized_stripe_event(normalized)

    def test_require_normalized_stripe_event_rejects_missing_event_type(self):
        normalized = normalize_stripe_event(
            FakeStripeObject(id="evt_missing_type", data=FakeStripeObject(object=FakeStripeObject(id="in_test")))
        )

        with self.assertRaises(ValueError):
            require_normalized_stripe_event(normalized)

    def test_webhook_claim_extraction_does_not_use_get(self):
        event = FakeStripeObject(
            id="evt_no_get",
            type="customer.subscription.updated",
            created=789,
            data=FakeStripeObject(object=FakeStripeObject(id="sub_no_get")),
        )

        try:
            event_created = stripe_value(event, "created")
            event_object = stripe_value(event, "data", "object")
            object_id = stripe_value(event_object, "id")
        except AttributeError as exc:
            self.fail(f"StripeObject-style event access raised AttributeError: {exc}")

        self.assertEqual(event_created, 789)
        self.assertEqual(object_id, "sub_no_get")

    def test_dispatch_uses_normalized_event_fields_without_item_access(self):
        for event_type in WEBHOOK_EVENT_TYPES:
            with self.subTest(event_type=event_type):
                event_object = FakeStripeObject(id="obj_no_item")
                event = NoItemEvent(event_type, event_object)

                self.assertFalse(hasattr(event, "get"))
                self.assertFalse(hasattr(event, "__getitem__"))

                normalized_type = stripe_value(event, "type")
                normalized_object = stripe_value(event, "data", "object")

                if normalized_type == "checkout.session.completed":
                    dispatched_object = normalized_object
                elif normalized_type == "invoice.payment_succeeded":
                    dispatched_object = normalized_object
                elif normalized_type == "invoice.payment_failed":
                    dispatched_object = normalized_object
                elif normalized_type == "customer.subscription.deleted":
                    dispatched_object = normalized_object
                elif normalized_type == "customer.subscription.updated":
                    dispatched_object = normalized_object
                elif normalized_type in ("checkout.session.expired", "checkout.session.async_payment_failed"):
                    dispatched_object = normalized_object
                else:
                    dispatched_object = None

                self.assertIs(dispatched_object, event_object)

    def test_stripe_webhook_has_no_raw_event_dispatch_indexing(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        start = source.index("async def stripe_webhook(request):")
        end = source.index("@dp.message_handler(commands=['test_auto_lesson']", start)
        webhook_source = source[start:end]

        for forbidden in (
            'event["type"]',
            "event['type']",
            'event["data"]["object"]',
            "event['data']['object']",
        ):
            self.assertNotIn(forbidden, webhook_source)

    def test_claim_exception_releases_event_and_reraises(self):
        calls = []

        async def claim(event_id, **kwargs):
            calls.append(("claim", event_id, kwargs))
            raise RuntimeError("db down")

        async def release(event_id):
            calls.append(("release", event_id))

        async def run_claim():
            await claim_normalized_stripe_event(
                claim,
                release,
                "evt_claim_error",
                event_created_at=None,
                event_type="invoice.payment_succeeded",
                object_id="in_test",
            )

        with self.assertRaises(RuntimeError):
            asyncio.run(run_claim())

        self.assertEqual(calls[0][0], "claim")
        self.assertEqual(calls[1], ("release", "evt_claim_error"))

    def test_claim_exception_reraises_when_release_also_fails(self):
        calls = []

        async def claim(event_id, **kwargs):
            calls.append(("claim", event_id))
            raise RuntimeError("claim failed")

        async def release(event_id):
            calls.append(("release", event_id))
            raise RuntimeError("release failed")

        async def run_claim():
            await claim_normalized_stripe_event(claim, release, "evt_release_error")

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(run_claim())

        self.assertEqual(str(ctx.exception), "claim failed")
        self.assertEqual(calls, [("claim", "evt_release_error"), ("release", "evt_release_error")])

    def test_claim_release_failure_logs_safe_event_id_and_reraises_claim_error(self):
        raw_event_id = "evt_m2_safe_logging_unique_1234567890abcdef"
        safe_event_id = "evt_***abcdef"

        async def claim(event_id, **kwargs):
            raise RuntimeError("claim failed")

        async def release(event_id):
            raise RuntimeError("release failed")

        async def run_claim():
            await claim_normalized_stripe_event(claim, release, raw_event_id)

        with self.assertLogs(level="ERROR") as logs:
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(run_claim())

        log_text = "\n".join(logs.output)
        self.assertEqual(str(ctx.exception), "claim failed")
        self.assertNotIn(raw_event_id, log_text)
        self.assertIn(safe_event_id, log_text)
        self.assertNotIn(SECRET_PREFIX, log_text)
        self.assertNotIn("Stripe-Signature", log_text)

    def test_real_stripe_webhook_empty_event_id_returns_500_without_claim_or_business_logic(self):
        secret = SECRET_PREFIX + "real_handler_empty_id"
        payload = stripe_payload("")
        signature = signed_header(payload, secret)
        main = import_main_for_stripe_webhook(secret)
        request = FakeWebhookRequest(payload, signature)

        claim = AsyncMock()
        release = AsyncMock()
        mark_processed = AsyncMock()
        checkout_action = Mock()

        with patch.dict(os.environ, stripe_webhook_test_env(secret), clear=False):
            with patch.object(main, "claim_event_processing", claim), \
                    patch.object(main, "release_event_processing", release), \
                    patch.object(main, "mark_event_processed", mark_processed), \
                    patch.object(main, "checkout_completion_action", checkout_action):
                response = asyncio.run(main.stripe_webhook(request))

        self.assertEqual(response.status, 500)
        claim.assert_not_called()
        release.assert_not_called()
        mark_processed.assert_not_called()
        checkout_action.assert_not_called()

    def test_real_stripe_webhook_claim_exception_returns_500_releases_and_skips_business_logic(self):
        secret = SECRET_PREFIX + "real_handler_claim_error"
        payload = stripe_payload(
            "evt_real_handler_claim_exception_123456",
            event_object={
                "id": "cs_real_handler_claim_exception",
                "object": "checkout.session",
                "client_reference_id": "777",
                "subscription": "sub_real_handler_claim_exception",
                "customer": "cus_real_handler_claim_exception",
            },
        )
        signature = signed_header(payload, secret)
        main = import_main_for_stripe_webhook(secret)
        request = FakeWebhookRequest(payload, signature)

        claim = AsyncMock(side_effect=RuntimeError("claim exploded"))
        release = AsyncMock()
        mark_processed = AsyncMock()
        checkout_action = Mock()

        with patch.dict(os.environ, stripe_webhook_test_env(secret), clear=False):
            with patch.object(main, "claim_event_processing", claim), \
                    patch.object(main, "release_event_processing", release), \
                    patch.object(main, "mark_event_processed", mark_processed), \
                    patch.object(main, "checkout_completion_action", checkout_action):
                response = asyncio.run(main.stripe_webhook(request))

        self.assertEqual(response.status, 500)
        claim.assert_awaited_once()
        release.assert_awaited_once_with("evt_real_handler_claim_exception_123456")
        mark_processed.assert_not_called()
        checkout_action.assert_not_called()

    def test_duplicate_processed_result_passes_through_without_release(self):
        calls = []

        async def claim(event_id, **kwargs):
            calls.append(("claim", event_id))
            return "duplicate_processed"

        async def release(event_id):
            calls.append(("release", event_id))

        async def run_claim():
            return await claim_normalized_stripe_event(claim, release, "evt_duplicate")

        result = asyncio.run(run_claim())

        self.assertEqual(result, "duplicate_processed")
        self.assertEqual(calls, [("claim", "evt_duplicate")])


if __name__ == "__main__":
    unittest.main()
