import hashlib
import hmac
import json
import time
import unittest
from pathlib import Path

from stripe_webhook_safety import (
    construct_verified_stripe_event,
    stripe_signature_error_class,
    stripe_signature_timestamp,
    stripe_value,
    stripe_webhook_diagnostics,
    webhook_secret_diagnostics,
)


SECRET_PREFIX = "wh" + "sec_"
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


class FakeStripeObject:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class NoItemEvent:
    def __init__(self, event_type, event_object):
        self.id = "evt_no_item"
        self.type = event_type
        self.created = 123
        self.data = FakeStripeObject(object=event_object)


class StripeWebhookSafetyTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
