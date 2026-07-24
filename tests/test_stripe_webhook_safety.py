import hashlib
import hmac
import json
import time
import unittest
from pathlib import Path

import stripe

from stripe_webhook_safety import (
    construct_verified_stripe_event,
    stripe_signature_error_class,
    stripe_signature_timestamp,
    stripe_webhook_diagnostics,
    webhook_secret_diagnostics,
)


ROOT = Path(__file__).resolve().parents[1]
MAIN_SOURCE = (ROOT / "main.py").read_text()
SECRET_PREFIX = "wh" + "sec_"


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
        self.assertNotIn(SECRET_PREFIX + "diag", str(diagnostics))

    def test_duplicate_stripe_event_returns_before_admin_payment_notification(self):
        webhook_start = MAIN_SOURCE.index("async def stripe_webhook")
        claim_pos = MAIN_SOURCE.index('if claim_result != "claimed":', webhook_start)
        early_return_pos = MAIN_SOURCE.index("return web.Response(status=200)", claim_pos)
        notify_pos = MAIN_SOURCE.index("notify_admins_payment_success_once", webhook_start)
        self.assertLess(early_return_pos, notify_pos)

    def test_admin_payment_notification_is_after_db_commit_and_idempotent(self):
        checkout_pos = MAIN_SOURCE.index("notify_admins_payment_success_once", MAIN_SOURCE.index("checkout.session.completed"))
        checkout_commit_pos = MAIN_SOURCE.rfind("conn.commit()", 0, checkout_pos)
        self.assertLess(checkout_commit_pos, checkout_pos)

        invoice_pos = MAIN_SOURCE.index("notify_admins_payment_success_once", MAIN_SOURCE.index("invoice.payment_succeeded"))
        invoice_commit_pos = MAIN_SOURCE.rfind("conn.commit()", 0, invoice_pos)
        self.assertLess(invoice_commit_pos, invoice_pos)
        self.assertIn('SUCCESSFUL_INVOICE_EVENT_TYPES = ("invoice.payment_succeeded", "invoice.paid")', MAIN_SOURCE)
        self.assertIn("event['type'] in SUCCESSFUL_INVOICE_EVENT_TYPES", MAIN_SOURCE)

        helper_start = MAIN_SOURCE.index("async def notify_admins_idempotent")
        helper_end = MAIN_SOURCE.index("async def notify_admins_payment_success_once")
        helper = MAIN_SOURCE[helper_start:helper_end]
        self.assertIn("notification_key(notification_type, 0, stripe_event_id)", helper)
        self.assertIn("claim_notification", helper)
        self.assertIn("mark_notification_sent", helper)


if __name__ == "__main__":
    unittest.main()
