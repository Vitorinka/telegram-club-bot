import asyncio
import logging
import unittest
from pathlib import Path
from unittest import mock

import stripe
from stripe import _util as stripe_util

import stripe_reconcile_audit


class StripeSdkLoggingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.logger = logging.getLogger("stripe")
        self.previous_level = self.logger.level
        self.logger.setLevel(logging.WARNING)

    def tearDown(self):
        self.logger.setLevel(self.previous_level)

    def capture_stripe_logs(self):
        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = Capture()
        handler.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)
        self.addCleanup(self.logger.removeHandler, handler)
        return records

    def test_actual_sdk_request_and_response_info_are_suppressed(self):
        self.assertIs(stripe_util.logger, logging.getLogger("stripe"))
        self.assertEqual(stripe_util.logger.name, "stripe")
        records = self.capture_stripe_logs()
        secret_url = "https://api.stripe.com/v1/subscriptions/sub_secret_test_value"

        stripe_util.log_info("Request to Stripe api", method="get", url=secret_url)
        stripe_util.log_info("Stripe API response", path=secret_url, response_code=200)

        self.assertEqual(records, [])

    def test_production_bootstrap_sets_only_stripe_sdk_logger_to_warning(self):
        source = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
        self.assertIn('logging.getLogger("stripe").setLevel(logging.WARNING)', source)

    def test_sdk_warning_and_error_visibility_is_preserved(self):
        records = self.capture_stripe_logs()

        self.logger.warning("Stripe SDK warning without resource identifier")
        self.logger.error("Stripe SDK error without resource identifier")

        self.assertEqual([record.levelno for record in records], [logging.WARNING, logging.ERROR])
        self.assertEqual(
            [record.getMessage() for record in records],
            ["Stripe SDK warning without resource identifier", "Stripe SDK error without resource identifier"],
        )

    async def test_reconciliation_suppresses_sdk_url_and_still_completes(self):
        records = self.capture_stripe_logs()
        raw_subscription = "sub_secret_test_value"
        raw_url = f"https://api.stripe.com/v1/subscriptions/{raw_subscription}"
        candidate = {
            "telegram_id": 777,
            "paid": False,
            "expiry_date": None,
            "user_customer_id": "cus_secret_test_value",
            "user_subscription_id": raw_subscription,
            "link_customer_id": "cus_secret_test_value",
            "link_subscription_id": raw_subscription,
            "payment_event_id": None,
            "payment_period_end": None,
            "payment_created_at": None,
            "has_access_override": False,
        }

        def retrieve(subscription_id):
            stripe_util.log_info("Request to Stripe api", method="get", url=raw_url)
            stripe_util.log_info("Stripe API response", path=raw_url, response_code=200)
            return {
                "id": subscription_id,
                "customer": "cus_secret_test_value",
                "status": "active",
                "current_period_end": None,
            }

        result = await stripe_reconcile_audit.reconcile_candidates([candidate], retrieve)
        rendered_logs = "\n".join(record.getMessage() for record in records)

        self.assertEqual(result["results"][0][1], "STRIPE_ACTIVE_LOCAL_UNPAID")
        self.assertNotIn(raw_subscription, rendered_logs)
        self.assertNotIn(raw_url, rendered_logs)

    async def test_checkout_retrieval_sdk_info_path_is_suppressed(self):
        records = self.capture_stripe_logs()
        raw_session = "cs_live_secret_test_value"
        raw_url = f"https://api.stripe.com/v1/checkout/sessions/{raw_session}"

        def retrieve(session_id):
            stripe_util.log_info("Request to Stripe api", method="get", url=raw_url)
            stripe_util.log_info("Stripe API response", path=raw_url, response_code=200)
            return {"id": session_id, "status": "complete"}

        with mock.patch.object(stripe.checkout.Session, "retrieve", side_effect=retrieve):
            result = await asyncio.to_thread(stripe.checkout.Session.retrieve, raw_session)

        self.assertEqual(result["status"], "complete")
        rendered_logs = "\n".join(record.getMessage() for record in records)
        self.assertNotIn(raw_session, rendered_logs)
        self.assertNotIn(raw_url, rendered_logs)


if __name__ == "__main__":
    unittest.main()
