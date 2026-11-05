import asyncio
import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from renewal_notifications import (
    WARNING_1D,
    WARNING_3D,
    RenewalNotificationError,
    delivery_key,
    recurring_checkout_disclosure,
    upcoming_snapshot,
    validate_price_for_terms,
    warning_text,
)


class RenewalNotificationContractTests(unittest.TestCase):
    def subscription(self, period_end, **overrides):
        values = dict(id="sub_123", status="active", cancel_at_period_end=False, current_period_end=period_end)
        values.update(overrides)
        return SimpleNamespace(**values)

    def invoice(self, period_end, amount=5000, currency="eur", **overrides):
        values = dict(subscription="sub_123", amount_due=amount, currency=currency, period_start=period_end)
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_three_day_and_one_day_messages_use_authoritative_preview(self):
        now = datetime(2026, 11, 1, 9, tzinfo=timezone.utc)
        for days, kind, phrase in ((3, WARNING_3D, "скоро продлится"), (1, WARNING_1D, "завтра")):
            period_end = int((now + timedelta(days=days)).timestamp())
            snapshot = upcoming_snapshot(self.subscription(period_end), self.invoice(period_end, 4250), now=now)
            self.assertEqual(snapshot["warning_type"], kind)
            self.assertEqual(snapshot["amount_minor"], 4250)
            self.assertIn("42,50 €", warning_text(snapshot, kind))
            self.assertIn(phrase, warning_text(snapshot, kind))

    def test_discount_and_changed_period_produce_distinct_identity(self):
        now = datetime(2026, 11, 1, 9, tzinfo=timezone.utc)
        period_end = int((now + timedelta(days=3)).timestamp())
        first = upcoming_snapshot(self.subscription(period_end), self.invoice(period_end, 5000), now=now)
        discounted = upcoming_snapshot(self.subscription(period_end), self.invoice(period_end, 4000), now=now)
        moved = upcoming_snapshot(
            self.subscription(period_end + 86400), self.invoice(period_end + 86400, 5000), now=now,
        )
        self.assertNotEqual(delivery_key(first, WARNING_3D), delivery_key(discounted, WARNING_3D))
        self.assertNotEqual(first["billing_reference"], moved["billing_reference"])

    def test_disabled_autorenew_and_unconfirmed_amount_fail_closed(self):
        period_end = int((datetime.now(timezone.utc) + timedelta(days=3)).timestamp())
        with self.assertRaisesRegex(RenewalNotificationError, "auto_renew_disabled"):
            upcoming_snapshot(self.subscription(period_end, cancel_at_period_end=True), self.invoice(period_end))
        with self.assertRaisesRegex(RenewalNotificationError, "upcoming_amount_unconfirmed"):
            upcoming_snapshot(self.subscription(period_end), self.invoice(period_end, amount=0))

    def test_checkout_disclosure_and_price_contract(self):
        text = recurring_checkout_disclosure("sub_1")
        self.assertIn("1 месяц, 50 €", text)
        self.assertIn("продлевается автоматически", text)
        self.assertIn("Подтверждаю", text)
        price = SimpleNamespace(
            active=True, currency="eur", unit_amount=5000,
            recurring=SimpleNamespace(interval="month", interval_count=1),
        )
        self.assertTrue(validate_price_for_terms(price, "sub_1"))
        price.unit_amount = 5100
        with self.assertRaisesRegex(RenewalNotificationError, "amount_mismatch"):
            validate_price_for_terms(price, "sub_1")


TEST_ENV = {
    "BOT_TOKEN": "123456:TEST_TOKEN_FOR_RENEWAL_ONLY",
    "DATABASE_URL": "postgresql://user:pass@localhost/test",
    "GROUP_ID": "-100123",
    "ADMIN_IDS": "1,2",
    "STRIPE_API_KEY": "sk_test_dummy",
    "STRIPE_WEBHOOK_SECRET": "renewal_test_webhook_secret",
    "WEBHOOK_SECRET": "telegram_secret",
    "YOUR_DOMAIN": "https://club.example",
    "PRICE_TRIAL": "price_trial",
    "PRICE_1M": "price_1m",
    "PRICE_6M": "price_6m",
    "PRICE_12M": "price_12m",
}
os.environ.update(TEST_ENV)
import main


class RenewalNotificationSchedulingTests(unittest.IsolatedAsyncioTestCase):
    async def test_feature_flag_off_does_not_touch_db_or_stripe(self):
        with patch.object(main, "RENEWAL_WARNINGS_ENABLED", False), \
             patch.object(main, "run_sync_db", AsyncMock()) as run_db, \
             patch.object(main, "retrieve_renewal_warning_snapshot", AsyncMock()) as stripe_call:
            result = await main.enqueue_due_renewal_warnings()
        self.assertEqual(result, {"checked": 0, "enqueued": 0, "skipped": 0})
        run_db.assert_not_awaited()
        stripe_call.assert_not_awaited()

    async def test_repeated_scheduler_uses_same_durable_identity(self):
        now = datetime(2026, 11, 1, 9, tzinfo=timezone.utc)
        period_end = int((now + timedelta(days=3)).timestamp())
        snapshot = upcoming_snapshot(
            SimpleNamespace(id="sub_123", status="active", cancel_at_period_end=False),
            SimpleNamespace(subscription="sub_123", amount_due=5000, currency="eur", period_start=period_end),
            now=now,
        )
        async def run_db(func, *args):
            if func is main.fetch_renewal_warning_candidates:
                return [(123, "sub_123")]
            return True
        with patch.object(main, "RENEWAL_WARNINGS_ENABLED", True), \
             patch.object(main, "run_sync_db", AsyncMock(side_effect=run_db)), \
             patch.object(main, "retrieve_renewal_warning_snapshot", AsyncMock(return_value=snapshot)):
            first = await main.enqueue_due_renewal_warnings(now=now)
            second = await main.enqueue_due_renewal_warnings(now=now)
        self.assertEqual(first["enqueued"], 1)
        self.assertEqual(second["enqueued"], 1)
        self.assertEqual(delivery_key(snapshot, WARNING_3D), delivery_key(snapshot, WARNING_3D))

    async def test_stripe_failure_is_skipped_without_breaking_subscription_job(self):
        async def run_db(func, *args):
            return [(123, "sub_123")] if func is main.fetch_renewal_warning_candidates else False
        with patch.object(main, "RENEWAL_WARNINGS_ENABLED", True), \
             patch.object(main, "run_sync_db", AsyncMock(side_effect=run_db)), \
             patch.object(main, "retrieve_renewal_warning_snapshot", AsyncMock(side_effect=RuntimeError("stripe down"))):
            result = await main.enqueue_due_renewal_warnings()
        self.assertEqual(result, {"checked": 1, "enqueued": 0, "skipped": 1})


if __name__ == "__main__":
    unittest.main()
