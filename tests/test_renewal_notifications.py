import asyncio
import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from renewal_notifications import (
    WARNING_1D,
    WARNING_3D,
    RECURRING_TERMS_VERSION,
    RenewalNotificationError,
    delivery_key,
    recurring_price_snapshot,
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
        price = SimpleNamespace(
            id="price_1m", active=True, currency="eur", unit_amount=5000,
            recurring=SimpleNamespace(interval="month", interval_count=1),
        )
        snapshot = recurring_price_snapshot(price, "sub_1", "price_1m")
        text = recurring_checkout_disclosure("sub_1", snapshot)
        self.assertIn("1 месяц, 50 €", text)
        self.assertIn("продлевается автоматически", text)
        self.assertIn("Подтверждаю", text)
        self.assertEqual(snapshot["terms_version"], RECURRING_TERMS_VERSION)
        self.assertTrue(validate_price_for_terms(price, "sub_1", "price_1m"))
        with self.assertRaisesRegex(RenewalNotificationError, "identity_mismatch"):
            validate_price_for_terms(price, "sub_1", "price_replaced")
        price.unit_amount = 5100
        with self.assertRaisesRegex(RenewalNotificationError, "amount_mismatch"):
            validate_price_for_terms(price, "sub_1")

    def test_disclosure_rejects_unverified_terms(self):
        with self.assertRaisesRegex(RenewalNotificationError, "unverified"):
            recurring_checkout_disclosure("sub_1", {"amount_minor": 5000})

    def test_active_billing_date_requires_matching_invoice_boundary(self):
        now = datetime(2026, 11, 1, 9, tzinfo=timezone.utc)
        for months, amount in ((1, 5000), (6, 24000), (12, 41000)):
            boundary = int((now + timedelta(days=months)).timestamp())
            snapshot = upcoming_snapshot(
                self.subscription(boundary), self.invoice(boundary, amount), now=now,
            )
            self.assertEqual(int(snapshot["billing_at"].timestamp()), boundary)

    def test_trial_billing_date_uses_confirmed_trial_end(self):
        now = datetime(2026, 11, 1, 9, tzinfo=timezone.utc)
        trial_end = int((now + timedelta(days=3)).timestamp())
        subscription = self.subscription(
            trial_end, status="trialing", trial_end=trial_end,
        )
        snapshot = upcoming_snapshot(subscription, self.invoice(trial_end), now=now)
        self.assertEqual(snapshot["warning_type"], WARNING_3D)
        self.assertEqual(int(snapshot["billing_at"].timestamp()), trial_end)

    def test_unconfirmed_or_changed_billing_date_fails_closed(self):
        now = datetime(2026, 11, 1, 9, tzinfo=timezone.utc)
        boundary = int((now + timedelta(days=3)).timestamp())
        with self.assertRaisesRegex(RenewalNotificationError, "date_unconfirmed"):
            upcoming_snapshot(
                self.subscription(boundary),
                self.invoice(boundary, period_start=None, next_payment_attempt=None),
                now=now,
            )
        with self.assertRaisesRegex(RenewalNotificationError, "date_mismatch"):
            upcoming_snapshot(
                self.subscription(boundary), self.invoice(boundary + 3600), now=now,
            )
        with self.assertRaisesRegex(RenewalNotificationError, "invoice_subscription_mismatch"):
            upcoming_snapshot(
                self.subscription(boundary),
                self.invoice(boundary, subscription="sub_changed_plan"),
                now=now,
            )
        with self.assertRaisesRegex(RenewalNotificationError, "date_mismatch"):
            upcoming_snapshot(
                self.subscription(boundary),
                self.invoice(
                    boundary, next_payment_attempt=boundary + 7200,
                ),
                now=now,
            )


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
    @staticmethod
    def price(amount=5000, price_id="price_1m", active=True):
        return SimpleNamespace(
            id=price_id, active=active, currency="eur", unit_amount=amount,
            recurring=SimpleNamespace(interval="month", interval_count=1),
        )

    @staticmethod
    def callback(data="sub_1", user_id=123):
        message = SimpleNamespace(answer=AsyncMock())
        return SimpleNamespace(
            data=data, from_user=SimpleNamespace(id=user_id),
            message=message, answer=AsyncMock(),
        )

    class State:
        def __init__(self):
            self.data = {}

        async def update_data(self, **kwargs):
            self.data.update(kwargs)

        async def get_data(self):
            return dict(self.data)

    async def test_tariff_buttons_do_not_claim_unverified_amounts(self):
        keyboard = main.get_tariffs_keyboard(show_trial=False)
        labels = [row[0].text for row in keyboard.inline_keyboard]
        self.assertEqual(labels, ["💳 1 месяц", "💳 6 месяцев", "💳 12 месяцев"])
        self.assertNotIn("€", " ".join(labels))

    async def test_terms_are_shown_only_after_live_stripe_price_validation(self):
        callback = self.callback()
        state = self.State()
        with patch.object(main.stripe.Price, "retrieve", return_value=self.price()) as retrieve:
            await main.show_recurring_payment_terms(callback, state)
        retrieve.assert_called_once_with("price_1m")
        callback.message.answer.assert_awaited_once()
        confirmation = state.data["recurring_checkout_confirmation"]
        self.assertEqual(confirmation["user_id"], 123)
        self.assertEqual(confirmation["price_id"], "price_1m")
        markup = callback.message.answer.await_args.kwargs["reply_markup"]
        callback_data = markup.inline_keyboard[0][0].callback_data
        self.assertEqual(callback_data, f"confirm_recurring:sub_1:{confirmation['token']}")

    async def test_changed_price_is_not_shown_and_does_not_open_checkout(self):
        callback = self.callback()
        state = self.State()
        with patch.object(main.stripe.Price, "retrieve", return_value=self.price(amount=5100)), \
             patch.object(main, "process_payment", AsyncMock()) as process_payment:
            await main.show_recurring_payment_terms(callback, state)
        callback.message.answer.assert_not_awaited()
        process_payment.assert_not_awaited()
        self.assertTrue(callback.answer.await_args.kwargs["show_alert"])

    async def test_old_callback_button_is_rejected(self):
        callback = self.callback("confirm_recurring:sub_1")
        state = self.State()
        with patch.object(main, "process_payment", AsyncMock()) as process_payment:
            await main.confirm_recurring_payment(callback, state)
        process_payment.assert_not_awaited()
        self.assertIn("устарели", callback.answer.await_args.args[0])

    async def test_confirmation_is_bound_to_original_user_and_terms_version(self):
        callback = self.callback(user_id=123)
        state = self.State()
        with patch.object(main.stripe.Price, "retrieve", return_value=self.price()):
            await main.show_recurring_payment_terms(callback, state)
        confirmation = state.data["recurring_checkout_confirmation"]
        foreign_callback = self.callback(
            f"confirm_recurring:sub_1:{confirmation['token']}", user_id=456,
        )
        with patch.object(main, "process_payment", AsyncMock()) as process_payment:
            await main.confirm_recurring_payment(foreign_callback, state)
        process_payment.assert_not_awaited()
        self.assertIn("устарели", foreign_callback.answer.await_args.args[0])

    async def test_confirmation_rechecks_price_and_repeated_callback_reuses_context(self):
        callback = self.callback()
        state = self.State()
        with patch.object(main.stripe.Price, "retrieve", return_value=self.price()):
            await main.show_recurring_payment_terms(callback, state)
        confirmation = state.data["recurring_checkout_confirmation"]
        callback.data = f"confirm_recurring:sub_1:{confirmation['token']}"
        with patch.object(main.stripe.Price, "retrieve", return_value=self.price()), \
             patch.object(main, "process_payment", AsyncMock()) as process_payment:
            await main.confirm_recurring_payment(callback, state)
            await main.confirm_recurring_payment(callback, state)
        self.assertEqual(process_payment.await_count, 2)
        for call in process_payment.await_args_list:
            self.assertEqual(call.kwargs["selected_sub_type"], "sub_1")
            self.assertEqual(call.kwargs["recurring_confirmation"]["price_id"], "price_1m")

    async def test_price_change_after_terms_blocks_confirmation(self):
        callback = self.callback()
        state = self.State()
        with patch.object(main.stripe.Price, "retrieve", return_value=self.price()):
            await main.show_recurring_payment_terms(callback, state)
        confirmation = state.data["recurring_checkout_confirmation"]
        callback.data = f"confirm_recurring:sub_1:{confirmation['token']}"
        with patch.object(main.stripe.Price, "retrieve", return_value=self.price(price_id="price_replaced")), \
             patch.object(main, "process_payment", AsyncMock()) as process_payment:
            await main.confirm_recurring_payment(callback, state)
        process_payment.assert_not_awaited()
        self.assertIn("изменились", callback.answer.await_args.args[0])

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
            SimpleNamespace(
                id="sub_123", status="active", cancel_at_period_end=False,
                current_period_end=period_end,
            ),
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
