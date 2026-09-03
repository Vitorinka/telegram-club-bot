import asyncio
import inspect
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


os.environ.setdefault("BOT_TOKEN", "123456:TEST")
os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")
os.environ.setdefault("GROUP_ID", "-100123")
os.environ.setdefault("ADMIN_IDS", "1,2")
os.environ.setdefault("STRIPE_API_KEY", "sk_test_dummy")
os.environ.setdefault("STRIPE_WEBHOOK_SECRET", "test_webhook_secret")
os.environ.setdefault("WEBHOOK_SECRET", "telegram_test")
os.environ.setdefault("YOUR_DOMAIN", "https://club.example")
os.environ.setdefault("PRICE_TRIAL", "price_trial")
os.environ.setdefault("PRICE_1M", "price_1m")
os.environ.setdefault("PRICE_6M", "price_6m")
os.environ.setdefault("PRICE_12M", "price_12m")

import main


class FailedSubscriptionTerminationTests(unittest.TestCase):
    def test_failed_message_keyboard_has_distinct_actions(self):
        markup = main.get_failed_subscription_actions_keyboard()
        buttons = [button for row in markup.inline_keyboard for button in row]
        self.assertEqual(
            [(button.text, button.callback_data) for button in buttons],
            [
                ("💳 Оплатить / сменить карту", "failed_subscription_payment_action"),
                ("❌ Отписаться и закрыть доступ", "cancel_failed_subscription"),
            ],
        )
        self.assertNotEqual(buttons[1].callback_data, "cancel_subscription")
        confirmation = main.get_failed_subscription_cancel_confirmation_keyboard()
        self.assertEqual(confirmation.inline_keyboard[1][0].callback_data, "failed_subscription_actions")

    def test_normal_cancel_still_uses_end_of_period(self):
        source = inspect.getsource(main.cancel_subscription)
        self.assertIn("cancel_at_period_end=True", source)
        self.assertNotIn("terminate_failed_subscription", source)

    def test_failed_grace_is_fixed_and_hourly_candidate_is_narrow(self):
        source = Path(main.__file__).read_text(encoding="utf-8")
        failed = source[source.index("elif event_type == 'invoice.payment_failed'"):source.index("# ---------- 4. ПОЛЬЗОВАТЕЛЬ")]
        self.assertIn("COALESCE(payment_failed_at, NOW()) + (%s * INTERVAL '1 hour')", failed)
        self.assertNotIn("NOW() + (%s * INTERVAL '1 hour')\n                        )", failed)
        hourly = inspect.getsource(main.process_expired_failed_subscription_grace)
        self.assertIn("payment_failed = TRUE", hourly)
        self.assertIn("grace_period_end <= NOW()", hourly)
        self.assertIn("status = 'processing'", hourly)
        self.assertIn("lease_until <= (NOW() AT TIME ZONE 'UTC')", hourly)
        self.assertIn("terminate_failed_subscription", hourly)

    def test_failed_cycle_delivery_key_dedupes_different_events_in_same_grace(self):
        from datetime import datetime
        deadline = datetime(2026, 9, 3, 10, 0)
        self.assertEqual(
            main.failed_subscription_cycle_delivery_key("sub_same", deadline),
            main.failed_subscription_cycle_delivery_key("sub_same", deadline),
        )
        self.assertNotIn("sub_same", main.failed_subscription_cycle_delivery_key("sub_same", deadline))

    def test_immediate_cancel_closes_only_verified_failed_invoice(self):
        source = inspect.getsource(main.terminate_failed_subscription)
        self.assertIn("subscription.cancel", source)
        self.assertIn("stop_subscription_invoice_retries", source)

    def test_open_failed_invoice_is_voided_and_confirmed(self):
        invoice = mock.Mock(id="in_failed", subscription="sub_failed", status="open")
        invoice.void_invoice.return_value = mock.Mock(status="void")
        confirmation = mock.Mock(id="in_failed", subscription="sub_failed", status="void")
        with mock.patch.object(
                 main.stripe.Invoice, "retrieve", side_effect=[invoice, confirmation],
             ) as retrieve, \
             mock.patch.object(main, "failed_termination_phase", return_value=True) as phase, \
             mock.patch.object(main, "failed_termination_failure") as failure, \
             mock.patch.object(main, "enqueue_failed_termination_admin_alert") as alert:
            result = asyncio.run(main.stop_subscription_invoice_retries(
                "op", 1, "sub_failed", "in_failed", "owner", 2,
            ))
        self.assertEqual(result, "completed")
        invoice.void_invoice.assert_called_once_with()
        self.assertEqual(retrieve.call_args_list, [mock.call("in_failed"), mock.call("in_failed")])
        phase.assert_called_once_with("op", "owner", 2, "processing", "collection_stopped_at")
        failure.assert_not_called()
        alert.assert_not_called()

    def test_void_invoice_is_idempotent(self):
        invoice = mock.Mock(id="in_failed", subscription="sub_failed", status="void")
        with mock.patch.object(main.stripe.Invoice, "retrieve", return_value=invoice), \
             mock.patch.object(main, "failed_termination_phase", return_value=True):
            result = asyncio.run(main.stop_subscription_invoice_retries(
                "op", 1, "sub_failed", "in_failed", "owner", 2,
            ))
        self.assertEqual(result, "completed")
        invoice.void_invoice.assert_not_called()

    def test_parent_subscription_shape_is_voided_and_confirmed(self):
        parent = {"subscription_details": {"subscription": "sub_failed"}}
        invoice = SimpleNamespace(
            id="in_parent", subscription=None, parent=parent, status="open",
            void_invoice=mock.Mock(return_value=SimpleNamespace(status="void")),
        )
        confirmation = SimpleNamespace(
            id="in_parent", subscription=None, parent=parent, status="void",
        )
        with mock.patch.object(
                 main.stripe.Invoice, "retrieve", side_effect=[invoice, confirmation],
             ), mock.patch.object(main, "failed_termination_phase", return_value=True):
            result = asyncio.run(main.stop_subscription_invoice_retries(
                "op", 1, "sub_failed", "in_parent", "owner", 2,
            ))
        self.assertEqual(result, "completed")
        invoice.void_invoice.assert_called_once_with()

    def test_parent_subscription_shape_mismatch_is_never_voided(self):
        invoice = SimpleNamespace(
            id="in_parent", subscription=None,
            parent={"subscription_details": {"subscription": "sub_other"}},
            status="open",
            void_invoice=mock.Mock(),
        )
        with mock.patch.object(main.stripe.Invoice, "retrieve", return_value=invoice), \
             mock.patch.object(main, "failed_termination_failure") as failure, \
             mock.patch.object(main, "enqueue_failed_termination_admin_alert"):
            result = asyncio.run(main.stop_subscription_invoice_retries(
                "op", 1, "sub_failed", "in_parent", "owner", 2,
            ))
        self.assertEqual(result, "manual_review")
        invoice.void_invoice.assert_not_called()
        failure.assert_called_once_with(
            "op", "owner", 2, "failed_invoice_identity_mismatch", terminal=True,
        )

    def test_uncollectible_invoice_is_voided_and_confirmed(self):
        invoice = mock.Mock(id="in_bad_debt", subscription="sub_failed", status="uncollectible")
        confirmation = mock.Mock(id="in_bad_debt", subscription="sub_failed", status="void")
        with mock.patch.object(
                 main.stripe.Invoice, "retrieve", side_effect=[invoice, confirmation],
             ), mock.patch.object(main, "failed_termination_phase", return_value=True):
            result = asyncio.run(main.stop_subscription_invoice_retries(
                "op", 1, "sub_failed", "in_bad_debt", "owner", 2,
            ))
        self.assertEqual(result, "completed")
        invoice.void_invoice.assert_called_once_with()

    def test_uncollectible_invoice_for_other_subscription_is_not_voided(self):
        invoice = mock.Mock(id="in_bad_debt", subscription="sub_other", status="uncollectible")
        with mock.patch.object(main.stripe.Invoice, "retrieve", return_value=invoice), \
             mock.patch.object(main, "failed_termination_failure") as failure, \
             mock.patch.object(main, "enqueue_failed_termination_admin_alert"):
            result = asyncio.run(main.stop_subscription_invoice_retries(
                "op", 1, "sub_failed", "in_bad_debt", "owner", 2,
            ))
        self.assertEqual(result, "manual_review")
        invoice.void_invoice.assert_not_called()
        failure.assert_called_once_with(
            "op", "owner", 2, "failed_invoice_identity_mismatch", terminal=True,
        )

    def test_uncollectible_void_failure_remains_retryable(self):
        invoice = mock.Mock(id="in_bad_debt", subscription="sub_failed", status="uncollectible")
        invoice.void_invoice.side_effect = RuntimeError("temporary")
        with mock.patch.object(main.stripe.Invoice, "retrieve", return_value=invoice), \
             mock.patch.object(main, "failed_termination_failure") as failure, \
             mock.patch.object(main, "enqueue_failed_termination_admin_alert"):
            result = asyncio.run(main.stop_subscription_invoice_retries(
                "op", 1, "sub_failed", "in_bad_debt", "owner", 2,
            ))
        self.assertEqual(result, "retryable")
        failure.assert_called_once_with(
            "op", "owner", 2, "failed_invoice_void_failed", terminal=False,
        )

    def test_paid_or_wrong_subscription_invoice_is_never_voided(self):
        for status, invoice_subscription, category in (
            ("paid", "sub_failed", "failed_invoice_already_paid"),
            ("open", "sub_other", "failed_invoice_identity_mismatch"),
        ):
            with self.subTest(status=status, invoice_subscription=invoice_subscription):
                invoice = mock.Mock(id="in_failed", subscription=invoice_subscription, status=status)
                with mock.patch.object(main.stripe.Invoice, "retrieve", return_value=invoice), \
                     mock.patch.object(main, "failed_termination_failure") as failure, \
                     mock.patch.object(main, "enqueue_failed_termination_admin_alert") as alert:
                    result = asyncio.run(main.stop_subscription_invoice_retries(
                        "op", 1, "sub_failed", "in_failed", "owner", 2,
                    ))
                self.assertEqual(result, "manual_review")
                invoice.void_invoice.assert_not_called()
                failure.assert_called_once_with("op", "owner", 2, category, terminal=True)
                alert.assert_called_once()

    def test_void_failure_remains_retryable(self):
        invoice = mock.Mock(id="in_failed", subscription="sub_failed", status="open")
        invoice.void_invoice.side_effect = RuntimeError("temporary")
        with mock.patch.object(main.stripe.Invoice, "retrieve", return_value=invoice), \
             mock.patch.object(main, "failed_termination_failure") as failure, \
             mock.patch.object(main, "enqueue_failed_termination_admin_alert") as alert:
            result = asyncio.run(main.stop_subscription_invoice_retries(
                "op", 1, "sub_failed", "in_failed", "owner", 2,
            ))
        self.assertEqual(result, "retryable")
        failure.assert_called_once_with(
            "op", "owner", 2, "failed_invoice_void_failed", terminal=False,
        )
        alert.assert_called_once()

    def test_late_webhook_fences_are_subscription_specific(self):
        source = Path(main.__file__).read_text(encoding="utf-8")
        self.assertIn("failed_subscription_terminations fst", source)
        self.assertIn("fst.stripe_subscription_id = users.stripe_subscription_id", source)
        self.assertIn("fst.status <> 'superseded'", source)


if __name__ == "__main__":
    unittest.main()
