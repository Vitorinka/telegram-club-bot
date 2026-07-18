import unittest
from datetime import datetime, timedelta

from stripe_invoice_rules import (
    has_future_trial,
    is_paid_out_of_band_invoice,
    is_zero_subscription_update_invoice,
    should_ignore_payment_failed_for_active_trial,
    successful_invoice_action,
)


class StripeInvoiceRulesTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 7, 18, 12, 0, 0)
        self.future_trial_end = int((self.now + timedelta(days=30)).timestamp())
        self.past_trial_end = int((self.now - timedelta(days=1)).timestamp())

    def test_regular_monthly_payment_is_processed(self):
        self.assertEqual(
            successful_invoice_action(
                amount_paid=3900,
                billing_reason="subscription_cycle",
                subscription_status="active",
                trial_end=None,
                now=self.now,
                invoice={
                    "amount_due": 3900,
                    "payments": {
                        "data": [
                            {
                                "status": "paid",
                                "amount_paid": 3900,
                                "payment": {
                                    "type": "payment_intent",
                                    "payment_intent": "pi_123",
                                },
                            }
                        ]
                    },
                },
                amount_due=3900,
            ),
            "process_payment",
        )

    def test_zero_subscription_update_without_trial_is_ignored(self):
        self.assertTrue(is_zero_subscription_update_invoice(0, "subscription_update"))
        self.assertEqual(
            successful_invoice_action(
                amount_paid=0,
                billing_reason="subscription_update",
                subscription_status="active",
                trial_end=None,
                now=self.now,
            ),
            "ignore_zero",
        )

    def test_zero_subscription_update_with_future_trial_syncs_trial(self):
        self.assertEqual(
            successful_invoice_action(
                amount_paid=0,
                billing_reason="subscription_update",
                subscription_status="trialing",
                trial_end=self.future_trial_end,
                now=self.now,
            ),
            "sync_trial",
        )

    def test_payment_failed_past_due_is_not_ignored(self):
        self.assertFalse(
            should_ignore_payment_failed_for_active_trial(
                subscription_status="past_due",
                trial_end=None,
                now=self.now,
            )
        )

    def test_payment_failed_old_invoice_is_ignored_during_active_trial(self):
        self.assertTrue(
            should_ignore_payment_failed_for_active_trial(
                subscription_status="trialing",
                trial_end=self.future_trial_end,
                now=self.now,
            )
        )

    def test_failed_payment_after_trial_end_is_not_ignored(self):
        self.assertFalse(
            should_ignore_payment_failed_for_active_trial(
                subscription_status="trialing",
                trial_end=self.past_trial_end,
                now=self.now,
            )
        )

    def test_successful_payment_after_trial_end_is_processed(self):
        self.assertEqual(
            successful_invoice_action(
                amount_paid=3900,
                billing_reason="subscription_cycle",
                subscription_status="active",
                trial_end=self.past_trial_end,
                now=self.now,
                invoice={
                    "amount_due": 3900,
                    "payments": {
                        "data": [
                            {
                                "status": "paid",
                                "amount_paid": 3900,
                                "payment": {
                                    "type": "payment_intent",
                                    "payment_intent": "pi_after_trial",
                                },
                            }
                        ]
                    },
                },
                amount_due=3900,
            ),
            "process_payment",
        )

    def test_duplicate_delivery_uses_same_decision(self):
        first = successful_invoice_action(
            amount_paid=3900,
            billing_reason="subscription_cycle",
            subscription_status="active",
            trial_end=None,
            now=self.now,
            invoice={"payments": {"data": []}},
            amount_due=3900,
        )
        second = successful_invoice_action(
            amount_paid=3900,
            billing_reason="subscription_cycle",
            subscription_status="active",
            trial_end=None,
            now=self.now,
            invoice={"payments": {"data": []}},
            amount_due=3900,
        )
        self.assertEqual(first, second)

    def test_out_of_order_zero_invoice_after_paid_invoice_is_ignored_without_trial(self):
        self.assertEqual(
            successful_invoice_action(
                amount_paid=0,
                billing_reason="subscription_update",
                subscription_status="active",
                trial_end=None,
                now=self.now,
            ),
            "ignore_zero",
        )

    def test_has_future_trial_requires_trialing_status(self):
        self.assertFalse(has_future_trial("active", self.future_trial_end, now=self.now))

    def test_old_api_paid_out_of_band_invoice_is_not_recurring_payment(self):
        invoice = {
            "status": "paid",
            "amount_due": 5000,
            "amount_paid": 5000,
            "payment_intent": None,
            "paid_out_of_band": True,
        }
        self.assertTrue(is_paid_out_of_band_invoice(invoice, 5000, 5000))
        self.assertEqual(
            successful_invoice_action(
                amount_paid=5000,
                billing_reason="subscription_cycle",
                subscription_status="active",
                trial_end=None,
                now=self.now,
                invoice=invoice,
                amount_due=5000,
            ),
            "process_out_of_band",
        )

    def test_new_api_payment_record_invoice_is_not_recurring_payment(self):
        invoice = {
            "status": "paid",
            "amount_due": 5000,
            "amount_paid": 5000,
            "payments": {
                "data": [
                    {
                        "status": "paid",
                        "amount_paid": 5000,
                        "payment": {
                            "type": "payment_record",
                            "payment_record": "inpayrec_123",
                        },
                    }
                ]
            },
        }
        self.assertEqual(
            successful_invoice_action(
                amount_paid=5000,
                billing_reason="subscription_cycle",
                subscription_status="active",
                trial_end=None,
                now=self.now,
                invoice=invoice,
                amount_due=5000,
            ),
            "process_out_of_band",
        )

    def test_customer_balance_without_out_of_band_marker_is_not_forced_to_out_of_band(self):
        invoice = {
            "status": "paid",
            "amount_due": 5000,
            "amount_paid": 5000,
            "payment_intent": None,
            "payments": {
                "data": [
                    {
                        "status": "paid",
                        "amount_paid": 5000,
                        "payment": {
                            "type": "customer_balance",
                        },
                    }
                ]
            },
        }
        self.assertFalse(is_paid_out_of_band_invoice(invoice, 5000, 5000))


if __name__ == "__main__":
    unittest.main()
