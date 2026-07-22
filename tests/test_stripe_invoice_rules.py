import unittest
from datetime import datetime, timedelta
from pathlib import Path

from stripe_invoice_rules import (
    checkout_completion_action,
    claim_stripe_event,
    has_future_trial,
    invoice_payment_kind,
    is_paid_out_of_band_invoice,
    is_zero_subscription_update_invoice,
    merge_expiry_without_regression,
    redact_email,
    redact_identifier,
    redact_url,
    release_stripe_event_claim,
    should_send_rejoin_invite,
    should_ignore_payment_failed_for_active_trial,
    should_skip_invoice_notice_for_current_expiry,
    subscription_update_period,
    successful_invoice_action,
)


class FakeStripeEventCursor:
    def __init__(self, processed=None, stale=False):
        self.processed = processed
        self.stale = stale
        self.results = []
        self.queries = []

    def execute(self, query, params):
        self.queries.append((query, params))
        normalized = " ".join(query.split()).upper()
        if normalized.startswith("INSERT INTO STRIPE_EVENTS"):
            if self.processed is None:
                self.processed = False
                self.stale = False
                self.results.append((params[0],))
            elif self.processed is False and self.stale:
                self.stale = False
                self.results.append((params[0],))
            else:
                self.results.append(None)
        elif normalized.startswith("SELECT PROCESSED"):
            self.results.append((self.processed,))
        elif normalized.startswith("DELETE FROM STRIPE_EVENTS"):
            if self.processed is False:
                self.processed = None

    def fetchone(self):
        return self.results.pop(0)


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

    def test_subscription_create_is_initial_purchase(self):
        self.assertEqual(
            invoice_payment_kind("subscription_create", "process_payment"),
            "initial_subscription",
        )

    def test_subscription_cycle_is_recurring(self):
        self.assertEqual(
            invoice_payment_kind("subscription_cycle", "process_payment"),
            "recurring",
        )

    def test_subscription_create_is_not_recurring_notice(self):
        self.assertNotEqual(
            invoice_payment_kind("subscription_create", "process_payment"),
            "recurring",
        )

    def test_presynced_initial_subscription_still_sends_initial_notice(self):
        presynced_expiry = self.now + timedelta(days=30)
        self.assertEqual(
            invoice_payment_kind("subscription_create", "process_payment"),
            "initial_subscription",
        )
        self.assertFalse(
            should_skip_invoice_notice_for_current_expiry(
                "initial_subscription",
                presynced_expiry,
                presynced_expiry,
            )
        )

    def test_presynced_subscription_cycle_still_sends_recurring_notice(self):
        presynced_expiry = self.now + timedelta(days=30)
        self.assertEqual(
            invoice_payment_kind("subscription_cycle", "process_payment"),
            "recurring",
        )
        self.assertFalse(
            should_skip_invoice_notice_for_current_expiry(
                "recurring",
                presynced_expiry,
                presynced_expiry,
            )
        )

    def test_presynced_adjustment_without_payment_notice_can_skip(self):
        presynced_expiry = self.now + timedelta(days=30)
        self.assertTrue(
            should_skip_invoice_notice_for_current_expiry(
                "subscription_adjustment",
                presynced_expiry,
                presynced_expiry,
            )
        )

    def test_presynced_invoice_duplicate_event_is_blocked_by_claim(self):
        presynced_expiry = self.now + timedelta(days=30)
        cursor = FakeStripeEventCursor()
        self.assertEqual(claim_stripe_event(cursor, "evt_presynced_invoice"), "claimed")
        cursor.processed = True
        self.assertFalse(
            should_skip_invoice_notice_for_current_expiry(
                "initial_subscription",
                presynced_expiry,
                presynced_expiry,
            )
        )
        self.assertFalse(
            should_skip_invoice_notice_for_current_expiry(
                "recurring",
                presynced_expiry,
                presynced_expiry,
            )
        )
        self.assertEqual(
            claim_stripe_event(cursor, "evt_presynced_invoice"),
            "duplicate_processed",
        )

    def test_checkout_subscription_only_links_until_invoice(self):
        self.assertEqual(checkout_completion_action("subscription", "sub_live_123"), "link_only")

    def test_checkout_subscription_without_subscription_id_still_links_only(self):
        self.assertEqual(checkout_completion_action("subscription", None), "link_only")

    def test_checkout_payment_still_activates_trial_access(self):
        self.assertEqual(checkout_completion_action("payment", None), "activate_access")

    def test_empty_current_period_preserves_existing_expiry(self):
        existing = self.now + timedelta(days=10)
        self.assertEqual(merge_expiry_without_regression(existing, None), existing)

    def test_earlier_stripe_period_does_not_shorten_access(self):
        existing = self.now + timedelta(days=20)
        stripe_period = self.now + timedelta(days=5)
        self.assertEqual(merge_expiry_without_regression(existing, stripe_period), existing)

    def test_later_stripe_period_extends_access(self):
        existing = self.now + timedelta(days=5)
        stripe_period = self.now + timedelta(days=20)
        self.assertEqual(merge_expiry_without_regression(existing, stripe_period), stripe_period)

    def test_cancel_at_period_end_keeps_existing_expiry_without_period(self):
        existing = self.now + timedelta(days=20)
        period, source = subscription_update_period("active", None, None)
        self.assertIsNone(period)
        self.assertIsNone(source)
        self.assertEqual(merge_expiry_without_regression(existing, period), existing)

    def test_trialing_uses_trial_end_when_current_period_missing(self):
        period, source = subscription_update_period("trialing", None, self.future_trial_end)
        self.assertEqual(period, self.future_trial_end)
        self.assertEqual(source, "trial_end")

    def test_active_group_member_does_not_get_rejoin_link(self):
        expired = self.now - timedelta(days=1)
        self.assertFalse(should_send_rejoin_invite(expired, self.now, "member"))
        self.assertFalse(should_send_rejoin_invite(expired, self.now, "administrator"))
        self.assertFalse(should_send_rejoin_invite(expired, self.now, "creator"))
        self.assertFalse(should_send_rejoin_invite(expired, self.now, "restricted"))

    def test_left_or_kicked_member_gets_rejoin_link(self):
        expired = self.now - timedelta(days=1)
        self.assertTrue(should_send_rejoin_invite(expired, self.now, "left"))
        self.assertTrue(should_send_rejoin_invite(expired, self.now, "kicked"))

    def test_active_expiry_left_member_still_gets_rejoin_link(self):
        active = self.now + timedelta(days=1)
        self.assertTrue(should_send_rejoin_invite(active, self.now, "left"))

    def test_membership_check_error_does_not_send_blind_rejoin_link(self):
        expired = self.now - timedelta(days=1)
        self.assertFalse(should_send_rejoin_invite(expired, self.now, None))

    def test_sensitive_values_are_redacted(self):
        self.assertEqual(redact_email("client@example.com"), "c***@example.com")
        self.assertEqual(redact_identifier("cus_1234567890abcdef"), "cus_***abcdef")
        self.assertEqual(redact_identifier("cs_live_abcdefghijklmnopqrstuvwxyz"), "cs_***uvwxyz")
        self.assertEqual(
            redact_url("https://invoice.stripe.com/i/acct_123/test_secret?foo=bar"),
            "https://invoice.stripe.com/***",
        )

    def test_atomic_event_claim_allows_only_first_processor(self):
        cursor = FakeStripeEventCursor()
        self.assertEqual(claim_stripe_event(cursor, "evt_123"), "claimed")
        self.assertEqual(claim_stripe_event(cursor, "evt_123"), "duplicate_processing")

    def test_fresh_processing_claim_is_not_reclaimed(self):
        cursor = FakeStripeEventCursor(processed=False, stale=False)
        self.assertEqual(claim_stripe_event(cursor, "evt_123"), "duplicate_processing")

    def test_stale_processing_claim_can_be_reclaimed(self):
        cursor = FakeStripeEventCursor(processed=False, stale=True)
        self.assertEqual(claim_stripe_event(cursor, "evt_123"), "claimed")
        self.assertFalse(cursor.stale)

    def test_processed_event_claim_is_duplicate_processed(self):
        cursor = FakeStripeEventCursor(processed=True)
        self.assertEqual(claim_stripe_event(cursor, "evt_123"), "duplicate_processed")

    def test_failed_event_claim_can_be_released_for_retry(self):
        cursor = FakeStripeEventCursor()
        self.assertEqual(claim_stripe_event(cursor, "evt_123"), "claimed")
        release_stripe_event_claim(cursor, "evt_123")
        self.assertIsNone(cursor.processed)
        self.assertEqual(claim_stripe_event(cursor, "evt_123"), "claimed")

    def test_subscription_updated_active_does_not_set_paid_true(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        start = source.index("elif event['type'] == 'customer.subscription.updated'")
        end = source.index("# ---------- 5. СЕССИЯ ОПЛАТЫ", start)
        block = source[start:end]
        active_start = block.index('elif status in ("active", "trialing")')
        active_end = block.index("else:", active_start)
        active_block = block[active_start:active_end]
        self.assertNotIn("SET paid = TRUE", active_block)

    def test_webhook_outer_exception_releases_claim(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        self.assertIn("try:\n\n        def stripe_value", source)
        self.assertIn("except Exception as e:\n        await release_event_processing(event_id)", source)
        self.assertNotIn("claim_released", source)
        self.assertIn("await release_event_processing(event_id)", source)
        self.assertIn("STRIPE_WEBHOOK_UNHANDLED_EXCEPTION", source)

    def test_presynced_invoice_path_uses_payment_kind_before_expiry_skip(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        self.assertIn(
            "should_skip_invoice_notice_for_current_expiry(payment_kind, old_expiry, new_expiry)",
            source,
        )
        self.assertNotIn("if old_expiry and old_expiry >= new_expiry:", source)

    def test_rejoin_invite_callers_have_no_outer_expiry_gate(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        self.assertNotIn("if needs_link and await payment_needs_rejoin_invite", source)
        self.assertNotIn("if needs_rejoin_invite and await payment_needs_rejoin_invite", source)
        self.assertNotIn("needs_rejoin_invite =", source)
        self.assertIn("should_send_invoice_rejoin_invite = await payment_needs_rejoin_invite", source)


if __name__ == "__main__":
    unittest.main()
