import unittest
from datetime import datetime, timedelta
from pathlib import Path

from access_removal import claim_access_removal, mark_access_removal_failed, mark_access_removal_removed
from checkout_safety import (
    normalize_stripe_identifier,
    should_block_paid_checkout_for_manual_review,
    stripe_identity_conflict_queries,
    stripe_identity_normalization_queries,
)
from notification_outbox import claim_notification, mark_notification_sent, notification_key
from stripe_invoice_rules import claim_stripe_event, mark_stripe_event_failed, successful_invoice_action


ROOT = Path(__file__).resolve().parents[1]
MAIN_SOURCE = (ROOT / "main.py").read_text()


class FakeCursor:
    def __init__(self, fetches=None):
        self.fetches = list(fetches or [])
        self.queries = []
        self.last_status = None
        self.notification_status = None
        self.removal_status = None
        self.removal_attempts = 0
        self.stripe_event = None

    def execute(self, query, params=()):
        self.queries.append((query, params))
        normalized = " ".join(query.split()).lower()
        if "insert into access_removal_jobs" in normalized:
            if self.removal_status in (None, "pending", "failed"):
                self.removal_status = "processing"
                self.removal_attempts += 1
                self.fetches.append((params[0], "processing", self.removal_attempts))
            else:
                self.fetches.append(None)
        elif "select status, attempt_count from access_removal_jobs" in normalized:
            self.fetches.append((self.removal_status, self.removal_attempts))
        elif "set status = 'removed'" in normalized:
            self.removal_status = "removed"
        elif "update access_removal_jobs" in normalized and "last_error" in normalized:
            self.removal_status = params[0]
        elif "insert into notification_outbox" in normalized:
            if self.notification_status in (None, "pending", "failed"):
                self.notification_status = "sending"
                self.fetches.append((params[0],))
            else:
                self.fetches.append(None)
        elif "select status from notification_outbox" in normalized:
            self.fetches.append((self.notification_status,))
        elif "set status = 'sent'" in normalized:
            self.notification_status = "sent"
        elif "insert into stripe_events" in normalized:
            if self.stripe_event is None:
                self.stripe_event = {"processed": False, "dead_letter": False, "attempt_count": 1}
                self.fetches.append((params[0],))
            elif not self.stripe_event["processed"] and not self.stripe_event["dead_letter"] and self.stripe_event["attempt_count"] < params[-2]:
                self.stripe_event["attempt_count"] += 1
                self.fetches.append((params[0],))
            else:
                self.fetches.append(None)
        elif "select processed, dead_letter, attempt_count from stripe_events" in normalized:
            event = self.stripe_event or {"processed": False, "dead_letter": False, "attempt_count": 0}
            self.fetches.append((event["processed"], event["dead_letter"], event["attempt_count"]))
        elif "update stripe_events" in normalized and "dead_letter" in normalized:
            if self.stripe_event:
                self.stripe_event["dead_letter"] = self.stripe_event["attempt_count"] >= params[1]

    def fetchone(self):
        if self.fetches:
            return self.fetches.pop(0)
        return None


class BotSafetyV8Tests(unittest.TestCase):
    def test_zero_subscription_update_invoice_is_behaviorally_ignored_in_webhook_path(self):
        self.assertEqual(
            successful_invoice_action(
                amount_paid=0,
                billing_reason="subscription_update",
                subscription_status="active",
                trial_end=None,
                now=datetime(2026, 7, 24, 12, 0),
            ),
            "ignore_zero",
        )
        zero_branch = MAIN_SOURCE[MAIN_SOURCE.index('if invoice_action == "ignore_zero"'):MAIN_SOURCE.index('if invoice_action == "sync_trial"')]
        self.assertIn("await mark_event_processed(event_id)", zero_branch)
        self.assertNotIn("UPDATE users", zero_branch)
        self.assertNotIn("insert_payment_event", zero_branch)
        self.assertNotIn("send_idempotent_user_message", zero_branch)

    def test_sync_trial_requires_future_trialing_status(self):
        now = datetime(2026, 7, 24, 12, 0)
        future = int((now + timedelta(days=7)).timestamp())
        past = int((now - timedelta(days=1)).timestamp())
        self.assertEqual(successful_invoice_action(0, "subscription_update", "trialing", future, now=now), "sync_trial")
        self.assertEqual(successful_invoice_action(0, "subscription_update", "trialing", past, now=now), "ignore_zero")
        self.assertEqual(successful_invoice_action(0, "subscription_update", "active", future, now=now), "ignore_zero")

    def test_stripe_identity_cleanup_queries_and_normalizer(self):
        for value in (None, "", "  ", "NULL", "None", "нет"):
            self.assertIsNone(normalize_stripe_identifier(value))
        self.assertEqual(normalize_stripe_identifier("sub_123"), "sub_123")
        self.assertIn("users_subscription", dict(stripe_identity_normalization_queries()))
        for _, query in stripe_identity_conflict_queries():
            lowered = query.lower()
            self.assertIn("btrim", lowered)
            self.assertIn("not in ('null', 'none', 'нет')", lowered)

    def test_paid_checkout_fail_closed_for_missing_ids_with_history(self):
        cur = FakeCursor(fetches=[(True, False, False, False)])
        decision = should_block_paid_checkout_for_manual_review(
            cur,
            123,
            "subscription",
            stripe_customer_id=None,
            stripe_subscription_id=None,
            first_payment_done=False,
            paid=False,
            expiry_date=None,
        )
        self.assertTrue(decision["block"])
        self.assertIn("successful_payment_events", decision["reasons"])

    def test_paid_checkout_allows_brand_new_user_without_history(self):
        cur = FakeCursor(fetches=[(False, False, False, False)])
        decision = should_block_paid_checkout_for_manual_review(cur, 123, "subscription")
        self.assertFalse(decision["block"])

    def test_paid_checkout_allows_existing_valid_stripe_id_guard_to_handle(self):
        cur = FakeCursor()
        decision = should_block_paid_checkout_for_manual_review(cur, 123, "subscription", stripe_subscription_id="sub_123")
        self.assertFalse(decision["block"])
        self.assertEqual(cur.queries, [])

    def test_access_removal_state_machine_claim_removed_and_failed(self):
        cur = FakeCursor()
        claim = claim_access_removal(cur, 123, now=datetime(2026, 7, 24, 12, 0))
        self.assertEqual(claim["status"], "claimed")
        mark_access_removal_removed(cur, claim["job_key"])
        duplicate = claim_access_removal(cur, 123, now=datetime(2026, 7, 24, 12, 1))
        self.assertEqual(duplicate["status"], "duplicate_removed")

        failed_cur = FakeCursor()
        failed_claim = claim_access_removal(failed_cur, 456)
        mark_access_removal_failed(failed_cur, failed_claim["job_key"], "not enough rights", manual_review=True)
        self.assertEqual(failed_cur.removal_status, "manual_review")

    def test_notification_outbox_prevents_duplicate_send(self):
        cur = FakeCursor()
        key = notification_key("stripe_recurring_success", 123, "evt_1")
        self.assertEqual(claim_notification(cur, key, 123, "stripe_recurring_success", "evt_1"), "claimed")
        mark_notification_sent(cur, key)
        self.assertEqual(claim_notification(cur, key, 123, "stripe_recurring_success", "evt_1"), "already_sent")

    def test_stale_stripe_event_attempts_dead_letter(self):
        cur = FakeCursor()
        self.assertEqual(claim_stripe_event(cur, "evt_1", lease_seconds=0, max_attempts=2), "claimed")
        self.assertEqual(claim_stripe_event(cur, "evt_1", lease_seconds=0, max_attempts=2), "claimed")
        mark_stripe_event_failed(cur, "evt_1", "boom", max_attempts=2)
        self.assertEqual(claim_stripe_event(cur, "evt_1", lease_seconds=0, max_attempts=2), "dead_letter")

    def test_refund_dispute_events_are_manual_review_only(self):
        manual_start = MAIN_SOURCE.index("REFUND / DISPUTE / CHARGEBACK")
        manual_block = MAIN_SOURCE[manual_start:MAIN_SOURCE.index("# ---------- 5. СЕССИЯ", manual_start)]
        for event_type in ("charge.refunded", "refund.created", "charge.dispute.created", "charge.dispute.closed", "payment_intent.payment_failed"):
            self.assertIn(event_type, manual_block)
        self.assertIn("save_stripe_manual_review_event", manual_block)
        self.assertNotIn("kick_chat_member", manual_block)
        self.assertNotIn("paid = FALSE", manual_block)

    def test_production_dangerous_commands_require_confirmation(self):
        for needle in (
            '"command": "test_grace"',
            '"command": "test_expiry"',
            '"command": "test_backup"',
            '"command": "send_user"',
            '"command": "unblock_user"',
            '"command": "unban_user"',
        ):
            self.assertIn(needle, MAIN_SOURCE)
        self.assertIn("is_production_environment()", MAIN_SOURCE)
        self.assertIn('"dangerous_admin_command"', MAIN_SOURCE)

    def test_stats_text_names_match_sql_semantics(self):
        stats_block = MAIN_SOURCE[MAIN_SOURCE.index("async def stats_command"):MAIN_SOURCE.index("@dp.message_handler(commands=['weekly_report']", MAIN_SOURCE.index("async def stats_command"))]
        self.assertIn("paid=TRUE:", stats_block)
        self.assertIn("Активных по expiry_date > NOW()", stats_block)
        self.assertIn("auto_renew=TRUE без stripe_subscription_id", stats_block)
        self.assertIn("Неподтверждённые Telegram removal", stats_block)
        self.assertIn("count_unconfirmed_removals_query()", stats_block)


if __name__ == "__main__":
    unittest.main()

