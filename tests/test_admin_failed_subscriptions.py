import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import admin_failed_subscriptions as queue


def row(*, status="retryable_failed", lease=None, reason="grace_period_expired",
        error="telegram_timeout", timestamps=None):
    now = datetime(2026, 9, 9, 12, 0)
    phases = timestamps or {}
    return (
        "fst-01234567890123456789-abcdef123456", 42, "member", "Member",
        status, reason, 3, now-timedelta(days=1), now-timedelta(minutes=1), lease,
        now-timedelta(days=2), "sub_secret_full_123456", "in_secret_full_654321",
        error, phases.get("stripe"), phases.get("collection"), phases.get("ban"),
        phases.get("removed"), phases.get("db"), phases.get("completed"), now, 4,
    )


class AdminFailedSubscriptionsTests(unittest.TestCase):
    def test_safe_projection_masks_identifiers_and_sanitizes_strings(self):
        result = queue._projection(row(reason="<script>unsafe</script>", error="boom: raw secret"), True)
        serialized = str(result)
        self.assertNotIn("sub_secret_full_123456", serialized)
        self.assertNotIn("in_secret_full_654321", serialized)
        self.assertNotIn("<script>", serialized)
        self.assertNotIn("boom: raw secret", serialized)
        self.assertEqual(result["reason_label"], "Другая причина")
        self.assertEqual(result["last_error_category"], "unknown_error")
        self.assertNotIn("owner_id", result)

    def test_retry_and_stale_classification(self):
        now=datetime(2026,9,9,12,0)
        active=queue._projection(row(status="processing",lease=now+timedelta(minutes=5)))
        stale=queue._projection(row(status="processing",lease=now-timedelta(seconds=1)))
        self.assertFalse(active["stale"]); self.assertFalse(active["retry_allowed"])
        self.assertTrue(stale["stale"]); self.assertTrue(stale["retry_allowed"])
        for status in ("completed","superseded","manual_review"):
            self.assertFalse(queue._projection(row(status=status))["retry_allowed"])
        self.assertTrue(queue._projection(row(status="retryable_failed"))["needs_attention"])

    def test_phase_comes_from_timestamps(self):
        self.assertEqual(queue._projection(row(status="processing"))["current_phase"],"waiting_for_stripe")
        self.assertEqual(queue._projection(row(status="processing",timestamps={"stripe":datetime.utcnow()}))["current_phase"],"waiting_for_invoice")
        self.assertEqual(queue._projection(row(status="processing",timestamps={"stripe":datetime.utcnow(),"collection":datetime.utcnow()}))["current_phase"],"waiting_for_telegram")

    def test_terminal_phase_requires_persisted_db_finalization(self):
        inconsistent = queue._projection(row(status="completed"), True)
        self.assertNotEqual(inconsistent["current_phase"], "completed")
        self.assertFalse(inconsistent["retry_allowed"])
        self.assertTrue(inconsistent["needs_attention"])

        completed = queue._projection(
            row(status="completed", timestamps={"db": datetime.utcnow()}), True
        )
        self.assertEqual(completed["current_phase"], "completed")
        self.assertFalse(completed["retry_allowed"])
        self.assertFalse(completed["needs_attention"])

        for status in ("manual_review", "superseded"):
            projected = queue._projection(row(status=status), True)
            self.assertEqual(projected["current_phase"], status)
            self.assertFalse(projected["retry_allowed"])

    def test_future_retryable_statuses_are_not_implicitly_allowed(self):
        with patch("admin_failed_subscriptions.RETRYABLE_STATUSES", queue.RETRYABLE_STATUSES | {"future_state"}):
            # A future lifecycle state must be consciously added to public status policy too.
            self.assertEqual(queue._projection(row(status="future_state"))["status"], "unknown")

    def test_filter_limit_and_cursor_validation(self):
        with self.assertRaisesRegex(queue.AdminFailedSubscriptionsError,"invalid_limit"):
            queue._limit(0)
        with self.assertRaisesRegex(queue.AdminFailedSubscriptionsError,"invalid_cursor"):
            queue._cursor_decode("not-a-cursor")
        encoded=queue._cursor_encode(datetime(2026,1,1),"fst-01234567890123456789-abcdef123456")
        self.assertEqual(queue._cursor_decode(encoded)[1],"fst-01234567890123456789-abcdef123456")


if __name__ == "__main__":
    unittest.main()
