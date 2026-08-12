import asyncio
import os
import unittest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch


TEST_ENV = {
    "BOT_TOKEN": "123456:TEST_TOKEN",
    "DATABASE_URL": "postgresql://user:pass@localhost/test",
    "GROUP_ID": "-100123",
    "ADMIN_IDS": "1,2",
    "STRIPE_API_KEY": "sk_test_dummy",
    "STRIPE_WEBHOOK_SECRET": "test_webhook_secret",
    "WEBHOOK_SECRET": "telegram_secret",
    "YOUR_DOMAIN": "https://club.example",
    "PRICE_TRIAL": "price_trial",
    "PRICE_1M": "price_1m",
    "PRICE_6M": "price_6m",
    "PRICE_12M": "price_12m",
    "GIFT_TOKEN_SECRET": "unit-test-gift-token-secret-32chars",
    "GIFT_PRICE_1M": "price_gift_1m",
    "GIFT_PRICE_6M": "price_gift_6m",
    "GIFT_PRICE_12M": "price_gift_12m",
}
os.environ.update(TEST_ENV)

OUTBOX_IMPORT_LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(OUTBOX_IMPORT_LOOP)
import main
import scheduled_jobs


class EscalationCursor:
    def __init__(self, first_seen=None):
        self.first_seen = first_seen
        self.last_query = ""
        self.last_params = ()

    def execute(self, query, params=()):
        self.last_query = query
        self.last_params = params
        if "INSERT INTO admin_alerts" in query and self.first_seen is None:
            self.first_seen = params[2]

    def fetchone(self):
        if "SELECT MIN(created_at)" in self.last_query:
            return (self.first_seen,)
        return None


class FailureConnection:
    def __init__(self):
        self.cursor_obj = Mock()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class OutboxAlertPolicyTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def tearDownClass(cls):
        if not OUTBOX_IMPORT_LOOP.is_closed():
            OUTBOX_IMPORT_LOOP.close()
        asyncio.set_event_loop(None)

    def test_retryable_stage_policy_first_second_one_hour_six_hours_and_independent_keys(self):
        now = datetime(2026, 8, 12, 12, 0)
        first = main.claim_outbox_retry_escalation(
            EscalationCursor(), "stripe:secret-event:first", "first_purchase_recovery_reminder", 1, now=now
        )
        second = main.claim_outbox_retry_escalation(
            EscalationCursor(now - timedelta(minutes=20)),
            "stripe:secret-event:first", "first_purchase_recovery_reminder", 2, now=now
        )
        one_hour = main.claim_outbox_retry_escalation(
            EscalationCursor(now - timedelta(hours=1)),
            "stripe:secret-event:first", "first_purchase_recovery_reminder", 3, now=now
        )
        six_hours = main.claim_outbox_retry_escalation(
            EscalationCursor(now - timedelta(hours=6)),
            "stripe:secret-event:first", "first_purchase_recovery_reminder", 8, now=now
        )
        independent = main.claim_outbox_retry_escalation(
            EscalationCursor(now - timedelta(hours=1)),
            "stripe:another-secret:second", "first_purchase_recovery_reminder", 3, now=now
        )

        self.assertIsNone(first["stage"])
        self.assertIsNone(second["stage"])
        self.assertEqual(one_hour["stage"], "age_1h")
        self.assertEqual(six_hours["stage"], "age_6h")
        self.assertNotEqual(one_hour["delivery_hash"], independent["delivery_hash"])

    def test_critical_delivery_escalates_at_attempt_three_but_normal_reminder_does_not(self):
        now = datetime(2026, 8, 12, 12, 0)
        critical = main.claim_outbox_retry_escalation(
            EscalationCursor(now - timedelta(minutes=20)),
            "access-restore:secret", main.ACCESS_RESTORE_DELIVERY_TYPE, 3, now=now
        )
        normal = main.claim_outbox_retry_escalation(
            EscalationCursor(now - timedelta(minutes=20)),
            "first_purchase_recovery:hash", "first_purchase_recovery_reminder", 3, now=now
        )
        self.assertEqual(critical["stage"], "critical_attempt_3")
        self.assertTrue(critical["critical"])
        self.assertIsNone(normal["stage"])
        self.assertFalse(normal["critical"])

    async def test_retryable_log_only_and_staged_alert_use_safe_deterministic_key(self):
        notify = AsyncMock()
        log_only = {
            "stage": None, "retry_age": timedelta(minutes=10), "critical": False,
            "delivery_hash": "safehash1",
        }
        warning = {
            "stage": "age_1h", "retry_age": timedelta(minutes=75), "critical": False,
            "delivery_hash": "safehash1",
        }
        with patch.object(main, "get_db_conn", return_value=FailureConnection()), \
             patch.object(main, "claim_outbox_retry_escalation", side_effect=[log_only, warning, warning]), \
             patch.object(main, "notify_admins", notify):
            await main.notify_retryable_outbox_failure("raw-secret-key", "free_lesson", 1, RuntimeError("private"), {})
            await main.notify_retryable_outbox_failure("raw-secret-key", "free_lesson", 3, RuntimeError("private"), {})
            await main.notify_retryable_outbox_failure("raw-secret-key", "free_lesson", 4, RuntimeError("private"), {})

        self.assertEqual(notify.await_count, 2)
        for call in notify.await_args_list:
            self.assertEqual(call.kwargs["alert_key"], "outbox-retry:safehash1:age_1h")
            self.assertTrue(call.kwargs["dedupe_forever"])
            self.assertNotIn("raw-secret-key", call.args[0])
            self.assertNotIn("private", call.args[0])

    async def test_permanent_critical_and_blocked_alert_once_key_while_normal_is_log_only(self):
        notify = AsyncMock()
        with patch.object(main, "notify_admins", notify):
            await main.notify_permanent_outbox_failure(
                "access-restore:raw-secret", main.ACCESS_RESTORE_DELIVERY_TYPE, 1,
                RuntimeError("private body"), blocked=True,
            )
            await main.notify_permanent_outbox_failure(
                "free_lesson_followup:123", "free_lesson_followup", 10,
                RuntimeError("private body"), blocked=False,
            )
        notify.assert_awaited_once()
        call = notify.await_args
        self.assertTrue(call.kwargs["alert_key"].startswith("outbox-permanent:"))
        self.assertEqual(call.kwargs["severity"], "CRITICAL")
        self.assertTrue(call.kwargs["dedupe_forever"])
        self.assertNotIn("raw-secret", call.args[0])
        self.assertNotIn("private body", call.args[0])

    async def test_runtime_retry_callback_runs_after_unchanged_failure_state(self):
        connection = FailureConnection()
        retry_callback = AsyncMock()
        decision = {
            "blocked": False,
            "retryable": True,
            "permanently_failed": False,
            "retry_delay_minutes": 10,
        }
        error = RuntimeError("network")

        async def fail_send():
            raise error

        with patch.object(scheduled_jobs, "mark_delivery_failed", return_value="failed") as mark_failed:
            result = await scheduled_jobs.process_already_claimed_delivery(
                lambda: connection,
                "delivery-key",
                777,
                "first_purchase_recovery_reminder",
                fail_send,
                4,
                attempt_count=2,
                classify_error_func=lambda *_args, **_kwargs: decision,
                retryable_error_callback=retry_callback,
                retryable_state_func=lambda *_args: {"stage": None},
            )

        self.assertEqual(result, "failed")
        mark_failed.assert_called_once_with(
            connection.cursor_obj, "delivery-key", 4, error,
            retry_delay_minutes=10, permanently_failed=False,
        )
        self.assertEqual(connection.commits, 1)
        retry_callback.assert_awaited_once_with(error, decision, 2, {"stage": None})


if __name__ == "__main__":
    unittest.main()
