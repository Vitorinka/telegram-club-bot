import asyncio
import os
import time
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler

os.environ.update({
    "BOT_TOKEN": "123456:TEST_TOKEN_FOR_SCHEDULER_ONLY",
    "DATABASE_URL": "postgresql://user:pass@localhost/test",
    "GROUP_ID": "-100123",
    "ADMIN_IDS": "1,2",
    "STRIPE_API_KEY": "sk_test_dummy",
    "STRIPE_WEBHOOK_SECRET": "scheduler_test_webhook_secret",
    "WEBHOOK_SECRET": "telegram_secret",
    "YOUR_DOMAIN": "https://club.example",
    "PRICE_TRIAL": "price_trial",
    "PRICE_1M": "price_1m",
    "PRICE_6M": "price_6m",
    "PRICE_12M": "price_12m",
})

SCHEDULER_IMPORT_LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(SCHEDULER_IMPORT_LOOP)
import main


@contextmanager
def process_timezone(name):
    previous = os.environ.get("TZ")
    os.environ["TZ"] = name
    if hasattr(time, "tzset"):
        time.tzset()
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        if hasattr(time, "tzset"):
            time.tzset()


class SchedulerTimezoneTests(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        if not SCHEDULER_IMPORT_LOOP.is_closed():
            SCHEDULER_IMPORT_LOOP.close()
        asyncio.set_event_loop(None)

    def setUp(self):
        self.created_schedulers = []

    def tearDown(self):
        self.assertTrue(all(not scheduler.running for scheduler in self.created_schedulers))

    def registered_scheduler(self):
        scheduler = AsyncIOScheduler(timezone=main.SCHEDULER_TZ)
        self.created_schedulers.append(scheduler)
        with patch.object(main, "scheduler", scheduler), \
             patch.object(main, "SCHEDULER_JOBS_REGISTERED", False):
            main.register_scheduler_jobs_once()
        return scheduler

    def test_scheduler_and_all_cron_timezones_are_explicit_and_job_count_is_ten(self):
        scheduler = self.registered_scheduler()
        jobs = scheduler.get_jobs()
        self.assertEqual(len(jobs), 10)
        self.assertEqual(str(scheduler.timezone), "UTC")
        by_func = {job.func: job for job in jobs}
        self.assertEqual(str(by_func[main.send_weekly_admin_report].trigger.timezone), "Europe/Moscow")
        for func, job in by_func.items():
            if func is not main.send_weekly_admin_report:
                self.assertEqual(str(job.trigger.timezone), "UTC")

    def test_weekly_report_next_run_is_monday_ten_moscow_from_fixed_utc(self):
        scheduler = self.registered_scheduler()
        weekly = next(job for job in scheduler.get_jobs() if job.func is main.send_weekly_admin_report)
        fixed_utc = datetime(2026, 8, 9, 8, 0, tzinfo=timezone.utc)
        next_run = weekly.trigger.get_next_fire_time(None, fixed_utc)
        self.assertEqual(next_run.astimezone(timezone.utc), datetime(2026, 8, 10, 7, 0, tzinfo=timezone.utc))
        self.assertEqual(
            next_run.astimezone(ZoneInfo("Europe/Moscow")),
            datetime(2026, 8, 10, 10, 0, tzinfo=ZoneInfo("Europe/Moscow")),
        )

    def test_subscription_cron_preserves_ten_utc_and_process_job_preserves_five_minute_cadence(self):
        scheduler = self.registered_scheduler()
        by_func = {job.func: job for job in scheduler.get_jobs()}
        fixed_utc = datetime(2026, 8, 9, 8, 2, tzinfo=timezone.utc)
        subscription = by_func[main.scheduled_check_subscriptions_and_reminders]
        process_outbox = by_func[main.scheduled_process_message_deliveries]
        self.assertEqual(
            subscription.trigger.get_next_fire_time(None, fixed_utc),
            datetime(2026, 8, 9, 10, 0, tzinfo=ZoneInfo("UTC")),
        )
        self.assertEqual(
            process_outbox.trigger.get_next_fire_time(None, fixed_utc),
            datetime(2026, 8, 9, 8, 5, tzinfo=ZoneInfo("UTC")),
        )

    def test_next_runs_do_not_depend_on_process_timezone(self):
        fixed_utc = datetime(2026, 8, 9, 8, 2, tzinfo=timezone.utc)
        observed = []
        for name in ("UTC", "Europe/Amsterdam", "America/New_York"):
            with process_timezone(name):
                scheduler = self.registered_scheduler()
                by_func = {job.func: job for job in scheduler.get_jobs()}
                observed.append((
                    by_func[main.scheduled_check_subscriptions_and_reminders].trigger.get_next_fire_time(None, fixed_utc),
                    by_func[main.send_weekly_admin_report].trigger.get_next_fire_time(None, fixed_utc),
                ))
        self.assertEqual(observed[0], observed[1])
        self.assertEqual(observed[1], observed[2])

    def test_job_cadence_configuration_includes_hourly_failed_subscription_termination(self):
        scheduler = self.registered_scheduler()
        by_func = {job.func: str(job.trigger) for job in scheduler.get_jobs()}
        expected_fragments = {
            main.scheduled_check_subscriptions_and_reminders: "hour='10', minute='0'",
            main.scheduled_process_expired_access: "minute='5'",
            main.scheduled_process_expired_failed_subscription_grace: "minute='5'",
            main.scheduled_check_auto_free_lessons: "minute='15'",
            main.scheduled_check_free_lesson_followups: "minute='30'",
            main.scheduled_send_db_backup: "day_of_week='mon', hour='3', minute='0'",
            main.scheduled_process_message_deliveries: "minute='*/5'",
            main.scheduled_enqueue_first_purchase_recovery_reminders: "minute='45'",
            main.scheduled_cleanup_stale_postgres_fsm_storage: "hour='4', minute='10'",
            main.send_weekly_admin_report: "day_of_week='mon', hour='10', minute='0'",
        }
        self.assertEqual(set(by_func), set(expected_fragments))
        for func, fragment in expected_fragments.items():
            self.assertIn(fragment, by_func[func])

    def test_manual_event_loop_cleanup_closes_loop_and_restores_global_state(self):
        previous_policy = asyncio.get_event_loop_policy()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            self.assertIs(asyncio.get_event_loop(), loop)
            self.assertFalse(loop.is_closed())
        finally:
            loop.close()
            asyncio.set_event_loop_policy(previous_policy)
            asyncio.set_event_loop(None)
        self.assertTrue(loop.is_closed())
        with self.assertRaises(RuntimeError):
            asyncio.get_event_loop()


if __name__ == "__main__":
    unittest.main()
