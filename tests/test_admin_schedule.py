import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from admin_schedule import (
    AdminScheduleQueryError,
    current_moscow_month,
    decode_schedule_cursor,
    encode_schedule_cursor,
    parse_schedule_limit,
    schedule_month_label,
    schedule_window,
    validate_schedule_status,
)


class AdminScheduleTests(unittest.TestCase):
    def test_limit_status_and_cursor_validation(self):
        self.assertEqual(parse_schedule_limit(None), 25)
        self.assertEqual(parse_schedule_limit("50"), 50)
        for invalid in (0, 51, "bad"):
            with self.subTest(invalid=invalid), self.assertRaises(AdminScheduleQueryError):
                parse_schedule_limit(invalid)
        self.assertEqual(validate_schedule_status("upcoming"), "upcoming")
        with self.assertRaises(AdminScheduleQueryError):
            validate_schedule_status("active")
        cursor = encode_schedule_cursor("2026-08")
        self.assertEqual(decode_schedule_cursor(cursor), "2026-08")
        with self.assertRaises(AdminScheduleQueryError):
            decode_schedule_cursor("bad")

    def test_date_window_is_bounded_and_validated(self):
        now = datetime(2026, 8, 22, 12, tzinfo=ZoneInfo("Europe/Moscow"))
        start, end = schedule_window(None, None, now=now)
        self.assertEqual(start.isoformat(), "2026-08-15")
        self.assertEqual(end.isoformat(), "2026-10-21")
        for values in (("bad", None), (None, "bad"), ("2026-09-01", "2026-08-01")):
            with self.subTest(values=values), self.assertRaises(AdminScheduleQueryError):
                schedule_window(*values, now=now)
        with self.assertRaises(AdminScheduleQueryError):
            schedule_window("2025-01-01", "2027-01-02", now=now)

    def test_timezone_matches_existing_moscow_month_behavior(self):
        utc = ZoneInfo("UTC")
        crossing_midnight = datetime(2026, 8, 31, 21, 30, tzinfo=utc)
        self.assertEqual(current_moscow_month(crossing_midnight), "2026-09")
        self.assertEqual(schedule_month_label("2026-09"), "Сентябрь 2026")


if __name__ == "__main__":
    unittest.main()
