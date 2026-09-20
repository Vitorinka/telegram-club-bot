import unittest
from datetime import datetime

from admin_analytics import (
    AdminAnalyticsQueryError,
    analytics_period_bounds,
    analytics_projection,
)


class AdminAnalyticsTests(unittest.TestCase):
    def test_period_bounds_are_bounded_and_include_comparison(self):
        now = datetime(2026, 9, 20, 12, 0, 0)
        start, end, comparison_start, comparison_end = analytics_period_bounds(30, now)
        self.assertEqual(end, now)
        self.assertEqual((end - start).days, 30)
        self.assertEqual((comparison_end - comparison_start).days, 30)
        self.assertEqual(comparison_end, start)
        for invalid in (None, "", 0, 31, 366, "week"):
            with self.subTest(invalid=invalid), self.assertRaises(AdminAnalyticsQueryError):
                analytics_period_bounds(invalid, now)

    def test_projection_uses_real_metrics_and_marks_untracked_metrics(self):
        start, end, _, _ = analytics_period_bounds(7, datetime(2026, 9, 20))
        result = analytics_projection(
            7,
            start,
            end,
            {"active_paid_now": 12, "new_registrations": 3,
             "successful_payments": 4, "revenue_by_currency": {"rub": 150000}},
            {"active_paid_now": 10, "new_registrations": 1},
        )
        self.assertEqual(result["metrics"]["active_paid_now"], 12)
        self.assertEqual(result["comparison"]["new_registrations"], 1)
        self.assertEqual(result["revenue_by_currency"], {"rub": 150000})
        self.assertEqual(result["metrics"]["failed_payments"], 0)
        self.assertTrue(all(value is False for value in result["tracking"].values()))


if __name__ == "__main__":
    unittest.main()
