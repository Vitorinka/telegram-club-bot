import unittest
from datetime import datetime, timedelta

from admin_subscriptions import (
    AdminSubscriptionsQueryError,
    _projection,
    _subscription_state,
    decode_subscriptions_cursor,
    encode_subscriptions_cursor,
    normalize_subscriptions_query,
    parse_subscriptions_limit,
)


class AdminSubscriptionsTests(unittest.TestCase):
    def test_limit_query_and_cursor_validation(self):
        self.assertEqual(parse_subscriptions_limit(None), 25)
        self.assertEqual(parse_subscriptions_limit("50"), 50)
        for invalid in ("no", "0", "51", -1):
            with self.subTest(invalid=invalid), self.assertRaises(AdminSubscriptionsQueryError):
                parse_subscriptions_limit(invalid)
        self.assertEqual(normalize_subscriptions_query("  member  "), "member")
        with self.assertRaises(AdminSubscriptionsQueryError):
            normalize_subscriptions_query("x" * 65)
        cursor = encode_subscriptions_cursor(987654)
        self.assertNotIn("987654", cursor)
        self.assertEqual(decode_subscriptions_cursor(cursor), 987654)
        with self.assertRaises(AdminSubscriptionsQueryError):
            decode_subscriptions_cursor("bad")

    def test_subscription_state_priority_keeps_active_grace_above_expired_access(self):
        now = datetime(2026, 8, 22, 12, 0)
        self.assertEqual(
            _subscription_state(True, now - timedelta(days=1), True, True, now + timedelta(hours=1), now),
            "active_grace",
        )
        self.assertEqual(
            _subscription_state(True, now - timedelta(days=1), True, True, now - timedelta(hours=1), now),
            "expired_grace",
        )
        self.assertEqual(
            _subscription_state(True, now + timedelta(days=1), True, False, None, now),
            "active_renewing",
        )
        self.assertEqual(
            _subscription_state(True, now + timedelta(days=1), False, False, None, now),
            "active_non_renewing",
        )

    def test_list_projection_contains_no_raw_stripe_identifier(self):
        now = datetime(2026, 8, 22, 12, 0)
        result = _projection((
            100, "member", "Member", True, now + timedelta(days=1), True,
            False, None, None, False, True, True, False, None, now,
        ))
        self.assertTrue(result["stripe_linked"])
        self.assertNotIn("stripe_customer_id", result)
        self.assertNotIn("stripe_subscription_id", result)
        self.assertNotIn("cus_", str(result))
        self.assertEqual(result["subscription_state"], "active_renewing")


if __name__ == "__main__":
    unittest.main()
