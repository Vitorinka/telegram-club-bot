import unittest
from datetime import datetime

from admin_gifts import (
    AdminGiftsQueryError,
    GIFT_DURATIONS,
    GIFT_STATUSES,
    decode_gifts_cursor,
    encode_gifts_cursor,
    normalize_gifts_query,
    parse_gifts_limit,
    validate_gift_duration,
    validate_gift_reference,
    validate_gift_status,
)


class AdminGiftsTests(unittest.TestCase):
    def test_limit_status_and_duration_use_closed_schema_values(self):
        self.assertEqual(parse_gifts_limit(None), 25)
        self.assertEqual(parse_gifts_limit("50"), 50)
        for invalid in (0, 51, "bad"):
            with self.subTest(invalid=invalid), self.assertRaises(AdminGiftsQueryError):
                parse_gifts_limit(invalid)
        self.assertEqual(validate_gift_status("redeemed"), "redeemed")
        self.assertEqual(validate_gift_duration("gift_6m"), "gift_6m")
        self.assertIn("review_required", GIFT_STATUSES)
        self.assertEqual(
            GIFT_DURATIONS, frozenset({"all", "gift_1m", "gift_6m", "gift_12m"})
        )
        with self.assertRaises(AdminGiftsQueryError):
            validate_gift_status("expired")
        with self.assertRaises(AdminGiftsQueryError):
            validate_gift_duration("6m")

    def test_query_reference_and_cursor_are_bounded(self):
        self.assertEqual(normalize_gifts_query("  Анастасия  "), "Анастасия")
        with self.assertRaises(AdminGiftsQueryError):
            normalize_gifts_query("x" * 65)
        reference = validate_gift_reference("GIFT-ABCDEF0123456789")
        created_at = datetime(2026, 8, 22, 12, 30)
        cursor = encode_gifts_cursor(created_at, reference)
        self.assertEqual(decode_gifts_cursor(cursor), (created_at, reference))
        for invalid in ("gift-ABCDEF0123456789", "GIFT-secret", "bad"):
            with self.subTest(invalid=invalid), self.assertRaises(AdminGiftsQueryError):
                validate_gift_reference(invalid)
        with self.assertRaises(AdminGiftsQueryError):
            decode_gifts_cursor("not-a-cursor")


if __name__ == "__main__":
    unittest.main()
