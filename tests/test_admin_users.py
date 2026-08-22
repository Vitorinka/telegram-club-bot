import unittest
from datetime import datetime

from admin_users import (
    AdminUsersQueryError,
    decode_users_cursor,
    encode_users_cursor,
    escape_like,
    mask_stripe_identifier,
    normalize_users_query,
    parse_users_limit,
)


class AdminUsersTests(unittest.TestCase):
    def test_limits_and_query_validation(self):
        self.assertEqual(parse_users_limit(None), 25)
        self.assertEqual(parse_users_limit("50"), 50)
        for invalid in ("no", "0", "51", -1):
            with self.subTest(invalid=invalid), self.assertRaises(AdminUsersQueryError):
                parse_users_limit(invalid)
        self.assertEqual(normalize_users_query("  admin  "), "admin")
        with self.assertRaises(AdminUsersQueryError):
            normalize_users_query("x" * 65)

    def test_cursor_round_trip_is_opaque_and_validated(self):
        timestamp = datetime(2026, 8, 22, 12, 30)
        cursor = encode_users_cursor(timestamp, 123456)
        self.assertNotIn("123456", cursor)
        self.assertEqual(decode_users_cursor(cursor), (timestamp, 123456))
        for invalid in ("%%%", "Zm9v", None):
            if invalid is None:
                self.assertIsNone(decode_users_cursor(invalid))
            else:
                with self.assertRaises(AdminUsersQueryError):
                    decode_users_cursor(invalid)

    def test_like_wildcards_are_escaped(self):
        self.assertEqual(escape_like(r"a%b_c\d"), r"a\%b\_c\\d")

    def test_stripe_identifiers_are_masked(self):
        raw = "cus_secret_value_123456"
        masked = mask_stripe_identifier(raw)
        self.assertEqual(masked, "cus_***123456")
        self.assertNotIn(raw, masked)
        self.assertIsNone(mask_stripe_identifier(None))


if __name__ == "__main__":
    unittest.main()
