import unittest
from datetime import datetime, timedelta

from admin_system import (
    AdminSystemQueryError,
    _delivery_projection,
    decode_deliveries_cursor,
    delivery_type_label,
    encode_deliveries_cursor,
    parse_deliveries_limit,
    safe_error,
    validate_delivery_status,
    validate_delivery_type,
)


class AdminSystemTests(unittest.TestCase):
    def test_delivery_query_validation_and_cursor(self):
        self.assertEqual(parse_deliveries_limit(None), 25)
        self.assertEqual(parse_deliveries_limit("50"), 50)
        for value in (0, 51, "bad"):
            with self.subTest(value=value), self.assertRaises(AdminSystemQueryError):
                parse_deliveries_limit(value)
        self.assertEqual(validate_delivery_status("permanently_failed"), "permanently_failed")
        with self.assertRaises(AdminSystemQueryError):
            validate_delivery_status("unknown")
        self.assertEqual(validate_delivery_type("access_restore_invite"), "access_restore_invite")
        with self.assertRaises(AdminSystemQueryError):
            validate_delivery_type("invented_type")
        stamp = datetime(2026, 8, 22, 10, 0)
        delivery_id = "a" * 32
        cursor = encode_deliveries_cursor(stamp, delivery_id)
        self.assertNotIn("private:key:123", cursor)
        self.assertEqual(decode_deliveries_cursor(cursor), (stamp, delivery_id))

    def test_known_and_unknown_delivery_labels_are_safe(self):
        self.assertEqual(
            delivery_type_label("access_restore_invite"),
            "Приглашение восстановить доступ",
        )
        self.assertEqual(
            delivery_type_label("future_internal_type"),
            "Неизвестный тип доставки",
        )

    def test_projection_hides_raw_key_error_and_payload(self):
        now = datetime(2026, 8, 22, 10, 0)
        raw_key = "failed-renewal:sub_secret_123:telegram:99"
        raw_error = "network timeout for cus_secret_456"
        item = _delivery_projection((
            raw_key, 99, "stripe_user_message", "permanently_failed", 9,
            now - timedelta(hours=1), None, None, now, raw_error, now, now,
        ))
        rendered = str(item)
        self.assertNotIn(raw_key, str({k: v for k, v in item.items() if not k.startswith("_")}))
        self.assertNotIn(raw_error, rendered)
        self.assertNotIn("cus_secret_456", rendered)
        self.assertNotIn("payload_json", item)
        self.assertTrue(item["requires_attention"])
        self.assertEqual(item["last_error"]["category"], "transient_transport")
        self.assertIn("максимального числа попыток", item["explanation"])

    def test_safe_error_never_returns_raw_exception(self):
        raw = "malformed payload contains pi_private_value"
        result = safe_error(raw)
        self.assertEqual(result["category"], "manual_intervention")
        self.assertNotIn(raw, str(result))
        self.assertNotIn("pi_private_value", str(result))


if __name__ == "__main__":
    unittest.main()
