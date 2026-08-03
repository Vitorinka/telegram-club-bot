import importlib
import os
from pathlib import Path
import sys
import unittest

TEST_ENV = {
    "BOT_TOKEN": "123456:TEST_TOKEN_FOR_GIFT_ONLY",
    "DATABASE_URL": "postgresql://user:pass@localhost/db",
    "GROUP_ID": "-100123",
    "ADMIN_IDS": "1,2",
    "STRIPE_API_KEY": "sk_test_dummy",
    "STRIPE_WEBHOOK_SECRET": "gift_test_webhook_secret",
    "WEBHOOK_SECRET": "telegram_secret",
    "YOUR_DOMAIN": "https://club.example",
    "PRICE_TRIAL": "price_trial",
    "PRICE_1M": "price_1m",
    "PRICE_6M": "price_6m",
    "PRICE_12M": "price_12m",
}


def import_main():
    env = dict(TEST_ENV)
    env.update({
        "BOT_USERNAME": "ClubGiftBot",
        "GIFT_TOKEN_SECRET": "unit-test-gift-token-secret",
        "GIFT_PRICE_1M": "price_gift_1m",
        "GIFT_PRICE_6M": "price_gift_6m",
        "GIFT_PRICE_12M": "price_gift_12m",
    })
    os.environ.update(env)
    if "main" in sys.modules:
        return sys.modules["main"]
    return importlib.import_module("main")


class GiftAccessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.main = import_main()

    def test_gift_prices_do_not_block_core_startup_env(self):
        self.assertNotIn("GIFT_PRICE_1M", self.main.REQUIRED_ENV_VARS)
        self.assertNotIn("GIFT_PRICE_6M", self.main.REQUIRED_ENV_VARS)
        self.assertNotIn("GIFT_PRICE_12M", self.main.REQUIRED_ENV_VARS)
        self.assertNotIn("GIFT_TOKEN_SECRET", self.main.REQUIRED_ENV_VARS)

    def test_gift_tariff_duration_and_price_mapping(self):
        self.assertEqual(self.main.gift_duration_days("gift_1m"), 30)
        self.assertEqual(self.main.gift_duration_days("gift_6m"), 180)
        self.assertEqual(self.main.gift_duration_days("gift_12m"), 365)
        self.assertEqual(self.main.gift_price_id("gift_1m"), "price_gift_1m")
        self.assertIsNone(self.main.gift_duration_days("sub_1"))

    def test_gift_deep_link_uses_signed_token_but_token_hash_is_separate(self):
        token = self.main.generate_gift_token("GIFT-ABCD1234", 3)
        self.assertEqual(token, self.main.generate_gift_token("GIFT-ABCD1234", 3))
        self.assertNotEqual(token, self.main.generate_gift_token("GIFT-ABCD1234", 4))
        link = self.main.gift_deep_link(token)
        self.assertIn("start=gift_", link)
        self.assertIn(token, link)
        self.assertNotEqual(self.main.gift_token_hash(token), token)
        self.assertEqual(len(self.main.gift_token_hash(token)), 64)
        self.assertEqual(self.main.parse_gift_token(token), ("GIFT-ABCD1234", 3))
        self.assertIsNone(self.main.parse_gift_token(token + "x"))

    def test_gift_text_is_sanitized_and_html_escaped(self):
        raw = "<Natalia>\x00\n" + "x" * 400
        sanitized = self.main.sanitize_gift_text(raw, 80)
        self.assertNotIn("\x00", sanitized)
        self.assertLessEqual(len(sanitized), 80)
        self.assertEqual(self.main.gift_safe_user_text("<tag>"), "&lt;tag&gt;")

    def test_gift_payment_metadata_requires_payment_mode_and_server_mapping(self):
        gift_row = {
            "id": "gift-id",
            "purchaser_telegram_id": 123,
            "tariff_code": "gift_1m",
            "duration_days": 30,
        }
        session = {
            "id": "cs_gift",
            "mode": "payment",
            "payment_status": "paid",
            "client_reference_id": "123",
            "amount_total": 5000,
            "currency": "usd",
            "metadata": {
                "payment_kind": self.main.GIFT_PAYMENT_KIND,
                "gift_id": "gift-id",
                "purchaser_telegram_id": "123",
                "tariff_code": "gift_1m",
                "duration_days": "30",
            },
        }
        self.assertTrue(self.main.gift_payment_metadata_valid(session["metadata"], gift_row, session))
        bad_session = {**session, "mode": "subscription"}
        self.assertFalse(self.main.gift_payment_metadata_valid(session["metadata"], gift_row, bad_session))
        bad_metadata = {**session["metadata"], "duration_days": "365"}
        self.assertFalse(self.main.gift_payment_metadata_valid(bad_metadata, gift_row, session))
        gift_row["stripe_session_id"] = "cs_gift"
        line_item = {"quantity": 1}
        price = {"id": "price_gift_1m", "type": "one_time", "active": True, "unit_amount": 5000, "currency": "usd"}
        self.assertTrue(self.main.validate_gift_payment_proof(session, line_item, price, gift_row))
        self.assertFalse(self.main.validate_gift_payment_proof({**session, "payment_status": "no_payment_required"}, line_item, price, gift_row))
        self.assertFalse(self.main.validate_gift_payment_proof(session, {"quantity": 2}, price, gift_row))

    def test_gift_certificate_caption_has_activation_button_and_no_none_text(self):
        row = {
            "recipient_name": None,
            "sender_name": "Sender",
            "gift_message": None,
            "tariff_code": "gift_6m",
        }
        caption = self.main.gift_certificate_caption(row)
        self.assertNotIn("None", caption)
        self.assertIn("6 месяцев", caption)

    def test_gift_migration_defines_required_tables_without_raw_token_column(self):
        migration_sql = Path("migrations/0005_gift_access.sql").read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE IF NOT EXISTS gift_access_grants", migration_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS gift_certificate_templates", migration_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS gift_access_events", migration_sql)
        self.assertIn("token_hash", migration_sql)
        self.assertNotIn("raw_token", migration_sql)

    def test_gift_admin_text_uses_public_reference_not_token_hash(self):
        row = {
            "public_reference": "GIFT-ABCD1234",
            "purchaser_telegram_id": 123,
            "recipient_telegram_id": None,
            "tariff_code": "gift_12m",
            "status": "paid_unclaimed",
            "token_hash": "secret-hash",
        }
        text = self.main.gift_admin_text("Gift", row)
        self.assertIn("GIFT-ABCD1234", text)
        self.assertNotIn("secret-hash", text)

    def test_gift_certificate_delivery_payload_does_not_store_token_or_url(self):
        class FakeCursor:
            description = [
                ("delivery_key",),
            ]

            def __init__(self):
                self.queries = []
                self.payload = None

            def execute(self, query, params=None):
                self.queries.append((query, params))
                if "INSERT INTO message_delivery_events" in query:
                    self.payload = params[3]

            def fetchone(self):
                query = self.queries[-1][0]
                if "SELECT file_id" in query:
                    return ("photo_file",)
                if "INSERT INTO message_delivery_events" in query:
                    return ("gift:GIFT-ABCD1234:certificate:buyer:v1",)
                return None

        row = {
            "public_reference": "GIFT-ABCD1234",
            "recipient_name": "Recipient",
            "sender_name": "Sender",
            "gift_message": "",
            "tariff_code": "gift_1m",
            "token_version": 1,
            "token_hash": self.main.gift_token_hash_for_reference("GIFT-ABCD1234", 1),
        }
        cur = FakeCursor()
        self.assertTrue(self.main.enqueue_gift_certificate_delivery(cur, row, 123, self.main.GIFT_CERTIFICATE_BUYER))
        payload = cur.payload
        self.assertNotIn("gift_", payload)
        self.assertNotIn("button_url", payload)
        self.assertIn('"public_reference": "GIFT-ABCD1234"', payload)
        self.assertIn('"token_version": 1', payload)


if __name__ == "__main__":
    unittest.main()
