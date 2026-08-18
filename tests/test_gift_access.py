import importlib
import asyncio
import html
import os
from pathlib import Path
import sys
import unittest
from unittest import mock
import scheduled_jobs
import stripe

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
        "GIFT_TOKEN_SECRET": "unit-test-gift-token-secret-32chars",
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
        token = self.main.generate_gift_token("GIFT-ABCD1234ABCD1234", 3)
        self.assertEqual(token, self.main.generate_gift_token("GIFT-ABCD1234ABCD1234", 3))
        self.assertNotEqual(token, self.main.generate_gift_token("GIFT-ABCD1234ABCD1234", 4))
        link = self.main.gift_deep_link(token)
        self.assertIn("start=gift_", link)
        self.assertIn(token, link)
        self.assertNotIn(".", token)
        self.assertNotEqual(self.main.gift_token_hash(token), token)
        self.assertEqual(len(self.main.gift_token_hash(token)), 64)
        self.assertEqual(self.main.parse_gift_token(token), ("GIFT-ABCD1234ABCD1234", 3))
        self.assertIsNone(self.main.parse_gift_token(token + "x"))

    def test_gift_start_parameter_is_telegram_safe_and_tamper_proof(self):
        reference = self.main.gift_public_reference()
        for version in (1, 999999):
            token = self.main.generate_gift_token(reference, version)
            start_parameter = "gift_" + token
            self.assertLessEqual(len(start_parameter), 64)
            self.assertRegex(start_parameter, r"^[A-Za-z0-9_-]{1,64}$")
            self.assertEqual(self.main.parse_gift_token(token), (reference, version))
            replacement = "A" if token[0] != "A" else "B"
            self.assertIsNone(self.main.parse_gift_token(replacement + token[1:]))

    def test_gift_public_reference_uses_wider_entropy(self):
        reference = self.main.gift_public_reference()
        self.assertRegex(reference, r"^GIFT-[0-9A-F]{16}$")

    def test_gift_outbox_helper_does_not_override_shared_scheduled_jobs_enqueue(self):
        self.assertIs(self.main.enqueue_message_delivery, scheduled_jobs.enqueue_message_delivery)
        self.assertIsNot(self.main.enqueue_gift_message_delivery, scheduled_jobs.enqueue_message_delivery)

    def test_gift_token_secret_requires_missing_and_minimum_length(self):
        main = self.main
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "gift_token_secret_missing") as missing:
                main.gift_token_secret()
            self.assertNotIn("GIFT_TOKEN_SECRET", str(missing.exception))

        with mock.patch.dict(os.environ, {"GIFT_TOKEN_SECRET": "x" * 31}, clear=True):
            with self.assertRaisesRegex(ValueError, "gift_token_secret_too_short") as short:
                main.gift_token_secret()
            self.assertNotIn("x" * 31, str(short.exception))

        with mock.patch.dict(os.environ, {"GIFT_TOKEN_SECRET": "x" * 32}, clear=True):
            self.assertEqual(main.gift_token_secret(), "x" * 32)
            self.assertTrue(main.generate_gift_token("GIFT-ABCD1234ABCD1234", 1))

        with mock.patch.dict(os.environ, {"GIFT_TOKEN_SECRET": "y" * 64}, clear=True):
            self.assertEqual(main.gift_token_secret(), "y" * 64)

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
        archived_price = {**price, "active": False}
        self.assertTrue(self.main.validate_gift_payment_proof(session, line_item, archived_price, gift_row))
        self.assertFalse(self.main.validate_gift_payment_proof({**session, "payment_status": "no_payment_required"}, line_item, price, gift_row))
        self.assertFalse(self.main.validate_gift_payment_proof(session, {"quantity": 2}, price, gift_row))

    def test_gift_payment_metadata_accepts_real_stripe_object_without_get(self):
        gift_row = {
            "id": "gift-id",
            "purchaser_telegram_id": 123,
            "tariff_code": "gift_1m",
            "duration_days": 30,
            "stripe_session_id": "cs_gift",
        }
        session = stripe.StripeObject.construct_from({
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
        }, None)
        self.assertFalse(hasattr(session.metadata, "get"))
        line_item = stripe.StripeObject.construct_from({"quantity": 1}, None)
        price = stripe.StripeObject.construct_from({
            "id": "price_gift_1m",
            "type": "one_time",
            "active": False,
            "unit_amount": 5000,
            "currency": "usd",
        }, None)

        self.assertTrue(self.main.gift_payment_metadata_valid(session.metadata, gift_row, session))
        self.assertTrue(self.main.validate_gift_payment_proof(session, line_item, price, gift_row))

    def test_gift_stripe_metadata_paths_do_not_call_get_on_stripe_metadata(self):
        source = Path("main.py").read_text(encoding="utf-8")
        self.assertNotIn("gift_metadata.get(", source)
        helper_source = source[
            source.index("def gift_payment_metadata_valid"):
            source.index("def _stripe_collection_first")
        ]
        self.assertNotIn("metadata.get(", helper_source)

    def test_gift_fsm_requires_tariff_before_recipient_name(self):
        source = Path("main.py").read_text(encoding="utf-8")
        self.assertIn("tariff = State()", source)
        self.assertIn("await state.set_state(GiftPurchaseStates.tariff)", source)
        self.assertIn('@router.callback_query(F.data.startswith("gift_tariff:"), StateFilter(GiftPurchaseStates.tariff))', source)
        self.assertIn("certificate_name = State()", source)
        self.assertIn("await state.set_state(GiftPurchaseStates.certificate_name)", source)
        self.assertNotIn('@router.callback_query(F.data.startswith("gift_tariff:"), StateFilter(GiftPurchaseStates.recipient_name))', source)

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
        self.assertIn("Как получить подарок:", caption)
        self.assertIn("Если Telegram попросит запустить бот", caption)

    def test_gift_certificate_delivery_caption_adds_runtime_deep_link(self):
        row = {
            "recipient_name": "Анна",
            "sender_name": "Виктория",
            "gift_message": "С любовью",
            "tariff_code": "gift_1m",
        }
        base_caption = self.main.gift_certificate_caption(row)
        token = self.main.generate_gift_token("GIFT-ABCD1234ABCD1234", 1)
        button_url = self.main.gift_deep_link(token)
        caption = self.main.gift_certificate_delivery_caption(base_caption, button_url)

        self.assertIn("https://t.me/ClubGiftBot?start=gift_", caption)
        self.assertIn("🎁 Вам подарили доступ в клуб Натальи Ребковец", caption)
        self.assertIn("Для: Анна", caption)
        self.assertIn("От: Виктория", caption)
        self.assertIn("Срок доступа: 1 месяц", caption)
        self.assertIn("С любовью", caption)
        self.assertIn("Если кнопка не открывается, используйте эту ссылку:", caption)

    def test_gift_certificate_delivery_caption_stays_within_telegram_limit(self):
        row = {
            "recipient_name": "<" * self.main.GIFT_NAME_LIMIT,
            "sender_name": ">" * self.main.GIFT_NAME_LIMIT,
            "gift_message": "&" * self.main.GIFT_MESSAGE_LIMIT,
            "tariff_code": "gift_12m",
        }
        base_caption = self.main.gift_certificate_caption(row)
        token = self.main.generate_gift_token("GIFT-ABCD1234ABCD1234", 999999)
        button_url = self.main.gift_deep_link(token)
        caption = self.main.gift_certificate_delivery_caption(base_caption, button_url)
        visible_caption = html.unescape(caption)

        self.assertLessEqual(len(visible_caption), self.main.GIFT_CERTIFICATE_CAPTION_LIMIT)
        self.assertIn(button_url, visible_caption)
        self.assertEqual(button_url, visible_caption.rsplit("\n", 1)[-1])
        self.assertIn("<" * self.main.GIFT_NAME_LIMIT, visible_caption)
        self.assertIn(">" * self.main.GIFT_NAME_LIMIT, visible_caption)
        self.assertIn("&" * self.main.GIFT_MESSAGE_LIMIT, visible_caption)

    def test_gift_certificate_delivery_caption_does_not_cut_html_entity_for_maximum_text(self):
        row = {
            "recipient_name": "<" * self.main.GIFT_NAME_LIMIT,
            "sender_name": ">" * self.main.GIFT_NAME_LIMIT,
            "gift_message": "&" * self.main.GIFT_MESSAGE_LIMIT,
            "tariff_code": "gift_12m",
        }
        base_caption = self.main.gift_certificate_caption(row)
        token = self.main.generate_gift_token("GIFT-ABCD1234ABCD1234", 999999)
        button_url = self.main.gift_deep_link(token)
        caption = self.main.gift_certificate_delivery_caption(base_caption, button_url)
        visible_caption = html.unescape(caption)

        self.assertEqual(button_url, visible_caption.rsplit("\n", 1)[-1])
        self.assertNotIn("<" * self.main.GIFT_NAME_LIMIT, caption)
        self.assertNotIn(">" * self.main.GIFT_NAME_LIMIT, caption)
        self.assertIn("&lt;", caption)
        self.assertIn("&gt;", caption)
        self.assertIn("&amp;", caption)
        self.assertFalse(caption.rstrip().endswith(("&", "&a", "&am", "&amp")))

    def test_gift_migration_defines_required_tables_without_raw_token_column(self):
        migration_sql = Path("migrations/0005_gift_access.sql").read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE IF NOT EXISTS gift_access_grants", migration_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS gift_certificate_templates", migration_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS gift_access_events", migration_sql)
        self.assertIn("token_hash", migration_sql)
        self.assertNotIn("raw_token", migration_sql)

    def test_gift_admin_text_uses_public_reference_not_token_hash(self):
        row = {
            "public_reference": "GIFT-ABCD1234ABCD1234",
            "purchaser_telegram_id": 123,
            "recipient_telegram_id": None,
            "tariff_code": "gift_12m",
            "status": "paid_unclaimed",
            "token_hash": "secret-hash",
        }
        text = self.main.gift_admin_text("Gift", row)
        self.assertIn("id_***CD1234", text)
        self.assertNotIn("GIFT-ABCD1234ABCD1234", text)
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
                    return ("gift:GIFT-ABCD1234ABCD1234:certificate:buyer:v1",)
                return None

        row = {
            "public_reference": "GIFT-ABCD1234ABCD1234",
            "recipient_name": "Recipient",
            "sender_name": "Sender",
            "gift_message": "",
            "tariff_code": "gift_1m",
            "token_version": 1,
            "token_hash": self.main.gift_token_hash_for_reference("GIFT-ABCD1234ABCD1234", 1),
        }
        cur = FakeCursor()
        self.assertTrue(self.main.enqueue_gift_certificate_delivery(cur, row, 123, self.main.GIFT_CERTIFICATE_BUYER))
        payload = cur.payload
        raw_token = self.main.generate_gift_token("GIFT-ABCD1234ABCD1234", 1)
        self.assertNotIn("gift_", payload)
        self.assertNotIn("start=gift_", payload)
        self.assertNotIn("https://t.me/", payload)
        self.assertNotIn("button_url", payload)
        self.assertNotIn("photo_file_id", payload)
        self.assertNotIn(raw_token, payload)
        self.assertIn('"public_reference": "GIFT-ABCD1234ABCD1234"', payload)
        self.assertIn('"token_version": 1', payload)

    def test_gift_certificate_uses_local_assets_not_legacy_file_id_table(self):
        source = Path("main.py").read_text(encoding="utf-8")
        helper = source[source.index("def enqueue_gift_certificate_delivery"):source.index("def gift_row_dict")]
        self.assertNotIn("gift_certificate_templates", helper)
        self.assertNotIn("photo_file_id", helper)
        self.assertIn("render_gift_certificate", source)
        self.assertIn("FSInputFile", source)

    def test_certificate_failure_fallback_payload_stores_no_token_or_name(self):
        class Cursor:
            def __init__(self):
                self.queries = []

            def execute(self, query, params=None):
                self.queries.append((query, params))

            def fetchone(self):
                return ("inserted",)

            def close(self):
                pass

        class Connection:
            def __init__(self):
                self.cursor_obj = Cursor()

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                pass

            def rollback(self):
                pass

            def close(self):
                pass

        connection = Connection()
        gift_row = {
            "public_reference": "GIFT-ABCD1234ABCD1234",
            "token_version": 3,
            "certificate_name": "Секретное Имя",
            "purchaser_telegram_id": 123,
            "recipient_telegram_id": None,
            "tariff_code": "gift_1m",
            "status": "paid_unclaimed",
        }
        with mock.patch.object(self.main, "get_db_conn", return_value=connection), \
             mock.patch.object(self.main, "ADMIN_IDS", []):
            self.main.enqueue_gift_certificate_failure_notices(
                gift_row["public_reference"], 123, gift_row
            )
        payload = next(
            params[3]
            for query, params in connection.cursor_obj.queries
            if "INSERT INTO message_delivery_events" in query
        )
        raw_token = self.main.generate_gift_token(gift_row["public_reference"], 3)
        self.assertNotIn(raw_token, payload)
        self.assertNotIn("Секретное Имя", payload)
        self.assertNotIn("https://t.me/", payload)
        self.assertIn('"token_version": 3', payload)

    def test_gift_subscription_state_retrieves_live_stripe_for_any_subscription_id(self):
        main = self.main

        class Cursor:
            def execute(self, query, params=None):
                pass

            def fetchone(self):
                return (False, None, False, "sub_live")

            def close(self):
                pass

        class Conn:
            def cursor(self):
                return Cursor()

            def close(self):
                pass

        with mock.patch.object(main, "get_db_conn", return_value=Conn()), \
             mock.patch.object(main.stripe.Subscription, "retrieve", return_value={"status": "active", "cancel_at_period_end": False}) as retrieve:
            result = asyncio.run(main.gift_recipient_subscription_state(123))

        retrieve.assert_called_once_with("sub_live")
        self.assertEqual(result["action"], "block_active_auto_renew")

    def test_gift_subscription_state_status_matrix(self):
        main = self.main

        class Cursor:
            def execute(self, query, params=None):
                pass

            def fetchone(self):
                return (True, None, True, "sub_status")

            def close(self):
                pass

        class Conn:
            def cursor(self):
                return Cursor()

            def close(self):
                pass

        cases = [
            ({"status": "trialing", "cancel_at_period_end": False}, "block_active_auto_renew"),
            ({"status": "active", "cancel_at_period_end": True}, "apply_after_current_expiry"),
            ({"status": "canceled", "cancel_at_period_end": False}, "apply"),
            ({"status": "unpaid", "cancel_at_period_end": False}, "apply"),
            ({"status": "paused", "cancel_at_period_end": False}, "fail"),
        ]
        for subscription, expected_action in cases:
            with self.subTest(subscription=subscription), \
                 mock.patch.object(main, "get_db_conn", return_value=Conn()), \
                 mock.patch.object(main.stripe.Subscription, "retrieve", return_value=subscription):
                self.assertEqual(asyncio.run(main.gift_recipient_subscription_state(123))["action"], expected_action)

    def test_gift_refund_pending_and_failed_do_not_change_status(self):
        main = self.main
        events = []

        class Cursor:
            def execute(self, query, params=None):
                events.append((query, params))

        row = {
            "id": "gift-id",
            "public_reference": "GIFT-ABCDEFABCDEF0000",
            "purchaser_telegram_id": 123,
            "status": "paid_unclaimed",
            "stripe_payment_intent_id": "pi_refund",
            "amount_total": 5000,
            "token_version": 1,
        }
        for status in ("pending", "requires_action", "canceled", "failed", "unknown"):
            result = main.apply_gift_refund_event(
                Cursor(),
                f"evt_{status}",
                "refund.updated",
                {"payment_intent": "pi_refund", "amount": 5000, "status": status},
                row,
            )
            self.assertIs(result, row)
        update_queries = [query for query, _ in events if "UPDATE gift_access_grants" in query]
        self.assertEqual(update_queries, [])

    def test_new_gift_logs_do_not_use_raw_str_exception_text(self):
        source = Path("main.py").read_text(encoding="utf-8")
        gift_log_lines = [
            line for line in source.splitlines()
            if line.strip().startswith('"GIFT_') or line.strip().startswith("'GIFT_")
        ]
        self.assertFalse(any("error=%s" in line for line in gift_log_lines))
        self.assertFalse(any("str(e)" in line for line in gift_log_lines))


if __name__ == "__main__":
    unittest.main()
