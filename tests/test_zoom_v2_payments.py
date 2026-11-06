import asyncio
import concurrent.futures
import json
import logging
import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.test_aiogram3_bootstrap import FakeMiniAppRequest, import_main
from tests.test_postgres_migrations_integration import (
    POSTGRES_TEST_DSN,
    create_temp_db,
    drop_temp_db,
    connect,
)
from db_migrations import run_migrations
from zoom_v2_service import create_or_get_v2_booking, create_v2_class
from zoom_v2_payments import (
    AMOUNT,
    CONSENT_VERSION,
    CURRENCY,
    PAYMENT_KIND,
    attach_payment_intent,
    authorization_owner,
    apply_stripe_authorization,
    finish_cancel_operation,
    mark_request_uncertain,
    payment_terms,
    prepare_authorization,
    prepare_cancel_operation,
)


class ZoomV2PaymentApiTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.main = import_main()

    def setUp(self):
        self.flag = patch.dict(os.environ, {"ZOOM_V2_API_ENABLED": "true"})
        self.flag.start()

    def tearDown(self):
        self.flag.stop()

    def request(self, telegram_id=41):
        request = FakeMiniAppRequest(
            None, method="POST", match_info={"class_id": "class-a"},
            json_data={
                "consent_version": CONSENT_VERSION,
                "terms_hash": payment_terms()["terms_hash"],
                "explicit_consent": True,
                "amount": 999999,
            },
        )
        request["miniapp_member"] = SimpleNamespace(telegram_id=telegram_id)
        return request

    async def test_authorize_uses_fixed_manual_capture_terms_and_stable_idempotency(self):
        prepared = {
            "booking_id": "booking-a", "operation_id": "operation-a",
            "generation": 7, "idempotency_key": "zoom-v2:authorize:booking-a:7",
        }
        owner = {"payment_intent_id": None}
        intent = SimpleNamespace(
            id="pi_test", status="requires_action", client_secret="secret_for_owner",
            last_response=SimpleNamespace(request_id="req_test"),
        )
        runner = AsyncMock(side_effect=[prepared, owner, None])
        with patch.object(self.main, "run_sync_db", new=runner), \
             patch.object(self.main.stripe.PaymentIntent, "create", return_value=intent) as create:
            response = await self.main.miniapp_member_zoom_v2_authorize(self.request())
        kwargs = create.call_args.kwargs
        self.assertEqual((kwargs["amount"], kwargs["currency"]), (AMOUNT, CURRENCY))
        self.assertEqual(kwargs["capture_method"], "manual")
        self.assertEqual(kwargs["payment_method_types"], ["card"])
        self.assertEqual(kwargs["idempotency_key"], prepared["idempotency_key"])
        self.assertEqual(kwargs["metadata"], {
            "payment_kind": PAYMENT_KIND, "booking_id": "booking-a",
            "payment_generation": "7",
        })
        body = json.loads(response.text)
        self.assertEqual(body["client_secret"], "secret_for_owner")
        self.assertEqual(body["amount"], AMOUNT)

    async def test_timeout_marks_operation_uncertain_without_second_identity(self):
        prepared = {
            "booking_id": "booking-a", "operation_id": "operation-a",
            "generation": 3, "idempotency_key": "stable-key", "request_uncertain": True,
        }
        runner = AsyncMock(side_effect=[prepared, {"payment_intent_id": None}, None])
        with patch.object(self.main, "run_sync_db", new=runner), \
             patch.object(self.main.stripe.PaymentIntent, "create", side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                await self.main.miniapp_member_zoom_v2_authorize(self.request())
        self.assertIs(runner.await_args_list[-1].args[0], mark_request_uncertain)
        self.assertEqual(runner.await_args_list[-1].args[2], "operation-a")

    async def test_existing_intent_is_retrieved_and_not_created_again(self):
        prepared = {
            "booking_id": "booking-a", "operation_id": "operation-a",
            "generation": 3, "idempotency_key": "stable-key", "existing": True,
        }
        owner = {"payment_intent_id": "pi_existing"}
        intent = SimpleNamespace(
            id="pi_existing", status="requires_action", client_secret="owner-secret",
        )
        runner = AsyncMock(side_effect=[prepared, owner])
        with patch.object(self.main, "run_sync_db", new=runner), \
             patch.object(self.main.stripe.PaymentIntent, "retrieve", return_value=intent) as retrieve, \
             patch.object(self.main.stripe.PaymentIntent, "create") as create:
            response = await self.main.miniapp_member_zoom_v2_authorize(self.request())
        retrieve.assert_called_once_with("pi_existing")
        create.assert_not_called()
        self.assertEqual(json.loads(response.text)["client_secret"], "owner-secret")

    async def test_client_secret_is_scoped_by_owner_lookup_and_not_logged(self):
        error = self.main.ZoomV2PaymentError("booking_not_found", 404)
        runner = AsyncMock(side_effect=error)
        logger = logging.getLogger()
        with patch.object(self.main, "run_sync_db", new=runner), \
             self.assertLogs(logger, level="INFO") as captured:
            response = await self.main.miniapp_member_zoom_v2_authorize(self.request(99))
            logging.info("owner lookup denied")
        self.assertEqual(response.status, 404)
        self.assertNotIn("client_secret", "\n".join(captured.output))
        self.assertNotIn("secret", response.text.lower())

    async def test_webhook_isolated_from_subscription_and_gift_events(self):
        event = {"id": "pi_other", "metadata": {"payment_kind": "gift"}}
        with patch.object(self.main.stripe.PaymentIntent, "retrieve") as retrieve, \
             patch.object(self.main, "run_sync_db", new=AsyncMock()) as runner:
            handled = await self.main.process_zoom_v2_payment_intent_event(event, "evt_other")
        self.assertFalse(handled)
        retrieve.assert_not_called()
        runner.assert_not_awaited()


@unittest.skipUnless(POSTGRES_TEST_DSN, "POSTGRES_TEST_DSN is required")
class ZoomV2PaymentPostgresTests(unittest.TestCase):
    def setUp(self):
        self.db_name, self.dsn = create_temp_db("codex_zoom_v2_payment")
        run_migrations(self.get_conn)
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.telegram_id = 975001
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (telegram_id,paid) VALUES (%s,FALSE)",
                    (self.telegram_id,),
                )
            conn.commit()
        finally:
            conn.close()
        created = create_v2_class(
            self.get_conn, 1, title="Payment concurrency",
            starts_at=self.now + timedelta(days=3), published_at=self.now,
            zoom_url="https://zoom.example/payment-private",
        )
        self.class_id = created["class_id"]
        self.booking = create_or_get_v2_booking(
            self.get_conn, self.telegram_id, self.class_id, now=self.now,
        )

    def tearDown(self):
        drop_temp_db(self.db_name)

    def get_conn(self):
        conn = connect(self.dsn)
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
        return conn

    def query_one(self, query, params=()):
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()
        finally:
            conn.close()

    def prepare(self):
        terms = payment_terms()
        return prepare_authorization(
            self.get_conn, self.telegram_id, self.class_id,
            consent_version=terms["consent_version"],
            consent_hash=terms["terms_hash"], explicit_consent=True, now=self.now,
        )

    def intent(self, prepared, *, capture_before=None, status="requires_capture"):
        if capture_before is None:
            capture_before = self.now + timedelta(days=5)
        return {
            "id": "pi_zoom_v2_one", "status": status, "amount": AMOUNT,
            "currency": CURRENCY, "capture_method": "manual",
            "amount_capturable": AMOUNT if status == "requires_capture" else 0,
            "metadata": {
                "payment_kind": PAYMENT_KIND,
                "booking_id": prepared["booking_id"],
                "payment_generation": str(prepared["generation"]),
            },
            "latest_charge": {"payment_method_details": {"card": {
                "capture_before": int(capture_before.replace(tzinfo=timezone.utc).timestamp()),
            }}},
        }

    def test_concurrent_requests_share_one_operation_and_idempotency_key(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _index: self.prepare(), range(2)))
        self.assertEqual({item["operation_id"] for item in results}, {results[0]["operation_id"]})
        self.assertEqual({item["idempotency_key"] for item in results}, {results[0]["idempotency_key"]})
        self.assertEqual({item["generation"] for item in results}, {1})
        self.assertEqual(self.query_one(
            "SELECT COUNT(*) FROM class_payment_operations WHERE booking_id=%s AND operation_type='authorize'",
            (self.booking["booking_id"],),
        )[0], 1)

    def test_lost_response_and_restart_reuse_original_operation(self):
        prepared = self.prepare()
        mark_request_uncertain(self.get_conn, prepared["operation_id"])
        recovered = self.prepare()
        self.assertTrue(recovered["request_uncertain"])
        self.assertEqual(recovered["operation_id"], prepared["operation_id"])
        self.assertEqual(recovered["idempotency_key"], prepared["idempotency_key"])
        self.assertEqual(recovered["generation"], prepared["generation"])

    def test_webhook_before_api_response_attaches_identity_and_duplicate_is_idempotent(self):
        prepared = self.prepare()
        intent = self.intent(prepared)
        first = apply_stripe_authorization(self.get_conn, intent, event_id="evt_before_response")
        self.assertEqual(first, {"status": "booked_guest", "cancel_required": False})
        attach_payment_intent(self.get_conn, prepared["operation_id"], intent["id"])
        duplicate = apply_stripe_authorization(self.get_conn, intent, event_id="evt_duplicate")
        self.assertEqual(duplicate["status"], "booked_guest")
        owner = authorization_owner(self.get_conn, self.telegram_id, self.class_id)
        self.assertEqual(owner["access_status"], "booked_guest")
        self.assertEqual(self.query_one(
            "SELECT COUNT(*) FROM class_payment_operations WHERE booking_id=%s AND operation_type='authorize'",
            (prepared["booking_id"],),
        )[0], 1)

    def test_unverified_or_stale_authorization_never_opens_access(self):
        prepared = self.prepare()
        missing = self.intent(prepared)
        missing["latest_charge"]["payment_method_details"]["card"].pop("capture_before")
        missing_result = apply_stripe_authorization(self.get_conn, missing)
        self.assertEqual(missing_result, {
            "status": "missing_capture_before", "cancel_required": True,
        })
        too_short = self.now + timedelta(days=3, hours=12)
        result = apply_stripe_authorization(
            self.get_conn, self.intent(prepared, capture_before=too_short),
        )
        self.assertEqual(result["status"], "insufficient_authorization_window")
        self.assertTrue(result["cancel_required"])
        owner = authorization_owner(self.get_conn, self.telegram_id, self.class_id)
        self.assertNotEqual(owner["access_status"], "booked_guest")
        stale = self.intent(prepared)
        stale["metadata"]["payment_generation"] = "0"
        with self.assertRaisesRegex(Exception, "payment_identity_mismatch"):
            apply_stripe_authorization(self.get_conn, stale)

    def test_wrong_amount_currency_and_non_capturable_status_fail_closed(self):
        prepared = self.prepare()
        wrong = self.intent(prepared)
        wrong["amount"] = 999
        wrong["currency"] = "usd"
        result = apply_stripe_authorization(self.get_conn, wrong)
        self.assertEqual(result, {
            "status": "invalid_payment_identity", "cancel_required": False,
        })
        self.assertNotEqual(
            authorization_owner(self.get_conn, self.telegram_id, self.class_id)["access_status"],
            "booked_guest",
        )
        failed = self.intent(prepared, status="requires_payment_method")
        failed_result = apply_stripe_authorization(self.get_conn, failed)
        self.assertEqual(failed_result, {
            "status": "requires_payment_method", "cancel_required": False,
        })

    def test_cancel_failure_is_durable_and_completed_cancel_wins_race(self):
        prepared = self.prepare()
        short = self.now + timedelta(days=3, hours=12)
        result = apply_stripe_authorization(
            self.get_conn, self.intent(prepared, capture_before=short),
        )
        self.assertTrue(result["cancel_required"])
        cancel = prepare_cancel_operation(
            self.get_conn, prepared["booking_id"], prepared["generation"], result["status"],
        )
        finish_cancel_operation(
            self.get_conn, cancel["operation_id"], success=False, error="network_timeout",
        )
        self.assertEqual(self.query_one(
            "SELECT status,last_error FROM class_payment_operations WHERE operation_id=%s",
            (cancel["operation_id"],),
        ), ("retryable_failed", "network_timeout"))
        finish_cancel_operation(self.get_conn, cancel["operation_id"], success=True)
        late = apply_stripe_authorization(self.get_conn, self.intent(prepared), event_id="evt_late")
        self.assertEqual(late, {
            "status": "authorization_already_cancelled", "cancel_required": False,
        })
        self.assertEqual(self.query_one(
            "SELECT access_status,payment_state,authorization_reconciliation_status FROM class_bookings WHERE booking_id=%s",
            (prepared["booking_id"],),
        ), ("access_required", "cancelled", "cancelled"))

    def test_client_secret_has_no_database_column(self):
        columns = self.query_one("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema='public' AND table_name IN ('class_bookings','class_payment_operations')
              AND column_name='client_secret'
        """)[0]
        self.assertEqual(columns, 0)


if __name__ == "__main__":
    unittest.main()
