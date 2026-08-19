import asyncio
import ast
import hashlib
import hmac
import importlib
import io
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from aiohttp import web
from aiogram.exceptions import TelegramNetworkError
import stripe


TEST_ENV = {
    "BOT_TOKEN": "123456:TEST_TOKEN_FOR_BOOTSTRAP_ONLY",
    "DATABASE_URL": "postgresql://user:pass@localhost/db",
    "GROUP_ID": "-100123",
    "ADMIN_IDS": "1,2",
    "STRIPE_API_KEY": "sk_test_dummy",
    "STRIPE_WEBHOOK_SECRET": "whsec" + "_bootstrap_secret",
    "WEBHOOK_SECRET": "telegram_secret",
    "YOUR_DOMAIN": "https://club.example",
    "PRICE_TRIAL": "price_trial",
    "PRICE_1M": "price_1m",
    "PRICE_6M": "price_6m",
    "PRICE_12M": "price_12m",
}


class FakeWebhookInfo:
    url = "https://club.example/webhook/telegram_secret"
    pending_update_count = 0
    last_error_message = None


class FakeScheduler:
    def __init__(self):
        self.jobs = []
        self.running = False
        self.shutdown_calls = 0
        self.start_calls = 0

    def add_job(self, *args, **kwargs):
        self.jobs.append((args, kwargs))

    def start(self):
        self.running = True
        self.start_calls += 1

    def shutdown(self, wait=True):
        self.running = False
        self.shutdown_calls += 1


def signed_header(payload, secret, timestamp=None):
    if timestamp is None:
        timestamp = int(time.time())
    signed_payload = f"{timestamp}.".encode("utf-8") + payload
    signature = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={signature}"


def import_main():
    os.environ.update(TEST_ENV)
    if "main" in sys.modules:
        return sys.modules["main"]
    return importlib.import_module("main")


class FakeTelegramRequest:
    def __init__(self, payload, secret_token=TEST_ENV["WEBHOOK_SECRET"]):
        self.payload = payload
        self.headers = {}
        if secret_token is not None:
            self.headers["X-Telegram-Bot-Api-Secret-Token"] = secret_token

    async def json(self, loads=json.loads):
        return self.payload


class FakeStripeRequest:
    path = "/stripe-payment"
    host = "club.example"
    content_type = "application/json"

    def __init__(self, payload, headers):
        self.payload = payload
        self.headers = headers

    async def read(self):
        return self.payload


class FakeMessage:
    def __init__(self, user_id=123):
        self.from_user = SimpleNamespace(id=user_id)
        self.chat = SimpleNamespace(type="private")
        self.answers = []
        self.replies = []
        self.edits = []

    async def answer(self, text, **kwargs):
        self.answers.append((text, kwargs))

    async def reply(self, text, **kwargs):
        self.replies.append((text, kwargs))

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


class FakeCallback:
    def __init__(self, user_id=123):
        self.message = FakeMessage()
        self.from_user = SimpleNamespace(id=user_id)
        self.answers = []

    async def answer(self, text=None, **kwargs):
        self.answers.append((text, kwargs))


class FakeState:
    def __init__(self):
        self.clear_calls = 0
        self.states = []
        self.data = {}

    async def clear(self):
        self.clear_calls += 1

    async def set_state(self, state):
        self.states.append(state)

    async def update_data(self, **kwargs):
        self.data.update(kwargs)

    async def get_data(self):
        return dict(self.data)


class FakeCursor:
    def __init__(self, fetches=None):
        self.queries = []
        self.fetches = list(fetches or [])
        self.outbox_result = None

    def execute(self, query, params=None):
        self.queries.append((query, params))
        if "UPDATE message_delivery_events" in query and "RETURNING delivery_key" in query:
            self.outbox_result = (params[-2] if "claim_generation" in query else params[-1],)
        elif "UPDATE message_delivery_events" in query and "RETURNING invite_link" in query:
            self.outbox_result = (params[0],)

    def fetchone(self):
        if self.outbox_result is not None:
            result = self.outbox_result
            self.outbox_result = None
            return result
        return self.fetches.pop(0) if self.fetches else None

    def fetchall(self):
        return self.fetches.pop(0) if self.fetches else []

    def close(self):
        pass


class FakeConnection:
    def __init__(self, fetches=None):
        self.cursor_obj = FakeCursor(fetches=fetches)
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class ExecuteFailingCursor(FakeCursor):
    def __init__(self, fetches=None, error=None):
        super().__init__(fetches=fetches)
        self.error = error or RuntimeError("execute failed")

    def execute(self, query, params=None):
        super().execute(query, params)
        raise self.error


class ExecuteFailingConnection(FakeConnection):
    def __init__(self, error=None):
        self.cursor_obj = ExecuteFailingCursor(error=error)
        self.commits = 0
        self.rollbacks = 0
        self.closed = False


class ConditionalFailingCursor(FakeCursor):
    def __init__(self, fetches=None, error=None, fail_when=None):
        super().__init__(fetches=fetches)
        self.error = error or RuntimeError("execute failed")
        self.fail_when = fail_when or (lambda query, params: False)

    def execute(self, query, params=None):
        super().execute(query, params)
        if self.fail_when(query, params):
            raise self.error


class ConditionalFailingConnection(FakeConnection):
    def __init__(self, fetches=None, error=None, fail_when=None):
        self.cursor_obj = ConditionalFailingCursor(fetches=fetches, error=error, fail_when=fail_when)
        self.commits = 0
        self.rollbacks = 0
        self.closed = False


def adapted_json_value(value):
    return getattr(value, "adapted", value)


class FakeIncomingMessage:
    def __init__(self, user_id=123):
        self.from_user = SimpleNamespace(id=user_id)
        self.chat = SimpleNamespace(type="private")
        self.answers = []
        self.replies = []

    async def answer(self, text, **kwargs):
        self.answers.append((text, kwargs))

    async def reply(self, text, **kwargs):
        self.replies.append((text, kwargs))


class Aiogram3BootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.main = import_main()

    async def asyncTearDown(self):
        await self.main.bot.session.close()

    async def test_storage_diagnostics_private_admin_only(self):
        diagnostic_data = {"tables": [], "operational": {}, "retention": {}}
        private_admin = FakeIncomingMessage(user_id=1)
        with patch.object(self.main, "get_db_conn", return_value=FakeConnection()), \
             patch.object(self.main, "collect_storage_diagnostics", return_value=diagnostic_data), \
             patch.object(self.main, "render_storage_diagnostics", return_value=["safe aggregate"]):
            await self.main.storage_diagnostics_command(private_admin)
        self.assertEqual(private_admin.answers, [("safe aggregate", {})])

        non_admin = FakeIncomingMessage(user_id=777)
        with patch.object(self.main, "get_db_conn") as get_conn:
            await self.main.storage_diagnostics_command(non_admin)
        get_conn.assert_not_called()
        self.assertEqual(non_admin.answers, [])

    async def test_storage_diagnostics_group_admin_gets_no_database_information(self):
        group_admin = FakeIncomingMessage(user_id=1)
        group_admin.chat.type = "group"
        with patch.object(self.main, "get_db_conn") as get_conn:
            await self.main.storage_diagnostics_command(group_admin)
        get_conn.assert_not_called()
        self.assertEqual(group_admin.answers, [])
        self.assertIn("личном чате", group_admin.replies[0][0])

    async def test_constraint_audit_private_admin_only(self):
        audit_data = {"grouped": {}, "structural": {}, "external": {}}
        private_admin = FakeIncomingMessage(user_id=1)
        with patch.object(self.main, "get_db_conn", return_value=FakeConnection()), \
             patch.object(self.main, "collect_constraint_audit", return_value=audit_data), \
             patch.object(self.main, "render_constraint_audit", return_value=["safe aggregate"]):
            await self.main.constraint_audit_command(private_admin)
        self.assertEqual(private_admin.answers, [("safe aggregate", {})])

        non_admin = FakeIncomingMessage(user_id=777)
        with patch.object(self.main, "get_db_conn") as get_conn:
            await self.main.constraint_audit_command(non_admin)
        get_conn.assert_not_called()
        self.assertEqual(non_admin.answers, [])

    async def test_constraint_audit_group_admin_exposes_no_database_information(self):
        group_admin = FakeIncomingMessage(user_id=1)
        group_admin.chat.type = "group"
        with patch.object(self.main, "get_db_conn") as get_conn:
            await self.main.constraint_audit_command(group_admin)
        get_conn.assert_not_called()
        self.assertEqual(group_admin.answers, [])
        self.assertIn("личном чате", group_admin.replies[0][0])

    async def test_stripe_reconcile_audit_is_private_admin_only(self):
        private_admin = FakeIncomingMessage(user_id=1)
        audit = {"aborted": False, "partial": False, "results": [], "calls": 0, "duration": 0.01}
        with patch.object(self.main, "get_db_conn", return_value=FakeConnection(fetches=[[]])), \
             patch.object(self.main, "load_reconcile_candidates", return_value=[]), \
             patch.object(self.main, "reconcile_candidates", AsyncMock(return_value=audit)), \
             patch.object(self.main, "render_reconcile_audit", return_value=["safe audit"]):
            await self.main.stripe_reconcile_audit_command(private_admin)
        self.assertEqual(private_admin.answers, [("safe audit", {})])

        non_admin = FakeIncomingMessage(user_id=777)
        with patch.object(self.main, "get_db_conn") as get_conn:
            await self.main.stripe_reconcile_audit_command(non_admin)
        get_conn.assert_not_called()
        self.assertEqual(non_admin.answers, [])

        group_admin = FakeIncomingMessage(user_id=1)
        group_admin.chat.type = "group"
        with patch.object(self.main, "get_db_conn") as get_conn:
            await self.main.stripe_reconcile_audit_command(group_admin)
        get_conn.assert_not_called()
        self.assertEqual(group_admin.answers, [])

    def stripe_object(self, payload):
        return stripe.StripeObject.construct_from(payload, None)

    def route_count(self, app, method, path):
        return sum(
            1
            for route in app.router.routes()
            if route.method == method and getattr(route.resource, "canonical", None) == path
        )

    def route_handler(self, app, method, path):
        for route in app.router.routes():
            if route.method == method and getattr(route.resource, "canonical", None) == path:
                return route.handler
        self.fail(f"route not found: {method} {path}")

    def assert_invalid_telegram_webhook_secret_fails_closed(self, raw_secret):
        env = os.environ.copy()
        env.update(TEST_ENV)
        env["WEBHOOK_SECRET"] = raw_secret
        result = subprocess.run(
            [sys.executable, "-c", "import main"],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        output = result.stdout + result.stderr

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "WEBHOOK_SECRET is incompatible with Telegram webhook secret_token requirements",
            output,
        )
        self.assertNotIn(raw_secret, output)

    def delivery_inserts(self, conn):
        return [
            params for query, params in conn.cursor_obj.queries
            if "INSERT INTO message_delivery_events" in query
        ]

    def delivery_map(self, conn):
        return {params[0]: params for params in self.delivery_inserts(conn)}

    def admin_delivery_inserts(self, conn):
        return [params for params in self.delivery_inserts(conn) if params[2] == "stripe_admin_message"]

    def user_delivery_inserts(self, conn):
        return [params for params in self.delivery_inserts(conn) if params[2] != "stripe_admin_message"]

    def invoice_payment_event(
        self,
        event_id,
        *,
        period_end=None,
        paid_out_of_band=False,
        billing_reason="subscription_cycle",
        include_line_period=True,
    ):
        if period_end is None and include_line_period:
            period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        period_start = int((datetime.utcnow() - timedelta(days=1)).timestamp())
        line_period = {"start": period_start}
        if include_line_period:
            line_period["end"] = period_end
        payment = SimpleNamespace(
            status="paid",
            amount_paid=1000,
            payment=SimpleNamespace(type="payment_intent", payment_intent="pi_" + event_id[-6:]),
        )
        invoice = SimpleNamespace(
            id="in_" + event_id[-8:],
            subscription="sub_rejoin",
            customer="cus_rejoin",
            customer_email="paid@example.com",
            amount_paid=1000,
            amount_due=1000,
            currency="rub",
            billing_reason=billing_reason,
            status="paid",
            paid_out_of_band=paid_out_of_band,
            metadata={},
            payments=SimpleNamespace(data=[payment]),
            lines=SimpleNamespace(data=[
                SimpleNamespace(
                    subscription="sub_rejoin",
                    period=SimpleNamespace(**line_period),
                    price=SimpleNamespace(id="price_1m"),
                )
            ]),
        )
        event = SimpleNamespace(
            id=event_id,
            type="invoice.payment_succeeded",
            created=1720000000,
            data=SimpleNamespace(object=invoice),
        )
        subscription = SimpleNamespace(
            id="sub_rejoin",
            customer="cus_rejoin",
            status="active",
            trial_end=None,
            current_period_end=period_end,
            metadata={},
        )
        payload = json.dumps(
            {
                "id": event_id,
                "object": "event",
                "type": "invoice.payment_succeeded",
                "created": 1720000000,
                "data": {"object": {"id": invoice.id, "object": "invoice"}},
            },
            separators=(",", ":"),
        ).encode("utf-8")
        return payload, event, subscription

    async def run_invoice_webhook_with_future_expiry(self, event_id, *, paid_out_of_band=False, billing_reason="subscription_cycle"):
        payload, event, subscription = self.invoice_payment_event(
            event_id,
            paid_out_of_band=paid_out_of_band,
            billing_reason=billing_reason,
        )
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        effective_expiry = datetime.utcfromtimestamp(subscription.current_period_end)
        conn = FakeConnection(fetches=[
            (123,),
            *self.stripe_identity_available_fetches(
                include_subscription=True,
                target_user_row=(123, "cus_rejoin", "sub_rejoin"),
            ),
            (123, datetime.utcnow() + timedelta(days=14), False, effective_expiry),
            None,
            ("stripe:%s:rejoin_invite" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        self.assertTrue(conn.closed)
        enqueue_params = next(
            params for query, params in conn.cursor_obj.queries
            if "INSERT INTO message_delivery_events" in query
        )
        self.assertEqual(enqueue_params[0], "stripe:%s:rejoin_invite" % event_id)
        self.assertEqual(enqueue_params[2], "stripe_rejoin_check")
        return enqueue_params

    async def test_invoice_subscription_create_enqueues_initial_success_and_rejoin_separately(self):
        event_id = "evt_invoice_initial_success"
        payload, event, subscription = self.invoice_payment_event(event_id, billing_reason="subscription_create")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        effective_expiry = datetime.utcfromtimestamp(subscription.current_period_end)
        conn = FakeConnection(fetches=[
            (123,),
            *self.stripe_identity_available_fetches(
                include_subscription=True,
                target_user_row=(123, "cus_rejoin", "sub_rejoin"),
            ),
            (123, None, False, effective_expiry),
            None,
            ("stripe:%s:rejoin_invite" % event_id,),
            ("stripe:%s:payment_success" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        deliveries = self.delivery_map(conn)
        self.assertIn("stripe:%s:payment_success" % event_id, deliveries)
        self.assertIn("stripe:%s:rejoin_invite" % event_id, deliveries)
        payment_payload = json.loads(adapted_json_value(deliveries["stripe:%s:payment_success" % event_id][3]))
        self.assertIn("Оплата прошла успешно 🤍", payment_payload["text"])
        self.assertIn("Спасибо, что присоединились", payment_payload["text"])
        self.assertNotIn("Подписка успешно продлена", payment_payload["text"])
        self.assertFalse(self.admin_delivery_inserts(conn))

    async def test_invoice_webhook_enqueues_renewal_success_with_new_expiry(self):
        event_id = "evt_invoice_renewal_success"
        payload, event, subscription = self.invoice_payment_event(event_id)
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        effective_expiry = datetime.utcfromtimestamp(subscription.current_period_end)
        conn = FakeConnection(fetches=[
            (123,),
            *self.stripe_identity_available_fetches(
                include_subscription=True,
                target_user_row=(123, "cus_rejoin", "sub_rejoin"),
            ),
            (123, datetime.utcnow() + timedelta(days=14), False, effective_expiry),
            None,
            ("stripe:%s:rejoin_invite" % event_id,),
            ("stripe:%s:renewal_success" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        deliveries = self.delivery_map(conn)
        payload_json = adapted_json_value(deliveries["stripe:%s:renewal_success" % event_id][3])
        payload_data = json.loads(payload_json)
        self.assertEqual(deliveries["stripe:%s:renewal_success" % event_id][2], "stripe_user_message")
        self.assertIn("Подписка успешно продлена 🤍", payload_data["text"])
        self.assertIn(datetime.utcfromtimestamp(subscription.current_period_end).strftime("%d.%m.%Y"), payload_data["text"])
        self.assertNotIn("None", payload_data["text"])
        rejoin_payload = json.loads(adapted_json_value(deliveries["stripe:%s:rejoin_invite" % event_id][3]))
        self.assertNotIn("Подписка успешно продлена", rejoin_payload["text"])
        self.assertFalse(self.admin_delivery_inserts(conn))

    async def test_invoice_exact_subscription_uses_effective_db_expiry_for_notices(self):
        event_id = "evt_invoice_effective_exact"
        stripe_period_ts = int(datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc).timestamp())
        stripe_period_expiry = datetime.utcfromtimestamp(stripe_period_ts)
        effective_expiry = datetime(2026, 11, 1, 0, 0)
        payload, event, subscription = self.invoice_payment_event(
            event_id,
            period_end=stripe_period_ts,
        )
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[
            (123,),
            *self.stripe_identity_available_fetches(
                include_subscription=True,
                target_user_row=(123, "cus_rejoin", "sub_rejoin"),
            ),
            (123, effective_expiry, False, effective_expiry),
            None,
            ("stripe:%s:rejoin_invite" % event_id,),
            ("stripe:%s:renewal_success" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        deliveries = self.delivery_map(conn)
        renewal_payload = json.loads(adapted_json_value(deliveries["stripe:%s:renewal_success" % event_id][3]))
        rejoin_payload = json.loads(adapted_json_value(deliveries["stripe:%s:rejoin_invite" % event_id][3]))
        self.assertIn("01.11.2026", renewal_payload["text"])
        self.assertNotIn("01.10.2026", renewal_payload["text"])
        self.assertEqual(renewal_payload["new_expiry"], "2026-11-01T00:00:00")
        self.assertIn("01.11.2026", rejoin_payload["text"])
        self.assertNotIn("01.10.2026", rejoin_payload["text"])
        payment_params = next(
            params for query, params in conn.cursor_obj.queries
            if "INSERT INTO payment_events" in query
        )
        access_params = next(
            params for query, params in conn.cursor_obj.queries
            if "INSERT INTO access_events" in query
        )
        self.assertEqual(payment_params[15], stripe_period_expiry)
        self.assertEqual(access_params[4], effective_expiry)
        self.assertFalse(self.admin_delivery_inserts(conn))

    async def test_invoice_customer_fallback_uses_effective_db_expiry_for_notices(self):
        event_id = "evt_invoice_effective_customer"
        stripe_period_ts = int(datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc).timestamp())
        stripe_period_expiry = datetime.utcfromtimestamp(stripe_period_ts)
        effective_expiry = datetime(2026, 11, 1, 0, 0)
        payload, event, subscription = self.invoice_payment_event(
            event_id,
            period_end=stripe_period_ts,
        )
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[
            None,
            (123,),
            *self.stripe_identity_available_fetches(
                include_subscription=True,
                target_user_row=(123, "cus_rejoin", "sub_rejoin"),
            ),
            (123, effective_expiry, False, effective_expiry),
            None,
            ("stripe:%s:rejoin_invite" % event_id,),
            ("stripe:%s:renewal_success" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        deliveries = self.delivery_map(conn)
        renewal_payload = json.loads(adapted_json_value(deliveries["stripe:%s:renewal_success" % event_id][3]))
        rejoin_payload = json.loads(adapted_json_value(deliveries["stripe:%s:rejoin_invite" % event_id][3]))
        self.assertIn("01.11.2026", renewal_payload["text"])
        self.assertEqual(renewal_payload["new_expiry"], "2026-11-01T00:00:00")
        self.assertIn("01.11.2026", rejoin_payload["text"])
        payment_params = next(
            params for query, params in conn.cursor_obj.queries
            if "INSERT INTO payment_events" in query
        )
        access_params = next(
            params for query, params in conn.cursor_obj.queries
            if "INSERT INTO access_events" in query
        )
        self.assertEqual(payment_params[15], stripe_period_expiry)
        self.assertEqual(access_params[4], effective_expiry)

    async def test_invoice_webhook_enqueues_recovery_success_and_clears_failure_state(self):
        event_id = "evt_invoice_recovered_success"
        payload, event, subscription = self.invoice_payment_event(event_id)
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        effective_expiry = datetime.utcfromtimestamp(subscription.current_period_end)
        conn = FakeConnection(fetches=[
            (123,),
            *self.stripe_identity_available_fetches(
                include_subscription=True,
                target_user_row=(123, "cus_rejoin", "sub_rejoin"),
            ),
            (123, datetime.utcnow() - timedelta(days=1), True, effective_expiry),
            None,
            ("stripe:%s:rejoin_invite" % event_id,),
            ("stripe:%s:payment_recovered" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        update_sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("payment_failed = FALSE", update_sql)
        self.assertIn("payment_failed_at = NULL", update_sql)
        self.assertIn("grace_period_end = NULL", update_sql)
        deliveries = self.delivery_map(conn)
        payload_data = json.loads(adapted_json_value(deliveries["stripe:%s:payment_recovered" % event_id][3]))
        self.assertIn("Подписка снова активна", payload_data["text"])
        self.assertIn(datetime.utcfromtimestamp(subscription.current_period_end).strftime("%d.%m.%Y"), payload_data["text"])
        self.assertFalse(self.admin_delivery_inserts(conn))

    async def test_admin_success_is_log_only_and_does_not_rollback_payment_flow(self):
        event_id = "evt_invoice_admin_formatting_failure"
        payload, event, subscription = self.invoice_payment_event(event_id)
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        effective_expiry = datetime.utcfromtimestamp(subscription.current_period_end)
        conn = FakeConnection(fetches=[
            (123,),
            *self.stripe_identity_available_fetches(
                include_subscription=True,
                target_user_row=(123, "cus_rejoin", "sub_rejoin"),
            ),
            (123, datetime.utcnow() + timedelta(days=14), False, effective_expiry),
            None,
            ("stripe:%s:rejoin_invite" % event_id,),
            ("stripe:%s:renewal_success" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn), \
             self.assertLogs(level="INFO") as logs:
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        self.assertEqual(conn.rollbacks, 0)
        self.assertEqual(conn.commits, 1)
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("INSERT INTO payment_events", sql)
        self.assertIn("INSERT INTO access_events", sql)
        self.assertIn("SAVEPOINT admin_payment_notification", sql)
        self.assertNotIn("ROLLBACK TO SAVEPOINT admin_payment_notification", sql)
        deliveries = self.delivery_map(conn)
        self.assertIn("stripe:%s:renewal_success" % event_id, deliveries)
        self.assertFalse(self.admin_delivery_inserts(conn))
        log_output = "\n".join(logs.output)
        self.assertIn("ADMIN_NOTIFICATION_SUPPRESSED", log_output)
        self.assertIn("category=renewal_success", log_output)

    async def test_bot_dispatcher_storage_and_app_are_created(self):
        from aiogram import Bot, Dispatcher
        from postgres_fsm_storage import PostgresFSMStorage

        app = self.main.create_app()

        self.assertIsInstance(self.main.bot, Bot)
        self.assertIsInstance(self.main.dp, Dispatcher)
        self.assertIsInstance(self.main.storage, PostgresFSMStorage)
        self.assertIsInstance(app, web.Application)

    async def test_payment_success_message_renderer_formats_confirmed_date(self):
        expiry = datetime(2026, 9, 1, 12, 30)
        self.assertEqual(
            self.main.build_user_payment_success_message("payment_success", expiry),
            "Оплата прошла успешно 🤍\n\n"
            "Доступ к клубу открыт до 01.09.2026.\n\n"
            "Спасибо, что присоединились. Все материалы уже доступны в меню.",
        )
        self.assertEqual(
            self.main.build_user_payment_success_message("renewal_success", expiry),
            "Подписка успешно продлена 🤍\n\n"
            "Доступ к клубу сохранён до 01.09.2026.\n\n"
            "Спасибо, что остаётесь с нами.",
        )
        self.assertEqual(
            self.main.build_user_payment_success_message("payment_recovered", expiry),
            "Оплата прошла успешно 🤍\n\n"
            "Подписка снова активна, а доступ к клубу продлён до 01.09.2026.\n\n"
            "Спасибо, что всё получилось. Все материалы уже доступны в меню.",
        )
        for action in ("payment_success", "renewal_success", "payment_recovered"):
            message = self.main.build_user_payment_success_message(action, expiry)
            self.assertIn("01.09.2026", message)
            self.assertNotIn("12:30", message)
            self.assertNotIn("+", message)

    async def test_payment_success_message_missing_expiry_uses_safe_fallback(self):
        message = self.main.build_user_payment_success_message("payment_success", None)
        self.assertNotIn("None", message)
        self.assertNotIn("01.09.2026", message)
        self.assertEqual(
            message,
            "Оплата прошла успешно 🤍\n\n"
            "Доступ к клубу открыт.\n\n"
            "Мы дополнительно проверяем дату окончания подписки.",
        )

    async def test_restore_access_command_is_admin_private_and_requires_numeric_id(self):
        non_admin = FakeIncomingMessage(user_id=999)
        await self.main.restore_access_command(non_admin, SimpleNamespace(args="123"))
        self.assertEqual(non_admin.replies, [])

        group_admin = FakeIncomingMessage(user_id=1)
        group_admin.chat.type = "group"
        await self.main.restore_access_command(group_admin, SimpleNamespace(args="123"))
        self.assertIn("личном чате", group_admin.replies[0][0])

        admin = FakeIncomingMessage(user_id=1)
        await self.main.restore_access_command(admin, SimpleNamespace(args="not-a-number"))
        self.assertIn("telegram_id должен быть числом", admin.replies[0][0])

    async def test_restore_access_command_user_not_found(self):
        message = FakeIncomingMessage(user_id=1)
        conn = FakeConnection(fetches=[None])
        closed_during_reply = []

        async def reply_after_close(text, **kwargs):
            closed_during_reply.append(conn.closed)
            message.replies.append((text, kwargs))

        message.reply = reply_after_close

        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.restore_access_command(message, SimpleNamespace(args="123"))

        self.assertEqual(message.replies[0][0], "❌ Пользователь не найден в базе.")
        self.assertTrue(conn.closed)
        self.assertEqual(closed_during_reply, [True])

    async def test_restore_access_command_creates_confirmation_with_user_state(self):
        message = FakeIncomingMessage(user_id=1)
        user = (
            True,
            datetime(2026, 9, 1, 12, 0),
            False,
            None,
            False,
            "sub_test",
            "cus_test",
            True,
        )
        conn = FakeConnection(fetches=[user])
        closed_during_confirmation = []

        async def reply_after_close(text, **kwargs):
            closed_during_confirmation.append(conn.closed)
            message.replies.append((text, kwargs))

        message.reply = reply_after_close

        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.restore_access_command(message, SimpleNamespace(args="123"))

        self.assertTrue(any("INSERT INTO admin_action_requests" in query for query, _ in conn.cursor_obj.queries))
        text, kwargs = message.replies[0]
        self.assertIn("Подтвердите восстановление доступа", text)
        self.assertIn("telegram_id: 123", text)
        self.assertIn("paid: True", text)
        self.assertIn(f"stripe_subscription_id: {self.main.safe_log_id('sub_test')}", text)
        self.assertIn("reply_markup", kwargs)
        self.assertEqual(closed_during_confirmation, [True])

    async def test_has_restorable_group_access_requires_paid_future_expiry(self):
        now = datetime(2026, 9, 1, 12, 0)
        self.assertTrue(self.main.has_restorable_group_access(True, now + timedelta(days=1), now=now))
        self.assertFalse(self.main.has_restorable_group_access(False, now + timedelta(days=1), now=now))
        self.assertFalse(self.main.has_restorable_group_access(True, now - timedelta(seconds=1), now=now))
        self.assertFalse(self.main.has_restorable_group_access(True, None, now=now))

    async def test_restore_access_active_db_member_does_not_extend_or_enqueue(self):
        expiry = datetime.utcnow() + timedelta(days=10)
        user = (True, expiry, False, None, False, "sub_123", "cus_123", True)
        read_conn = FakeConnection(fetches=[user])
        event_conn = FakeConnection()

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, event_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="member"))), \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link:
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertTrue(result["already_member"])
        self.assertIn("✅ Доступ подтверждён", result["admin_message"])
        self.assertFalse(any("UPDATE users" in query for query, _ in read_conn.cursor_obj.queries))
        self.assertTrue(any("restore_access_already_member" in str(params) for _, params in event_conn.cursor_obj.queries))
        create_link.assert_not_awaited()

    async def test_restore_access_active_db_left_user_enqueues_delivery(self):
        expiry = datetime.utcnow() + timedelta(days=10)
        user = (True, expiry, False, None, False, "sub_123", "cus_123", True)
        read_conn = FakeConnection(fetches=[user])
        enqueue_conn = FakeConnection(fetches=[("access-restore:act1:123",)])

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, enqueue_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="left"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban:
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertTrue(result["delivery_created"])
        self.assertIn("✅ Восстановление доступа поставлено в очередь", result["admin_message"])
        unban.assert_not_awaited()
        delivery = self.delivery_inserts(enqueue_conn)[0]
        self.assertEqual(delivery[0], "access-restore:act1:123")
        self.assertEqual(delivery[2], self.main.ACCESS_RESTORE_DELIVERY_TYPE)
        payload = json.loads(adapted_json_value(delivery[3]))
        self.assertEqual(payload["telegram_id"], 123)
        self.assertEqual(payload["source"], self.main.ACCESS_RESTORE_SOURCE_ADMIN)
        self.assertEqual(payload["requested_by_admin_id"], 1)
        self.assertNotIn("cus_123", json.dumps(payload))
        self.assertNotIn("sub_123", json.dumps(payload))

    async def test_restore_access_kicked_user_unbans_before_delivery(self):
        expiry = datetime.utcnow() + timedelta(days=10)
        user = (True, expiry, False, None, False, "sub_123", "cus_123", True)
        read_conn = FakeConnection(fetches=[user])
        enqueue_conn = FakeConnection(fetches=[("access-restore:act1:123",)])

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, enqueue_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="kicked"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban:
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertTrue(result["delivery_created"])
        unban.assert_awaited_once()

    async def test_restore_access_inactive_db_active_stripe_syncs_exact_period(self):
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        stripe_expiry = datetime.utcfromtimestamp(period_end)
        user = (False, None, False, None, False, "sub_123", "cus_old", True)
        read_conn = FakeConnection(fetches=[user])
        sync_conn = FakeConnection(fetches=[("sub_123", None)])
        enqueue_conn = FakeConnection(fetches=[("access-restore:act1:123",)])
        subscription = SimpleNamespace(
            status="active",
            current_period_end=period_end,
            customer="cus_new",
            cancel_at_period_end=False,
        )

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, sync_conn, enqueue_conn]), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="left"))):
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertTrue(result["delivery_created"])
        update_params = next(params for query, params in sync_conn.cursor_obj.queries if "UPDATE users" in query)
        self.assertEqual(update_params[0], stripe_expiry)
        self.assertEqual(update_params[1], "cus_new")
        self.assertTrue(any("restore_access_stripe_sync" in str(params) for _, params in sync_conn.cursor_obj.queries))

    async def test_restore_access_inactive_stripe_or_error_fails_closed(self):
        for subscription_result in (
            SimpleNamespace(status="past_due", current_period_end=int((datetime.utcnow() + timedelta(days=30)).timestamp()), customer="cus"),
            RuntimeError("stripe down customer cus_raw"),
        ):
            with self.subTest(subscription_result=type(subscription_result).__name__):
                user = (False, None, False, None, False, "sub_123", "cus_old", True)
                read_conn = FakeConnection(fetches=[user])
                to_thread = AsyncMock()
                if isinstance(subscription_result, Exception):
                    to_thread.side_effect = subscription_result
                else:
                    to_thread.return_value = subscription_result

                with patch.object(self.main, "get_db_conn", return_value=read_conn), \
                     patch.object(self.main.asyncio, "to_thread", to_thread), \
                     patch.object(self.main.bot, "get_chat_member", AsyncMock()) as member:
                    result = await self.main.execute_confirmed_restore_access({
                        "telegram_id": 123,
                        "admin_id": 1,
                        "action_id": "act1",
                    })

                self.assertFalse(result["restored"])
                if isinstance(subscription_result, Exception):
                    self.assertEqual(result["status"], "failed")
                    self.assertEqual(result["reason"], "stripe_unavailable")
                    self.assertIn("Не удалось проверить активный период в Stripe", result["admin_message"])
                    self.assertIn("ref: restore_access_stripe_sync:", result["admin_message"])
                else:
                    self.assertEqual(result["status"], "completed")
                    self.assertEqual(result["reason"], "stripe_not_active")
                    self.assertIn("Подписка Stripe не активна", result["admin_message"])
                    self.assertIn("status: past_due", result["admin_message"])
                self.assertFalse(any("UPDATE users" in query for query, _ in read_conn.cursor_obj.queries))
                member.assert_not_awaited()

    async def test_restore_access_grace_without_future_expiry_is_not_restorable(self):
        expired = datetime.utcnow() - timedelta(days=1)
        grace = datetime.utcnow() + timedelta(days=1)
        user = (True, expired, True, grace, False, None, "cus_old", True)
        read_conn = FakeConnection(fetches=[user])

        with patch.object(self.main, "get_db_conn", return_value=read_conn), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock()) as to_thread, \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as member:
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertFalse(result["restored"])
        self.assertIn("Активный оплаченный период не подтверждён", result["admin_message"])
        self.assertNotIn("очередь", result["admin_message"])
        to_thread.assert_not_awaited()
        member.assert_not_awaited()

        key = "access-restore:act1:123"
        payload = json.dumps({"effective_expiry": expired.isoformat(), "source": self.main.ACCESS_RESTORE_SOURCE_ADMIN})
        claim_conn = FakeConnection(fetches=[[(key, 123, self.main.ACCESS_RESTORE_DELIVERY_TYPE, payload, 1, None, 1)]])
        recheck_conn = FakeConnection(fetches=[(True, expired, True, grace)])

        with patch.object(self.main, "get_db_conn", side_effect=[claim_conn, recheck_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as worker_member:
            worker_result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(worker_result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        self.assertTrue(any("access_restore_inactive" in str(params) for _, params in recheck_conn.cursor_obj.queries))
        worker_member.assert_not_awaited()

    async def test_restore_access_active_stripe_fallback_after_grace_syncs_and_queues(self):
        expired = datetime.utcnow() - timedelta(days=1)
        grace = datetime.utcnow() + timedelta(days=1)
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        stripe_expiry = datetime.utcfromtimestamp(period_end)
        user = (True, expired, True, grace, False, "sub_123", "cus_old", True)
        read_conn = FakeConnection(fetches=[user])
        sync_conn = FakeConnection(fetches=[("sub_123", expired)])
        enqueue_conn = FakeConnection(fetches=[("access-restore:act1:123",)])
        subscription = SimpleNamespace(
            status="trialing",
            current_period_end=period_end,
            customer="cus_new",
            cancel_at_period_end=False,
        )

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, sync_conn, enqueue_conn]), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="left"))):
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertTrue(result["delivery_created"])
        update_params = next(params for query, params in sync_conn.cursor_obj.queries if "UPDATE users" in query)
        self.assertEqual(update_params[0], stripe_expiry)

    async def test_access_restore_worker_keeps_command_restorable_delivery(self):
        key = "access-restore:act1:123"
        expiry = datetime.utcnow() + timedelta(days=5)
        payload = json.dumps({"effective_expiry": expiry.isoformat(), "source": self.main.ACCESS_RESTORE_SOURCE_ADMIN})
        claim_conn = FakeConnection(fetches=[[(key, 123, self.main.ACCESS_RESTORE_DELIVERY_TYPE, payload, 1, None, 1)]])
        recheck_conn = FakeConnection(fetches=[(True, expiry, True, datetime.utcnow() + timedelta(days=1))])
        already_conn = FakeConnection()

        with patch.object(self.main, "get_db_conn", side_effect=[claim_conn, recheck_conn, already_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="member"))):
            result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(result["sent"], 1)
        self.assertFalse(any("access_restore_inactive" in str(params) for _, params in recheck_conn.cursor_obj.queries))

    async def test_restore_access_active_db_telegram_membership_error_message_is_specific(self):
        from aiogram.exceptions import TelegramBadRequest

        expiry = datetime.utcnow() + timedelta(days=10)
        user = (True, expiry, False, None, False, "sub_123", "cus_123", True)
        read_conn = FakeConnection(fetches=[user])

        with patch.object(self.main, "get_db_conn", return_value=read_conn), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(side_effect=TelegramBadRequest(method=None, message="bot is not admin raw"))):
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertEqual(result["status"], "failed")
        self.assertIn("Оплаченный период подтверждён", result["admin_message"])
        self.assertIn("Telegram-проверка", result["admin_message"])
        self.assertNotIn("Активный оплаченный период не подтверждён", result["admin_message"])
        self.assertNotIn("bot is not admin raw", result["admin_message"])

    async def test_restore_access_stripe_synced_telegram_error_message_is_specific(self):
        from aiogram.exceptions import TelegramBadRequest

        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        user = (False, None, False, None, False, "sub_123", "cus_old", True)
        read_conn = FakeConnection(fetches=[user])
        sync_conn = FakeConnection(fetches=[("sub_123", None)])
        subscription = SimpleNamespace(
            status="active",
            current_period_end=period_end,
            customer="cus_new",
            cancel_at_period_end=False,
        )

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, sync_conn]), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(side_effect=TelegramBadRequest(method=None, message="raw telegram failure"))):
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertEqual(result["status"], "failed")
        self.assertIn("Stripe-данные синхронизированы", result["admin_message"])
        self.assertNotIn("Данные пользователя не изменены", result["admin_message"])
        self.assertNotIn("raw telegram failure", result["admin_message"])
        self.assertTrue(any("UPDATE users" in query for query, _ in sync_conn.cursor_obj.queries))

    async def test_restore_access_unban_permission_error_message_is_specific(self):
        from aiogram.exceptions import TelegramBadRequest

        expiry = datetime.utcnow() + timedelta(days=10)
        user = (True, expiry, False, None, False, "sub_123", "cus_123", True)
        read_conn = FakeConnection(fetches=[user])

        with patch.object(self.main, "get_db_conn", return_value=read_conn), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="kicked"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramBadRequest(method=None, message="bot lacks rights raw"))):
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertEqual(result["status"], "failed")
        self.assertIn("бан не снят", result["admin_message"])
        self.assertNotIn("Активный оплаченный период не подтверждён", result["admin_message"])
        self.assertNotIn("bot lacks rights raw", result["admin_message"])

    async def test_restore_access_stripe_unavailable_is_failed_with_safe_ref(self):
        user = (False, None, False, None, False, "sub_123", "cus_raw", True)
        read_conn = FakeConnection(fetches=[user])

        with patch.object(self.main, "get_db_conn", return_value=read_conn), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(side_effect=RuntimeError("network down cus_raw sub_123"))), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as member:
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["reason"], "stripe_unavailable")
        self.assertIn("Не удалось проверить активный период в Stripe", result["admin_message"])
        self.assertIn("ref:", result["admin_message"])
        self.assertNotIn("Подписка Stripe не активна", result["admin_message"])
        self.assertNotIn("cus_raw", result["admin_message"])
        self.assertNotIn("sub_123", result["admin_message"])
        self.assertFalse(any("UPDATE users" in query for query, _ in read_conn.cursor_obj.queries))
        member.assert_not_awaited()

    async def test_restore_access_stripe_inactive_status_is_completed_without_delivery(self):
        for status in ("past_due", "canceled"):
            with self.subTest(status=status):
                user = (False, None, False, None, False, "sub_123", "cus_old", True)
                read_conn = FakeConnection(fetches=[user])
                subscription = SimpleNamespace(
                    status=status,
                    current_period_end=int((datetime.utcnow() + timedelta(days=30)).timestamp()),
                    customer="cus_new",
                    cancel_at_period_end=False,
                )

                with patch.object(self.main, "get_db_conn", return_value=read_conn), \
                     patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
                     patch.object(self.main.bot, "get_chat_member", AsyncMock()) as member:
                    result = await self.main.execute_confirmed_restore_access({
                        "telegram_id": 123,
                        "admin_id": 1,
                        "action_id": "act1",
                    })

                self.assertEqual(result["status"], "completed")
                self.assertEqual(result["reason"], "stripe_not_active")
                self.assertIn(f"status: {status}", result["admin_message"])
                self.assertFalse(any("INSERT INTO message_delivery_events" in query for query, _ in read_conn.cursor_obj.queries))
                member.assert_not_awaited()

    async def test_restore_access_active_stripe_without_period_is_period_missing(self):
        user = (False, None, False, None, False, "sub_123", "cus_old", True)
        read_conn = FakeConnection(fetches=[user])
        subscription = SimpleNamespace(status="active", current_period_end=None, customer="cus_new", cancel_at_period_end=False)
        invoices = SimpleNamespace(data=[])

        with patch.object(self.main, "get_db_conn", return_value=read_conn), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(side_effect=[subscription, invoices])), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as member:
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["reason"], "stripe_period_missing")
        self.assertIn("оплаченный период не найден", result["admin_message"])
        self.assertFalse(any("UPDATE users" in query for query, _ in read_conn.cursor_obj.queries))
        member.assert_not_awaited()

    async def test_restore_access_period_not_future_has_exact_admin_message(self):
        past_period = int((datetime.utcnow() - timedelta(hours=1)).timestamp())
        user = (False, None, False, None, False, "sub_123", "cus_old", True)
        read_conn = FakeConnection(fetches=[user])
        subscription = SimpleNamespace(
            status="active",
            current_period_end=past_period,
            customer="cus_new",
            cancel_at_period_end=False,
        )

        with patch.object(self.main, "get_db_conn", return_value=read_conn), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as member:
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        expected = (
            "⚠️ Будущий оплаченный период Stripe не подтверждён\n\n"
            "telegram_id: 123\n"
            "status: active\n"
            "Найденный период уже завершился.\n"
            "Доступ не восстановлен, delivery не создан."
        )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["reason"], "stripe_period_not_future")
        self.assertEqual(result["admin_message"], expected)
        self.assertFalse(any("UPDATE users" in query for query, _ in read_conn.cursor_obj.queries))
        self.assertFalse(any("INSERT INTO message_delivery_events" in query for query, _ in read_conn.cursor_obj.queries))
        member.assert_not_awaited()

    async def test_restore_access_membership_decision_restricted_and_unknown(self):
        self.assertEqual(self.main.restore_access_membership_decision("restricted", True), "already_member")
        self.assertEqual(self.main.restore_access_membership_decision("restricted", False), "needs_invite")
        self.assertEqual(self.main.restore_access_membership_decision("left", True), "needs_invite")
        self.assertEqual(self.main.restore_access_membership_decision("kicked", True), "needs_unban_and_invite")
        self.assertEqual(self.main.restore_access_membership_decision(None, True), "fail_closed")
        self.assertEqual(self.main.restore_access_membership_decision("mystery", True), "fail_closed")

    async def test_restore_access_restricted_non_member_queues_like_worker_invites(self):
        expiry = datetime.utcnow() + timedelta(days=10)
        user = (True, expiry, False, None, False, "sub_123", "cus_123", True)
        read_conn = FakeConnection(fetches=[user])
        enqueue_conn = FakeConnection(fetches=[("access-restore:act1:123",)])

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, enqueue_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="restricted", is_member=False))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban:
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertTrue(result["delivery_created"])
        unban.assert_not_awaited()

        key = "access-restore:act1:123"
        payload = json.dumps({"effective_expiry": expiry.isoformat(), "source": self.main.ACCESS_RESTORE_SOURCE_ADMIN})
        claim_conn = FakeConnection(fetches=[[(key, 123, self.main.ACCESS_RESTORE_DELIVERY_TYPE, payload, 1, "https://t.me/+saved", 1)]])
        recheck_conn = FakeConnection(fetches=[(True, expiry, False, None)])
        sent_conn = FakeConnection(fetches=[(expiry,)])

        with patch.object(self.main, "get_db_conn", side_effect=[claim_conn, recheck_conn, sent_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="restricted", is_member=False))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as worker_unban, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            worker_result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(worker_result["sent"], 1)
        worker_unban.assert_not_awaited()
        send_message.assert_awaited_once()

    async def test_restore_access_unknown_membership_fails_without_delivery(self):
        expiry = datetime.utcnow() + timedelta(days=10)
        user = (True, expiry, False, None, False, "sub_123", "cus_123", True)
        read_conn = FakeConnection(fetches=[user])

        with patch.object(self.main, "get_db_conn", return_value=read_conn), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status=None))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban:
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["reason"], "telegram_membership_status_unknown")
        self.assertFalse(any("INSERT INTO message_delivery_events" in query for query, _ in read_conn.cursor_obj.queries))
        unban.assert_not_awaited()

    async def test_admin_action_failed_result_marks_failed_and_shows_custom_message(self):
        callback = FakeCallback(user_id=1)
        callback.data = "admin_action:confirm:act_failed"
        callback.message.chat = SimpleNamespace(type="private")
        claim_payload = json.dumps({"telegram_id": 123})
        claim_conn = FakeConnection(fetches=[("restore_access", claim_payload)])
        fail_conn = FakeConnection()
        result = {
            "status": "failed",
            "admin_message": "custom restore failure",
        }

        with patch.object(self.main, "get_db_conn", side_effect=[claim_conn, fail_conn]), \
             patch.object(self.main, "execute_confirmed_admin_action", AsyncMock(return_value=result)):
            await self.main.admin_action_confirm_callback(callback)

        self.assertIn("custom restore failure", callback.message.answers[0][0])
        self.assertNotIn("Действие выполнено", callback.message.answers[0][0])
        self.assertTrue(any("SET status = 'failed'" in query for query, _ in fail_conn.cursor_obj.queries))
        self.assertFalse(any("SET status = 'completed'" in query for query, _ in fail_conn.cursor_obj.queries))

    async def test_admin_action_completed_restore_outcomes_mark_completed(self):
        for result in (
            {"status": "completed", "admin_message": "queued"},
            {"status": "completed", "admin_message": "already_member"},
            {"status": "completed", "admin_message": "no_active"},
            {"status": "completed_with_warning", "warnings": ["x"]},
        ):
            with self.subTest(result=result):
                callback = FakeCallback(user_id=1)
                callback.data = "admin_action:confirm:act_ok"
                callback.message.chat = SimpleNamespace(type="private")
                claim_conn = FakeConnection(fetches=[("restore_access", json.dumps({"telegram_id": 123}))])
                complete_conn = FakeConnection()

                with patch.object(self.main, "get_db_conn", side_effect=[claim_conn, complete_conn]), \
                     patch.object(self.main, "execute_confirmed_admin_action", AsyncMock(return_value=result)):
                    await self.main.admin_action_confirm_callback(callback)

                self.assertTrue(any("SET status = 'completed'" in query for query, _ in complete_conn.cursor_obj.queries))
                self.assertFalse(any("SET status = 'failed'" in query for query, _ in complete_conn.cursor_obj.queries))

    async def test_access_restore_worker_rechecks_access_and_cancels_expired(self):
        key = "access-restore:act1:123"
        payload = json.dumps({
            "effective_expiry": (datetime.utcnow() - timedelta(days=1)).isoformat(),
            "source": self.main.ACCESS_RESTORE_SOURCE_ADMIN,
        })
        claim_conn = FakeConnection(fetches=[[(key, 123, self.main.ACCESS_RESTORE_DELIVERY_TYPE, payload, 1, None, 1)]])
        recheck_conn = FakeConnection(fetches=[(True, datetime.utcnow() - timedelta(days=1), False, None)])

        with patch.object(self.main, "get_db_conn", side_effect=[claim_conn, recheck_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as member:
            result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        self.assertTrue(any("restore_access_cancelled_inactive" in str(params) for _, params in recheck_conn.cursor_obj.queries))
        member.assert_not_awaited()

    async def test_access_restore_worker_already_joined_marks_sent_without_invite(self):
        key = "access-restore:act1:123"
        expiry = datetime.utcnow() + timedelta(days=5)
        payload = json.dumps({"effective_expiry": expiry.isoformat(), "source": self.main.ACCESS_RESTORE_SOURCE_ADMIN})
        claim_conn = FakeConnection(fetches=[[(key, 123, self.main.ACCESS_RESTORE_DELIVERY_TYPE, payload, 1, None, 1)]])
        recheck_conn = FakeConnection(fetches=[(True, expiry, False, None)])
        already_conn = FakeConnection()

        with patch.object(self.main, "get_db_conn", side_effect=[claim_conn, recheck_conn, already_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="administrator"))), \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(result["sent"], 1)
        create_link.assert_not_awaited()
        send_message.assert_not_awaited()
        self.assertTrue(any("restore_access_already_member" in str(params) for _, params in already_conn.cursor_obj.queries))

    async def test_access_restore_worker_sends_exact_text_button_and_persists_invite(self):
        key = "access-restore:act1:123"
        expiry = datetime(2026, 9, 1, 12, 0)
        payload = json.dumps({"effective_expiry": expiry.isoformat(), "source": self.main.ACCESS_RESTORE_SOURCE_ADMIN, "admin_action_id": "act1"})
        claim_conn = FakeConnection(fetches=[[(key, 123, self.main.ACCESS_RESTORE_DELIVERY_TYPE, payload, 1, None, 1)]])
        recheck_conn = FakeConnection(fetches=[(True, expiry, False, None)])
        link_conn = FakeConnection()
        sent_conn = FakeConnection(fetches=[(expiry,)])
        invite = SimpleNamespace(invite_link="https://t.me/+restore")

        with patch.object(self.main, "get_db_conn", side_effect=[claim_conn, recheck_conn, link_conn, sent_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="left"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()), \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock(return_value=invite)) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(result["sent"], 1)
        create_link.assert_awaited_once()
        options = create_link.await_args.kwargs
        self.assertEqual(options["member_limit"], 1)
        self.assertEqual(options["name"], "access_restore_123")
        self.assertLessEqual(options["expire_date"], datetime.utcnow() + timedelta(hours=24, minutes=1))
        send_message.assert_awaited_once()
        sent_text = send_message.await_args.args[1]
        self.assertEqual(sent_text, self.main.access_restore_invite_text(expiry))
        markup = send_message.await_args.kwargs["reply_markup"]
        self.assertEqual(markup.inline_keyboard[0][0].text, "Вступить в клуб")
        self.assertEqual(markup.inline_keyboard[0][0].url, "https://t.me/+restore")
        self.assertTrue(any("invite_link = COALESCE" in query for query, _ in link_conn.cursor_obj.queries))
        self.assertTrue(any("restore_access_invite_sent" in str(params) for _, params in sent_conn.cursor_obj.queries))

    async def test_access_restore_worker_forbidden_blocks_user_permanently(self):
        from aiogram.exceptions import TelegramForbiddenError

        key = "access-restore:act1:123"
        expiry = datetime.utcnow() + timedelta(days=5)
        payload = json.dumps({"effective_expiry": expiry.isoformat(), "source": self.main.ACCESS_RESTORE_SOURCE_ADMIN})
        claim_conn = FakeConnection(fetches=[[(key, 123, self.main.ACCESS_RESTORE_DELIVERY_TYPE, payload, 1, "https://t.me/+saved", 1)]])
        recheck_conn = FakeConnection(fetches=[(True, expiry, False, None)])
        fail_conn = FakeConnection()

        with patch.object(self.main, "get_db_conn", side_effect=[claim_conn, recheck_conn, fail_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="left"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()), \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="blocked"))), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 1})
        create_link.assert_not_awaited()
        sql = "\n".join(query for query, _ in fail_conn.cursor_obj.queries)
        self.assertIn("UPDATE users SET blocked_bot = TRUE", sql)
        self.assertIn("UPDATE message_delivery_events", sql)
        self.assertTrue(any("restore_access_user_blocked" in str(params) for _, params in fail_conn.cursor_obj.queries))

    async def test_access_restore_worker_network_retry_reuses_existing_invite(self):
        from aiogram.exceptions import TelegramNetworkError

        key = "access-restore:act1:123"
        expiry = datetime.utcnow() + timedelta(days=5)
        payload = json.dumps({"effective_expiry": expiry.isoformat(), "source": self.main.ACCESS_RESTORE_SOURCE_ADMIN})
        claim_conn = FakeConnection(fetches=[[(key, 123, self.main.ACCESS_RESTORE_DELIVERY_TYPE, payload, 2, "https://t.me/+saved", 2)]])
        recheck_conn = FakeConnection(fetches=[(True, expiry, False, None)])
        fail_conn = FakeConnection()

        with patch.object(self.main, "get_db_conn", side_effect=[claim_conn, recheck_conn, fail_conn]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="left"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()), \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramNetworkError(method=None, message="network"))), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        create_link.assert_not_awaited()
        fail_params = next(params for query, params in fail_conn.cursor_obj.queries if "UPDATE message_delivery_events" in query)
        self.assertEqual(fail_params[0], "failed")

    async def test_sync_stripe_user_enqueues_auto_membership_repair_stable_key(self):
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        user = (False, None, "sub_123", "cus_old", False, None, False)
        read_conn = FakeConnection(fetches=[user])
        write_conn = FakeConnection(fetches=[("sub_123", "cus_old", None), ("access-restore:auto-sync:123:%s" % period_end,)])
        message = FakeIncomingMessage(user_id=1)
        subscription = SimpleNamespace(
            status="active",
            current_period_end=period_end,
            customer="cus_new",
            cancel_at_period_end=False,
        )

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn]), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "log_access_event", AsyncMock()):
            await self.main.sync_stripe_user_command(message, SimpleNamespace(args="123"))

        delivery = self.delivery_inserts(write_conn)[0]
        self.assertEqual(delivery[0], "access-restore:auto-sync:123:%s" % period_end)
        self.assertEqual(delivery[2], self.main.ACCESS_RESTORE_DELIVERY_TYPE)
        payload = json.loads(adapted_json_value(delivery[3]))
        self.assertEqual(payload["telegram_id"], 123)
        self.assertEqual(payload["source"], self.main.ACCESS_RESTORE_SOURCE_AUTO_SYNC)
        self.assertEqual(payload["reason"], "sync_stripe_user_active_period")
        self.assertTrue(any("manual_stripe_sync" in str(params) for _, params in write_conn.cursor_obj.queries))
        self.assertTrue(read_conn.closed)
        self.assertTrue(write_conn.closed)

    async def test_sync_stripe_user_never_shortens_existing_expiry(self):
        stripe_expiry = datetime(2026, 10, 1)
        cases = (
            (datetime(2026, 9, 1), stripe_expiry, "stripe_later"),
            (datetime(2026, 12, 1), datetime(2026, 12, 1), "existing_later"),
            (None, stripe_expiry, "existing_null"),
            (datetime(2025, 1, 1), stripe_expiry, "existing_past"),
        )

        for existing_expiry, expected_expiry, label in cases:
            with self.subTest(label=label):
                period_end = int(stripe_expiry.replace(tzinfo=timezone.utc).timestamp())
                user = (False, existing_expiry, "sub_123", "cus_old", True, datetime.utcnow(), True)
                read_conn = FakeConnection(fetches=[user])
                write_conn = FakeConnection(fetches=[
                    ("sub_123", "cus_old", existing_expiry),
                    (f"access-restore:auto-sync:123:{int(expected_expiry.timestamp())}",),
                ])
                message = FakeIncomingMessage(user_id=1)
                subscription = SimpleNamespace(
                    status="active",
                    current_period_end=period_end,
                    customer="cus_new",
                    cancel_at_period_end=True,
                )

                with patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn]), \
                     patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)):
                    await self.main.sync_stripe_user_command(message, SimpleNamespace(args="123"))

                update_params = next(
                    params for query, params in write_conn.cursor_obj.queries
                    if "UPDATE users" in query and "SET paid = TRUE" in query
                )
                self.assertEqual(update_params, (expected_expiry, "cus_new", False, 123))
                self.assertIn(f"expiry_date: {expected_expiry.strftime('%d.%m.%Y %H:%M')}", message.replies[0][0])
                self.assertIn("auto_renew: False", message.replies[0][0])
                self.assertTrue(any("manual_stripe_sync" in str(params) for _, params in write_conn.cursor_obj.queries))

    def test_non_decreasing_expiry_handles_timezone_aware_values(self):
        stripe_expiry = datetime(2026, 9, 1)
        later_aware = datetime(2026, 12, 1, tzinfo=timezone.utc)

        self.assertEqual(self.main.non_decreasing_expiry(later_aware, stripe_expiry), datetime(2026, 12, 1))
        self.assertIs(self.main.non_decreasing_expiry(None, stripe_expiry), stripe_expiry)

    async def test_stripe_lookup_errors_log_safe_references_without_raw_ids(self):
        raw_subscription = "sub_secret_logging_value"
        raw_customer = "cus_secret_logging_value"
        raw_url = f"https://api.stripe.com/v1/subscriptions/{raw_subscription}"

        with patch.object(
            self.main.asyncio,
            "to_thread",
            AsyncMock(side_effect=RuntimeError(f"request failed: {raw_url}")),
        ), self.assertLogs(level="ERROR") as captured:
            self.assertEqual(
                await self.main.get_open_invoice_url_for_subscription(raw_subscription),
                (None, None),
            )
            self.assertIsNone(await self.main.create_billing_portal_url(raw_customer))

        output = "\n".join(captured.output)
        self.assertNotIn(raw_subscription, output)
        self.assertNotIn(raw_customer, output)
        self.assertNotIn(raw_url, output)
        self.assertIn("error_type=RuntimeError", output)
        self.assertIn("error_ref=", output)

    async def test_sync_stripe_user_closes_db_during_stripe_and_reply(self):
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        user = (False, None, "sub_123", "cus_old", False, None, False)
        read_conn = FakeConnection(fetches=[user])
        write_conn = FakeConnection(fetches=[("sub_123", "cus_old", None), ("access-restore:auto-sync:123:%s" % period_end,)])
        message = FakeIncomingMessage(user_id=1)
        closed_during_retrieve = []
        closed_during_reply = []

        async def reply_after_close(text, **kwargs):
            closed_during_reply.append(write_conn.closed)
            message.replies.append((text, kwargs))

        async def to_thread(func, *args, **kwargs):
            closed_during_retrieve.append(read_conn.closed)
            return SimpleNamespace(
                status="active",
                current_period_end=period_end,
                customer="cus_new",
                cancel_at_period_end=False,
            )

        message.reply = reply_after_close
        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn]), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(side_effect=to_thread)):
            await self.main.sync_stripe_user_command(message, SimpleNamespace(args="123"))

        self.assertEqual(closed_during_retrieve, [True])
        self.assertEqual(closed_during_reply, [True])

    async def test_sync_stripe_user_closes_db_during_invoice_fallback(self):
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        user = (False, None, "sub_123", "cus_old", False, None, False)
        read_conn = FakeConnection(fetches=[user])
        write_conn = FakeConnection(fetches=[("sub_123", "cus_old", None), ("access-restore:auto-sync:123:%s" % period_end,)])
        closed_during_calls = []

        async def to_thread(func, *args, **kwargs):
            closed_during_calls.append(read_conn.closed)
            if len(closed_during_calls) == 1:
                return SimpleNamespace(status="active", current_period_end=None, customer="cus_new", cancel_at_period_end=False)
            return SimpleNamespace(data=[
                SimpleNamespace(status="paid", lines=SimpleNamespace(data=[
                    SimpleNamespace(period=SimpleNamespace(end=period_end))
                ]))
            ])

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn]), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(side_effect=to_thread)):
            await self.main.sync_stripe_user_command(FakeIncomingMessage(user_id=1), SimpleNamespace(args="123"))

        self.assertEqual(closed_during_calls, [True, True])
        self.assertTrue(any("manual_stripe_sync" in str(params) for _, params in write_conn.cursor_obj.queries))

    async def test_sync_stripe_user_db_failure_rolls_back_atomic_update_event_and_delivery(self):
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        user = (False, None, "sub_123", "cus_old", False, None, False)
        read_conn = FakeConnection(fetches=[user])
        write_conn = ExecuteFailingConnection()
        subscription = SimpleNamespace(status="active", current_period_end=period_end, customer="cus_new", cancel_at_period_end=False)

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn]), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)):
            await self.main.sync_stripe_user_command(FakeIncomingMessage(user_id=1), SimpleNamespace(args="123"))

        self.assertEqual(write_conn.rollbacks, 1)
        self.assertEqual(write_conn.commits, 0)

    async def test_sync_stripe_user_identity_changed_does_not_update_event_or_delivery(self):
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        user = (False, None, "sub_A", "cus_old", False, None, False)
        read_conn = FakeConnection(fetches=[user])
        write_conn = FakeConnection(fetches=[("sub_B", "cus_newer", None)])
        message = FakeIncomingMessage(user_id=1)
        subscription = SimpleNamespace(status="active", current_period_end=period_end, customer="cus_A", cancel_at_period_end=False)

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn]), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)):
            await self.main.sync_stripe_user_command(message, SimpleNamespace(args="123"))

        reply = message.replies[0][0]
        self.assertIn("Stripe subscription пользователя изменилась", reply)
        self.assertIn("Повторите /sync_stripe_user", reply)
        self.assertNotIn("sub_A", reply)
        self.assertNotIn("sub_B", reply)
        self.assertEqual(write_conn.rollbacks, 1)
        self.assertEqual(write_conn.commits, 0)
        self.assertFalse(any("UPDATE users" in query for query, _ in write_conn.cursor_obj.queries))
        self.assertFalse(any("manual_stripe_sync" in str(params) for _, params in write_conn.cursor_obj.queries))
        self.assertEqual(self.delivery_inserts(write_conn), [])

    async def test_sync_stripe_user_identity_changed_active_without_period_does_not_clear_failure_state(self):
        user = (False, None, "sub_A", "cus_old", True, datetime.utcnow() + timedelta(hours=1), False)
        read_conn = FakeConnection(fetches=[user])
        write_conn = FakeConnection(fetches=[("sub_B", "cus_newer", None)])
        message = FakeIncomingMessage(user_id=1)
        subscription = SimpleNamespace(status="active", current_period_end=None, customer="cus_A", cancel_at_period_end=False)
        invoices = SimpleNamespace(data=[])

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn]), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(side_effect=[subscription, invoices])):
            await self.main.sync_stripe_user_command(message, SimpleNamespace(args="123"))

        reply = message.replies[0][0]
        self.assertIn("Stripe subscription пользователя изменилась", reply)
        self.assertNotIn("sub_A", reply)
        self.assertNotIn("sub_B", reply)
        self.assertEqual(write_conn.rollbacks, 1)
        self.assertEqual(write_conn.commits, 0)
        self.assertFalse(any("UPDATE users" in query for query, _ in write_conn.cursor_obj.queries))

    async def test_restore_access_stripe_identity_changed_fails_without_delivery(self):
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        user = (False, None, False, None, False, "sub_A", "cus_old", True)
        read_conn = FakeConnection(fetches=[user])
        sync_conn = FakeConnection(fetches=[("sub_B", None)])
        subscription = SimpleNamespace(status="active", current_period_end=period_end, customer="cus_A", cancel_at_period_end=False)

        with patch.object(self.main, "get_db_conn", side_effect=[read_conn, sync_conn]), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as member:
            result = await self.main.execute_confirmed_restore_access({
                "telegram_id": 123,
                "admin_id": 1,
                "action_id": "act1",
            })

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["reason"], "stripe_identity_changed")
        self.assertIn("Stripe subscription пользователя изменилась", result["admin_message"])
        self.assertNotIn("sub_A", result["admin_message"])
        self.assertNotIn("sub_B", result["admin_message"])
        self.assertEqual(sync_conn.rollbacks, 1)
        self.assertEqual(sync_conn.commits, 0)
        self.assertFalse(any("UPDATE users" in query for query, _ in sync_conn.cursor_obj.queries))
        self.assertFalse(any("restore_access_stripe_sync" in str(params) for _, params in sync_conn.cursor_obj.queries))
        self.assertEqual(self.delivery_inserts(sync_conn), [])
        member.assert_not_awaited()

    async def test_manual_give_access_and_set_expiry_enqueue_repair_without_direct_invite(self):
        give_conn = FakeConnection(fetches=[None, ("access-restore:act-give:123",)])
        set_conn = FakeConnection(fetches=[(datetime.utcnow(),), ("access-restore:act-set:123",)])
        log_access_event = AsyncMock()

        with patch.object(self.main, "get_db_conn", side_effect=[give_conn, set_conn]), \
             patch.object(self.main, "log_access_event", log_access_event), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban:
            give_result = await self.main.perform_give_access({
                "telegram_id": 123,
                "days": 30,
                "admin_id": 1,
                "action_id": "act-give",
            })
            set_expiry = datetime.utcnow() + timedelta(days=60)
            set_result = await self.main.perform_set_expiry({
                "telegram_id": 123,
                "expiry_date": set_expiry.isoformat(),
                "expiry_text": "01.10.2026 23:59 MSK",
                "admin_id": 1,
                "action_id": "act-set",
            })

        self.assertTrue(give_result["repair_enqueued"])
        self.assertTrue(set_result["repair_enqueued"])
        self.assertEqual(self.delivery_inserts(give_conn)[0][0], "access-restore:act-give:123")
        self.assertEqual(self.delivery_inserts(set_conn)[0][0], "access-restore:act-set:123")
        send_message.assert_not_awaited()
        create_link.assert_not_awaited()
        unban.assert_not_awaited()

    async def test_active_paid_user_is_not_removed_by_subscription_scheduler_guard(self):
        claim_conn = FakeConnection(fetches=[None, (123,)])
        active_expiry = datetime.utcnow() + timedelta(days=3)
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "fetch_subscription_removal_user", return_value=(
                 True,
                 active_expiry,
                 "sub_123",
                 False,
                 None,
                 None,
                 True,
                 "cus_123",
             )), \
             patch.object(self.main, "mark_subscription_removal_short") as mark_short, \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock()) as ban:
            result = await self.main.ban_user_logic(123)

        self.assertEqual(result, "active_in_db")
        mark_short.assert_called_with(123, "superseded", "active_access_in_db")
        ban.assert_not_awaited()

    def test_manual_revoke_removal_is_not_due_after_new_access(self):
        revoked_at = datetime.utcnow() - timedelta(hours=2)
        active_expiry = datetime.utcnow() + timedelta(days=5)
        conn = FakeConnection(fetches=[
            ("manual_access_revoked",),
            (revoked_at,),
            (True, active_expiry),
        ])

        with patch.object(self.main, "get_db_conn", return_value=conn):
            self.assertFalse(self.main.subscription_refund_group_removal_still_due(123))

        queries = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("manual_access_revoked", str(conn.cursor_obj.queries))
        self.assertNotIn("subscription_refund_reconciliations", queries)

    def test_new_revoke_reopens_superseded_removal_cycle(self):
        now = datetime.utcnow()
        conn = FakeConnection()

        self.main.enqueue_subscription_refund_group_removal(
            conn.cursor_obj,
            123,
            reason="manual_access_revoked",
            revoke_started_at=now,
        )

        query, params = conn.cursor_obj.queries[-1]
        self.assertIn("ON CONFLICT (telegram_id) DO UPDATE", query)
        self.assertIn("status = 'pending'", query)
        self.assertIn("owner_id = NULL", query)
        self.assertIn("claimed_at = NULL", query)
        self.assertIn("lease_until = NULL", query)
        self.assertIn("telegram_removed_at = NULL", query)
        self.assertIn("db_finalized_at = NULL", query)
        self.assertIn("attempt_count = 0", query)
        self.assertIn("revoke_started_at = EXCLUDED.revoke_started_at", query)
        self.assertEqual(params, (123, "manual_access_revoked", now))

    def test_superseded_removal_without_new_revoke_is_not_claimed(self):
        conn = FakeConnection(fetches=[(
            "superseded", None, None, "manual_access_revoked",
            None, None, 1, datetime.utcnow(), datetime.utcnow(),
        )])

        result = self.main.claim_subscription_removal(conn.cursor_obj, 123, "subscription_expired")

        self.assertEqual(result, "superseded")
        queries = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertNotIn("SET status = 'processing'", queries)

    async def test_stale_access_revoke_removal_is_superseded_without_telegram_kick(self):
        claim_conn = FakeConnection(fetches=[])
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "claim_subscription_removal", return_value="claimed"), \
             patch.object(self.main, "fetch_subscription_removal_user", return_value=(
                 False,
                 datetime.utcnow() - timedelta(days=1),
                 None,
                 False,
                 None,
                 None,
                 False,
                 None,
             )), \
             patch.object(self.main, "refresh_active_stripe_subscription", AsyncMock(return_value=False)), \
             patch.object(self.main, "subscription_refund_group_removal_still_due", return_value=False), \
             patch.object(self.main, "mark_subscription_removal_short") as mark_short, \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock()) as ban:
            result = await self.main.ban_user_logic(123)

        self.assertEqual(result, "access_revoke_no_longer_current")
        mark_short.assert_called_with(123, "superseded", "access_revoke_no_longer_current")
        ban.assert_not_awaited()

    async def test_second_revoke_after_superseded_can_reach_telegram_removal(self):
        claim_conn = FakeConnection()
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "claim_subscription_removal", return_value="claimed"), \
             patch.object(self.main, "fetch_subscription_removal_user", return_value=(
                 False,
                 datetime.utcnow() - timedelta(days=1),
                 None,
                 False,
                 None,
                 None,
                 False,
                 None,
             )), \
             patch.object(self.main, "refresh_active_stripe_subscription", AsyncMock(return_value=False)), \
             patch.object(self.main, "subscription_refund_group_removal_still_due", return_value=True), \
             patch.object(self.main, "mark_subscription_removal_short") as mark_short, \
             patch.object(self.main, "finalize_subscription_removal_in_db") as finalize_db, \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="member"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()), \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock()) as ban, \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban:
            result = await self.main.ban_user_logic(123)

        self.assertEqual(result, "removed")
        ban.assert_awaited_once_with(chat_id=-100123, user_id=123)
        unban.assert_awaited_once_with(chat_id=-100123, user_id=123, only_if_banned=True)
        finalize_db.assert_called_once()
        self.assertEqual(finalize_db.call_args.args[0], 123)
        self.assertTrue(any(call.args[:2] == (123, "telegram_removed") for call in mark_short.call_args_list))

    async def test_live_past_due_recheck_creates_bounded_grace_without_removal(self):
        subscription = SimpleNamespace(status="past_due", current_period_end=None)
        first_failure = datetime.utcnow()
        grace_until = first_failure + timedelta(hours=48)
        with patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(
                 self.main,
                 "ensure_failed_renewal_grace_from_recheck",
                 return_value=(first_failure, grace_until),
             ) as ensure_grace:
            result = await self.main.refresh_active_stripe_subscription(123, "sub_retry")

        self.assertEqual(result, "STRIPE_GRACE_ACTIVE")
        ensure_grace.assert_called_once_with(123, "sub_retry")

    async def test_subscription_removal_ban_failure_is_retryable_and_not_finalized(self):
        claim_conn = FakeConnection()
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "claim_subscription_removal", return_value="claimed"), \
             patch.object(self.main, "fetch_subscription_removal_user", return_value=(
                 True, datetime.utcnow() - timedelta(days=1), "sub_retry", False, None, None, True, "cus_retry",
             )), \
             patch.object(self.main, "refresh_active_stripe_subscription", AsyncMock(return_value=False)), \
             patch.object(self.main, "subscription_refund_group_removal_still_due", return_value=True), \
             patch.object(self.main, "mark_subscription_removal_short") as mark_short, \
             patch.object(self.main, "finalize_subscription_removal_in_db") as finalize_db, \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="member"))), \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock(side_effect=TelegramNetworkError(method=None, message="network"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban:
            result = await self.main.ban_user_logic(123)

        self.assertEqual(result, "kick_failed")
        finalize_db.assert_not_called()
        unban.assert_not_awaited()
        self.assertTrue(any(call.args[:2] == (123, "telegram_failed") for call in mark_short.call_args_list))

    async def test_subscription_removal_unban_failure_keeps_db_open_for_retry(self):
        claim_conn = FakeConnection()
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "claim_subscription_removal", return_value="claimed"), \
             patch.object(self.main, "fetch_subscription_removal_user", return_value=(
                 True, datetime.utcnow() - timedelta(days=1), "sub_retry", False, None, None, True, "cus_retry",
             )), \
             patch.object(self.main, "refresh_active_stripe_subscription", AsyncMock(return_value=False)), \
             patch.object(self.main, "subscription_refund_group_removal_still_due", return_value=True), \
             patch.object(self.main, "mark_subscription_removal_short") as mark_short, \
             patch.object(self.main, "finalize_subscription_removal_in_db") as finalize_db, \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="member"))), \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock()) as ban, \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramNetworkError(method=None, message="network"))) as unban:
            result = await self.main.ban_user_logic(123)

        self.assertEqual(result, "unban_failed")
        ban.assert_awaited_once()
        unban.assert_awaited_once_with(chat_id=-100123, user_id=123, only_if_banned=True)
        finalize_db.assert_not_called()
        self.assertTrue(any(call.args[:2] == (123, "telegram_failed") for call in mark_short.call_args_list))

    async def test_failed_renewal_billing_portal_action_uses_canonical_customer(self):
        conn = FakeConnection(fetches=[("cus_canonical",)])
        with patch.object(self.main, "get_db_conn", return_value=conn), \
             patch.object(self.main, "create_billing_portal_url", AsyncMock(return_value="https://billing.example/session")) as create_portal:
            keyboard = await self.main.stripe_delivery_reply_markup_for_user(
                {"keyboard_kind": "billing_portal"},
                123,
            )

        create_portal.assert_awaited_once_with("cus_canonical")
        button = keyboard.inline_keyboard[0][0]
        self.assertEqual(button.text, "Обновить способ оплаты")
        self.assertEqual(button.url, "https://billing.example/session")

    async def test_expired_failed_renewal_active_subscription_is_synced_not_cancelled(self):
        subscription = SimpleNamespace(status="active")
        with patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)) as to_thread, \
             patch.object(self.main, "refresh_active_stripe_subscription", AsyncMock(return_value="STRIPE_ACTIVE")) as refresh:
            result = await self.main.cancel_failed_renewal_subscription_after_grace(123, "sub_recovered")

        self.assertEqual(result, "recovered")
        refresh.assert_awaited_once_with(123, "sub_recovered")
        self.assertEqual(to_thread.await_count, 1)

    async def test_expired_failed_renewal_past_due_is_cancelled_after_live_recheck(self):
        subscription = SimpleNamespace(status="past_due", latest_invoice=None)
        with patch.object(
            self.main.asyncio,
            "to_thread",
            AsyncMock(side_effect=[subscription, SimpleNamespace(status="canceled")]),
        ) as to_thread:
            result = await self.main.cancel_failed_renewal_subscription_after_grace(123, "sub_past_due")

        self.assertEqual(result, "canceled")
        self.assertEqual(to_thread.await_count, 2)
        self.assertEqual(to_thread.await_args_list[0].args[0].__name__, "retrieve")
        self.assertEqual(to_thread.await_args_list[1].args[0].__name__, "delete")
        self.assertEqual(to_thread.await_args_list[1].args[1], "sub_past_due")

    async def test_paid_latest_invoice_prevents_cancellation_during_status_transition(self):
        subscription = SimpleNamespace(status="past_due", latest_invoice="in_recovered")
        invoice = SimpleNamespace(status="paid", paid=True)
        with patch.object(
            self.main.asyncio,
            "to_thread",
            AsyncMock(side_effect=[subscription, invoice]),
        ) as to_thread, patch.object(
            self.main,
            "refresh_active_stripe_subscription",
            AsyncMock(return_value="STRIPE_ACTIVE"),
        ) as refresh:
            result = await self.main.cancel_failed_renewal_subscription_after_grace(123, "sub_recovered")

        self.assertEqual(result, "recovered")
        self.assertEqual(to_thread.await_count, 2)
        self.assertEqual(to_thread.await_args_list[1].args[0].__name__, "retrieve")
        self.assertEqual(to_thread.await_args_list[1].args[1], "in_recovered")
        refresh.assert_awaited_once_with(123, "sub_recovered")

    async def test_expired_failed_renewal_already_canceled_is_idempotent(self):
        subscription = SimpleNamespace(status="canceled")
        with patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)) as to_thread:
            result = await self.main.cancel_failed_renewal_subscription_after_grace(123, "sub_canceled")

        self.assertEqual(result, "already_canceled")
        self.assertEqual(to_thread.await_count, 1)

    async def test_cancel_response_lost_then_remote_canceled_is_retryable_without_secret_logs(self):
        raw_subscription_id = "sub_secret_response_lost"
        raw_error = "timeout after cancel for sub_secret_response_lost"
        with patch.object(
            self.main.asyncio,
            "to_thread",
            AsyncMock(side_effect=[
                SimpleNamespace(status="past_due", latest_invoice=None),
                TimeoutError(raw_error),
                SimpleNamespace(status="canceled"),
            ]),
        ), patch.object(self.main, "notify_admins", AsyncMock()) as notify_admins, \
             self.assertLogs(level="ERROR") as captured:
            first = await self.main.cancel_failed_renewal_subscription_after_grace(123, raw_subscription_id)
            second = await self.main.cancel_failed_renewal_subscription_after_grace(123, raw_subscription_id)

        self.assertEqual(first, "cancel_failed")
        self.assertEqual(second, "already_canceled")
        self.assertNotIn(raw_subscription_id, "\n".join(captured.output))
        self.assertNotIn(raw_error, "\n".join(captured.output))
        self.assertNotIn(raw_subscription_id, notify_admins.await_args.args[0])

    async def test_expired_failed_renewal_cancel_failure_blocks_telegram_and_local_finalization(self):
        claim_conn = FakeConnection()
        expired = datetime.utcnow() - timedelta(days=3)
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "claim_subscription_removal", return_value="claimed"), \
             patch.object(self.main, "fetch_subscription_removal_user", return_value=(
                 True, expired, "sub_retry", True, expired, expired + timedelta(days=2), True, "cus_retry",
             )), \
             patch.object(self.main, "refresh_active_stripe_subscription", AsyncMock(return_value=False)), \
             patch.object(
                 self.main,
                 "cancel_failed_renewal_subscription_after_grace",
                 AsyncMock(return_value="cancel_failed"),
             ), \
             patch.object(self.main, "mark_subscription_removal_short") as mark_short, \
             patch.object(self.main, "finalize_subscription_removal_in_db") as finalize_db, \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as get_member, \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock()) as ban:
            result = await self.main.ban_user_logic(123)

        self.assertEqual(result, "stripe_cancel_failed")
        mark_short.assert_called_with(123, "pending", "stripe_cancellation_cancel_failed")
        get_member.assert_not_awaited()
        ban.assert_not_awaited()
        finalize_db.assert_not_called()

    async def test_expired_failed_renewal_cancels_then_removes_and_finalizes(self):
        claim_conn = FakeConnection()
        expired = datetime.utcnow() - timedelta(days=3)
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "claim_subscription_removal", return_value="claimed"), \
             patch.object(self.main, "fetch_subscription_removal_user", return_value=(
                 True, expired, "sub_retry", True, expired, expired + timedelta(days=2), True, "cus_retry",
             )), \
             patch.object(self.main, "refresh_active_stripe_subscription", AsyncMock(return_value=False)), \
             patch.object(
                 self.main,
                 "cancel_failed_renewal_subscription_after_grace",
                 AsyncMock(return_value="canceled"),
             ) as cancel_subscription, \
             patch.object(self.main, "subscription_refund_group_removal_still_due", return_value=True), \
             patch.object(self.main, "mark_subscription_removal_short"), \
             patch.object(self.main, "finalize_subscription_removal_in_db") as finalize_db, \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="member"))), \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock()) as ban, \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban:
            result = await self.main.ban_user_logic(123)

        self.assertEqual(result, "removed")
        cancel_subscription.assert_awaited_once_with(123, "sub_retry")
        ban.assert_awaited_once()
        unban.assert_awaited_once_with(chat_id=-100123, user_id=123, only_if_banned=True)
        finalize_db.assert_called_once_with(
            123,
            expired,
            subscription_cancelled_after_grace=True,
        )

    async def test_canceled_subscription_then_telegram_failure_remains_retryable(self):
        claim_conn = FakeConnection()
        expired = datetime.utcnow() - timedelta(days=3)
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "claim_subscription_removal", return_value="claimed"), \
             patch.object(self.main, "fetch_subscription_removal_user", return_value=(
                 True, expired, "sub_retry", True, expired, expired + timedelta(days=2), True, "cus_retry",
             )), \
             patch.object(self.main, "refresh_active_stripe_subscription", AsyncMock(return_value=False)), \
             patch.object(
                 self.main,
                 "cancel_failed_renewal_subscription_after_grace",
                 AsyncMock(return_value="already_canceled"),
             ) as cancel_subscription, \
             patch.object(self.main, "subscription_refund_group_removal_still_due", return_value=True), \
             patch.object(self.main, "mark_subscription_removal_short") as mark_short, \
             patch.object(self.main, "finalize_subscription_removal_in_db") as finalize_db, \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="member"))), \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock(side_effect=TelegramNetworkError(method=None, message="network"))):
            result = await self.main.ban_user_logic(123)

        self.assertEqual(result, "kick_failed")
        cancel_subscription.assert_awaited_once_with(123, "sub_retry")
        finalize_db.assert_not_called()
        self.assertTrue(any(call.args[:2] == (123, "telegram_failed") for call in mark_short.call_args_list))

    async def test_canceled_subscription_unban_failure_retries_and_then_finalizes(self):
        claim_conn = FakeConnection()
        expired = datetime.utcnow() - timedelta(days=3)
        user_row = (
            True, expired, "sub_retry", True, expired, expired + timedelta(days=2), True, "cus_retry",
        )
        canceled_at = datetime.utcnow() - timedelta(minutes=1)
        banned_at = datetime.utcnow()
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "claim_subscription_removal", return_value="claimed"), \
             patch.object(self.main, "fetch_subscription_removal_user", return_value=user_row), \
             patch.object(self.main, "fetch_subscription_removal_context", side_effect=[
                 ("sub_retry", expired, canceled_at, None, None, "processing"),
                 ("sub_retry", expired, canceled_at, banned_at, None, "telegram_failed"),
             ]), \
             patch.object(self.main, "refresh_active_stripe_subscription", AsyncMock(return_value=False)), \
             patch.object(
                 self.main,
                 "cancel_failed_renewal_subscription_after_grace",
                 AsyncMock(return_value="already_canceled"),
             ) as cancel_subscription, \
             patch.object(self.main, "subscription_refund_group_removal_still_due", return_value=True), \
             patch.object(self.main, "mark_subscription_removal_short"), \
             patch.object(self.main, "finalize_subscription_removal_in_db") as finalize_db, \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="member"))), \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock()) as ban, \
             patch.object(
                 self.main.bot,
                 "unban_chat_member",
                 AsyncMock(side_effect=[TelegramNetworkError(method=None, message="network"), None]),
             ) as unban:
            first = await self.main.ban_user_logic(123)
            second = await self.main.ban_user_logic(123)

        self.assertEqual(first, "unban_failed")
        self.assertEqual(second, "removed")
        cancel_subscription.assert_not_awaited()
        self.assertEqual(ban.await_count, 1)
        self.assertEqual(unban.await_count, 2)
        finalize_db.assert_called_once_with(
            123,
            expired,
            subscription_cancelled_after_grace=True,
        )

    async def test_completed_failed_renewal_removal_duplicate_is_idempotent(self):
        claim_conn = FakeConnection()
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "claim_subscription_removal", return_value="already_finalized"), \
             patch.object(self.main, "cancel_failed_renewal_subscription_after_grace", AsyncMock()) as cancel_subscription, \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock()) as ban:
            result = await self.main.ban_user_logic(123)

        self.assertEqual(result, "already_finalized")
        cancel_subscription.assert_not_awaited()
        ban.assert_not_awaited()

    async def test_recovered_access_after_failed_unban_is_unbanned_and_not_finalized(self):
        claim_conn = FakeConnection()
        future_expiry = datetime.utcnow() + timedelta(days=30)
        banned_at = datetime.utcnow() - timedelta(minutes=5)
        with patch.object(self.main, "get_db_conn", return_value=claim_conn), \
             patch.object(self.main, "claim_subscription_removal", return_value="claimed"), \
             patch.object(self.main, "fetch_subscription_removal_user", return_value=(
                 True, future_expiry, "sub_new", False, None, None, True, "cus_new",
             )), \
             patch.object(self.main, "fetch_subscription_removal_context", return_value=(
                 "sub_old", future_expiry - timedelta(days=31), banned_at, banned_at, None, "telegram_failed",
             )), \
             patch.object(self.main, "mark_subscription_removal_short") as mark_short, \
             patch.object(self.main, "finalize_subscription_removal_in_db") as finalize_db, \
             patch.object(self.main.bot, "ban_chat_member", AsyncMock()) as ban, \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban:
            result = await self.main.ban_user_logic(123)

        self.assertEqual(result, "active_in_db")
        unban.assert_awaited_once_with(chat_id=-100123, user_id=123, only_if_banned=True)
        ban.assert_not_awaited()
        finalize_db.assert_not_called()
        mark_short.assert_called_with(123, "superseded", "active_access_in_db")

    def test_real_aiogram_bot_api_has_no_kick_and_production_uses_ban_unban(self):
        from aiogram import Bot

        self.assertFalse(hasattr(Bot, "kick_chat_member"))
        self.assertTrue(hasattr(Bot, "ban_chat_member"))
        self.assertTrue(hasattr(Bot, "unban_chat_member"))
        source = Path(self.main.__file__).read_text()
        self.assertNotIn("bot.kick_chat_member", source)
        removal = source[source.index("async def ban_user_logic"):source.index("async def check_subscriptions_and_reminders")]
        self.assertIn("bot.ban_chat_member", removal)
        self.assertIn("only_if_banned=True", removal)

    async def test_postgres_fsm_storage_module_imports_independently(self):
        import postgres_fsm_storage

        source = Path(self.main.__file__).read_text()
        self.assertNotIn("class PostgresFSMStorage", source)
        self.assertIs(self.main.PostgresFSMStorage, postgres_fsm_storage.PostgresFSMStorage)

    async def test_postgres_fsm_storage_decodes_dict_string_null_and_invalid_data(self):
        from postgres_fsm_storage import decode_fsm_data

        self.assertEqual(decode_fsm_data({"step": 1}), {"step": 1})
        self.assertEqual(decode_fsm_data('{"step":1}'), {"step": 1})
        self.assertEqual(decode_fsm_data(None), {})
        self.assertEqual(decode_fsm_data(""), {})
        with self.assertLogs(level="WARNING"):
            self.assertEqual(decode_fsm_data("{bad-json"), {})
        with self.assertLogs(level="WARNING"):
            self.assertEqual(decode_fsm_data("[1,2]"), {})
        with self.assertLogs(level="WARNING"):
            self.assertEqual(decode_fsm_data(["unexpected"]), {})

    async def test_postgres_fsm_storage_round_trip_and_clear(self):
        from aiogram.fsm.storage.base import StorageKey
        from postgres_fsm_storage import PostgresFSMStorage

        records = {}

        class FsmCursor:
            def __init__(self):
                self.row = None

            def execute(self, query, params=None):
                params = tuple(params or ())
                normalized = " ".join(query.split())
                key = params[:6]
                if normalized.startswith("INSERT INTO aiogram_fsm_states") and "EXCLUDED.state" in normalized:
                    records.setdefault(key, {"state": None, "data_json": {}})["state"] = params[6]
                    return
                if normalized.startswith("INSERT INTO aiogram_fsm_states") and "EXCLUDED.data_json" in normalized:
                    records.setdefault(key, {"state": None, "data_json": {}})["data_json"] = adapted_json_value(params[6])
                    return
                if normalized.startswith("DELETE FROM aiogram_fsm_states"):
                    record = records.get(key)
                    if record and record["state"] is None and record["data_json"] == {}:
                        records.pop(key)
                    return
                if normalized.startswith("SELECT state"):
                    record = records.get(key)
                    self.row = (record["state"],) if record else None
                    return
                if normalized.startswith("SELECT data_json"):
                    record = records.get(key)
                    self.row = (record["data_json"],) if record else None

            def fetchone(self):
                return self.row

            def close(self):
                pass

        class FsmConnection:
            def __init__(self):
                self.cursor_obj = FsmCursor()
                self.closed = False
                self.commits = 0
                self.rollbacks = 0

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                self.commits += 1

            def rollback(self):
                self.rollbacks += 1

            def close(self):
                self.closed = True

        storage = PostgresFSMStorage(FsmConnection)
        key = StorageKey(bot_id=10, chat_id=20, user_id=30)

        await storage.set_state(key, self.main.ContactState.waiting_for_message)
        await storage.set_data(key, {"reply_to_user": 42, "text": "hello"})

        self.assertEqual(await storage.get_state(key), self.main.ContactState.waiting_for_message.state)
        self.assertEqual(await storage.get_data(key), {"reply_to_user": 42, "text": "hello"})

        await storage.set_state(key, None)
        self.assertEqual(await storage.get_data(key), {"reply_to_user": 42, "text": "hello"})
        await storage.set_data(key, {})

        self.assertIsNone(await storage.get_state(key))
        self.assertEqual(await storage.get_data(key), {})
        self.assertEqual(records, {})

    async def test_postgres_fsm_storage_concurrent_update_data_keeps_all_fields(self):
        from aiogram.fsm.storage.base import StorageKey
        from postgres_fsm_storage import PostgresFSMStorage

        records = {}
        record_lock = threading.Lock()
        connections = []

        class FsmCursor:
            def __init__(self):
                self.row = None
                self.lock_held = False

            def execute(self, query, params=None):
                params = tuple(params or ())
                normalized = " ".join(query.split())
                if normalized.startswith("INSERT INTO aiogram_fsm_states"):
                    key = params[:6]
                    with record_lock:
                        records.setdefault(key, {"state": None, "data_json": {}})
                    return
                if normalized.startswith("SELECT data_json") and "FOR UPDATE" in normalized:
                    key = params[:6]
                    record_lock.acquire()
                    self.lock_held = True
                    record = records.get(key, {"data_json": {}})
                    self.row = (record["data_json"],)
                    return
                if normalized.startswith("SELECT data_json"):
                    key = params[:6]
                    record = records.get(key, {"data_json": {}})
                    self.row = (record["data_json"],)
                    return
                if normalized.startswith("UPDATE aiogram_fsm_states"):
                    data_json = adapted_json_value(params[0])
                    key = params[1:7]
                    records.setdefault(key, {"state": None, "data_json": {}})["data_json"] = data_json
                    return
                if normalized.startswith("DELETE FROM aiogram_fsm_states"):
                    key = params[:6]
                    record = records.get(key)
                    if record and record["state"] is None and record["data_json"] == {}:
                        records.pop(key)

            def fetchone(self):
                return self.row

            def close(self):
                if self.lock_held:
                    self.lock_held = False
                    record_lock.release()

        class FsmConnection:
            def __init__(self):
                self.cursor_obj = FsmCursor()
                self.closed = False
                connections.append(self)

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                pass

            def rollback(self):
                pass

            def close(self):
                self.closed = True

        key = StorageKey(bot_id=10, chat_id=20, user_id=30)
        first = PostgresFSMStorage(FsmConnection)
        second = PostgresFSMStorage(FsmConnection)

        await asyncio.gather(
            first.update_data(key, {"first": 1}),
            second.update_data(key, {"second": 2}),
        )

        self.assertEqual(await first.get_data(key), {"first": 1, "second": 2})
        self.assertTrue(all(conn.closed for conn in connections))

    async def test_first_purchase_recovery_delivery_skips_when_no_longer_due(self):
        delivery = (
            self.main.first_purchase_recovery_delivery_key(123),
            123,
            "first_purchase_recovery_reminder",
            json.dumps({"text": "retry", "keyboard_kind": "retry_payment"}),
            1,
            None,
            1,
        )
        claim_conn = FakeConnection(fetches=[[delivery]])
        check_conn = FakeConnection(fetches=[None])
        conns = iter([claim_conn, check_conn])

        with patch.object(self.main, "get_db_conn", side_effect=lambda: next(conns)), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(result["sent"], 0)
        self.assertEqual(result["retryable_failed"], 0)
        send_message.assert_not_awaited()
        self.assertTrue(claim_conn.closed)
        self.assertTrue(check_conn.closed)
        sql = "\n".join(query for query, _ in check_conn.cursor_obj.queries)
        self.assertIn("checkout_sessions", sql)
        self.assertIn("checkout_retry_events", sql)
        self.assertIn("status = 'cancelled'", sql)
        self.assertIn("UPDATE message_delivery_events", sql)

    async def test_first_purchase_recovery_delay_is_fixed_twenty_four_hours(self):
        self.assertEqual(self.main.FIRST_PURCHASE_RECOVERY_REMINDER_DELAY_HOURS, 24)

    async def test_first_purchase_recovery_user_text_is_final_retry_offer(self):
        self.assertEqual(
            self.main.first_purchase_recovery_reminder_text(),
            (
                "Похоже, вчера оформление доступа не завершилось.\n\n"
                "Мы всё проверили — сейчас можно попробовать ещё раз 🤍\n\n"
                "Если снова что-то не получится, просто напишите нам, и мы поможем."
            ),
        )

    async def test_first_purchase_recovery_key_is_hashed_and_stable(self):
        first = self.main.first_purchase_recovery_delivery_key(123456789)
        second = self.main.first_purchase_recovery_delivery_key(123456789)

        self.assertEqual(first, second)
        self.assertTrue(first.startswith("first_purchase_recovery:"))
        self.assertNotIn("123456789", first)
        self.assertNotIn("payment_failed_at", first)

    async def test_first_purchase_recovery_due_query_uses_checkout_attempts_and_not_payment_failed(self):
        row = (123, datetime(2026, 7, 30, 10, 0), "sub_1", "expired", "checkout_session", "checkout.session.expired")
        conn = FakeConnection(fetches=[[row]])

        due = self.main.fetch_due_first_purchase_recovery_users(conn.cursor_obj, limit=10)

        self.assertEqual(due, [row])
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("FROM checkout_sessions", sql)
        self.assertIn("FROM checkout_retry_events", sql)
        self.assertIn("attempt_status", sql)
        self.assertIn("attempt_source", sql)
        self.assertIn("attempt_error_context", sql)
        self.assertIn("status = ANY", sql)
        self.assertIn("NOW() AT TIME ZONE 'UTC'", sql)
        self.assertIn("payment_events", sql)
        self.assertIn("access_events", sql)
        self.assertIn("stripe_links", sql)
        self.assertNotIn("completed_cs.status = 'completed'", sql)
        self.assertNotIn("FROM checkout_sessions completed_cs", sql)
        self.assertNotIn("payment_failed = TRUE", sql)
        self.assertNotIn("u.stripe_subscription_id IS NULL", sql)
        self.assertNotIn("checkout_completed", sql)
        self.assertNotIn("sl.current_period_end IS NOT NULL", sql)
        self.assertNotIn("sl.current_period_end >", sql)

    async def test_first_purchase_recovery_checkout_retry_event_without_payment_failed_is_eligible(self):
        row = (123, datetime(2026, 7, 30, 10, 0), "sub_1", "checkout_retry_unresolved", "checkout_retry_event", None)
        conn = FakeConnection(fetches=[row])

        row = self.main.fetch_first_purchase_recovery_user_if_due(conn.cursor_obj, 123)

        self.assertEqual(row[0], 123)
        self.assertEqual(row[3], "checkout_retry_unresolved")
        self.assertEqual(row[4], "checkout_retry_event")
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("checkout_retry_events", sql)
        self.assertNotIn("payment_failed = TRUE", sql)

    async def test_first_purchase_recovery_attempt_statuses_are_real_checkout_states(self):
        self.assertEqual(
            self.main.FIRST_PURCHASE_RECOVERY_ATTEMPT_STATUSES,
            ("creating", "creation_unknown", "open", "expired", "failed", "completed"),
        )

    async def test_first_purchase_recovery_enqueue_uses_retry_payment_keyboard_and_one_delivery(self):
        conn = FakeConnection(fetches=[("first_purchase_recovery:key",)])
        created = self.main.enqueue_first_purchase_recovery_reminder(
            conn.cursor_obj,
            123456789,
            datetime(2026, 7, 30, 10, 0),
        )

        self.assertTrue(created)
        query, params = conn.cursor_obj.queries[0]
        self.assertIn("ON CONFLICT (delivery_key) DO UPDATE", query)
        self.assertIn("WHERE message_delivery_events.status = 'cancelled'", query)
        self.assertIn("sent_at = NULL", query)
        self.assertIn("first_purchase_recovery_reminder", query)
        self.assertNotIn("123456789", params[0])
        payload = json.loads(params[2])
        self.assertEqual(payload["text"], self.main.first_purchase_recovery_reminder_text())
        self.assertEqual(payload["keyboard_kind"], "retry_payment")
        self.assertEqual(payload["reason_category"], "unknown_payment_error")
        self.assertEqual(payload["stage"], "checkout")
        self.assertNotIn("123456789", payload["safe_ref"])

    async def test_first_purchase_recovery_enqueue_stores_safe_attempt_context(self):
        conn = FakeConnection(fetches=[("first_purchase_recovery:key",)])
        context = self.main.first_purchase_recovery_context(
            123456789,
            datetime(2026, 7, 30, 10, 0),
            tariff_code="sub_1",
            attempt_status="expired",
            attempt_source="checkout_session",
        )

        self.main.enqueue_first_purchase_recovery_reminder(
            conn.cursor_obj,
            123456789,
            datetime(2026, 7, 30, 10, 0),
            context,
        )

        payload = json.loads(conn.cursor_obj.queries[0][1][2])
        self.assertEqual(payload["reason_category"], "checkout_expired")
        self.assertEqual(payload["reason_label"], "ссылка оплаты истекла")
        self.assertEqual(payload["stage_label"], "Checkout")
        self.assertEqual(payload["tariff_code"], "sub_1")
        self.assertEqual(payload["attempt_status"], "expired")
        self.assertEqual(payload["attempt_source"], "checkout_session")
        self.assertEqual(payload["attempt_error_context"], "unknown")
        self.assertNotIn("123456789", payload["safe_ref"])

    async def test_first_purchase_recovery_context_priority_uses_allowlisted_error_tokens(self):
        cases = [
            ("failed", "checkout.session.async_payment_failed", "checkout_async_payment_failed"),
            ("expired", "checkout.session.expired", "checkout_expired"),
            ("completed", "invoice_payment_failed", "invoice_payment_failed"),
            ("completed", "invoice_payment_failed:card_declined", "card_declined"),
            ("completed", "invoice_payment_failed:insufficient_funds", "insufficient_funds"),
            ("completed", "invoice_payment_failed:authentication_required", "authentication_required"),
            ("creation_unknown", "stripe_api_unavailable", "stripe_api_unavailable"),
            ("failed", "checkout_creation_failed", "checkout_creation_failed"),
            ("completed", None, "payment_confirmation_pending"),
        ]

        for status, error_context, category in cases:
            with self.subTest(status=status, error_context=error_context):
                result, _ = self.main.classify_first_purchase_recovery_context(
                    attempt_status=status,
                    attempt_source="checkout_session",
                    attempt_error_context=error_context,
                )
                self.assertEqual(result, category)

    async def test_first_purchase_recovery_failed_without_safe_context_does_not_guess_creation_failure(self):
        category, _ = self.main.classify_first_purchase_recovery_context(
            attempt_status="failed",
            attempt_source="checkout_session",
            attempt_error_context="raw Stripe exception with invoice in_123 customer cus_123",
        )

        self.assertEqual(category, "unknown_payment_error")

    async def test_invoice_payment_failed_recovery_context_token_uses_safe_codes_only(self):
        self.assertEqual(
            self.main.invoice_payment_failed_recovery_context_token(None),
            "invoice_payment_failed",
        )
        self.assertEqual(
            self.main.invoice_payment_failed_recovery_context_token("card_declined"),
            "invoice_payment_failed:card_declined",
        )
        self.assertEqual(
            self.main.invoice_payment_failed_recovery_context_token("insufficient_funds"),
            "invoice_payment_failed:insufficient_funds",
        )
        self.assertEqual(
            self.main.invoice_payment_failed_recovery_context_token("authentication_required"),
            "invoice_payment_failed:authentication_required",
        )
        self.assertEqual(
            self.main.invoice_payment_failed_recovery_context_token("do_not_guess_card_declined"),
            "invoice_payment_failed",
        )

    async def test_checkout_creation_recovery_error_token_uses_only_safe_contexts(self):
        class APIConnectionError(Exception):
            pass

        class InvalidRequestError(Exception):
            pass

        api_token = self.main.checkout_creation_recovery_error_token(
            APIConnectionError("timeout talking to customer cus_raw")
        )
        invalid_token = self.main.checkout_creation_recovery_error_token(
            InvalidRequestError("No such price: price_raw")
        )
        unknown_token = self.main.checkout_creation_recovery_error_token(
            RuntimeError("unexpected customer cus_raw invoice in_raw person@example.com")
        )

        self.assertEqual(api_token, "stripe_api_unavailable")
        self.assertEqual(invalid_token, "checkout_creation_failed")
        self.assertEqual(unknown_token, "checkout_creation_failed")
        for token, expected_category in (
            (api_token, "stripe_api_unavailable"),
            (invalid_token, "checkout_creation_failed"),
            (unknown_token, "checkout_creation_failed"),
        ):
            with self.subTest(token=token):
                category, stage = self.main.classify_first_purchase_recovery_context(
                    attempt_status="failed",
                    attempt_source="checkout_session",
                    attempt_error_context=token,
                )
                self.assertEqual(category, expected_category)
                self.assertEqual(stage, "checkout_creation")

        payload = self.main.first_purchase_recovery_context(
            123456789,
            datetime(2026, 7, 30, 10, 0),
            tariff_code="sub_1",
            attempt_status="failed",
            attempt_source="checkout_session",
            attempt_error_context=unknown_token,
        )
        payload_text = json.dumps(payload, ensure_ascii=False)
        self.assertEqual(payload["attempt_error_context"], "checkout_creation_failed")
        self.assertNotIn("cus_raw", payload_text)
        self.assertNotIn("in_raw", payload_text)
        self.assertNotIn("person@example.com", payload_text)

    async def test_process_payment_checkout_failure_persists_token_not_raw_exception(self):
        source = Path(self.main.__file__).read_text(encoding="utf-8")

        self.assertIn("recovery_error_token = checkout_creation_recovery_error_token(e)", source)
        self.assertIn("mark_checkout_failed(\n                        failed_cur,", source)
        self.assertIn("recovery_error_token,\n                        status=failed_status,", source)
        self.assertNotIn('mark_checkout_failed(failed_cur, checkout_record["id"], e', source)

    async def test_first_purchase_recovery_payload_never_copies_raw_checkout_last_error(self):
        conn = FakeConnection(fetches=[("first_purchase_recovery:key",)])
        context = self.main.first_purchase_recovery_context(
            123456789,
            datetime(2026, 7, 30, 10, 0),
            tariff_code="sub_1",
            attempt_status="failed",
            attempt_source="checkout_session",
            attempt_error_context="raw Stripe exception customer cus_real invoice in_real email person@example.com",
        )

        self.main.enqueue_first_purchase_recovery_reminder(
            conn.cursor_obj,
            123456789,
            datetime(2026, 7, 30, 10, 0),
            context,
        )

        payload_text = conn.cursor_obj.queries[0][1][2]
        payload = json.loads(payload_text)
        self.assertEqual(payload["reason_category"], "unknown_payment_error")
        self.assertEqual(payload["attempt_error_context"], "unknown")
        self.assertNotIn("cus_real", payload_text)
        self.assertNotIn("in_real", payload_text)
        self.assertNotIn("person@example.com", payload_text)

    async def test_first_purchase_recovery_admin_sent_notice_is_log_only(self):
        recovery_key = self.main.first_purchase_recovery_delivery_key(123456789)
        conn = FakeConnection(fetches=[("admin1",), ("admin2",)])
        payload = self.main.stripe_delivery_payload(
            self.main.first_purchase_recovery_reminder_text(),
            reason_label="ссылка оплаты истекла",
            stage_label="Checkout",
            tariff_code="sub_1",
            safe_ref="first_purchase_recovery:abc123",
        )

        created = self.main.enqueue_first_purchase_recovery_admin_sent_notices(
            conn.cursor_obj,
            recovery_key,
            123456789,
            payload,
        )

        self.assertEqual(created, 0)
        keys = [params[0] for query, params in conn.cursor_obj.queries if "INSERT INTO message_delivery_events" in query]
        self.assertEqual(keys, [])

    async def test_first_purchase_recovery_admin_sent_text_formats_attempt_time_and_invalid_date(self):
        text = self.main.build_first_purchase_recovery_admin_sent_text(
            123456789,
            {
                "attempted_at": "2026-07-30T10:15:00+03:00",
                "stage_label": "ошибка invoice",
                "reason_label": "банк отклонил карту",
                "tariff_code": "unknown",
                "safe_ref": "first_purchase_recovery:safe",
            },
        )

        self.assertIn("🔁 Повторная попытка оплаты предложена", text)
        self.assertIn("Последняя попытка: 30.07.2026 07:15 UTC", text)
        self.assertIn("Напоминание: отправлено", text)
        self.assertIn("Тариф: не определён", text)
        self.assertIn("Reference: first_purchase_recovery:safe", text)
        self.assertNotIn("2026-07-30T10:15:00+03:00", text)

        invalid_text = self.main.build_first_purchase_recovery_admin_sent_text(
            123456789,
            {
                "attempted_at": "not-a-date with raw_error",
                "stage_label": "Checkout",
                "reason_label": "оплата не завершилась",
                "tariff_code": None,
                "safe_ref": "first_purchase_recovery:safe",
            },
        )
        self.assertIn("Последняя попытка: не определена", invalid_text)
        self.assertNotIn("not-a-date", invalid_text)
        self.assertNotIn("raw_error", invalid_text)

    async def test_first_purchase_recovery_success_is_log_only_after_user_send(self):
        delivery_key = self.main.first_purchase_recovery_delivery_key(123)
        payload = json.dumps({
            "text": self.main.first_purchase_recovery_reminder_text(),
            "keyboard_kind": "retry_payment",
            "reason_label": "ссылка оплаты истекла",
            "stage_label": "Checkout",
            "tariff_code": "sub_1",
            "safe_ref": "first_purchase_recovery:test",
        })
        delivery = (delivery_key, 123, "first_purchase_recovery_reminder", payload, 1, None, 1)
        claim_conn = FakeConnection(fetches=[[delivery]])
        check_conn = FakeConnection(fetches=[(123, datetime(2026, 7, 30, 10, 0), "sub_1", "expired", "checkout_session", None)])
        sent_conn = FakeConnection(fetches=[("admin1",), ("admin2",)])
        conns = iter([claim_conn, check_conn, sent_conn])
        closed_before_send = []

        async def send_after_close(*args, **kwargs):
            closed_before_send.append((claim_conn.closed, check_conn.closed))

        with patch.object(self.main, "get_db_conn", side_effect=lambda: next(conns)), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=send_after_close)) as send_message:
            result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(result["sent"], 1)
        self.assertEqual(closed_before_send, [(True, True)])
        send_message.assert_awaited_once()
        sql = "\n".join(query for query, _ in sent_conn.cursor_obj.queries)
        delivery_keys = [params[0] for query, params in sent_conn.cursor_obj.queries if "INSERT INTO message_delivery_events" in query]
        self.assertFalse(any(key.startswith("first_purchase_recovery_admin_sent:") for key in delivery_keys))
        self.assertEqual(sent_conn.commits, 1)

    async def test_first_purchase_recovery_user_failure_does_not_enqueue_admin_sent_notice(self):
        from aiogram.exceptions import TelegramNetworkError

        delivery_key = self.main.first_purchase_recovery_delivery_key(123)
        payload = json.dumps({"text": "retry", "keyboard_kind": "retry_payment"})
        delivery = (delivery_key, 123, "first_purchase_recovery_reminder", payload, 1, None, 1)
        claim_conn = FakeConnection(fetches=[[delivery]])
        check_conn = FakeConnection(fetches=[(123, datetime(2026, 7, 30, 10, 0), None, None, None, None)])
        fail_conn = FakeConnection()
        conns = iter([claim_conn, check_conn, fail_conn])

        with patch.object(self.main, "get_db_conn", side_effect=lambda: next(conns)), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramNetworkError(method=None, message="network"))), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries(limit=1)

        self.assertEqual(result["retryable_failed"], 1)
        sql = "\n".join(query for query, _ in fail_conn.cursor_obj.queries)
        self.assertIn("UPDATE message_delivery_events", sql)
        self.assertNotIn("first_purchase_recovery_admin_sent", sql)

    async def test_first_purchase_recovery_success_cancels_pending_failed_processing(self):
        conn = FakeConnection()

        self.main.cancel_first_purchase_recovery_deliveries(conn.cursor_obj, 123456789, reason="paid")

        query, params = conn.cursor_obj.queries[0]
        self.assertIn("status = 'cancelled'", query)
        self.assertIn("status IN ('pending', 'failed', 'processing')", query)
        self.assertNotIn("123456789", params[1])
        self.assertEqual(params[2], 123456789)

    async def test_first_purchase_recovery_recheck_excludes_current_processing_delivery(self):
        delivery_key = self.main.first_purchase_recovery_delivery_key(123)
        conn = FakeConnection(fetches=[(123, datetime(2026, 7, 30, 10, 0))])

        self.assertTrue(self.main.first_purchase_recovery_reminder_still_due(conn.cursor_obj, 123, delivery_key))

        query, params = conn.cursor_obj.queries[0]
        self.assertIn("md.delivery_key <> %s", query)
        self.assertEqual(params[-2:], [delivery_key, delivery_key])

    async def test_stats_command_closes_db_before_reply(self):
        conn = FakeConnection(fetches=[(10,), (4,), (6,), (2,), (1,), (0,), (0,), (1,), (0,)])
        message = FakeIncomingMessage(user_id=1)
        closed_during_answer = []

        async def answer_after_close(*args, **kwargs):
            closed_during_answer.append(conn.closed)

        message.answer = AsyncMock(side_effect=answer_after_close)
        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.stats_command(message)

        self.assertEqual(closed_during_answer, [True])

    async def test_test_grace_closes_db_before_telegram_awaits(self):
        conn = FakeConnection()
        message = FakeIncomingMessage(user_id=1)
        command = SimpleNamespace(args="123")
        closed_during_reply = []
        closed_during_send = []

        async def reply_after_close(*args, **kwargs):
            closed_during_reply.append(conn.closed)

        async def send_after_close(*args, **kwargs):
            closed_during_send.append(conn.closed)

        message.reply = AsyncMock(side_effect=reply_after_close)
        with patch.object(self.main, "get_db_conn", return_value=conn), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=send_after_close)):
            await self.main.test_grace(message, command)

        self.assertEqual(closed_during_reply, [True])
        self.assertEqual(closed_during_send, [True])

    async def test_test_followup_closes_setup_db_before_delivery_and_reply(self):
        setup_conn = FakeConnection()
        delivery_conns = [
            FakeConnection(fetches=[("free_lesson_followup:123",), (1,)]),
            FakeConnection(),
        ]
        all_conns = [setup_conn] + delivery_conns
        message = FakeIncomingMessage(user_id=1)
        command = SimpleNamespace(args="123")
        closed_before_delivery = []
        closed_during_reply = []

        async def send_after_setup_close(*args, **kwargs):
            closed_before_delivery.append(setup_conn.closed)

        async def answer_after_close(*args, **kwargs):
            closed_during_reply.append(setup_conn.closed)

        message.answer = AsyncMock(side_effect=answer_after_close)
        with patch.object(self.main, "get_db_conn", side_effect=all_conns), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=send_after_setup_close)):
            await self.main.test_followup_command(message, command)

        self.assertEqual(closed_before_delivery, [True])
        self.assertEqual(closed_during_reply, [True])

    async def test_handlers_are_registered_on_native_aiogram3_router(self):
        self.assertEqual(len(self.main.router.message.handlers), 77)
        self.assertEqual(len(self.main.router.callback_query.handlers), 35)

    async def test_ast_handler_inventory_matches_expected_commands_and_callbacks(self):
        source = Path(self.main.__file__).read_text()
        tree = ast.parse(source)
        message_handlers = []
        callback_handlers = []
        commands = []
        callback_filters = []
        catch_all_messages = []

        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                text = ast.unparse(decorator)
                if text.startswith("router.message"):
                    message_handlers.append(node.name)
                    if "CommandStart()" in text:
                        commands.append("start")
                    elif "Command(" in text and isinstance(decorator, ast.Call):
                        command_call = decorator.args[0]
                        for arg in getattr(command_call, "args", []):
                            if isinstance(arg, ast.Constant):
                                commands.append(arg.value)
                    if "Command(" not in text and "CommandStart()" not in text and "F." not in text and "StateFilter(" not in text:
                        catch_all_messages.append((node.name, text))
                elif text.startswith("router.callback_query"):
                    callback_handlers.append(node.name)
                    callback_filters.append(text)

        self.assertEqual(len(message_handlers), 77)
        self.assertEqual(len(callback_handlers), 35)
        self.assertEqual(
            commands,
            [
                "promo_trial", "cancel", "menu", "ask", "start", "profile",
                "send_user", "broadcast", "give_access", "set_expiry", "restore_access",
                "gift_templates", "gift_template_upload", "gift_info", "gift_cancel",
                "gift_reissue", "gifts_pending", "gift_status", "revoke_access",
                "refund_info", "sync_stripe_user",
                "expired_users", "user", "access_history", "recent_access_events",
                "outbox_status", "retry_delivery", "find_by_stripe", "bot_health", "storage_diagnostics", "constraint_audit", "stripe_reconcile_audit", "access_mismatches", "admin", "admin_help", "expiring_users",
                "test_followup", "help", "stats", "weekly_report", "weekly_report_current",
                "weekly_report_send", "test_expiry", "test_grace", "test_auto_lesson",
                "test_backup", "unblock_user", "send_invite_link", "unlinked_stripe",
                "stripe_links", "duplicate_subscriptions", "revoke_invite_links",
                "resolve_checkout", "stripe_conflicts", "link_stripe_user", "unban_user",
            ],
        )
        self.assertEqual(len(commands), len(set(commands)))
        self.assertEqual(len(callback_handlers), len(set(callback_handlers)))
        self.assertEqual(catch_all_messages, [])
        self.assertTrue(any("F.data.startswith('sub_')" in item for item in callback_filters))
        self.assertTrue(any("F.data == 'retry_payment'" in item for item in callback_filters))
        self.assertFalse(any("gift_cancel_checkout:" in item for item in callback_filters))

    async def test_keyboard_callback_data_are_routable(self):
        source = Path(self.main.__file__).read_text()
        tree = ast.parse(source)
        callback_values = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.keyword) and node.arg == "callback_data":
                if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                    callback_values.add(node.value.value)

        exact = {
            "confirm_promo", "cancel_promo", "feedback_join", "feedback_question",
            "feedback_think", "to_desc", "to_rules", "to_choice", "retry_payment",
            "back_to_tariffs", "cancel_subscription", "show_renew_options",
        }
        prefix_covered = {"sub_trial", "sub_1", "sub_6", "sub_12", "admin_menu:back"}
        self.assertTrue(exact.issubset(callback_values))
        self.assertTrue(prefix_covered.issubset(callback_values))
        for value in exact:
            self.assertIn(f'F.data == "{value}"', source)
        self.assertIn("F.data.startswith('sub_')", source)
        self.assertIn('F.data.startswith("admin_menu:")', source)

    async def test_production_code_no_longer_uses_aiogram2_handler_or_fsm_api(self):
        source = Path(self.main.__file__).read_text()
        self.assertNotIn("@dp.message_handler", source)
        self.assertNotIn("@dp.callback_query_handler", source)
        self.assertNotIn("_legacy_message_handler", source)
        self.assertNotIn("_legacy_callback_query_handler", source)
        self.assertNotIn("dp.message_handler =", source)
        self.assertNotIn("dp.callback_query_handler =", source)
        self.assertNotIn("get_new_configured_app", source)
        self.assertNotIn("Dispatcher(bot", source)
        self.assertNotIn("MemoryStorage", source)
        self.assertNotIn("BotBlocked", source)
        self.assertNotIn("aiogram.utils.exceptions", source)
        self.assertNotIn("await state.finish()", source)
        self.assertNotIn("InlineKeyboardMarkup(row_width", source)
        self.assertNotIn("ReplyKeyboardMarkup(row_width", source)
        self.assertNotIn("InlineKeyboardMarkup().add", source)

    async def test_telegram_exception_helpers_prioritize_aiogram3_classes(self):
        from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNetworkError, TelegramRetryAfter

        forbidden = TelegramForbiddenError(method=None, message="bot was blocked by the user")
        bad_request = TelegramBadRequest(method=None, message="chat not found")
        network = TelegramNetworkError(method=None, message="temporary network error")
        retry_after = TelegramRetryAfter(method=None, message="Too Many Requests", retry_after=125)

        self.assertTrue(self.main.is_undeliverable_user_error(forbidden))
        self.assertTrue(self.main.is_undeliverable_user_error(bad_request))
        self.assertFalse(self.main.is_undeliverable_user_error(network))
        self.assertFalse(self.main.is_undeliverable_user_error(retry_after))
        self.assertEqual(self.main.telegram_retry_delay_minutes(retry_after, attempt_count=1), 3)

    async def test_message_delivery_forbidden_marks_blocked_permanent(self):
        from aiogram.exceptions import TelegramForbiddenError

        connections = []
        failed_calls = []

        def fake_conn():
            conn = FakeConnection()
            connections.append(conn)
            return conn

        def fake_mark_failed(cur, delivery_key, claim_generation, error, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(error).__name__, retry_delay_minutes, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        with patch.object(self.main, "get_db_conn", side_effect=fake_conn), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[("key1", 123, "notice", '{"text":"hi"}', 1, None, 1)]), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="blocked"))), \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 1})
        self.assertEqual(failed_calls, [("key1", "TelegramForbiddenError", None, True)])
        self.assertTrue(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        mark_sent.assert_not_called()

    async def test_message_delivery_bad_request_and_network_errors_remain_retryable(self):
        from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter

        for error, expected_delay in (
            (TelegramBadRequest(method=None, message="message is not modified"), 5),
            (TelegramNetworkError(method=None, message="network down"), 5),
            (TelegramRetryAfter(method=None, message="retry later", retry_after=125), 3),
        ):
            with self.subTest(error=type(error).__name__):
                connections = []
                failed_calls = []

                def fake_conn():
                    conn = FakeConnection()
                    connections.append(conn)
                    return conn

                def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
                    failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))
                    return "permanently_failed" if permanently_failed else "failed"

                with patch.object(self.main, "get_db_conn", side_effect=fake_conn), \
                     patch.object(self.main, "claim_pending_message_deliveries", return_value=[("key2", 123, "notice", '{"text":"hi"}', 1, None, 1)]), \
                     patch.object(self.main.bot, "send_message", AsyncMock(side_effect=error)), \
                     patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
                     patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
                     patch.object(self.main, "notify_admins", AsyncMock()):
                    result = await self.main.process_pending_message_deliveries()

                self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
                self.assertEqual(failed_calls, [("key2", type(error).__name__, expected_delay, False)])
                self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
                mark_sent.assert_not_called()

    async def test_message_delivery_retry_keyboard_payload_renders_callback_data(self):
        send_message = AsyncMock()

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("key_retry", 123, "stripe_user_message", '{"text":"retry","keyboard_kind":"retry_payment"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent"), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        markup = send_message.await_args.kwargs["reply_markup"]
        self.assertEqual(markup.inline_keyboard[0][0].callback_data, "retry_payment")

    async def test_stale_generic_delivery_cannot_finalize_newer_claim_after_send(self):
        send_message = AsyncMock()
        sent_conn = FakeConnection()

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), sent_conn]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stale_generic", 123, "stripe_user_message", '{"text":"sent before ownership loss"}', 2, None, 7)
             ]), \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main, "mark_delivery_sent", return_value="not_owner") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            with self.assertLogs(level="WARNING") as logs:
                result = await self.main.process_pending_message_deliveries()

        send_message.assert_awaited_once()
        mark_sent.assert_called_once_with(sent_conn.cursor_obj, "stale_generic", 7)
        self.assertEqual(sent_conn.commits, 0)
        self.assertEqual(sent_conn.rollbacks, 1)
        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        self.assertIn("MESSAGE_DELIVERY_STALE_CLAIM", "\n".join(logs.output))

    async def test_legacy_free_lesson_null_payload_sends_video_with_trial_button(self):
        send_video = AsyncMock()

        with patch.dict(os.environ, {"FREE_LESSON_VIDEO_ID": "video_free_1"}), \
             patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson:123", 123, "free_lesson", None, 1, None, 1)
             ]), \
             patch.object(self.main.bot, "send_video", send_video), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        send_video.assert_awaited_once()
        self.assertEqual(send_video.await_args.kwargs["video"], "video_free_1")
        markup = send_video.await_args.kwargs["reply_markup"]
        self.assertEqual(markup.inline_keyboard[0][0].callback_data, "sub_trial")

    async def test_legacy_free_lesson_followup_null_payload_sends_feedback_keyboard(self):
        send_message = AsyncMock()

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson_followup:123", 123, "free_lesson_followup", None, 1, None, 1)
             ]), \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        send_message.assert_awaited_once()
        self.assertEqual(send_message.await_args.args[1], self.main.get_free_lesson_followup_text())
        self.assertIs(send_message.await_args.kwargs["reply_markup"].__class__, self.main.get_free_lesson_feedback_keyboard().__class__)
        self.assertEqual(send_message.await_args.kwargs["reply_markup"].inline_keyboard[0][0].callback_data, "feedback_join")

    async def test_success_free_lesson_updates_user_and_delivery_in_one_final_transaction(self):
        connections = []

        def fake_conn():
            conn = FakeConnection()
            connections.append(conn)
            return conn

        async def send_video_side_effect(**kwargs):
            self.assertTrue(all(conn.closed for conn in connections))

        with patch.dict(os.environ, {"FREE_LESSON_VIDEO_ID": "video_free_1"}), \
             patch.object(self.main, "get_db_conn", side_effect=fake_conn), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson:123", 123, "free_lesson", "{}", 1, None, 1)
             ]), \
             patch.object(self.main.bot, "send_video", AsyncMock(side_effect=send_video_side_effect)), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        final_queries = "\n".join(query for query, _ in connections[-1].cursor_obj.queries)
        self.assertIn("video_sent = TRUE", final_queries)
        self.assertIn("video_sent_at = NOW()", final_queries)
        self.assertIn("status = 'sent'", final_queries)
        self.assertEqual(connections[-1].commits, 1)

    async def test_success_followup_updates_user_and_delivery_in_one_final_transaction(self):
        connections = []

        def fake_conn():
            conn = FakeConnection()
            connections.append(conn)
            return conn

        async def send_message_side_effect(*args, **kwargs):
            self.assertTrue(all(conn.closed for conn in connections))

        with patch.object(self.main, "get_db_conn", side_effect=fake_conn), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson_followup:123", 123, "free_lesson_followup", "{}", 1, None, 1)
             ]), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=send_message_side_effect)), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        final_queries = "\n".join(query for query, _ in connections[-1].cursor_obj.queries)
        self.assertIn("feedback_sent = TRUE", final_queries)
        self.assertIn("feedback_sent_at = NOW()", final_queries)
        self.assertIn("status = 'sent'", final_queries)
        self.assertEqual(connections[-1].commits, 1)

    async def test_manual_free_lesson_forbidden_uses_shared_classification(self):
        from aiogram.exceptions import TelegramForbiddenError

        connections = [
            FakeConnection(fetches=[(False, False, False)]),
            FakeConnection(fetches=[("free_lesson:123",), (1,)]),
            FakeConnection(),
        ]
        message = FakeIncomingMessage(123)

        with patch.dict(os.environ, {"FREE_LESSON_VIDEO_ID": "video_free_1"}), \
             patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main.bot, "send_video", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="blocked"))), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            await self.main.free_lesson_button(message, FakeState())

        fail_queries = "\n".join(query for query, _ in connections[-1].cursor_obj.queries)
        self.assertIn("blocked_bot = TRUE", fail_queries)
        self.assertIn("status = %s", fail_queries)
        self.assertIn("next_attempt_at = NULL", fail_queries)
        self.assertEqual(connections[-1].cursor_obj.queries[-1][1][0], "permanently_failed")

    async def test_automatic_free_lesson_forbidden_uses_shared_classification(self):
        from aiogram.exceptions import TelegramForbiddenError

        connections = [
            FakeConnection(fetches=[("free_lesson:123",), (1,)]),
            FakeConnection(),
        ]

        with patch.dict(os.environ, {"FREE_LESSON_VIDEO_ID": "video_free_1"}), \
             patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main.bot, "send_video", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="blocked"))), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            sent = await self.main.send_auto_free_lesson(123)

        self.assertFalse(sent)
        self.assertIn("next_attempt_at = NULL", "\n".join(query for query, _ in connections[-1].cursor_obj.queries))
        self.assertEqual(connections[-1].cursor_obj.queries[-1][1][0], "permanently_failed")

    async def test_automatic_followup_forbidden_uses_shared_classification(self):
        from aiogram.exceptions import TelegramForbiddenError

        connections = [
            FakeConnection(fetches=[("free_lesson_followup:123",), (1,)]),
            FakeConnection(),
        ]

        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="blocked"))), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            sent = await self.main.send_free_lesson_followup(123)

        self.assertFalse(sent)
        self.assertIn("next_attempt_at = NULL", "\n".join(query for query, _ in connections[-1].cursor_obj.queries))
        self.assertEqual(connections[-1].cursor_obj.queries[-1][1][0], "permanently_failed")

    async def test_message_delivery_retry_after_network_bad_request_and_unknown_classification(self):
        from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter

        cases = [
            (TelegramRetryAfter(method=None, message="retry later", retry_after=125), 1, "failed", 3, "retryable_failed"),
            (TelegramNetworkError(method=None, message="network down"), 1, "failed", 5, "retryable_failed"),
            (TelegramBadRequest(method=None, message="chat not found"), 1, "permanently_failed", None, "permanently_failed"),
            (TelegramBadRequest(method=None, message="message is not modified"), 1, "failed", 5, "retryable_failed"),
            (RuntimeError("boom"), self.main.OUTBOX_UNKNOWN_FAILURE_LIMIT, "permanently_failed", None, "permanently_failed"),
        ]
        for error, attempt_count, status, delay, counter in cases:
            with self.subTest(error=type(error).__name__, message=str(error)):
                connections = [FakeConnection(), FakeConnection()]
                with patch.object(self.main, "get_db_conn", side_effect=connections), \
                     patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                         ("free_lesson_followup:123", 123, "free_lesson_followup", "{}", attempt_count, None, 1)
                     ]), \
                     patch.object(self.main.bot, "send_message", AsyncMock(side_effect=error)), \
                     patch.object(self.main, "notify_admins", AsyncMock()):
                    result = await self.main.process_pending_message_deliveries()

                expected = {"sent": 0, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0}
                expected[counter] += 1
                if status == "permanently_failed" and self.main.is_undeliverable_user_error(error):
                    expected["blocked"] += 1
                self.assertEqual(result, expected)
                params = next(
                    params
                    for query, params in connections[-1].cursor_obj.queries
                    if "UPDATE message_delivery_events" in query
                )
                self.assertEqual(params[0], status)
                if status == "failed":
                    self.assertEqual(params[2], delay)
                else:
                    self.assertIsNone(delay)

    async def test_missing_free_lesson_video_retries_until_limit_then_alerts_once(self):
        notify_admins = AsyncMock()

        with patch.dict(os.environ, {}, clear=False), \
             patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson:123", 123, "free_lesson", "{}", 1, None, 1)
             ]), \
             patch.object(self.main, "notify_admins", notify_admins):
            os.environ.pop("FREE_LESSON_VIDEO_ID", None)
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        notify_admins.assert_not_awaited()

        notify_admins.reset_mock()
        with patch.dict(os.environ, {}, clear=False), \
             patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("free_lesson:123", 123, "free_lesson", "{}", self.main.OUTBOX_MISSING_FREE_LESSON_VIDEO_LIMIT, None, 1)
             ]), \
             patch.object(self.main, "notify_admins", notify_admins):
            os.environ.pop("FREE_LESSON_VIDEO_ID", None)
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 0})
        self.assertGreaterEqual(notify_admins.await_count, 1)
        keys = [call.kwargs.get("alert_key") for call in notify_admins.await_args_list]
        self.assertIn(f"free_lesson_video_missing:{self.main.safe_delivery_hash('free_lesson:123')}", keys)

    async def test_two_replicas_do_not_send_same_claimed_free_lesson_twice(self):
        send_func = AsyncMock()
        connections = [
            FakeConnection(fetches=[("free_lesson:123",), (1,)]),
            FakeConnection(),
            FakeConnection(fetches=[None, ("processing",)]),
        ]

        first = await self.main.process_claimed_delivery(
            lambda: connections.pop(0),
            "free_lesson:123",
            123,
            "free_lesson",
            send_func,
            classify_error_func=self.main.classify_delivery_error,
        )
        second = await self.main.process_claimed_delivery(
            lambda: connections.pop(0),
            "free_lesson:123",
            123,
            "free_lesson",
            send_func,
            classify_error_func=self.main.classify_delivery_error,
        )

        self.assertEqual(first, "sent")
        self.assertEqual(second, "already_processing")
        send_func.assert_awaited_once()

    async def test_sent_delivery_is_not_sent_again(self):
        send_func = AsyncMock()
        connections = [FakeConnection(fetches=[None, ("sent",)])]

        result = await self.main.process_claimed_delivery(
            lambda: connections.pop(0),
            "free_lesson:123",
            123,
            "free_lesson",
            send_func,
            classify_error_func=self.main.classify_delivery_error,
        )

        self.assertEqual(result, "already_sent")
        send_func.assert_not_awaited()

    async def test_critical_alert_fingerprint_is_stable_and_full_text_sensitive(self):
        text = "CRITICAL: payment check failed abc123"
        self.assertEqual(
            self.main.critical_alert_fingerprint(text),
            self.main.critical_alert_fingerprint(text),
        )
        self.assertNotEqual(
            self.main.critical_alert_fingerprint("CRITICAL: first prefix abc123"),
            self.main.critical_alert_fingerprint("CRITICAL: second prefix abc123"),
        )

    async def test_critical_alert_race_two_replicas_sends_one_admin_alert(self):
        send_message = AsyncMock()
        connections = [
            FakeConnection(fetches=[(10,)]),
            FakeConnection(),
            FakeConnection(fetches=[None]),
        ]

        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main.asyncio, "sleep", AsyncMock()):
            first = await self.main.notify_admins("CRITICAL same alert abc123", severity="CRITICAL")
            second = await self.main.notify_admins("CRITICAL same alert abc123", severity="CRITICAL")

        self.assertEqual(first["delivered"], self.main.ADMIN_IDS)
        self.assertTrue(second["deduped"])
        self.assertEqual(send_message.await_count, len(self.main.ADMIN_IDS))
        first_dedupe_key = connections[0].cursor_obj.queries[0][1][0]
        self.assertTrue(first_dedupe_key.startswith("critical:"))
        self.assertNotIn("abc123", first_dedupe_key)

    async def test_critical_alert_cooldown_repeat_is_not_sent(self):
        send_message = AsyncMock()
        connections = [
            FakeConnection(fetches=[None]),
        ]

        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main.bot, "send_message", send_message):
            result = await self.main.notify_admins("CRITICAL repeat abc123", severity="CRITICAL")

        self.assertTrue(result["deduped"])
        send_message.assert_not_awaited()

    async def test_retry_delivery_closes_db_before_replies_and_confirmation(self):
        no_match_conn = FakeConnection(fetches=[[]])
        no_match_message = FakeIncomingMessage(user_id=1)

        async def reply_after_close(text, **kwargs):
            self.assertTrue(no_match_conn.closed)
            no_match_message.replies.append((text, kwargs))

        no_match_message.reply = reply_after_close
        with patch.object(self.main, "get_db_conn", return_value=no_match_conn):
            await self.main.retry_delivery_command(no_match_message, SimpleNamespace(args="missinghash"))

        self.assertEqual(no_match_message.replies[0][0], "❌ Delivery не найден или не подходит для retry.")

        delivery_key = "free_lesson:123"
        requested_hash = self.main.safe_delivery_hash(delivery_key)
        match_conn = FakeConnection(fetches=[[
            (delivery_key, 123, "free_lesson", "failed", 2, "network", None)
        ]])
        match_message = FakeIncomingMessage(user_id=1)
        confirmations = []

        async def confirmation_after_close(message, action_id, text):
            self.assertTrue(match_conn.closed)
            confirmations.append((action_id, text))

        with patch.object(self.main, "get_db_conn", return_value=match_conn), \
             patch.object(self.main, "send_admin_action_confirmation", AsyncMock(side_effect=confirmation_after_close)):
            await self.main.retry_delivery_command(match_message, SimpleNamespace(args=requested_hash))

        self.assertEqual(len(confirmations), 1)
        self.assertIn(f"delivery_hash: {requested_hash}", confirmations[0][1])

    def stripe_identity_available_fetches(self, include_subscription=False, target_user_row=None, target_link_rows=None):
        fetches = []
        if include_subscription:
            fetches.append([])
        fetches.append([])
        if include_subscription:
            fetches.append([])
        fetches.append([])
        fetches.append(target_user_row)
        fetches.append(target_link_rows or [])
        return fetches

    def known_unique_violation(self, constraint_name):
        class KnownUniqueViolation(self.main.psycopg2_errors.UniqueViolation):
            @property
            def diag(self):
                return SimpleNamespace(constraint_name=constraint_name)

        return KnownUniqueViolation("duplicate")

    def subscription_updated_event(self, event_id, status="active"):
        sub = SimpleNamespace(
            id=f"sub_{event_id}",
            customer=f"cus_{event_id}",
            status=status,
            current_period_end=int((datetime.utcnow() + timedelta(days=30)).timestamp()),
            trial_end=None,
            cancel_at_period_end=False,
        )
        event = SimpleNamespace(
            id=event_id,
            type="customer.subscription.updated",
            created=1720000000,
            data=SimpleNamespace(object=sub),
        )
        return event, sub

    def invoice_payment_failed_event(self, event_id):
        invoice = SimpleNamespace(
            id=f"in_{event_id}",
            subscription=f"sub_{event_id}",
            customer=f"cus_{event_id}",
            customer_email="failed@example.test",
            billing_reason="subscription_cycle",
            status="open",
            next_payment_attempt=None,
            metadata={},
            lines=SimpleNamespace(data=[]),
        )
        subscription = SimpleNamespace(
            id=f"sub_{event_id}",
            customer=f"cus_{event_id}",
            status="past_due",
            trial_end=None,
            cancel_at_period_end=False,
        )
        event = SimpleNamespace(
            id=event_id,
            type="invoice.payment_failed",
            created=1720000000,
            data=SimpleNamespace(object=invoice),
        )
        return event, invoice, subscription

    async def run_checkout_days_webhook(
        self,
        days_marker,
        notify_side_effect=None,
        payment_status="paid",
        retrieve_session=None,
        conn=None,
        event_id=None,
        session_id=None,
        checkout_status="open",
        checkout_tariff="sub_1",
        checkout_mode="payment",
        checkout_telegram_id=123,
        session_subscription=None,
        db_conns=None,
    ):
        session_id = session_id or f"cs_days_{days_marker if days_marker is not None else 'missing'}"
        session_payload = {
            "id": session_id,
            "client_reference_id": "123",
            "mode": "payment",
            "customer": f"cus_days_{days_marker if days_marker is not None else 'missing'}",
            "subscription": session_subscription,
            "amount_total": 1000,
            "currency": "usd",
        }
        if payment_status != "missing":
            session_payload["payment_status"] = payment_status
        if days_marker != "missing":
            session_payload["metadata"] = {"days": days_marker}
        event_id = event_id or f"evt_days_{days_marker if days_marker is not None else 'none'}"
        event = SimpleNamespace(
            id=event_id,
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(**session_payload)),
        )
        payload = json.dumps({
            "id": event_id,
            "object": "event",
            "type": "checkout.session.completed",
            "data": {"object": session_payload},
        }).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        notify = AsyncMock(side_effect=notify_side_effect)
        release = AsyncMock()
        mark_processed = AsyncMock()
        conn = conn or FakeConnection(fetches=[
            (checkout_telegram_id, checkout_tariff, checkout_mode, checkout_status),
            *self.stripe_identity_available_fetches(),
            (False, None, False, False),
        ])
        if db_conns is not None:
            get_db_conn = Mock(side_effect=db_conns)
        elif notify_side_effect:
            get_db_conn = Mock(side_effect=notify_side_effect)
        else:
            get_db_conn = Mock(return_value=conn)

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "notify_admins", notify), \
             patch.object(self.main.stripe.checkout.Session, "retrieve", Mock(return_value=retrieve_session)) as retrieve, \
             patch.object(self.main, "get_db_conn", get_db_conn):
            response = await self.main.stripe_webhook(request)

        return SimpleNamespace(
            response=response,
            notify=notify,
            release=release,
            mark_processed=mark_processed,
            get_db_conn=get_db_conn,
            conn=conn,
            event_id=event_id,
            retrieve=retrieve,
        )

    async def run_checkout_async_success_webhook(
        self,
        payment_status="paid",
        mode="payment",
        conn=None,
        event_id="evt_async_success",
        session_id="cs_async_success",
        claim_result=("claimed", 1),
    ):
        session_payload = {
            "id": session_id,
            "client_reference_id": "123",
            "mode": mode,
            "payment_status": payment_status,
            "customer": "cus_async_success",
            "subscription": "sub_async_success" if mode == "subscription" else None,
            "amount_total": 1000,
            "currency": "usd",
            "metadata": {"days": "30", "telegram_id": "123"},
        }
        event = SimpleNamespace(
            id=event_id,
            type="checkout.session.async_payment_succeeded",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(**session_payload)),
        )
        payload = json.dumps({
            "id": event_id,
            "object": "event",
            "type": "checkout.session.async_payment_succeeded",
            "data": {"object": session_payload},
        }).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        mark_processed = AsyncMock()
        release = AsyncMock()
        conn = conn or FakeConnection(fetches=[
            (123, "sub_1", "payment", "payment_pending"),
            *self.stripe_identity_available_fetches(),
            (False, None, False, False),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=claim_result)), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "claim_trial_redemption", return_value=True), \
             patch.object(self.main, "reset_checkout_retry_state_after_success") as reset_retry, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        return SimpleNamespace(
            response=response,
            mark_processed=mark_processed,
            release=release,
            reset_retry=reset_retry,
            conn=conn,
            event_id=event_id,
            session_id=session_id,
        )

    async def run_checkout_identity_webhook(self, client_reference_id="123", metadata_telegram_id="123", notify_side_effect=None):
        session_payload = {
            "id": f"cs_identity_{client_reference_id if client_reference_id not in (None, '') else 'empty'}_{metadata_telegram_id if metadata_telegram_id not in (None, '') else 'empty'}",
            "mode": "payment",
            "payment_status": "paid",
            "customer": "cus_identity",
            "subscription": None,
            "amount_total": 1000,
            "currency": "usd",
            "metadata": {"days": "7"},
        }
        if client_reference_id is not None:
            session_payload["client_reference_id"] = client_reference_id
        if metadata_telegram_id is not None:
            session_payload["metadata"]["telegram_id"] = metadata_telegram_id
        event_id = f"evt_identity_{client_reference_id if client_reference_id not in (None, '') else 'empty'}_{metadata_telegram_id if metadata_telegram_id not in (None, '') else 'empty'}"
        event = SimpleNamespace(
            id=event_id,
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(**session_payload)),
        )
        payload = json.dumps({
            "id": event_id,
            "object": "event",
            "type": "checkout.session.completed",
            "data": {"object": session_payload},
        }).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        notify = AsyncMock(side_effect=notify_side_effect)
        release = AsyncMock()
        mark_processed = AsyncMock()
        conn = FakeConnection(fetches=[
            (123, "sub_trial", "payment", "open"),
            *self.stripe_identity_available_fetches(),
            (False, None, False, False),
        ])
        get_db_conn = Mock(side_effect=notify_side_effect) if notify_side_effect else Mock(return_value=conn)

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "notify_admins", notify), \
             patch.object(self.main, "claim_trial_redemption", return_value=True), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main, "get_db_conn", get_db_conn):
            response = await self.main.stripe_webhook(request)

        return SimpleNamespace(
            response=response,
            notify=notify,
            release=release,
            mark_processed=mark_processed,
            conn=conn,
            get_db_conn=get_db_conn,
            event_id=event_id,
        )

    async def test_checkout_identity_accepts_client_reference_metadata_and_matching_sources(self):
        cases = (
            ("123", None),
            (None, "123"),
            ("123", "123"),
            ("", "123"),
        )
        for client_reference_id, metadata_telegram_id in cases:
            with self.subTest(client_reference_id=client_reference_id, metadata_telegram_id=metadata_telegram_id):
                result = await self.run_checkout_identity_webhook(client_reference_id, metadata_telegram_id)

                self.assertEqual(result.response.status, 200)
                result.mark_processed.assert_awaited_once_with(result.event_id, 1)
                result.release.assert_not_awaited()
                result.notify.assert_not_awaited()
                sql = "\n".join(query for query, _ in result.conn.cursor_obj.queries)
                self.assertIn("INSERT INTO users", sql)
                self.assertIn("INSERT INTO payment_events", sql)
                self.assertIn("INSERT INTO access_events", sql)

    async def test_checkout_identity_missing_invalid_or_conflicting_sources_fail_closed(self):
        cases = (
            (None, None),
            ("abc", "-1"),
            ("123", "456"),
        )
        for client_reference_id, metadata_telegram_id in cases:
            with self.subTest(client_reference_id=client_reference_id, metadata_telegram_id=metadata_telegram_id):
                result = await self.run_checkout_identity_webhook(client_reference_id, metadata_telegram_id)

                self.assertEqual(result.response.status, 500)
                result.release.assert_awaited_once_with(result.event_id, 1)
                result.mark_processed.assert_not_awaited()
                result.get_db_conn.assert_called_once()
                result.notify.assert_not_awaited()
                self.assertFalse(any(
                    params[2] != "stripe_admin_message"
                    for params in self.delivery_inserts(result.conn)
                ))
                admin_payloads = [json.loads(adapted_json_value(params[3])) for params in self.admin_delivery_inserts(result.conn)]
                self.assertEqual(len(admin_payloads), len(self.main.ADMIN_IDS))
                self.assertTrue(all(payload["severity"] == "CRITICAL" for payload in admin_payloads))
                alert_text = admin_payloads[0]["text"]
                self.assertIn("client_reference_id/metadata.telegram_id missing, invalid, or conflicting", alert_text)
                self.assertIn("access_granted: false", alert_text)
                self.assertNotIn(result.event_id, alert_text)
                self.assertNotIn("cs_identity", alert_text)

    async def test_checkout_identity_notify_failure_still_releases_claim_and_returns_500(self):
        result = await self.run_checkout_identity_webhook(None, None, notify_side_effect=RuntimeError("notify down"))

        self.assertEqual(result.response.status, 500)
        result.release.assert_awaited_once_with(result.event_id, 1)
        result.mark_processed.assert_not_awaited()
        result.get_db_conn.assert_called_once()

    async def test_checkout_invalid_days_missing_empty_text_zero_and_negative_fail_closed(self):
        for days_marker in ("missing", "", "abc", "0", "-30"):
            with self.subTest(days_marker=days_marker):
                result = await self.run_checkout_days_webhook(days_marker)

                self.assertEqual(result.response.status, 500)
                result.release.assert_awaited_once_with(result.event_id, 1)
                result.mark_processed.assert_not_awaited()
                result.get_db_conn.assert_called_once()
                result.notify.assert_not_awaited()
                self.assertFalse(any(
                    params[2] != "stripe_admin_message"
                    for params in self.delivery_inserts(result.conn)
                ))
                admin_payloads = [json.loads(adapted_json_value(params[3])) for params in self.admin_delivery_inserts(result.conn)]
                self.assertEqual(len(admin_payloads), len(self.main.ADMIN_IDS))
                self.assertTrue(all(payload["severity"] == "CRITICAL" for payload in admin_payloads))
                alert_text = admin_payloads[0]["text"]
                self.assertIn("metadata.days missing or invalid", alert_text)
                self.assertIn("access_granted: false", alert_text)
                self.assertNotIn(result.event_id, alert_text)

    async def test_checkout_invalid_days_notify_failure_still_releases_claim_and_returns_500(self):
        result = await self.run_checkout_days_webhook("abc", notify_side_effect=RuntimeError("notify down"))

        self.assertEqual(result.response.status, 500)
        result.release.assert_awaited_once_with(result.event_id, 1)
        result.mark_processed.assert_not_awaited()
        result.get_db_conn.assert_called_once()

    async def test_checkout_payment_status_unpaid_and_processing_do_not_grant_access(self):
        for payment_status in ("unpaid", "processing"):
            with self.subTest(payment_status=payment_status):
                result = await self.run_checkout_days_webhook("30", payment_status=payment_status)

                self.assertEqual(result.response.status, 200)
                result.mark_processed.assert_awaited_once_with(result.event_id, 1)
                result.release.assert_not_awaited()
                sql = "\n".join(query for query, _ in result.conn.cursor_obj.queries)
                self.assertTrue(any(params and params[0] == "payment_pending" for _, params in result.conn.cursor_obj.queries))
                self.assertNotIn("INSERT INTO users", sql)
                self.assertNotIn("INSERT INTO payment_events", sql)
                self.assertNotIn("INSERT INTO access_events", sql)

    async def test_checkout_payment_status_no_payment_required_requires_manual_review(self):
        result = await self.run_checkout_days_webhook("30", payment_status="no_payment_required")

        self.assertEqual(result.response.status, 200)
        result.mark_processed.assert_awaited_once_with(result.event_id, 1)
        result.release.assert_not_awaited()
        sql = "\n".join(query for query, _ in result.conn.cursor_obj.queries)
        self.assertTrue(any(params and params[0] == "manual_review_required" for _, params in result.conn.cursor_obj.queries))
        self.assertNotIn("INSERT INTO users", sql)
        self.assertNotIn("INSERT INTO payment_events", sql)
        self.assertNotIn("INSERT INTO access_events", sql)
        admin_payloads = [json.loads(adapted_json_value(params[3])) for params in self.admin_delivery_inserts(result.conn)]
        self.assertEqual(len(admin_payloads), len(self.main.ADMIN_IDS))
        self.assertTrue(all(payload["severity"] == "CRITICAL" for payload in admin_payloads))
        self.assertIn("Доступ не выдан", admin_payloads[0]["text"])

    async def test_checkout_payment_status_missing_retrieves_once_and_fails_closed(self):
        retrieved = SimpleNamespace(
            id="cs_days_30",
            client_reference_id="123",
            metadata={"days": "30"},
            mode="payment",
            payment_status="processing",
            customer="cus_days_30",
            subscription=None,
            amount_total=1000,
            currency="usd",
        )
        result = await self.run_checkout_days_webhook("30", payment_status="missing", retrieve_session=retrieved)

        self.assertEqual(result.response.status, 200)
        result.retrieve.assert_called_once()
        result.mark_processed.assert_awaited_once_with(result.event_id, 1)
        sql = "\n".join(query for query, _ in result.conn.cursor_obj.queries)
        self.assertTrue(any(params and params[0] == "payment_pending" for _, params in result.conn.cursor_obj.queries))
        self.assertNotIn("INSERT INTO users", sql)
        self.assertNotIn("INSERT INTO payment_events", sql)

    async def test_checkout_completed_open_row_applies_access_once(self):
        result = await self.run_checkout_days_webhook(
            "30",
            event_id="evt_completed_open_row",
            session_id="cs_completed_idempotent",
            checkout_status="open",
        )

        self.assertEqual(result.response.status, 200)
        result.mark_processed.assert_awaited_once_with(result.event_id, 1)
        result.release.assert_not_awaited()
        sql = "\n".join(query for query, _ in result.conn.cursor_obj.queries)
        self.assertIn("SELECT telegram_id, tariff_code, mode, status", sql)
        self.assertIn("FOR UPDATE", sql)
        self.assertIn("INSERT INTO users", sql)
        self.assertIn("INSERT INTO payment_events", sql)
        self.assertIn("INSERT INTO access_events", sql)

    async def test_repeat_checkout_completed_same_session_already_completed_is_noop(self):
        conn = FakeConnection(fetches=[(123, "sub_1", "payment", "completed")])
        result = await self.run_checkout_days_webhook(
            "30",
            conn=conn,
            event_id="evt_completed_repeated_after_commit",
            session_id="cs_completed_idempotent",
        )

        self.assertEqual(result.response.status, 200)
        result.mark_processed.assert_awaited_once_with("evt_completed_repeated_after_commit", 1)
        result.release.assert_not_awaited()
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("SELECT telegram_id, tariff_code, mode, status", sql)
        self.assertNotIn("INSERT INTO users", sql)
        self.assertNotIn("INSERT INTO payment_events", sql)
        self.assertNotIn("INSERT INTO access_events", sql)
        self.assertFalse(self.delivery_inserts(conn))

    async def test_checkout_completed_missing_row_fails_closed_with_durable_alert(self):
        conn = FakeConnection(fetches=[None])
        result = await self.run_checkout_days_webhook(
            "30",
            conn=conn,
            event_id="evt_completed_missing_checkout_row",
            session_id="cs_missing_checkout_row",
        )

        self.assertEqual(result.response.status, 200)
        result.mark_processed.assert_awaited_once_with(result.event_id, 1)
        result.release.assert_not_awaited()
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertNotIn("INSERT INTO users", sql)
        self.assertNotIn("INSERT INTO payment_events", sql)
        self.assertNotIn("INSERT INTO access_events", sql)
        admin_payloads = [json.loads(adapted_json_value(params[3])) for params in self.admin_delivery_inserts(conn)]
        self.assertEqual(len(admin_payloads), len(self.main.ADMIN_IDS))
        self.assertTrue(all(payload["severity"] == "CRITICAL" for payload in admin_payloads))
        self.assertIn("access_granted: false", admin_payloads[0]["text"])
        self.assertIn("checkout_session_row_missing", admin_payloads[0]["text"])

    async def test_checkout_completed_row_identity_tariff_or_mode_mismatch_fails_closed(self):
        cases = (
            ("telegram", 456, "sub_1", "payment"),
            ("tariff", 123, "sub_6", "payment"),
            ("mode", 123, "sub_1", "subscription"),
        )
        for label, checkout_telegram_id, checkout_tariff, checkout_mode in cases:
            with self.subTest(label=label):
                conn = FakeConnection(fetches=[(checkout_telegram_id, checkout_tariff, checkout_mode, "open")])
                result = await self.run_checkout_days_webhook(
                    "30",
                    conn=conn,
                    event_id=f"evt_completed_mismatch_{label}",
                    session_id=f"cs_completed_mismatch_{label}",
                )

                self.assertEqual(result.response.status, 200)
                result.mark_processed.assert_awaited_once_with(result.event_id, 1)
                result.release.assert_not_awaited()
                sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
                self.assertNotIn("INSERT INTO users", sql)
                self.assertNotIn("INSERT INTO payment_events", sql)
                self.assertNotIn("INSERT INTO access_events", sql)
                admin_payloads = [json.loads(adapted_json_value(params[3])) for params in self.admin_delivery_inserts(conn)]
                self.assertEqual(len(admin_payloads), len(self.main.ADMIN_IDS))

    async def test_checkout_completed_stripe_customer_conflict_rolls_back_and_records_audit(self):
        payment_conn = FakeConnection(fetches=[
            (123, "sub_1", "payment", "open"),
            [(999,)],
        ])
        audit_conn = FakeConnection()
        result = await self.run_checkout_days_webhook(
            "30",
            conn=payment_conn,
            db_conns=[payment_conn, audit_conn],
            event_id="evt_customer_identity_conflict",
            session_id="cs_customer_identity_conflict",
        )

        self.assertEqual(result.response.status, 200)
        result.mark_processed.assert_awaited_once_with("evt_customer_identity_conflict", 1)
        self.assertEqual(payment_conn.rollbacks, 1)
        payment_sql = "\n".join(query for query, _ in payment_conn.cursor_obj.queries)
        self.assertIn("FOR UPDATE", payment_sql)
        self.assertNotIn("INSERT INTO users", payment_sql)
        self.assertNotIn("INSERT INTO payment_events", payment_sql)
        self.assertNotIn("INSERT INTO access_events", payment_sql)

        audit_sql = "\n".join(query for query, _ in audit_conn.cursor_obj.queries)
        self.assertIn("INSERT INTO stripe_identity_conflicts", audit_sql)
        self.assertIn("ON CONFLICT (conflict_type, stripe_id, telegram_ids)", audit_sql)
        self.assertIn("INSERT INTO unlinked_stripe_events", audit_sql)
        admin_payloads = [json.loads(adapted_json_value(params[3])) for params in self.admin_delivery_inserts(audit_conn)]
        self.assertEqual(len(admin_payloads), len(self.main.ADMIN_IDS))
        self.assertTrue(all(payload["severity"] == "CRITICAL" for payload in admin_payloads))
        self.assertIn("users_customer_conflict", admin_payloads[0]["text"])

    async def test_known_unique_violation_populates_existing_owner_in_audit_transaction(self):
        class KnownUniqueViolation(self.main.psycopg2_errors.UniqueViolation):
            @property
            def diag(self):
                return SimpleNamespace(constraint_name="users_unique_stripe_customer")

        payment_conn = ConditionalFailingConnection(
            fetches=[
                (123, "sub_1", "payment", "open"),
                *self.stripe_identity_available_fetches(),
            ],
            error=KnownUniqueViolation("duplicate"),
            fail_when=lambda query, params: "INSERT INTO users" in query,
        )
        lookup_conn = FakeConnection(fetches=[(999,)])
        audit_conn = FakeConnection(fetches=[(999,)])
        result = await self.run_checkout_days_webhook(
            "30",
            conn=payment_conn,
            db_conns=[payment_conn, lookup_conn, audit_conn],
            event_id="evt_known_unique_identity_conflict",
            session_id="cs_known_unique_identity_conflict",
        )

        self.assertEqual(result.response.status, 200)
        result.mark_processed.assert_awaited_once_with("evt_known_unique_identity_conflict", 1)
        self.assertEqual(payment_conn.rollbacks, 1)
        conflict_insert = next(
            params for query, params in audit_conn.cursor_obj.queries
            if "INSERT INTO stripe_identity_conflicts" in query
        )
        self.assertEqual(conflict_insert[2], "[123,999]")
        admin_payloads = [json.loads(adapted_json_value(params[3])) for params in self.admin_delivery_inserts(audit_conn)]
        self.assertIn("existing_telegram_id: 999", admin_payloads[0]["text"])

    async def test_unknown_unique_violation_returns_500_and_releases_event(self):
        class UnknownUniqueViolation(self.main.psycopg2_errors.UniqueViolation):
            @property
            def diag(self):
                return SimpleNamespace(constraint_name="unknown_unique_constraint")

        payment_conn = ConditionalFailingConnection(
            fetches=[
                (123, "sub_1", "payment", "open"),
                *self.stripe_identity_available_fetches(),
            ],
            error=UnknownUniqueViolation("duplicate"),
            fail_when=lambda query, params: "INSERT INTO users" in query,
        )
        result = await self.run_checkout_days_webhook(
            "30",
            conn=payment_conn,
            db_conns=[payment_conn],
            event_id="evt_unknown_unique_identity_conflict",
            session_id="cs_unknown_unique_identity_conflict",
        )

        self.assertEqual(result.response.status, 500)
        result.mark_processed.assert_not_awaited()
        result.release.assert_awaited_once_with("evt_unknown_unique_identity_conflict", 1)

    async def test_identity_conflict_audit_failure_returns_500_without_mark_processed(self):
        payment_conn = FakeConnection(fetches=[
            (123, "sub_1", "payment", "open"),
            [(999,)],
        ])
        audit_conn = ExecuteFailingConnection(error=RuntimeError("audit down"))
        result = await self.run_checkout_days_webhook(
            "30",
            conn=payment_conn,
            db_conns=[payment_conn, audit_conn],
            event_id="evt_identity_audit_failure",
            session_id="cs_identity_audit_failure",
        )

        self.assertEqual(result.response.status, 500)
        result.mark_processed.assert_not_awaited()
        result.release.assert_awaited_once_with("evt_identity_audit_failure", 1)
        self.assertEqual(payment_conn.rollbacks, 1)
        self.assertEqual(audit_conn.rollbacks, 1)

    async def test_lawful_new_identity_for_same_user_with_old_history_passes(self):
        cur = FakeCursor(fetches=[
            [],
            [],
            [],
            [],
            (123, "cus_old", "sub_old"),
            [(123, "cus_old_link", "sub_old_link")],
        ])

        self.main.assert_stripe_identity_available(
            cur,
            123,
            customer_id="cus_new",
            subscription_id="sub_new",
            source="unit",
        )

        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("FROM stripe_links", sql)
        self.assertIn("FOR UPDATE", sql)

    async def test_recurring_invoice_current_subscription_ignores_old_links_same_user(self):
        cur = FakeCursor(fetches=[
            [(123,)],
            [(123,)],
            [],
            [],
            (123, "cus_current", "sub_current"),
            [(123, "cus_old", "sub_old")],
        ])

        self.main.assert_stripe_identity_available(
            cur,
            123,
            customer_id="cus_current",
            subscription_id="sub_current",
            source="invoice.payment_succeeded",
        )

    async def test_upsert_stripe_link_updates_same_subscription_same_user(self):
        cur = FakeCursor(fetches=[(123,)])

        self.main.upsert_stripe_link(
            cur,
            123,
            stripe_customer_id="cus_same",
            stripe_subscription_id="sub_same",
            customer_email="same@example.test",
            status="active",
            current_period_end=datetime(2026, 8, 7),
            is_active=True,
            source="invoice.payment_succeeded",
        )

        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("WHERE stripe_subscription_id = %s", sql)
        self.assertIn("FOR UPDATE", sql)
        self.assertIn("UPDATE stripe_links", sql)
        self.assertNotIn("ON CONFLICT (telegram_id, stripe_customer_id, stripe_subscription_id)", sql)

    async def test_upsert_stripe_link_blocks_same_subscription_other_user(self):
        cur = FakeCursor(fetches=[(999,)])

        with self.assertRaises(self.main.StripeIdentityConflictError) as raised:
            self.main.upsert_stripe_link(
                cur,
                123,
                stripe_customer_id="cus_other",
                stripe_subscription_id="sub_same",
                status="active",
                source="invoice.payment_succeeded",
            )

        self.assertEqual(raised.exception.conflict_type, "stripe_links_subscription_conflict")
        self.assertEqual(raised.exception.existing_telegram_id, 999)
        self.assertEqual(raised.exception.requested_telegram_id, 123)

    async def test_same_user_unique_violation_releases_event_without_conflict_audit(self):
        class KnownUniqueViolation(self.main.psycopg2_errors.UniqueViolation):
            @property
            def diag(self):
                return SimpleNamespace(constraint_name="stripe_links_unique_subscription_user")

        payment_conn = ConditionalFailingConnection(
            fetches=[
                (123, "sub_1", "payment", "open"),
                *self.stripe_identity_available_fetches(include_subscription=True),
                (False, None, False, False),
                None,
            ],
            error=KnownUniqueViolation("duplicate"),
            fail_when=lambda query, params: "INSERT INTO stripe_links" in query,
        )
        lookup_conn = FakeConnection(fetches=[(123,)])
        result = await self.run_checkout_days_webhook(
            "30",
            conn=payment_conn,
            db_conns=[payment_conn, lookup_conn],
            event_id="evt_same_user_unique_race",
            session_id="cs_same_user_unique_race",
            session_subscription="sub_same_user_unique_race",
        )

        self.assertEqual(result.response.status, 500)
        result.mark_processed.assert_not_awaited()
        result.release.assert_awaited_once_with("evt_same_user_unique_race", 1)
        lookup_sql = "\n".join(query for query, _ in lookup_conn.cursor_obj.queries)
        self.assertIn("FROM users", lookup_sql)
        payment_sql = "\n".join(query for query, _ in payment_conn.cursor_obj.queries)
        self.assertNotIn("INSERT INTO stripe_identity_conflicts", payment_sql)

    async def test_same_customer_and_subscription_same_user_are_idempotent(self):
        cur = FakeCursor(fetches=[
            [(123,)],
            [(123,)],
            [(123,)],
            [(123,)],
            (123, "cus_same", "sub_same"),
            [(123, "cus_same", "sub_same")],
        ])

        self.main.assert_stripe_identity_available(
            cur,
            123,
            customer_id="cus_same",
            subscription_id="sub_same",
            source="unit",
        )

        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("FOR UPDATE", sql)

    async def test_subscription_or_customer_other_user_is_controlled_conflict(self):
        cur = FakeCursor(fetches=[[(456,)]])

        with self.assertRaises(self.main.StripeIdentityConflictError) as raised:
            self.main.assert_stripe_identity_available(
                cur,
                123,
                customer_id="cus_other",
                source="unit",
            )

        self.assertEqual(raised.exception.conflict_type, "users_customer_conflict")
        self.assertEqual(raised.exception.existing_telegram_id, 456)
        self.assertEqual(raised.exception.requested_telegram_id, 123)
        self.assertEqual(raised.exception.safe_stripe_id, self.main.safe_log_id("cus_other"))

    async def test_unique_violation_classifier_uses_constraint_name_only(self):
        known_error = SimpleNamespace(diag=SimpleNamespace(constraint_name="users_unique_stripe_subscription"))
        conflict = self.main.stripe_identity_conflict_from_unique_violation(
            known_error,
            123,
            customer_id="cus_known",
            subscription_id="sub_known",
            source="unit",
        )
        self.assertIsInstance(conflict, self.main.StripeIdentityConflictError)
        self.assertEqual(conflict.conflict_type, "users_subscription_conflict")
        self.assertEqual(conflict.stripe_id, "sub_known")
        self.assertEqual(conflict.constraint_name, "users_unique_stripe_subscription")

        unknown_error = SimpleNamespace(diag=SimpleNamespace(constraint_name="some_other_constraint"))
        self.assertIsNone(
            self.main.stripe_identity_conflict_from_unique_violation(
                unknown_error,
                123,
                customer_id="cus_known",
                subscription_id="sub_known",
                source="unit",
            )
        )

    async def test_checkout_completed_then_async_success_same_session_second_is_noop(self):
        completed = await self.run_checkout_days_webhook(
            "30",
            event_id="evt_completed_before_async",
            session_id="cs_completed_then_async",
            checkout_status="open",
        )
        self.assertEqual(completed.response.status, 200)
        self.assertIn("INSERT INTO users", "\n".join(query for query, _ in completed.conn.cursor_obj.queries))

        async_conn = FakeConnection(fetches=[(123, "sub_1", "payment", "completed")])
        async_result = await self.run_checkout_async_success_webhook(
            conn=async_conn,
            event_id="evt_async_after_completed",
            session_id="cs_completed_then_async",
        )

        self.assertEqual(async_result.response.status, 200)
        async_result.mark_processed.assert_awaited_once_with("evt_async_after_completed", 1)
        sql = "\n".join(query for query, _ in async_conn.cursor_obj.queries)
        self.assertNotIn("INSERT INTO users", sql)
        self.assertNotIn("INSERT INTO payment_events", sql)
        self.assertNotIn("INSERT INTO access_events", sql)

    async def test_checkout_async_success_applies_payment_pending_checkout(self):
        result = await self.run_checkout_async_success_webhook()

        self.assertEqual(result.response.status, 200)
        result.mark_processed.assert_awaited_once_with(result.event_id, 1)
        result.release.assert_not_awaited()
        sql = "\n".join(query for query, _ in result.conn.cursor_obj.queries)
        self.assertIn("SELECT telegram_id, tariff_code, mode, status", sql)
        self.assertIn("INSERT INTO users", sql)
        self.assertIn("INSERT INTO payment_events", sql)
        self.assertIn("INSERT INTO access_events", sql)
        self.assertIn("SET status = 'completed'", sql)

    async def test_duplicate_checkout_async_success_does_not_apply_twice(self):
        conn = FakeConnection(fetches=[(123, "sub_1", "payment", "completed")])
        result = await self.run_checkout_async_success_webhook(
            conn=conn,
            event_id="evt_async_success_duplicate",
            session_id="cs_async_success_duplicate",
        )

        self.assertEqual(result.response.status, 200)
        result.mark_processed.assert_awaited_once_with(result.event_id, 1)
        result.release.assert_not_awaited()
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("SELECT telegram_id, tariff_code, mode, status", sql)
        self.assertNotIn("INSERT INTO users", sql)
        self.assertNotIn("INSERT INTO payment_events", sql)
        self.assertNotIn("INSERT INTO access_events", sql)

    async def test_checkout_async_success_without_paid_status_fails_closed(self):
        result = await self.run_checkout_async_success_webhook(payment_status="processing")

        self.assertEqual(result.response.status, 200)
        result.mark_processed.assert_awaited_once_with(result.event_id, 1)
        result.release.assert_not_awaited()
        sql = "\n".join(query for query, _ in result.conn.cursor_obj.queries)
        self.assertTrue(any(params and params[0] == "manual_review_required" for _, params in result.conn.cursor_obj.queries))
        self.assertNotIn("INSERT INTO users", sql)
        self.assertNotIn("INSERT INTO payment_events", sql)
        self.assertNotIn("INSERT INTO access_events", sql)

    async def test_checkout_async_payment_failed_marks_pending_failed_without_access(self):
        event_id = "evt_async_failed_pending"
        session_payload = {
            "id": "cs_async_failed_pending",
            "client_reference_id": "123",
            "metadata": {"days": "30", "telegram_id": "123"},
        }
        event = SimpleNamespace(
            id=event_id,
            type="checkout.session.async_payment_failed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(**session_payload)),
        )
        payload = json.dumps({
            "id": event_id,
            "object": "event",
            "type": "checkout.session.async_payment_failed",
            "data": {"object": session_payload},
        }).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertTrue(any(params and params[0] == "failed" for _, params in conn.cursor_obj.queries))
        self.assertNotIn("INSERT INTO users", sql)
        self.assertNotIn("INSERT INTO payment_events", sql)
        self.assertNotIn("INSERT INTO access_events", sql)

    async def test_checkout_creation_existing_payment_pending_does_not_create_new_session(self):
        callback = FakeCallback()
        callback.data = "sub_1"
        state = FakeState()
        initial_conn = FakeConnection(fetches=[(False, None, False, False, None, False, None, False)])
        claim_conn = FakeConnection()
        claim_result = {
            "action": "payment_pending",
            "record": {
                "id": 51,
                "stripe_session_id": "cs_pending_existing",
                "checkout_url": None,
                "status": "payment_pending",
                "expires_at": None,
                "idempotency_key": "idem_pending_existing",
                "created_at": datetime.utcnow(),
            },
        }

        with patch.object(self.main, "save_telegram_user_profile"), \
             patch.object(self.main, "register_checkout_attempt", return_value=(1, datetime.utcnow())), \
             patch.object(self.main, "claim_checkout_session_record", return_value=claim_result), \
             patch.object(self.main.stripe.checkout.Session, "create") as create_session, \
             patch.object(self.main, "get_db_conn", side_effect=[initial_conn, claim_conn]):
            await self.main.process_payment(callback, state)

        create_session.assert_not_called()
        self.assertIn("Оплата уже обрабатывается Stripe", callback.message.answers[-1][0])
        self.assertEqual(state.clear_calls, 1)

    async def test_unknown_subscription_callbacks_fail_safely_before_side_effects(self):
        invalid_callbacks = (
            "sub_foo",
            "sub_",
            "sub_unknown",
            "sub_24",
        )

        for callback_data in invalid_callbacks:
            with self.subTest(callback_data=callback_data):
                callback = FakeCallback()
                callback.data = callback_data
                state = FakeState()

                with patch.object(self.main, "save_telegram_user_profile") as save_profile, \
                     patch.object(self.main, "get_db_conn") as get_db_conn, \
                     patch.object(self.main, "register_checkout_attempt") as register_attempt, \
                     patch.object(self.main, "claim_checkout_session_record") as claim_checkout, \
                     patch.object(self.main.stripe.checkout.Session, "create") as create_session, \
                     patch.object(self.main.stripe.checkout.Session, "retrieve") as retrieve_session, \
                     patch.object(self.main.stripe.Subscription, "list") as list_subscriptions, \
                     patch.object(self.main.stripe.Subscription, "retrieve") as retrieve_subscription, \
                     patch.object(
                         self.main,
                         "enqueue_admin_payment_problem_now",
                         AsyncMock(),
                     ) as admin_alert, \
                     patch.object(
                         self.main,
                         "try_enqueue_checkout_preparation_failed_alert",
                         AsyncMock(),
                     ) as preparation_alert:
                    await self.main.process_payment(callback, state)

                self.assertEqual(len(callback.answers), 1)
                alert_text, alert_kwargs = callback.answers[0]
                self.assertTrue(alert_kwargs.get("show_alert"))
                self.assertIn("тариф больше недоступен", alert_text.lower())
                self.assertNotIn(callback_data, alert_text)
                self.assertEqual(callback.message.answers, [])
                self.assertEqual(state.clear_calls, 0)
                save_profile.assert_not_called()
                get_db_conn.assert_not_called()
                register_attempt.assert_not_called()
                claim_checkout.assert_not_called()
                create_session.assert_not_called()
                retrieve_session.assert_not_called()
                list_subscriptions.assert_not_called()
                retrieve_subscription.assert_not_called()
                admin_alert.assert_not_awaited()
                preparation_alert.assert_not_awaited()

    async def test_checkout_first_create_does_not_enqueue_reuse_alert(self):
        callback = FakeCallback()
        callback.data = "sub_1"
        state = FakeState()
        initial_conn = FakeConnection(fetches=[(False, None, False, False, None, False, None, False)])
        claim_conn = FakeConnection()
        open_conn = FakeConnection()
        claim_result = {
            "action": "create",
            "record": {
                "id": 71,
                "stripe_session_id": None,
                "checkout_url": None,
                "status": "creating",
                "expires_at": None,
                "idempotency_key": "idem_first_create",
                "created_at": datetime.utcnow(),
            },
        }
        session = SimpleNamespace(
            id="cs_first_create",
            url="https://checkout.stripe.test/first-create",
            expires_at=int(time.time()) + 600,
        )

        with patch.object(self.main, "save_telegram_user_profile"), \
             patch.object(self.main, "register_checkout_attempt", return_value=(1, datetime.utcnow().timestamp())), \
             patch.object(self.main, "claim_checkout_session_record", return_value=claim_result), \
             patch.object(self.main, "try_enqueue_checkout_preparation_failed_alert", AsyncMock()) as error_alert, \
             patch.object(self.main, "notify_admins_about_checkout_reuse", AsyncMock()) as reuse_alert, \
             patch.object(self.main.stripe.checkout.Session, "create", return_value=session) as create_session, \
             patch.object(self.main, "get_db_conn", side_effect=[initial_conn, claim_conn, open_conn]):
            await self.main.process_payment(callback, state)

        create_session.assert_called_once()
        reuse_alert.assert_not_awaited()
        error_alert.assert_not_awaited()
        self.assertIn("https://checkout.stripe.test/first-create", callback.message.answers[-1][0])
        self.assertEqual(state.clear_calls, 1)

    async def test_checkout_second_reuse_sends_button_without_admin_alert_or_new_session(self):
        callback = FakeCallback()
        callback.data = "sub_1"
        state = FakeState()
        initial_conn = FakeConnection(fetches=[(False, None, False, False, None, False, None, False)])
        claim_conn = FakeConnection()
        claim_result = {
            "action": "reuse_open",
            "record": {
                "id": 72,
                "stripe_session_id": "cs_reuse_second_full",
                "checkout_url": "https://checkout.stripe.test/stale",
                "status": "open",
                "expires_at": datetime.utcnow() + timedelta(minutes=10),
                "idempotency_key": "idem_reuse_second",
                "created_at": datetime.utcnow(),
            },
        }
        live_session = SimpleNamespace(
            id="cs_reuse_second_full",
            status="open",
            url="https://checkout.stripe.test/live-second",
            expires_at=int(time.time()) + 600,
        )

        with patch.object(self.main, "save_telegram_user_profile"), \
             patch.object(self.main, "register_checkout_attempt", return_value=(2, datetime.utcnow().timestamp())), \
             patch.object(self.main, "claim_checkout_session_record", return_value=claim_result), \
             patch.object(self.main.stripe.checkout.Session, "retrieve", return_value=live_session) as retrieve_session, \
             patch.object(self.main.stripe.checkout.Session, "create") as create_session, \
             patch.object(self.main, "try_enqueue_checkout_preparation_failed_alert", AsyncMock()) as error_alert, \
             patch.object(self.main, "get_db_conn", side_effect=[initial_conn, claim_conn]), \
             self.assertLogs(level="INFO") as logs:
            await self.main.process_payment(callback, state)

        retrieve_session.assert_called_once_with("cs_reuse_second_full")
        create_session.assert_not_called()
        error_alert.assert_not_awaited()
        self.assertIn("https://checkout.stripe.test/live-second", callback.message.answers[-1][0])
        log_output = "\n".join(logs.output)
        self.assertIn("CHECKOUT_REUSE_INFO", log_output)
        self.assertIn("alert_enqueued=False", log_output)
        self.assertNotIn("cs_reuse_second_full", log_output)
        self.assertNotIn("https://checkout.stripe.test/live-second", log_output)
        self.assertEqual(state.clear_calls, 1)

    async def test_checkout_third_reuse_enqueues_single_info_alert_without_failure_wording(self):
        callback = FakeCallback()
        callback.data = "sub_1"
        state = FakeState()
        initial_conn = FakeConnection(fetches=[(False, None, False, False, None, False, None, False)])
        claim_conn = FakeConnection()
        alert_conn = FakeConnection(fetches=[(None,), (73,), ("admin-1",), ("admin-2",)])
        claim_result = {
            "action": "reuse_open",
            "record": {
                "id": 73,
                "stripe_session_id": "cs_reuse_third_full",
                "checkout_url": "https://checkout.stripe.test/stale",
                "status": "open",
                "expires_at": datetime.utcnow() + timedelta(minutes=10),
                "idempotency_key": "idem_reuse_third",
                "created_at": datetime.utcnow(),
            },
        }
        live_session = SimpleNamespace(
            id="cs_reuse_third_full",
            status="open",
            url="https://checkout.stripe.test/live-third",
            expires_at=int(time.time()) + 600,
        )

        with patch.object(self.main, "save_telegram_user_profile"), \
             patch.object(self.main, "register_checkout_attempt", return_value=(3, datetime.utcnow().timestamp())), \
             patch.object(self.main, "claim_checkout_session_record", return_value=claim_result), \
             patch.object(self.main.stripe.checkout.Session, "retrieve", return_value=live_session), \
             patch.object(self.main.stripe.checkout.Session, "create") as create_session, \
             patch.object(self.main, "get_db_conn", side_effect=[initial_conn, claim_conn, alert_conn]):
            await self.main.process_payment(callback, state)

        create_session.assert_not_called()
        payloads = [json.loads(adapted_json_value(params[3])) for params in self.admin_delivery_inserts(alert_conn)]
        self.assertEqual(len(payloads), len(self.main.ADMIN_IDS))
        alert_text = payloads[0]["text"]
        self.assertEqual(payloads[0]["severity"], "INFO")
        self.assertIn("ℹ️ Повторный запрос ссылки на оплату", alert_text)
        self.assertIn("Попыток за последние 5 минут: 3", alert_text)
        self.assertIn("Активная Stripe Checkout ссылка была отправлена пользователю повторно.", alert_text)
        self.assertNotIn("не удалось создать ссылку оплаты", alert_text)
        self.assertNotIn("Оплата не завершена", alert_text)
        self.assertNotIn("Stripe Checkout успешно создан", alert_text)
        self.assertNotIn("https://checkout.stripe.test/live-third", alert_text)
        self.assertNotIn("cs_reuse_third_full", alert_text)

    async def test_checkout_fourth_reuse_in_cooldown_dedupes_info_alert(self):
        callback = FakeCallback()
        callback.data = "sub_1"
        state = FakeState()
        initial_conn = FakeConnection(fetches=[(False, None, False, False, None, False, None, False)])
        claim_conn = FakeConnection()
        alert_conn = FakeConnection(fetches=[(datetime.utcnow(),)])
        claim_result = {
            "action": "reuse_open",
            "record": {
                "id": 74,
                "stripe_session_id": "cs_reuse_fourth_full",
                "checkout_url": "https://checkout.stripe.test/stale",
                "status": "open",
                "expires_at": datetime.utcnow() + timedelta(minutes=10),
                "idempotency_key": "idem_reuse_fourth",
                "created_at": datetime.utcnow(),
            },
        }
        live_session = SimpleNamespace(
            id="cs_reuse_fourth_full",
            status="open",
            url="https://checkout.stripe.test/live-fourth",
            expires_at=int(time.time()) + 600,
        )

        with patch.object(self.main, "save_telegram_user_profile"), \
             patch.object(self.main, "register_checkout_attempt", return_value=(4, datetime.utcnow().timestamp())), \
             patch.object(self.main, "claim_checkout_session_record", return_value=claim_result), \
             patch.object(self.main.stripe.checkout.Session, "retrieve", return_value=live_session), \
             patch.object(self.main.stripe.checkout.Session, "create") as create_session, \
             patch.object(self.main, "get_db_conn", side_effect=[initial_conn, claim_conn, alert_conn]), \
             self.assertLogs(level="INFO") as logs:
            await self.main.process_payment(callback, state)

        create_session.assert_not_called()
        self.assertEqual(self.admin_delivery_inserts(alert_conn), [])
        self.assertIn("alert_enqueued=False", "\n".join(logs.output))
        self.assertEqual(state.clear_calls, 1)

    async def test_checkout_create_exception_enqueues_preparation_failed_alert(self):
        callback = FakeCallback()
        callback.data = "sub_1"
        state = FakeState()
        initial_conn = FakeConnection(fetches=[(False, None, False, False, None, False, None, False)])
        claim_conn = FakeConnection()
        failed_conn = FakeConnection()
        claim_result = {
            "action": "create",
            "record": {
                "id": 75,
                "stripe_session_id": None,
                "checkout_url": None,
                "status": "creating",
                "expires_at": None,
                "idempotency_key": "idem_create_failed",
                "created_at": datetime.utcnow(),
            },
        }

        with patch.object(self.main, "save_telegram_user_profile"), \
             patch.object(self.main, "register_checkout_attempt", return_value=(1, datetime.utcnow().timestamp())), \
             patch.object(self.main, "claim_checkout_session_record", return_value=claim_result), \
             patch.object(self.main.stripe.checkout.Session, "create", side_effect=RuntimeError("stripe create boom")), \
             patch.object(self.main, "try_enqueue_checkout_preparation_failed_alert", AsyncMock()) as error_alert, \
             patch.object(self.main, "get_db_conn", side_effect=[initial_conn, claim_conn, failed_conn]):
            await self.main.process_payment(callback, state)

        error_alert.assert_awaited_once()
        self.assertEqual(error_alert.await_args.args[2], "checkout_preparation_failed")
        self.assertEqual(callback.answers[-1][0], "Техническая ошибка. Попробуйте позже или напишите @re_tasha")
        self.assertTrue(callback.answers[-1][1]["show_alert"])

    async def test_checkout_reuse_retrieve_exception_enqueues_preparation_failed_alert_without_button(self):
        callback = FakeCallback()
        callback.data = "sub_1"
        state = FakeState()
        initial_conn = FakeConnection(fetches=[(False, None, False, False, None, False, None, False)])
        claim_conn = FakeConnection()
        claim_result = {
            "action": "reuse_open",
            "record": {
                "id": 76,
                "stripe_session_id": "cs_retrieve_failed_full",
                "checkout_url": "https://checkout.stripe.test/stale",
                "status": "open",
                "expires_at": datetime.utcnow() + timedelta(minutes=10),
                "idempotency_key": "idem_retrieve_failed",
                "created_at": datetime.utcnow(),
            },
        }

        with patch.object(self.main, "save_telegram_user_profile"), \
             patch.object(self.main, "register_checkout_attempt", return_value=(2, datetime.utcnow().timestamp())), \
             patch.object(self.main, "claim_checkout_session_record", return_value=claim_result), \
             patch.object(self.main.stripe.checkout.Session, "retrieve", side_effect=RuntimeError("stripe retrieve boom")), \
             patch.object(self.main.stripe.checkout.Session, "create") as create_session, \
             patch.object(self.main, "try_enqueue_checkout_preparation_failed_alert", AsyncMock()) as error_alert, \
             patch.object(self.main, "get_db_conn", side_effect=[initial_conn, claim_conn]):
            await self.main.process_payment(callback, state)

        create_session.assert_not_called()
        error_alert.assert_awaited_once()
        self.assertEqual(error_alert.await_args.args[2], "checkout_session_retrieve_failed")
        self.assertEqual(callback.message.answers, [])
        self.assertEqual(callback.answers[-1][0], "Техническая ошибка. Попробуйте позже или напишите @re_tasha")

    async def test_checkout_reuse_missing_url_enqueues_preparation_failed_alert_without_button(self):
        callback = FakeCallback()
        callback.data = "sub_1"
        state = FakeState()
        initial_conn = FakeConnection(fetches=[(False, None, False, False, None, False, None, False)])
        claim_conn = FakeConnection()
        claim_result = {
            "action": "reuse_open",
            "record": {
                "id": 77,
                "stripe_session_id": "cs_missing_url_full",
                "checkout_url": "https://checkout.stripe.test/stale",
                "status": "open",
                "expires_at": datetime.utcnow() + timedelta(minutes=10),
                "idempotency_key": "idem_missing_url",
                "created_at": datetime.utcnow(),
            },
        }
        live_session = SimpleNamespace(
            id="cs_missing_url_full",
            status="open",
            url=None,
            expires_at=int(time.time()) + 600,
        )

        with patch.object(self.main, "save_telegram_user_profile"), \
             patch.object(self.main, "register_checkout_attempt", return_value=(2, datetime.utcnow().timestamp())), \
             patch.object(self.main, "claim_checkout_session_record", return_value=claim_result), \
             patch.object(self.main.stripe.checkout.Session, "retrieve", return_value=live_session), \
             patch.object(self.main.stripe.checkout.Session, "create") as create_session, \
             patch.object(self.main, "try_enqueue_checkout_preparation_failed_alert", AsyncMock()) as error_alert, \
             patch.object(self.main, "get_db_conn", side_effect=[initial_conn, claim_conn]):
            await self.main.process_payment(callback, state)

        create_session.assert_not_called()
        error_alert.assert_awaited_once()
        self.assertEqual(error_alert.await_args.args[2], "checkout_session_missing_url")
        self.assertEqual(callback.message.answers, [])

    async def test_checkout_preparation_failed_text_is_checkout_specific(self):
        text = self.main.build_checkout_preparation_failed_text(
            123,
            "checkout_session_retrieve_failed",
            "checkout_preparation_failed:safe-ref",
        )

        self.assertIn("⚠️ Не удалось подготовить оплату", text)
        self.assertIn("Этап: Checkout", text)
        self.assertIn("Причина: checkout_session_retrieve_failed", text)
        self.assertIn("Error ref: checkout_preparation_failed:safe-ref", text)
        self.assertNotIn("Оплата не завершена", text)
        self.assertNotIn("не удалось создать ссылку оплаты", text)

    async def test_checkout_creation_pending_other_tariff_does_not_create_new_session(self):
        callback = FakeCallback()
        callback.data = "sub_6"
        state = FakeState()
        initial_conn = FakeConnection(fetches=[(False, None, False, False, None, False, None, False)])
        pending_created_at = datetime.utcnow() - timedelta(minutes=5)
        claim_conn = FakeConnection(fetches=[
            (61, "cs_pending_sub_1", None, "payment_pending", None, "idem_pending_sub_1", pending_created_at),
        ])

        with patch.object(self.main, "save_telegram_user_profile"), \
             patch.object(self.main, "register_checkout_attempt", return_value=(1, datetime.utcnow())), \
             patch.object(self.main.stripe.checkout.Session, "create") as create_session, \
             patch.object(self.main, "get_db_conn", side_effect=[initial_conn, claim_conn]):
            await self.main.process_payment(callback, state)

        create_session.assert_not_called()
        self.assertIn("Оплата уже обрабатывается Stripe", callback.message.answers[-1][0])
        self.assertEqual(state.clear_calls, 1)

    async def test_checkout_async_success_subscription_mode_stays_link_only(self):
        result = await self.run_checkout_async_success_webhook(
            mode="subscription",
            event_id="evt_async_success_subscription",
            session_id="cs_async_success_subscription",
        )

        self.assertEqual(result.response.status, 200)
        result.mark_processed.assert_awaited_once_with(result.event_id, 1)
        result.release.assert_not_awaited()
        sql = "\n".join(query for query, _ in result.conn.cursor_obj.queries)
        self.assertNotIn("INSERT INTO users", sql)
        self.assertNotIn("INSERT INTO payment_events", sql)
        self.assertNotIn("INSERT INTO access_events", sql)

    async def test_checkout_days_seven_success_path_still_processes(self):
        event = SimpleNamespace(
            id="evt_days_valid_7",
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(
                id="cs_days_valid_7",
                client_reference_id="123",
                metadata={"days": "7"},
                mode="payment",
                payment_status="paid",
                customer="cus_days_valid_7",
                subscription=None,
                amount_total=1000,
                currency="usd",
            )),
        )
        payload = json.dumps({
            "id": "evt_days_valid_7",
            "object": "event",
            "type": "checkout.session.completed",
            "data": {"object": {"id": "cs_days_valid_7", "client_reference_id": "123", "metadata": {"days": "7"}}},
        }).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[
            (123, "sub_trial", "payment", "open"),
            *self.stripe_identity_available_fetches(),
            (False, None, False, False),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "claim_trial_redemption", return_value=True), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with("evt_days_valid_7", 1)
        release.assert_not_awaited()
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("INSERT INTO users", sql)
        self.assertIn("INSERT INTO payment_events", sql)
        self.assertIn("INSERT INTO access_events", sql)

    async def test_outbox_status_duration_helpers_never_go_negative(self):
        now = datetime(2026, 7, 29, 13, 16)
        future = now + timedelta(minutes=9)
        past = now - timedelta(minutes=18)

        self.assertEqual(self.main.format_elapsed_duration(self.main.outbox_unresolved_age_seconds(now, future)), "0 мин.")
        self.assertEqual(self.main.format_elapsed_duration(self.main.outbox_unresolved_age_seconds(now, past)), "18 мин.")
        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, future)), "9 мин.")
        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, past)), "нет")

    async def test_outbox_status_duration_helpers_accept_aware_now_and_naive_timestamps(self):
        db_now = datetime(2026, 7, 29, 13, 16, tzinfo=timezone.utc)
        oldest_unresolved = datetime(2026, 7, 29, 13, 4)
        next_retry = datetime(2026, 7, 29, 13, 25)

        self.assertEqual(self.main.outbox_unresolved_age_seconds(db_now, oldest_unresolved), 12 * 60)
        self.assertEqual(self.main.outbox_next_retry_seconds(db_now, next_retry), 9 * 60)

    async def test_outbox_status_duration_helpers_accept_naive_now_and_aware_timestamps(self):
        db_now = datetime(2026, 7, 29, 13, 16)
        oldest_unresolved = datetime(2026, 7, 29, 13, 4, tzinfo=timezone.utc)
        next_retry = datetime(2026, 7, 29, 13, 25, tzinfo=timezone.utc)

        self.assertEqual(self.main.outbox_unresolved_age_seconds(db_now, oldest_unresolved), 12 * 60)
        self.assertEqual(self.main.outbox_next_retry_seconds(db_now, next_retry), 9 * 60)

    async def test_outbox_status_future_retry_rounds_up_minutes(self):
        now = datetime(2026, 7, 29, 13, 16)

        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, now + timedelta(seconds=20))), "1 мин.")
        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, now + timedelta(seconds=61))), "2 мин.")
        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, now)), "нет")
        self.assertEqual(self.main.format_future_duration(self.main.outbox_next_retry_seconds(now, now - timedelta(seconds=1))), "нет")

    async def test_outbox_status_uses_utc_naive_now_query(self):
        now = datetime(2026, 7, 29, 13, 16)
        conn = FakeConnection(fetches=[
            (now,),
            [],
            [],
            [],
            (0,),
            (None,),
            (None,),
            (None,),
            (0,),
            (0,),
            None,
            [],
        ])
        message = FakeIncomingMessage(user_id=1)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.outbox_status_command(message)

        self.assertEqual(conn.cursor_obj.queries[0][0], "SELECT NOW() AT TIME ZONE 'UTC'")

    async def test_feedback_join_closes_db_before_telegram_replies(self):
        conn = FakeConnection(fetches=[(False, False)])
        callback = FakeCallback(user_id=123)
        state = FakeState()
        answer_checks = []

        async def answer_after_close(*args, **kwargs):
            answer_checks.append(conn.closed)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            callback.message.answer = AsyncMock(side_effect=answer_after_close)
            callback.answer = AsyncMock(side_effect=lambda *args, **kwargs: answer_checks.append(conn.closed))
            await self.main.feedback_join(callback, state)

        self.assertEqual(answer_checks, [True, True])
        self.assertEqual(state.states, [self.main.RegistrationStates.choice])
        self.assertEqual(conn.commits, 1)

    async def test_feedback_think_closes_db_before_telegram_replies(self):
        conn = FakeConnection()
        callback = FakeCallback(user_id=123)
        state = FakeState()
        answer_checks = []

        async def answer_after_close(*args, **kwargs):
            answer_checks.append(conn.closed)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            callback.message.answer = AsyncMock(side_effect=answer_after_close)
            callback.answer = AsyncMock(side_effect=lambda *args, **kwargs: answer_checks.append(conn.closed))
            await self.main.feedback_think(callback, state)

        self.assertEqual(answer_checks, [True, True])
        self.assertEqual(state.clear_calls, 1)
        self.assertEqual(conn.commits, 1)

    def test_tracked_db_pool_concurrent_accounting_and_double_put_guard(self):
        class RawPool:
            def __init__(self, *args, **kwargs):
                self.lock = threading.Lock()
                self.next_id = 0
                self.puts = []

            def getconn(self):
                with self.lock:
                    self.next_id += 1
                    return SimpleNamespace(id=self.next_id)

            def putconn(self, conn, close=False):
                with self.lock:
                    self.puts.append((conn.id, close))

            def closeall(self):
                pass

        with patch.object(self.main.psycopg2_pool, "ThreadedConnectionPool", RawPool):
            pool = self.main.TrackedThreadedConnectionPool(1, 5, "postgresql://example")
            errors = []

            def worker():
                try:
                    conn = pool.getconn()
                    self.assertGreaterEqual(pool.health()["pool_used"], 1)
                    pool.putconn(conn)
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.assertEqual(errors, [])
            self.assertEqual(pool.health()["pool_used"], 0)
            self.assertEqual(pool.health()["pool_available"], 5)
            raw_conn = pool.getconn()
            pool.putconn(raw_conn)
            pool.putconn(raw_conn)
            self.assertEqual(pool.health()["pool_used"], 0)
            self.assertEqual(len(pool._pool.puts), 9)

    async def test_outbox_status_text_shows_retry_and_no_negative_age(self):
        now = datetime(2026, 7, 29, 13, 16)
        next_retry = now + timedelta(minutes=9)
        conn = FakeConnection(fetches=[
            (now,),
            [("pending", 1), ("failed", 1)],
            [("free_lesson", "failed", 1)],
            [("pending", 1), ("cancelled", 2)],
            (0,),
            (next_retry,),
            (next_retry,),
            (next_retry,),
            (2,),
            (0,),
            None,
            [],
        ])
        message = FakeIncomingMessage(user_id=1)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.outbox_status_command(message)

        text = message.answers[0][0]
        self.assertIn("oldest unresolved age: 0 мин.", text)
        self.assertIn("next retry in: 9 мин.", text)
        self.assertIn("nearest next_attempt_at: 29.07.2026 13:25", text)
        self.assertIn("First-purchase recovery:", text)
        self.assertIn("cancelled: 2", text)
        self.assertNotIn("oldest unresolved age: -", text)
        self.assertNotIn("next retry in: -", text)
        self.assertLess(len(text), 4096)

    async def test_outbox_status_due_or_empty_retry_text_is_none(self):
        now = datetime(2026, 7, 29, 13, 16)
        for oldest_unresolved, next_retry_at, expected_age in ((now - timedelta(minutes=3), None, "3 мин."), (None, None, "нет")):
            with self.subTest(oldest_unresolved=oldest_unresolved):
                conn = FakeConnection(fetches=[
                    (now,),
                    [],
                    [],
                    [],
                    (0,),
                    (oldest_unresolved,),
                    (next_retry_at,),
                    (next_retry_at,),
                    (0,),
                    (0,),
                    None,
                    [],
                ])
                message = FakeIncomingMessage(user_id=1)

                with patch.object(self.main, "get_db_conn", return_value=conn):
                    await self.main.outbox_status_command(message)

                text = message.answers[0][0]
                self.assertIn(f"oldest unresolved age: {expected_age}", text)
                self.assertIn("next retry in: нет", text)
                self.assertIn("nearest next_attempt_at: нет", text)
                self.assertLess(len(text), 4096)

    async def test_message_delivery_invalid_payload_does_not_stop_batch(self):
        connections = [FakeConnection(), FakeConnection(), FakeConnection()]
        failed_calls = []
        send_message = AsyncMock()

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("bad_payload", 123, "stripe_user_message", "{bad json", 1, None, 1),
                 ("good_payload", 124, "stripe_user_message", '{"text":"ok"}', 1, None, 1),
             ]), \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent"), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("bad_payload", "JSONDecodeError", False)])
        send_message.assert_awaited_once()

    async def test_stripe_admin_message_delivery_sends_to_admin_without_user_blocking(self):
        send_message = AsyncMock()
        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe-admin:evt_admin:payment_success:1", 1, "stripe_admin_message", '{"text":"admin ok"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        send_message.assert_awaited_once_with(1, "admin ok", parse_mode=None)
        mark_sent.assert_called_once()

    async def test_stripe_admin_message_forbidden_is_terminal_without_user_block(self):
        from aiogram.exceptions import TelegramForbiddenError

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        connections = [FakeConnection(), FakeConnection()]
        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe-admin:evt_admin:payment_success:1", 1, "stripe_admin_message", '{"text":"admin"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="bot blocked"))), \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe-admin:evt_admin:payment_success:1", "TelegramForbiddenError", None, True)])
        self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))

    async def test_stripe_admin_message_blocked_admin_does_not_stop_other_admin(self):
        from aiogram.exceptions import TelegramForbiddenError

        failed_calls = []
        sent_calls = []

        async def fake_send_message(chat_id, text, parse_mode=None):
            if chat_id == 1:
                raise TelegramForbiddenError(method=None, message="bot blocked")
            sent_calls.append((chat_id, text, parse_mode))

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        connections = [FakeConnection(), FakeConnection(), FakeConnection()]
        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe-admin:evt_admin:payment_success:1", 1, "stripe_admin_message", '{"text":"admin 1"}', 1, None, 1),
                 ("stripe-admin:evt_admin:payment_success:2", 2, "stripe_admin_message", '{"text":"admin 2"}', 1, None, 1),
             ]), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=fake_send_message)) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 1, "blocked": 0})
        self.assertEqual(send_message.await_count, 2)
        self.assertEqual(sent_calls, [(2, "admin 2", None)])
        self.assertEqual(failed_calls, [("stripe-admin:evt_admin:payment_success:1", "TelegramForbiddenError", None, True)])
        mark_sent.assert_called_once()
        self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))

    async def test_stripe_admin_message_network_error_is_retryable(self):
        from aiogram.exceptions import TelegramNetworkError

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe-admin:evt_admin:payment_success:1", 1, "stripe_admin_message", '{"text":"admin"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramNetworkError(method=None, message="network"))), \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe-admin:evt_admin:payment_success:1", "TelegramNetworkError", 5, False)])

    async def test_stripe_admin_message_invalid_payload_is_terminal(self):
        failed_calls = []

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, str(exc), retry_delay_minutes, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe-admin:evt_admin:payment_success:1", 1, "stripe_admin_message", '{}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe-admin:evt_admin:payment_success:1", "invalid_stripe_admin_message_payload", None, True)])
        send_message.assert_not_awaited()

    async def test_gift_certificate_delivery_runtime_caption_and_button_share_deep_link(self):
        public_reference = "GIFT-ABCD1234ABCD1234"
        token_version = 2
        gift_env = {
            "BOT_USERNAME": "ClubGiftBot",
            "GIFT_TOKEN_SECRET": "unit-test-gift-token-secret-32chars",
        }
        with patch.dict(os.environ, gift_env):
            token_hash = self.main.gift_token_hash_for_reference(public_reference, token_version)
        payload = {
            "public_reference": public_reference,
            "token_version": token_version,
            "recipient_kind": "buyer",
            "caption": (
                "🎁 Подарочный сертификат в клуб Натальи Ребковец\n\n"
                "Для: Анна\n"
                "От: Виктория\n"
                "Срок доступа: 1 месяц\n\n"
                "Активировать подарок можно по кнопке или ссылке ниже."
            ),
            "parse_mode": "HTML",
            "button_text": "🎁 Активировать подарок",
        }
        gift_row = {
            "public_reference": public_reference,
            "token_version": token_version,
            "token_hash": token_hash,
            "status": "paid_unclaimed",
            "tariff_code": "gift_1m",
            "certificate_name": "Анна",
        }
        send_photo = AsyncMock()

        with patch.dict(os.environ, gift_env), \
             patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 (
                     self.main.gift_delivery_key(public_reference, self.main.GIFT_CERTIFICATE_BUYER, token_version=token_version, recipient_kind="buyer"),
                     123,
                     self.main.GIFT_CERTIFICATE_BUYER,
                     json.dumps(payload, ensure_ascii=False),
                     1,
                     None,
                     1,
                 )
             ]), \
             patch.object(self.main, "fetch_gift_by_public_reference_version", return_value=gift_row), \
             patch.object(self.main.bot, "send_photo", send_photo), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        send_photo.assert_awaited_once()
        _, args, kwargs = send_photo.mock_calls[0]
        self.assertEqual(args[0], 123)
        self.assertIsInstance(args[1], self.main.FSInputFile)
        self.assertFalse(Path(args[1].path).exists())
        caption = kwargs["caption"]
        button_url = kwargs["reply_markup"].inline_keyboard[0][0].url
        self.assertIn("https://t.me/ClubGiftBot?start=gift_", caption)
        self.assertIn("🎁 Подарочный сертификат в клуб Натальи Ребковец", caption)
        self.assertIn("Для: Анна", caption)
        self.assertIn("Если кнопка не открывается, используйте эту ссылку:", caption)
        self.assertEqual(button_url, caption.rsplit("\n", 1)[-1])
        self.assertLessEqual(len(caption), self.main.GIFT_CERTIFICATE_CAPTION_LIMIT)
        mark_sent.assert_called_once()

    async def test_gift_certificate_generation_failure_keeps_delivery_retryable_and_enqueues_safe_notice(self):
        public_reference = "GIFT-ABCD1234ABCD1234"
        token_version = 2
        raw_name = "Секретное Имя"
        with patch.dict(os.environ, {
            "BOT_USERNAME": "ClubGiftBot",
            "GIFT_TOKEN_SECRET": "unit-test-gift-token-secret-32chars",
        }):
            token_hash = self.main.gift_token_hash_for_reference(public_reference, token_version)
        payload = {
            "public_reference": public_reference,
            "token_version": token_version,
            "recipient_kind": "buyer",
            "caption": "certificate",
            "parse_mode": "HTML",
            "button_text": "🎁 Активировать подарок",
        }
        gift_row = {
            "public_reference": public_reference,
            "token_version": token_version,
            "token_hash": token_hash,
            "status": "paid_unclaimed",
            "tariff_code": "gift_1m",
            "certificate_name": raw_name,
        }

        with patch.dict(os.environ, {
            "BOT_USERNAME": "ClubGiftBot",
            "GIFT_TOKEN_SECRET": "unit-test-gift-token-secret-32chars",
        }), patch.object(
            self.main,
            "get_db_conn",
            side_effect=[FakeConnection(), FakeConnection(), FakeConnection()],
        ), patch.object(self.main, "claim_pending_message_deliveries", return_value=[(
                 self.main.gift_delivery_key(
                     public_reference,
                     self.main.GIFT_CERTIFICATE_BUYER,
                     token_version=token_version,
                     recipient_kind="buyer",
                 ),
                 123,
                 self.main.GIFT_CERTIFICATE_BUYER,
                 json.dumps(payload, ensure_ascii=False),
                 1,
                 None,
                 1,
             )]), \
             patch.object(self.main, "fetch_gift_by_public_reference_version", return_value=gift_row), \
             patch.object(self.main, "render_gift_certificate", side_effect=RuntimeError("render_failed")), \
             patch.object(self.main, "enqueue_gift_certificate_failure_notices") as notices, \
             patch.object(self.main, "mark_delivery_failed", return_value="failed"), \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             self.assertLogs(level="WARNING") as logs:
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result["retryable_failed"], 1)
        notices.assert_called_once_with(public_reference, 123, gift_row)
        self.assertNotIn(raw_name, "\n".join(logs.output))

    async def test_admin_payment_helpers_enqueue_all_admins_and_sanitize_problem_text(self):
        conn = FakeConnection(fetches=[("admin-1",), ("admin-2",)])
        with patch.object(self.main, "ADMIN_IDS", [1, 2]):
            created = self.main.enqueue_admin_payment_problem(
                conn.cursor_obj,
                "evt_problem",
                "invoice_payment_failed",
                "invoice_payment_failed",
                telegram_id=123,
                stripe_code="card_declined",
                safe_ref="invoice_problem:abc123",
                note="payment_failed message: поставлено в outbox",
            )

        self.assertEqual(created, 2)
        deliveries = self.admin_delivery_inserts(conn)
        self.assertEqual([params[0] for params in deliveries], [
            "stripe-admin:evt_problem:invoice_payment_failed:1",
            "stripe-admin:evt_problem:invoice_payment_failed:2",
        ])
        payloads = [json.loads(adapted_json_value(params[3])) for params in deliveries]
        self.assertTrue(all(payload["category"] == "card_declined" for payload in payloads))
        self.assertTrue(all("банк отклонил карту" in payload["text"] for payload in payloads))
        self.assertTrue(all("None" not in payload["text"] for payload in payloads))

    async def test_admin_payment_problem_now_db_error_is_best_effort_and_sanitized(self):
        conn = ExecuteFailingConnection(error=RuntimeError("raw admin db secret"))

        with patch.object(self.main, "get_db_conn", return_value=conn), \
             self.assertLogs(level="WARNING") as logs:
            result = await self.main.try_enqueue_admin_payment_problem_now(
                event_id="evt_admin_now",
                purpose="checkout_creation_failed",
                stage="checkout_creation",
                telegram_id=123,
                category="checkout_creation_failed",
                stripe_retry="неизвестно",
                recovery_reminder="неизвестно",
                safe_ref="checkout_creation_failed:safe-ref",
                note="safe note",
            )

        self.assertFalse(result)
        self.assertEqual(conn.rollbacks, 1)
        self.assertEqual(conn.commits, 0)
        self.assertTrue(conn.closed)
        log_output = "\n".join(logs.output)
        self.assertIn("ADMIN_PAYMENT_NOTIFICATION_ENQUEUE_FAILED", log_output)
        self.assertIn("checkout_creation_failed", log_output)
        self.assertIn("checkout_creation_failed:safe-ref", log_output)
        self.assertNotIn("raw admin db secret", log_output)

    async def test_checkout_creation_admin_outbox_error_still_answers_callback(self):
        callback = FakeCallback()
        callback.data = "sub_1"
        state = FakeState()
        initial_conn = FakeConnection(fetches=[(False, None, False, False, None, False, None, False)])
        claim_conn = FakeConnection()
        admin_conn = ExecuteFailingConnection(error=RuntimeError("raw checkout admin secret"))

        with patch.object(self.main, "save_telegram_user_profile"), \
             patch.object(self.main, "register_checkout_attempt", return_value=(1, datetime.utcnow())), \
             patch.object(self.main, "claim_checkout_session_record", side_effect=RuntimeError("claim failed")), \
             patch.object(self.main, "get_db_conn", side_effect=[initial_conn, claim_conn, admin_conn]), \
             self.assertLogs(level="WARNING") as logs:
            await self.main.process_payment(callback, state)

        self.assertEqual(callback.answers[-1][0], "Техническая ошибка. Попробуйте позже или напишите @re_tasha")
        self.assertTrue(callback.answers[-1][1]["show_alert"])
        self.assertTrue(initial_conn.closed)
        self.assertTrue(claim_conn.closed)
        self.assertTrue(admin_conn.closed)
        self.assertEqual(admin_conn.rollbacks, 1)
        self.assertIn("ADMIN_PAYMENT_NOTIFICATION_ENQUEUE_FAILED", "\n".join(logs.output))
        self.assertNotIn("raw checkout admin secret", "\n".join(logs.output))

    async def test_webhook_error_path_admin_outbox_error_still_releases_claim(self):
        event_id = "evt_checkout_expired_db_error"
        session_payload = {"id": "cs_expired_db_error", "client_reference_id": "123"}
        event = SimpleNamespace(
            id=event_id,
            type="checkout.session.expired",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(**session_payload)),
        )
        request = FakeStripeRequest(
            json.dumps({"id": event_id, "type": "checkout.session.expired", "data": {"object": session_payload}}).encode("utf-8"),
            {"Stripe-Signature": "sig", "Content-Type": "application/json"},
        )

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "get_db_conn", side_effect=[
                 RuntimeError("primary checkout expired failure"),
                 RuntimeError("raw admin webhook secret"),
             ]), \
             self.assertLogs(level="WARNING") as logs:
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 500)
        release.assert_awaited_once_with(event_id, 1)
        mark_processed.assert_not_awaited()
        log_output = "\n".join(logs.output)
        self.assertIn("ADMIN_PAYMENT_NOTIFICATION_ENQUEUE_FAILED", log_output)
        self.assertNotIn("raw admin webhook secret", log_output)

    async def test_admin_recovery_reminder_status_uses_durable_proof(self):
        self.assertEqual(
            self.main.admin_recovery_reminder_status(immediate_retry_enqueued=True),
            "не применимо",
        )
        self.assertNotEqual(
            self.main.admin_recovery_reminder_status(immediate_retry_enqueued=True),
            "запланировано",
        )
        self.assertEqual(
            self.main.admin_recovery_reminder_status(durable_24h_enqueued=True),
            "запланировано",
        )
        self.assertEqual(
            self.main.admin_recovery_reminder_status(scheduler_will_check=True),
            "будет проверено через 24 часа",
        )
        self.assertEqual(self.main.admin_recovery_reminder_status(), "неизвестно")

    async def test_checkout_expired_is_log_only_and_keeps_retry_payment(self):
        event_id = "evt_checkout_expired_retry_note"
        session_payload = {"id": "cs_expired_retry_note", "client_reference_id": "123"}
        event = SimpleNamespace(
            id=event_id,
            type="checkout.session.expired",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(**session_payload)),
        )
        request = FakeStripeRequest(
            json.dumps({"id": event_id, "type": "checkout.session.expired", "data": {"object": session_payload}}).encode("utf-8"),
            {"Stripe-Signature": "sig", "Content-Type": "application/json"},
        )
        conn = FakeConnection(fetches=[
            ("stripe:%s:checkout_expired" % event_id,),
            ("stripe-admin:%s:checkout_expired:1" % event_id,),
            ("stripe-admin:%s:checkout_expired:2" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        user_payload = json.loads(adapted_json_value(self.user_delivery_inserts(conn)[0][3]))
        self.assertEqual(user_payload["keyboard_kind"], "retry_payment")
        self.assertFalse(self.admin_delivery_inserts(conn))

    async def test_stripeobject_metadata_regular_checkout_expired_does_not_enter_gift_branch(self):
        event_id = "evt_stripeobject_regular_expired"
        session = self.stripe_object({
            "id": "cs_regular_expired",
            "client_reference_id": "123",
            "metadata": {},
        })
        self.assertFalse(hasattr(session.metadata, "get"))
        event = self.stripe_object({
            "id": event_id,
            "type": "checkout.session.expired",
            "created": 1720000000,
            "data": {"object": session},
        })
        request = FakeStripeRequest(
            json.dumps({"id": event_id, "type": "checkout.session.expired", "data": {"object": {"id": "cs_regular_expired"}}}).encode("utf-8"),
            {"Stripe-Signature": "sig", "Content-Type": "application/json"},
        )
        conn = FakeConnection(fetches=[
            ("stripe:%s:checkout_expired" % event_id,),
            ("stripe-admin:%s:checkout_expired:1" % event_id,),
            ("stripe-admin:%s:checkout_expired:2" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        self.assertTrue(self.user_delivery_inserts(conn))

    async def test_stripeobject_metadata_gift_checkout_expired_cancels_and_is_idempotent(self):
        event_id = "evt_stripeobject_gift_expired"
        gift_id = "gift-id-expired"
        gift_row = {
            "id": gift_id,
            "public_reference": "GIFT-EXPIRED0000001",
            "purchaser_telegram_id": 123,
            "recipient_telegram_id": None,
            "tariff_code": "gift_1m",
            "status": "checkout_open",
            "token_version": 1,
            "token_hash": "safe-hash",
        }
        updated_row = {**gift_row, "status": "cancelled"}
        session = self.stripe_object({
            "id": "cs_gift_expired",
            "client_reference_id": "123",
            "metadata": {
                "payment_kind": self.main.GIFT_PAYMENT_KIND,
                "gift_id": gift_id,
                "purchaser_telegram_id": "123",
            },
        })
        self.assertFalse(hasattr(session.metadata, "get"))
        event = self.stripe_object({
            "id": event_id,
            "type": "checkout.session.expired",
            "created": 1720000000,
            "data": {"object": session},
        })
        request = FakeStripeRequest(
            json.dumps({"id": event_id, "type": "checkout.session.expired", "data": {"object": {"id": "cs_gift_expired"}}}).encode("utf-8"),
            {"Stripe-Signature": "sig", "Content-Type": "application/json"},
        )
        conn = FakeConnection(fetches=[
            ("select gift",),
            ("updated gift",),
            ("gift:GIFT-EXPIRED0000001:gift_checkout_expired_buyer",),
            ("gift-admin:GIFT-EXPIRED0000001:problem",),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(side_effect=[("claimed", 1), ("duplicate_processing", None)])), \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "gift_row_dict", side_effect=[gift_row, updated_row]), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response1 = await self.main.stripe_webhook(request)
            response2 = await self.main.stripe_webhook(request)

        self.assertEqual(response1.status, 200)
        self.assertEqual(response2.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        update_queries = [query for query, _ in conn.cursor_obj.queries if "UPDATE gift_access_grants" in query]
        self.assertTrue(update_queries)
        self.assertIn("cancelled_at", update_queries[0])
        self.assertFalse(self.admin_delivery_inserts(conn))

    async def test_subscription_notification_policy_only_alerts_on_errors(self):
        policy = self.main.subscription_check_requires_admin_notification
        self.assertFalse(policy(0, 0, 0))
        self.assertFalse(policy(3, 0, 0))
        self.assertTrue(policy(0, 1, 0))
        self.assertTrue(policy(0, 0, 1))

        with patch.object(self.main, "notify_admins", AsyncMock()) as notify:
            self.assertFalse(await self.main.notify_subscription_check_admins_if_needed("empty", 0, 0, 0))
            self.assertFalse(await self.main.notify_subscription_check_admins_if_needed("removed", 3, 0, 0))
            self.assertTrue(await self.main.notify_subscription_check_admins_if_needed("warning", 0, 1, 0))

        notify.assert_awaited_once_with("warning")

    async def test_weekly_report_all_admins_succeed_and_complete(self):
        conns = [FakeConnection() for _ in range(4)]
        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "claim_weekly_report_run", return_value={
                 "status": "claimed", "sent_admin_ids": [], "permanent_admin_ids": [],
             }), \
             patch.object(self.main, "build_weekly_admin_report", AsyncMock(return_value=("report", []))), \
             patch.object(self.main, "get_db_conn", side_effect=conns), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.send_weekly_admin_report()

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["sent_admin_ids"], [1, 2])
        self.assertEqual(send_message.await_count, 2)
        final_sql = "\n".join(query for query, _ in conns[-1].cursor_obj.queries)
        self.assertIn("status = 'completed'", final_sql)

    async def test_weekly_report_partial_retry_only_sends_missing_admin(self):
        from aiogram.exceptions import TelegramNetworkError

        first_conns = [FakeConnection() for _ in range(3)]
        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "claim_weekly_report_run", return_value={
                 "status": "claimed", "sent_admin_ids": [], "permanent_admin_ids": [],
             }), \
             patch.object(self.main, "build_weekly_admin_report", AsyncMock(return_value=("report", []))), \
             patch.object(self.main, "get_db_conn", side_effect=first_conns), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=[
                 None, TelegramNetworkError(method=None, message="temporary"),
             ])):
            first = await self.main.send_weekly_admin_report()

        self.assertEqual(first["status"], "partial")
        self.assertEqual(first["sent_admin_ids"], [1])
        final_params = first_conns[-1].cursor_obj.queries[-1][1]
        self.assertEqual(final_params[0], "1")

        retry_conns = [FakeConnection() for _ in range(3)]
        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "claim_weekly_report_run", return_value={
                 "status": "claimed", "sent_admin_ids": [1], "permanent_admin_ids": [],
             }), \
             patch.object(self.main, "build_weekly_admin_report", AsyncMock(return_value=("report", []))), \
             patch.object(self.main, "get_db_conn", side_effect=retry_conns), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as retry_send:
            retry = await self.main.send_weekly_admin_report()

        self.assertEqual(retry["status"], "completed")
        retry_send.assert_awaited_once()
        self.assertEqual(retry_send.await_args.args[0], 2)

    async def test_weekly_report_permanent_admin_is_durably_resolved(self):
        from aiogram.exceptions import TelegramForbiddenError

        conns = [FakeConnection() for _ in range(4)]
        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "claim_weekly_report_run", return_value={
                 "status": "claimed", "sent_admin_ids": [], "permanent_admin_ids": [],
             }), \
             patch.object(self.main, "build_weekly_admin_report", AsyncMock(return_value=("report", []))), \
             patch.object(self.main, "get_db_conn", side_effect=conns), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=[
                 None, TelegramForbiddenError(method=None, message="blocked"),
             ])):
            result = await self.main.send_weekly_admin_report()

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["sent_admin_ids"], [1])
        self.assertEqual(result["permanent_admin_ids"], [2])
        final_params = conns[-1].cursor_obj.queries[-1][1]
        self.assertIn('"permanent_admin_ids": [2]', final_params[1])

    async def test_weekly_report_crash_after_saved_recipient_skips_it_on_reclaim(self):
        crash_conns = [FakeConnection(), FakeConnection()]
        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "claim_weekly_report_run", return_value={
                 "status": "claimed", "sent_admin_ids": [], "permanent_admin_ids": [],
             }), \
             patch.object(self.main, "build_weekly_admin_report", AsyncMock(return_value=("report", []))), \
             patch.object(self.main, "get_db_conn", side_effect=crash_conns), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=[None, KeyboardInterrupt()])), \
             self.assertRaises(KeyboardInterrupt):
            await self.main.send_weekly_admin_report()

        saved_params = crash_conns[1].cursor_obj.queries[-1][1]
        self.assertEqual(saved_params[0], "1")

        retry_conns = [FakeConnection() for _ in range(3)]
        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "claim_weekly_report_run", return_value={
                 "status": "claimed", "sent_admin_ids": [1], "permanent_admin_ids": [],
             }), \
             patch.object(self.main, "build_weekly_admin_report", AsyncMock(return_value=("report", []))), \
             patch.object(self.main, "get_db_conn", side_effect=retry_conns), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as retry_send:
            result = await self.main.send_weekly_admin_report()

        self.assertEqual(result["status"], "completed")
        retry_send.assert_awaited_once()
        self.assertEqual(retry_send.await_args.args[0], 2)

    async def test_stripeobject_metadata_gift_checkout_completed_uses_gift_branch(self):
        event_id = "evt_stripeobject_gift_completed"
        gift_id = "gift-id-completed"
        session = self.stripe_object({
            "id": "cs_gift_completed",
            "mode": "payment",
            "payment_status": "paid",
            "metadata": {
                "payment_kind": self.main.GIFT_PAYMENT_KIND,
                "gift_id": gift_id,
                "purchaser_telegram_id": "123",
                "tariff_code": "gift_1m",
                "duration_days": "30",
            },
        })
        event = self.stripe_object({
            "id": event_id,
            "type": "checkout.session.completed",
            "created": 1720000000,
            "data": {"object": session},
        })
        request = FakeStripeRequest(
            json.dumps({"id": event_id, "type": "checkout.session.completed", "data": {"object": {"id": "cs_gift_completed"}}}).encode("utf-8"),
            {"Stripe-Signature": "sig", "Content-Type": "application/json"},
        )
        gift_row = {"id": gift_id, "public_reference": "GIFT-COMPLETED0001", "purchaser_telegram_id": 123}
        conn = FakeConnection(fetches=[("select gift",)])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "fetch_gift_checkout_payment_proof", return_value=(session, {"quantity": 1}, {"id": "price_gift_1m"})), \
             patch.object(self.main, "gift_row_dict", return_value=gift_row), \
             patch.object(self.main, "mark_gift_paid_and_enqueue", return_value={**gift_row, "status": "paid_unclaimed"}) as mark_gift, \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_gift.assert_called_once()
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()

    async def test_stripeobject_metadata_gift_async_payment_succeeded_uses_gift_branch(self):
        event_id = "evt_stripeobject_gift_async_success"
        gift_id = "gift-id-async"
        session = self.stripe_object({
            "id": "cs_gift_async",
            "metadata": {
                "payment_kind": self.main.GIFT_PAYMENT_KIND,
                "gift_id": gift_id,
                "purchaser_telegram_id": "123",
            },
        })
        event = self.stripe_object({
            "id": event_id,
            "type": "checkout.session.async_payment_succeeded",
            "created": 1720000000,
            "data": {"object": session},
        })
        request = FakeStripeRequest(
            json.dumps({"id": event_id, "type": "checkout.session.async_payment_succeeded", "data": {"object": {"id": "cs_gift_async"}}}).encode("utf-8"),
            {"Stripe-Signature": "sig", "Content-Type": "application/json"},
        )
        gift_row = {"id": gift_id, "public_reference": "GIFT-ASYNC00000001", "purchaser_telegram_id": 123}
        conn = FakeConnection(fetches=[("select gift",)])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "fetch_gift_checkout_payment_proof", return_value=(session, {"quantity": 1}, {"id": "price_gift_1m"})), \
             patch.object(self.main, "gift_row_dict", return_value=gift_row), \
             patch.object(self.main, "mark_gift_paid_and_enqueue", return_value={**gift_row, "status": "paid_unclaimed"}) as mark_gift, \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_gift.assert_called_once()
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()

    async def test_stripeobject_metadata_regular_checkout_completed_uses_subscription_path(self):
        event_id = "evt_stripeobject_regular_completed"
        session = self.stripe_object({
            "id": "cs_regular_completed",
            "client_reference_id": "123",
            "metadata": {},
            "mode": "subscription",
            "payment_status": "paid",
            "customer": "cus_regular_completed",
            "subscription": "sub_regular_completed",
        })
        self.assertFalse(hasattr(session.metadata, "get"))
        event = self.stripe_object({
            "id": event_id,
            "type": "checkout.session.completed",
            "created": 1720000000,
            "data": {"object": session},
        })
        request = FakeStripeRequest(
            json.dumps({"id": event_id, "type": "checkout.session.completed", "data": {"object": {"id": "cs_regular_completed"}}}).encode("utf-8"),
            {"Stripe-Signature": "sig", "Content-Type": "application/json"},
        )
        conn = FakeConnection()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()

    async def test_stripeobject_metadata_gift_error_path_reads_purchaser_safely(self):
        event_id = "evt_stripeobject_gift_error_path"
        session = self.stripe_object({
            "id": "cs_gift_error",
            "metadata": {
                "payment_kind": self.main.GIFT_PAYMENT_KIND,
                "gift_id": "gift-id-error",
                "purchaser_telegram_id": "123",
            },
        })
        event = self.stripe_object({
            "id": event_id,
            "type": "checkout.session.completed",
            "created": 1720000000,
            "data": {"object": session},
        })
        request = FakeStripeRequest(
            json.dumps({"id": event_id, "type": "checkout.session.completed", "data": {"object": {"id": "cs_gift_error"}}}).encode("utf-8"),
            {"Stripe-Signature": "sig", "Content-Type": "application/json"},
        )

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "fetch_gift_checkout_payment_proof", side_effect=RuntimeError("proof failed")), \
             patch.object(self.main, "enqueue_admin_payment_problem_now", AsyncMock()) as admin_problem, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release:
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 500)
        admin_problem.assert_awaited_once()
        self.assertEqual(admin_problem.await_args.kwargs["telegram_id"], "123")
        release.assert_awaited_once_with(event_id, 1)

    async def test_invoice_payment_failed_admin_alert_does_not_claim_24h_reminder(self):
        event_id = "evt_invoice_failed_retry_status"
        payload, event, subscription = self.invoice_payment_event(event_id)
        event.type = "invoice.payment_failed"
        invoice = event.data.object
        invoice.status = "open"
        invoice.amount_paid = 0
        invoice.amount_due = 1000
        invoice.next_payment_attempt = int((datetime.utcnow() + timedelta(hours=6)).timestamp())
        subscription.status = "past_due"
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[
            (123,),
            *self.stripe_identity_available_fetches(
                include_subscription=True,
                target_user_row=(123, "cus_rejoin", "sub_rejoin"),
            ),
            None,
            (123, True, datetime.utcnow() + timedelta(days=14), datetime.utcnow(), datetime.utcnow() + timedelta(hours=24)),
            ("stripe:%s:payment_failed" % event_id,),
            ("stripe-admin:%s:invoice_payment_failed:1" % event_id,),
            ("stripe-admin:%s:invoice_payment_failed:2" % event_id,),
        ])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "release_event_processing", AsyncMock()) as release, \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        admin_payload = json.loads(adapted_json_value(self.admin_delivery_inserts(conn)[0][3]))
        self.assertIn("Напоминание через 24 часа: не применимо", admin_payload["text"])
        self.assertNotIn("Напоминание через 24 часа: запланировано", admin_payload["text"])
        self.assertIn("payment_failed message: поставлено в outbox", admin_payload["text"])

    async def test_payment_problem_classification_event_types_and_unknown_fallback(self):
        self.assertEqual(
            self.main.classify_payment_problem(event_type="checkout.session.expired")["category"],
            "checkout_expired",
        )
        self.assertEqual(
            self.main.classify_payment_problem(event_type="checkout.session.async_payment_failed")["category"],
            "checkout_async_payment_failed",
        )
        self.assertEqual(
            self.main.classify_payment_problem(event_type="invoice.payment_failed")["category"],
            "invoice_payment_failed",
        )
        self.assertEqual(
            self.main.classify_payment_problem()["category"],
            "unknown_payment_error",
        )

    async def test_rejoin_delivery_reuses_saved_invite_after_restart(self):
        send_message = AsyncMock()

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 (
                     "stripe:evt_1:rejoin_invite",
                     123,
                     "stripe_rejoin_invite",
                     '{"text":"link {invite_link}","source":"invoice.payment_succeeded","stripe_event_id":"evt_1"}',
                     1,
                     "https://t.me/+saved",
                     1,
                 )
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", send_message), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent"), \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        unban.assert_awaited_once()
        create_link.assert_not_awaited()
        self.assertIn("https://t.me/+saved", send_message.await_args.args[1])

    async def test_rejoin_unban_network_failure_retries_without_send_or_sent(self):
        from aiogram.exceptions import TelegramNetworkError

        connections = [FakeConnection(), FakeConnection()]
        failed_calls = []

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_network_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramNetworkError(method=None, message="network down"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_network_secret:rejoin_invite", "TelegramNetworkError", 5, False)])
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()

    async def test_rejoin_unban_retry_after_sets_retry_delay(self):
        from aiogram.exceptions import TelegramRetryAfter

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_retry_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramRetryAfter(method=None, message="retry", retry_after=125))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_retry_secret:rejoin_invite", "TelegramRetryAfter", 3, False)])
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()

    async def test_rejoin_unban_group_permission_error_does_not_block_user(self):
        from aiogram.exceptions import TelegramForbiddenError

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        connections = [FakeConnection(), FakeConnection()]
        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_group_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="not enough rights"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_group_secret:rejoin_invite", "TelegramForbiddenError", False)])
        self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()

    async def test_rejoin_unban_group_permission_error_at_limit_is_terminal(self):
        from aiogram.exceptions import TelegramForbiddenError

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        connections = [FakeConnection(), FakeConnection()]
        notify_admins = AsyncMock()
        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_group_limit_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}","stripe_event_id":"evt_group_limit_secret"}', 3, None, 1)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="not enough rights"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", notify_admins):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_group_limit_secret:rejoin_invite", "TelegramForbiddenError", True)])
        self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()
        self.assertGreaterEqual(notify_admins.await_count, 1)
        alert_text = notify_admins.await_args_list[0].args[0]
        self.assertNotIn("evt_group_limit_secret", alert_text)
        self.assertIn("delivery_hash:", alert_text)
        self.assertNotIn("evt_***secret", alert_text)

    async def test_rejoin_unban_bot_not_administrator_is_not_benign(self):
        from aiogram.exceptions import TelegramBadRequest

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        connections = [FakeConnection(), FakeConnection()]
        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_bot_admin_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramBadRequest(method=None, message="bot is not an administrator"))), \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_bot_admin_secret:rejoin_invite", "TelegramBadRequest", False)])
        self.assertFalse(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        create_link.assert_not_awaited()
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()

    async def test_rejoin_unban_benign_administrator_error_continues(self):
        from aiogram.exceptions import TelegramBadRequest

        invite = SimpleNamespace(invite_link="https://t.me/+new")
        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_admin_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramBadRequest(method=None, message="user is an administrator"))), \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock(return_value=invite)) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed") as mark_failed, \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        create_link.assert_awaited_once()
        send_message.assert_awaited_once()
        mark_sent.assert_called_once()
        mark_failed.assert_not_called()

    async def test_rejoin_check_membership_timeout_is_retryable(self):
        from aiogram.exceptions import TelegramNetworkError

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, retry_delay_minutes, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_member_timeout:rejoin_invite", 123, "stripe_rejoin_check", '{"text":"link {invite_link}"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(side_effect=TelegramNetworkError(method=None, message="network down"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 1, "permanently_failed": 0, "blocked": 0})
        self.assertEqual(failed_calls, [("stripe:evt_member_timeout:rejoin_invite", "TelegramNetworkError", 5, False)])
        unban.assert_not_awaited()
        send_message.assert_not_awaited()
        mark_sent.assert_not_called()

    async def test_rejoin_check_member_completes_without_link_or_message(self):
        member = SimpleNamespace(status="member", is_member=True)

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_member_ok:rejoin_invite", 123, "stripe_rejoin_check", '{"text":"link {invite_link}"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=member)), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed") as mark_failed, \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        unban.assert_not_awaited()
        create_link.assert_not_awaited()
        send_message.assert_not_awaited()
        mark_sent.assert_called_once()
        mark_failed.assert_not_called()

    async def test_rejoin_check_kicked_unbans_and_sends_link(self):
        member = SimpleNamespace(status="kicked", is_member=False)
        invite = SimpleNamespace(invite_link="https://t.me/+new")

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_kicked:rejoin_invite", 123, "stripe_rejoin_check", '{"text":"link {invite_link}","stripe_event_id":"evt_kicked"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=member)), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock(return_value=invite)) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message, \
             patch.object(self.main, "mark_delivery_failed") as mark_failed, \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 1, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        unban.assert_awaited_once()
        create_link.assert_awaited_once()
        send_message.assert_awaited_once()
        self.assertIn("https://t.me/+new", send_message.await_args.args[1])
        mark_sent.assert_called_once()
        mark_failed.assert_not_called()

    async def test_rejoin_send_forbidden_blocks_and_permanently_fails(self):
        from aiogram.exceptions import TelegramForbiddenError

        failed_calls = []

        def fake_mark_failed(cur, delivery_key, claim_generation, exc, retry_delay_minutes=None, permanently_failed=False):
            failed_calls.append((delivery_key, type(exc).__name__, permanently_failed))
            return "permanently_failed" if permanently_failed else "failed"

        connections = [FakeConnection(), FakeConnection()]
        with patch.object(self.main, "get_db_conn", side_effect=connections), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_send_secret:rejoin_invite", 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, "https://t.me/+saved", 1)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()), \
             patch.object(self.main.bot, "send_message", AsyncMock(side_effect=TelegramForbiddenError(method=None, message="bot was blocked"))), \
             patch.object(self.main, "mark_delivery_failed", side_effect=fake_mark_failed), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent") as mark_sent, \
             patch.object(self.main, "notify_admins", AsyncMock()):
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 1, "blocked": 1})
        self.assertEqual(failed_calls, [("stripe:evt_send_secret:rejoin_invite", "TelegramForbiddenError", True)])
        self.assertTrue(any("blocked_bot = TRUE" in query for conn in connections for query, _ in conn.cursor_obj.queries))
        mark_sent.assert_not_called()

    async def test_rejoin_unban_logs_mask_delivery_key(self):
        from aiogram.exceptions import TelegramBadRequest

        full_key = "stripe:evt_full_secret_identifier_123456789:rejoin_invite"
        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 (full_key, 123, "stripe_rejoin_invite", '{"text":"link {invite_link}"}', 1, None, 1)
             ]), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock(side_effect=TelegramBadRequest(method=None, message="not enough rights"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()), \
             patch.object(self.main, "mark_delivery_failed"), \
             patch.object(self.main, "mark_delivery_sent", return_value="sent"), \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             self.assertLogs(level="WARNING") as logs:
            await self.main.process_pending_message_deliveries()

        log_text = "\n".join(logs.output)
        self.assertNotIn(full_key, log_text)
        self.assertNotIn("evt_full_secret_identifier_123456789", log_text)
        self.assertIn("stripe:evt_***invite", log_text)

    async def run_checkout_payment_message_webhook(self, event_id, *, days="30", payment_failed=False, duplicate=False):
        payload = json.dumps(
            {
                "id": event_id,
                "object": "event",
                "type": "checkout.session.completed",
                "created": 1720000000,
                "data": {
                    "object": {
                        "id": "cs_%s" % event_id[-10:],
                        "object": "checkout.session",
                        "client_reference_id": "123",
                        "metadata": {"days": days},
                        "mode": "payment",
                        "payment_status": "paid",
                        "customer": "cus_checkout_recovery",
                        "subscription": None,
                        "amount_total": 1000,
                        "currency": "usd",
                    }
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        event = SimpleNamespace(
            id=event_id,
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(
                id="cs_%s" % event_id[-10:],
                client_reference_id="123",
                metadata={"days": days},
                mode="payment",
                payment_status="paid",
                customer="cus_checkout_recovery",
                subscription=None,
                amount_total=1000,
                currency="usd",
            )),
        )
        if days == "7":
            success_key = "stripe:%s:trial_success" % event_id
        elif payment_failed:
            success_key = "stripe:%s:payment_recovered" % event_id
        else:
            success_key = "stripe:%s:payment_success" % event_id
        checkout_tariff = "sub_trial" if days == "7" else "sub_1"
        conn = FakeConnection(fetches=[
            (123, checkout_tariff, "payment", "open"),
            *self.stripe_identity_available_fetches(),
            (True, datetime.utcnow() - timedelta(days=1), True, payment_failed),
            (success_key,),
            ("stripe:%s:rejoin_invite" % event_id,),
        ])
        claim_results = [("claimed", 1), ("duplicate_processing", None)] if duplicate else [("claimed", 1)]

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(side_effect=claim_results)), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "release_event_processing", AsyncMock()), \
             patch.object(self.main, "claim_trial_redemption", return_value=True), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as get_member, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response1 = await self.main.stripe_webhook(request)
            response2 = await self.main.stripe_webhook(request) if duplicate else None

        get_member.assert_not_awaited()
        return SimpleNamespace(conn=conn, response=response1, duplicate_response=response2)

    async def test_checkout_webhook_enqueues_durable_rejoin_check_without_telegram_membership_call(self):
        payload = json.dumps(
            {
                "id": "evt_checkout_boundary",
                "object": "event",
                "type": "checkout.session.completed",
                "created": 1720000000,
                "data": {
                    "object": {
                        "id": "cs_boundary",
                        "object": "checkout.session",
                        "client_reference_id": "123",
                        "metadata": {"days": "30"},
                        "mode": "payment",
                        "payment_status": "paid",
                        "customer": "cus_boundary",
                        "subscription": None,
                        "amount_total": 1000,
                        "currency": "usd",
                    }
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[
            (123, "sub_1", "payment", "open"),
            *self.stripe_identity_available_fetches(),
            (True, datetime.utcnow() + timedelta(days=14), True),
            ("stripe:evt_checkout_boundary:rejoin_invite",),
        ])

        event = SimpleNamespace(
            id="evt_checkout_boundary",
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(
                id="cs_boundary",
                client_reference_id="123",
                metadata={"days": "30"},
                mode="payment",
                payment_status="paid",
                customer="cus_boundary",
                subscription=None,
                amount_total=1000,
                currency="usd",
            )),
        )

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as get_member, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        get_member.assert_not_awaited()
        self.assertTrue(conn.closed)
        queries = [query for query, _ in conn.cursor_obj.queries]
        enqueue_indices = [i for i, query in enumerate(queries) if "INSERT INTO message_delivery_events" in query]
        access_index = next(i for i, query in enumerate(queries) if "INSERT INTO users" in query)
        self.assertTrue(all(access_index < enqueue_index for enqueue_index in enqueue_indices))
        deliveries = self.delivery_map(conn)
        self.assertEqual(deliveries["stripe:evt_checkout_boundary:payment_success"][2], "stripe_user_message")
        payment_payload = json.loads(adapted_json_value(deliveries["stripe:evt_checkout_boundary:payment_success"][3]))
        self.assertIn("Оплата прошла успешно 🤍", payment_payload["text"])
        self.assertIn("Доступ к клубу открыт до", payment_payload["text"])
        self.assertEqual(deliveries["stripe:evt_checkout_boundary:rejoin_invite"][2], "stripe_rejoin_check")
        rejoin_payload = json.loads(adapted_json_value(deliveries["stripe:evt_checkout_boundary:rejoin_invite"][3]))
        self.assertNotIn("Оплата прошла успешно", rejoin_payload["text"])
        self.assertFalse(self.admin_delivery_inserts(conn))
        self.assertEqual(conn.commits, 1)

    async def test_checkout_payment_failed_user_gets_recovered_success_delivery(self):
        result = await self.run_checkout_payment_message_webhook(
            "evt_checkout_payment_recovered",
            payment_failed=True,
        )

        self.assertEqual(result.response.status, 200)
        queries = [query for query, _ in result.conn.cursor_obj.queries]
        deliveries = self.delivery_map(result.conn)
        self.assertIn("stripe:evt_checkout_payment_recovered:payment_recovered", deliveries)
        self.assertNotIn("stripe:evt_checkout_payment_recovered:payment_success", deliveries)
        payload_data = json.loads(adapted_json_value(
            deliveries["stripe:evt_checkout_payment_recovered:payment_recovered"][3]
        ))
        self.assertIn("Подписка снова активна", payload_data["text"])
        self.assertFalse(self.admin_delivery_inserts(result.conn))
        update_sql = "\n".join(queries)
        self.assertIn("payment_failed = FALSE", update_sql)
        self.assertIn("payment_failed_at = NULL", update_sql)
        self.assertIn("grace_period_end = NULL", update_sql)
        cleanup_index = next(i for i, query in enumerate(queries) if "payment_failed = FALSE" in query)
        enqueue_index = next(i for i, query in enumerate(queries) if "INSERT INTO message_delivery_events" in query)
        self.assertLess(cleanup_index, enqueue_index)
        self.assertEqual(result.conn.commits, 1)

    async def test_checkout_non_failed_user_gets_payment_success_delivery(self):
        result = await self.run_checkout_payment_message_webhook(
            "evt_checkout_payment_success",
            payment_failed=False,
        )

        self.assertEqual(result.response.status, 200)
        deliveries = self.delivery_map(result.conn)
        self.assertIn("stripe:evt_checkout_payment_success:payment_success", deliveries)
        self.assertNotIn("stripe:evt_checkout_payment_success:payment_recovered", deliveries)
        payload_data = json.loads(adapted_json_value(
            deliveries["stripe:evt_checkout_payment_success:payment_success"][3]
        ))
        self.assertIn("Спасибо, что присоединились", payload_data["text"])

    async def test_checkout_recovery_duplicate_webhook_creates_one_recovered_delivery(self):
        result = await self.run_checkout_payment_message_webhook(
            "evt_checkout_recovery_duplicate",
            payment_failed=True,
            duplicate=True,
        )

        self.assertEqual(result.response.status, 200)
        self.assertEqual(result.duplicate_response.status, 200)
        recovered_deliveries = [
            params for query, params in result.conn.cursor_obj.queries
            if "INSERT INTO message_delivery_events" in query
            and params[0] == "stripe:evt_checkout_recovery_duplicate:payment_recovered"
        ]
        payment_success_deliveries = [
            params for query, params in result.conn.cursor_obj.queries
            if "INSERT INTO message_delivery_events" in query
            and params[0] == "stripe:evt_checkout_recovery_duplicate:payment_success"
        ]
        self.assertEqual(len(recovered_deliveries), 1)
        self.assertEqual(payment_success_deliveries, [])

    async def test_trial_checkout_stays_trial_success_even_after_payment_failed_state(self):
        result = await self.run_checkout_payment_message_webhook(
            "evt_checkout_trial_stays_trial",
            days="7",
            payment_failed=True,
        )

        self.assertEqual(result.response.status, 200)
        deliveries = self.delivery_map(result.conn)
        self.assertIn("stripe:evt_checkout_trial_stays_trial:trial_success", deliveries)
        self.assertNotIn("stripe:evt_checkout_trial_stays_trial:payment_recovered", deliveries)
        trial_payload = json.loads(adapted_json_value(
            deliveries["stripe:evt_checkout_trial_stays_trial:trial_success"][3]
        ))
        self.assertIn("Пробная неделя активирована", trial_payload["text"])

    async def test_subscription_checkout_link_only_does_not_enqueue_rejoin_check(self):
        payload = json.dumps(
            {
                "id": "evt_checkout_link_only",
                "object": "event",
                "type": "checkout.session.completed",
                "created": 1720000000,
                "data": {
                    "object": {
                        "id": "cs_link_only",
                        "object": "checkout.session",
                        "client_reference_id": "123",
                        "metadata": {},
                        "mode": "subscription",
                        "payment_status": "paid",
                        "customer": "cus_link_only",
                        "subscription": "sub_link_only",
                    }
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection()
        event = SimpleNamespace(
            id="evt_checkout_link_only",
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(
                id="cs_link_only",
                client_reference_id="123",
                metadata={},
                mode="subscription",
                payment_status="paid",
                customer="cus_link_only",
                subscription="sub_link_only",
            )),
        )

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        self.assertTrue(conn.closed)
        queries = [query for query, _ in conn.cursor_obj.queries]
        self.assertFalse(any("INSERT INTO message_delivery_events" in query for query in queries))

    async def test_subscription_checkout_link_only_identity_conflict_records_audit(self):
        event_id = "evt_checkout_link_identity_conflict"
        payload = json.dumps(
            {
                "id": event_id,
                "object": "event",
                "type": "checkout.session.completed",
                "created": 1720000000,
                "data": {
                    "object": {
                        "id": "cs_link_identity_conflict",
                        "object": "checkout.session",
                        "client_reference_id": "123",
                        "metadata": {},
                        "mode": "subscription",
                        "payment_status": "paid",
                        "customer": "cus_link_identity_conflict",
                        "subscription": "sub_link_identity_conflict",
                    }
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        event = SimpleNamespace(
            id=event_id,
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(
                id="cs_link_identity_conflict",
                client_reference_id="123",
                metadata={},
                mode="subscription",
                payment_status="paid",
                customer="cus_link_identity_conflict",
                subscription="sub_link_identity_conflict",
            )),
        )
        payment_conn = FakeConnection(fetches=[[], [(999,)]])
        audit_conn = FakeConnection()
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "get_db_conn", side_effect=[payment_conn, audit_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        payment_sql = "\n".join(query for query, _ in payment_conn.cursor_obj.queries)
        self.assertIn("FOR UPDATE", payment_sql)
        self.assertNotIn("INSERT INTO users", payment_sql)
        self.assertNotIn("upsert_stripe_link", payment_sql)
        audit_sql = "\n".join(query for query, _ in audit_conn.cursor_obj.queries)
        self.assertIn("INSERT INTO stripe_identity_conflicts", audit_sql)
        self.assertIn("INSERT INTO unlinked_stripe_events", audit_sql)
        admin_payloads = [json.loads(adapted_json_value(params[3])) for params in self.admin_delivery_inserts(audit_conn)]
        self.assertEqual(len(admin_payloads), len(self.main.ADMIN_IDS))

    async def test_stripe_conflicts_command_private_admin_read_only_runtime(self):
        group_message = FakeIncomingMessage(user_id=1)
        group_message.chat.type = "group"
        get_db_conn = Mock(return_value=FakeConnection())

        with patch.object(self.main, "get_db_conn", get_db_conn):
            await self.main.stripe_conflicts_command(group_message)

        get_db_conn.assert_not_called()
        self.assertIn("личном чате", group_message.replies[0][0])

        private_message = FakeIncomingMessage(user_id=1)
        conn = FakeConnection(fetches=[[
            (
                "users_customer_conflict",
                "cus_conflict",
                "[123,999]",
                json.dumps({"source": "checkout.session.completed"}),
                datetime(2026, 8, 6, 10, 0),
            )
        ]])

        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.stripe_conflicts_command(private_message)

        self.assertTrue(conn.closed)
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("SELECT conflict_type, stripe_id, telegram_ids, details, created_at", sql)
        self.assertNotIn("INSERT", sql)
        self.assertNotIn("UPDATE", sql)
        self.assertIn("users_customer_conflict", private_message.replies[0][0])
        self.assertIn(self.main.safe_log_id("cus_conflict"), private_message.replies[0][0])

    async def test_access_mismatches_command_is_private_admin_read_only_and_redacted(self):
        non_admin = FakeIncomingMessage(user_id=999)
        group_admin = FakeIncomingMessage(user_id=1)
        group_admin.chat.type = "group"
        get_db_conn = Mock(return_value=FakeConnection())

        with patch.object(self.main, "get_db_conn", get_db_conn):
            await self.main.access_mismatches_command(non_admin)
            await self.main.access_mismatches_command(group_admin)

        get_db_conn.assert_not_called()
        self.assertIn("личном чате", group_admin.replies[0][0])

        raw_subscription = "sub_access_mismatch_sensitive_123456"
        raw_event = "evt_access_mismatch_sensitive_654321"
        private_admin = FakeIncomingMessage(user_id=1)
        conn = FakeConnection(fetches=[
            (1, 1, 1),
            [(123456789, raw_subscription, "active", False, None, raw_event, datetime.utcnow() + timedelta(days=30))],
        ])
        with patch.object(self.main, "get_db_conn", return_value=conn), \
             patch.object(self.main.stripe.Subscription, "retrieve", side_effect=AssertionError("Stripe API called")):
            await self.main.access_mismatches_command(private_admin)

        output = private_admin.answers[0][0]
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertNotIn("INSERT", sql)
        self.assertNotIn("UPDATE", sql)
        self.assertNotIn("DELETE", sql)
        self.assertNotIn(raw_subscription, output)
        self.assertNotIn(raw_event, output)
        self.assertIn(self.main.safe_log_id(raw_subscription), output)
        self.assertIn("local_payment_proof: yes", output)

    async def test_link_stripe_user_runtime_conflict_does_not_change_user(self):
        payment_conn = FakeConnection(fetches=[[(999,)]])
        audit_conn = FakeConnection()
        subscription = SimpleNamespace(
            status="active",
            current_period_end=int((datetime.utcnow() + timedelta(days=30)).timestamp()),
            customer="cus_link_conflict",
            cancel_at_period_end=False,
        )

        with patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "prepare_manual_link_payment_events", AsyncMock(return_value=[])), \
             patch.object(self.main, "get_db_conn", side_effect=[payment_conn, audit_conn]):
            result = await self.main.perform_link_stripe_user({
                "telegram_id": 123,
                "stripe_customer_id": "cus_link_conflict",
                "stripe_subscription_id": "sub_link_conflict",
                "admin_id": 1,
            })

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["reason"], "stripe_identity_conflict")
        self.assertEqual(payment_conn.rollbacks, 1)
        payment_sql = "\n".join(query for query, _ in payment_conn.cursor_obj.queries)
        self.assertNotIn("INSERT INTO users", payment_sql)
        self.assertNotIn("UPDATE unlinked_stripe_events", payment_sql)
        audit_sql = "\n".join(query for query, _ in audit_conn.cursor_obj.queries)
        self.assertIn("INSERT INTO stripe_identity_conflicts", audit_sql)
        self.assertIn("INSERT INTO unlinked_stripe_events", audit_sql)

    async def test_subscription_updated_identity_conflict_records_audit_and_closes_connections(self):
        event_id = "evt_subscription_updated_identity_conflict"
        sub = SimpleNamespace(
            id="sub_update_conflict",
            customer="cus_update_conflict",
            status="active",
            current_period_end=int((datetime.utcnow() + timedelta(days=30)).timestamp()),
            trial_end=None,
            cancel_at_period_end=False,
        )
        event = SimpleNamespace(
            id=event_id,
            type="customer.subscription.updated",
            created=1720000000,
            data=SimpleNamespace(object=sub),
        )
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        read_conn = FakeConnection(fetches=[(False, None, None, 123)])
        write_conn = FakeConnection(fetches=[(123,), [(123,)], [(999,)]])
        audit_conn = FakeConnection()
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn, audit_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        self.assertEqual(write_conn.rollbacks, 1)
        self.assertTrue(write_conn.closed)
        self.assertTrue(audit_conn.closed)
        write_sql = "\n".join(query for query, _ in write_conn.cursor_obj.queries)
        self.assertNotIn("UPDATE users", write_sql)
        audit_sql = "\n".join(query for query, _ in audit_conn.cursor_obj.queries)
        self.assertIn("INSERT INTO stripe_identity_conflicts", audit_sql)

    async def test_subscription_updated_identity_conflict_audit_failure_releases_claim(self):
        event_id = "evt_subscription_updated_identity_audit_failure"
        sub = SimpleNamespace(
            id="sub_update_audit_failure",
            customer="cus_update_audit_failure",
            status="active",
            current_period_end=int((datetime.utcnow() + timedelta(days=30)).timestamp()),
            trial_end=None,
            cancel_at_period_end=False,
        )
        event = SimpleNamespace(
            id=event_id,
            type="customer.subscription.updated",
            created=1720000000,
            data=SimpleNamespace(object=sub),
        )
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        read_conn = FakeConnection(fetches=[(False, None, None, 123)])
        write_conn = FakeConnection(fetches=[(123,), [(123,)], [(999,)]])
        audit_conn = ExecuteFailingConnection(error=RuntimeError("audit down"))
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn, audit_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 500)
        mark_processed.assert_not_awaited()
        release.assert_awaited_once_with(event_id, 1)
        self.assertEqual(write_conn.rollbacks, 1)
        self.assertTrue(write_conn.closed)
        self.assertTrue(audit_conn.closed)

    async def test_invoice_payment_failed_identity_conflict_records_audit(self):
        event_id = "evt_payment_failed_identity_conflict"
        invoice = SimpleNamespace(
            id="in_failed_conflict",
            subscription="sub_failed_conflict",
            customer="cus_failed_conflict",
            customer_email="failed@example.test",
            billing_reason="subscription_cycle",
            status="open",
            next_payment_attempt=None,
            metadata={},
            lines=SimpleNamespace(data=[]),
        )
        subscription = SimpleNamespace(
            id="sub_failed_conflict",
            customer="cus_failed_conflict",
            status="past_due",
            trial_end=None,
            cancel_at_period_end=False,
        )
        event = SimpleNamespace(
            id=event_id,
            type="invoice.payment_failed",
            created=1720000000,
            data=SimpleNamespace(object=invoice),
        )
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        main_conn = FakeConnection(fetches=[(123,), [(123,)], [(999,)]])
        audit_conn = FakeConnection()
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", side_effect=[main_conn, audit_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        self.assertEqual(main_conn.rollbacks, 1)
        self.assertTrue(main_conn.closed)
        main_sql = "\n".join(query for query, _ in main_conn.cursor_obj.queries)
        self.assertNotIn("UPDATE users", main_sql)
        self.assertIn("INSERT INTO stripe_identity_conflicts", "\n".join(query for query, _ in audit_conn.cursor_obj.queries))

    async def test_subscription_updated_known_unique_other_owner_records_audit(self):
        event_id = "evt_subscription_updated_unique_other"
        event, sub = self.subscription_updated_event(event_id)
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        read_conn = FakeConnection(fetches=[(False, None, None, 123)])
        write_conn = ConditionalFailingConnection(
            fetches=[
                (123,),
                [(123,)],
                [],
                [],
                [],
                (123, "cus_existing", sub.id),
                [],
            ],
            error=self.known_unique_violation("users_unique_stripe_customer"),
            fail_when=lambda query, params: "UPDATE users" in query,
        )
        lookup_conn = FakeConnection(fetches=[(999,)])
        audit_conn = FakeConnection()
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn, lookup_conn, audit_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        self.assertEqual(write_conn.rollbacks, 1)
        self.assertTrue(write_conn.closed)
        audit_sql = "\n".join(query for query, _ in audit_conn.cursor_obj.queries)
        self.assertIn("INSERT INTO stripe_identity_conflicts", audit_sql)

    async def test_subscription_updated_past_due_known_unique_other_owner_records_audit(self):
        event_id = "evt_subscription_updated_past_due_unique"
        event, sub = self.subscription_updated_event(event_id, status="past_due")
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        read_conn = FakeConnection(fetches=[(False, None, None, 123)])
        write_conn = ConditionalFailingConnection(
            fetches=[
                (123,),
                [(123,)],
                [],
                [],
                [],
                (123, "cus_existing", sub.id),
                [],
            ],
            error=self.known_unique_violation("users_unique_stripe_customer"),
            fail_when=lambda query, params: "UPDATE users" in query,
        )
        lookup_conn = FakeConnection(fetches=[(999,)])
        audit_conn = FakeConnection()
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn, lookup_conn, audit_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        self.assertIn(
            "INSERT INTO stripe_identity_conflicts",
            "\n".join(query for query, _ in audit_conn.cursor_obj.queries),
        )

    async def test_subscription_updated_terminal_known_unique_other_owner_records_audit(self):
        event_id = "evt_subscription_updated_terminal_unique"
        event, sub = self.subscription_updated_event(event_id, status="canceled")
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        read_conn = FakeConnection(fetches=[(False, None, None, 123)])
        write_conn = ConditionalFailingConnection(
            fetches=[
                (123,),
                [(123,)],
                [],
                [],
                [],
                (123, "cus_existing", sub.id),
                [],
            ],
            error=self.known_unique_violation("users_unique_stripe_customer"),
            fail_when=lambda query, params: "UPDATE users" in query,
        )
        lookup_conn = FakeConnection(fetches=[(999,)])
        audit_conn = FakeConnection()
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn, lookup_conn, audit_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        self.assertIn(
            "INSERT INTO stripe_identity_conflicts",
            "\n".join(query for query, _ in audit_conn.cursor_obj.queries),
        )

    async def test_subscription_updated_known_unique_same_owner_releases_without_audit(self):
        event_id = "evt_subscription_updated_unique_same"
        event, sub = self.subscription_updated_event(event_id)
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        read_conn = FakeConnection(fetches=[(False, None, None, 123)])
        write_conn = ConditionalFailingConnection(
            fetches=[
                (123,),
                [(123,)],
                [],
                [],
                [],
                (123, "cus_existing", sub.id),
                [],
            ],
            error=self.known_unique_violation("users_unique_stripe_customer"),
            fail_when=lambda query, params: "UPDATE users" in query,
        )
        lookup_conn = FakeConnection(fetches=[(123,)])
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn, lookup_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 500)
        mark_processed.assert_not_awaited()
        release.assert_awaited_once_with(event_id, 1)
        self.assertEqual(write_conn.rollbacks, 1)
        lookup_sql = "\n".join(query for query, _ in lookup_conn.cursor_obj.queries)
        self.assertIn("FROM users", lookup_sql)
        self.assertIn("FROM stripe_links", lookup_sql)

    async def test_invoice_payment_failed_known_unique_other_owner_records_audit(self):
        event_id = "evt_invoice_failed_unique_other"
        event, invoice, subscription = self.invoice_payment_failed_event(event_id)
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        main_conn = ConditionalFailingConnection(
            fetches=[
                (123,),
                [(123,)],
                [],
                [],
                [],
                (123, "cus_existing", subscription.id),
                [],
            ],
            error=self.known_unique_violation("users_unique_stripe_customer"),
            fail_when=lambda query, params: "UPDATE users" in query,
        )
        lookup_conn = FakeConnection(fetches=[(999,)])
        audit_conn = FakeConnection()
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", side_effect=[main_conn, lookup_conn, audit_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        release.assert_not_awaited()
        self.assertEqual(main_conn.rollbacks, 1)
        main_sql = "\n".join(query for query, _ in main_conn.cursor_obj.queries)
        self.assertNotIn("INSERT INTO access_events", main_sql)
        audit_sql = "\n".join(query for query, _ in audit_conn.cursor_obj.queries)
        self.assertIn("INSERT INTO stripe_identity_conflicts", audit_sql)

    async def test_invoice_payment_failed_known_unique_same_owner_releases_without_audit(self):
        event_id = "evt_invoice_failed_unique_same"
        event, invoice, subscription = self.invoice_payment_failed_event(event_id)
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        main_conn = ConditionalFailingConnection(
            fetches=[
                (123,),
                [(123,)],
                [],
                [],
                [],
                (123, "cus_existing", subscription.id),
                [],
            ],
            error=self.known_unique_violation("users_unique_stripe_customer"),
            fail_when=lambda query, params: "UPDATE users" in query,
        )
        lookup_conn = FakeConnection(fetches=[(123,)])
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", side_effect=[main_conn, lookup_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 500)
        mark_processed.assert_not_awaited()
        release.assert_awaited_once_with(event_id, 1)
        self.assertEqual(main_conn.rollbacks, 1)

    async def test_subscription_updated_unknown_unique_constraint_releases_without_audit(self):
        event_id = "evt_subscription_updated_unique_unknown"
        event, sub = self.subscription_updated_event(event_id)
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        read_conn = FakeConnection(fetches=[(False, None, None, 123)])
        write_conn = ConditionalFailingConnection(
            fetches=[
                (123,),
                [(123,)],
                [],
                [],
                [],
                (123, "cus_existing", sub.id),
                [],
            ],
            error=self.known_unique_violation("unknown_identity_constraint"),
            fail_when=lambda query, params: "UPDATE users" in query,
        )
        mark_processed = AsyncMock()
        release = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "release_event_processing", release), \
             patch.object(self.main, "get_db_conn", side_effect=[read_conn, write_conn]):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 500)
        mark_processed.assert_not_awaited()
        release.assert_awaited_once_with(event_id, 1)
        self.assertEqual(write_conn.rollbacks, 1)

    async def test_link_stripe_user_same_user_unique_race_does_not_create_false_conflict(self):
        payment_conn = ConditionalFailingConnection(
            fetches=[
                [],
                [],
                [],
                [],
                (123, "cus_link_same", "sub_link_same"),
                [],
                (datetime.utcnow() - timedelta(days=1),),
            ],
            error=self.known_unique_violation("users_unique_stripe_customer"),
            fail_when=lambda query, params: "INSERT INTO users" in query,
        )
        lookup_conn = FakeConnection(fetches=[(123,)])
        reread_conn = FakeConnection(fetches=[(1,)])
        subscription = SimpleNamespace(
            status="active",
            current_period_end=int((datetime.utcnow() + timedelta(days=30)).timestamp()),
            customer="cus_link_same",
            cancel_at_period_end=False,
        )

        with patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "prepare_manual_link_payment_events", AsyncMock(return_value=[])), \
             patch.object(self.main, "get_db_conn", side_effect=[payment_conn, lookup_conn, reread_conn]):
            result = await self.main.perform_link_stripe_user({
                "telegram_id": 123,
                "stripe_customer_id": "cus_link_same",
                "stripe_subscription_id": "sub_link_same",
                "admin_id": 1,
            })

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["reason"], "already_linked")
        self.assertEqual(payment_conn.rollbacks, 1)
        self.assertTrue(payment_conn.closed)
        audit_sql = "\n".join(query for conn in (payment_conn, lookup_conn, reread_conn) for query, _ in conn.cursor_obj.queries)
        self.assertNotIn("INSERT INTO stripe_identity_conflicts", audit_sql)

    async def test_link_stripe_user_same_user_unique_race_retry_required_when_link_absent(self):
        payment_conn = ConditionalFailingConnection(
            fetches=[
                [],
                [],
                [],
                [],
                (123, "cus_link_same", "sub_link_same"),
                [],
                (datetime.utcnow() - timedelta(days=1),),
            ],
            error=self.known_unique_violation("users_unique_stripe_customer"),
            fail_when=lambda query, params: "INSERT INTO users" in query,
        )
        lookup_conn = FakeConnection(fetches=[(123,)])
        reread_conn = FakeConnection(fetches=[None])
        subscription = SimpleNamespace(
            status="active",
            current_period_end=int((datetime.utcnow() + timedelta(days=30)).timestamp()),
            customer="cus_link_same",
            cancel_at_period_end=False,
        )

        with patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "prepare_manual_link_payment_events", AsyncMock(return_value=[])), \
             patch.object(self.main, "get_db_conn", side_effect=[payment_conn, lookup_conn, reread_conn]):
            result = await self.main.perform_link_stripe_user({
                "telegram_id": 123,
                "stripe_customer_id": "cus_link_same",
                "stripe_subscription_id": "sub_link_same",
                "admin_id": 1,
            })

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["reason"], "retry_required")
        self.assertNotIn(
            "INSERT INTO stripe_identity_conflicts",
            "\n".join(query for conn in (payment_conn, lookup_conn, reread_conn) for query, _ in conn.cursor_obj.queries),
        )

    async def test_subscription_deleted_does_not_assign_incoming_customer(self):
        event_id = "evt_subscription_deleted_no_customer_assign"
        sub = SimpleNamespace(
            id="sub_deleted",
            customer="cus_deleted_new",
            status="canceled",
        )
        event = SimpleNamespace(
            id=event_id,
            type="customer.subscription.deleted",
            created=1720000000,
            data=SimpleNamespace(object=sub),
        )
        request = FakeStripeRequest(b"{}", {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[(123, False, None)])
        mark_processed = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", mark_processed), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with(event_id, 1)
        sql = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("UPDATE subscription_removal_events", sql)
        self.assertIn("stripe_canceled_at = COALESCE", sql)
        self.assertIn("stripe_subscription_id = NULL", sql)
        self.assertNotIn("stripe_customer_id = COALESCE", sql)
        self.assertIn("UPDATE stripe_links", sql)

    async def test_checkout_webhook_duplicate_does_not_create_second_rejoin_check(self):
        payload = json.dumps(
            {
                "id": "evt_checkout_duplicate",
                "object": "event",
                "type": "checkout.session.completed",
                "created": 1720000000,
                "data": {
                    "object": {
                        "id": "cs_duplicate",
                        "object": "checkout.session",
                        "client_reference_id": "123",
                        "metadata": {"days": "30"},
                        "mode": "payment",
                        "payment_status": "paid",
                        "customer": "cus_boundary",
                        "subscription": None,
                        "amount_total": 1000,
                        "currency": "usd",
                    }
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[
            (123, "sub_1", "payment", "open"),
            *self.stripe_identity_available_fetches(),
            (False, datetime.utcnow() - timedelta(days=1), False),
            ("stripe:evt_checkout_duplicate:rejoin_invite",),
        ])

        event = SimpleNamespace(
            id="evt_checkout_duplicate",
            type="checkout.session.completed",
            created=1720000000,
            data=SimpleNamespace(object=SimpleNamespace(
                id="cs_duplicate",
                client_reference_id="123",
                metadata={"days": "30"},
                mode="payment",
                payment_status="paid",
                customer="cus_boundary",
                subscription=None,
                amount_total=1000,
                currency="usd",
            )),
        )

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(side_effect=[("claimed", 1), ("duplicate_processing", None)])), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "reset_checkout_retry_state_after_success"), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock()) as get_member, \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response1 = await self.main.stripe_webhook(request)
            response2 = await self.main.stripe_webhook(request)

        self.assertEqual(response1.status, 200)
        self.assertEqual(response2.status, 200)
        get_member.assert_not_awaited()
        self.assertTrue(conn.closed)
        self.assertEqual(len(self.user_delivery_inserts(conn)), 2)
        self.assertFalse(self.admin_delivery_inserts(conn))
        self.assertEqual(conn.rollbacks, 0)

    async def test_invoice_future_expiry_kicked_user_gets_rejoin_task_and_link(self):
        enqueue_params = await self.run_invoice_webhook_with_future_expiry("evt_invoice_future_kicked")
        payload_json = enqueue_params[3]

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_invoice_future_kicked:rejoin_invite", 123, "stripe_rejoin_check", payload_json, 1, None, 1)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="kicked"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock(return_value=SimpleNamespace(invite_link="https://t.me/+again"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result["sent"], 1)
        unban.assert_awaited_once()
        send_message.assert_awaited_once()
        self.assertIn("https://t.me/+again", send_message.await_args.args[1])

    async def test_invoice_future_expiry_member_gets_rejoin_task_completed_without_message(self):
        enqueue_params = await self.run_invoice_webhook_with_future_expiry("evt_invoice_future_member")
        payload_json = enqueue_params[3]

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_invoice_future_member:rejoin_invite", 123, "stripe_rejoin_check", payload_json, 1, None, 1)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="member"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock()) as create_link, \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result["sent"], 1)
        unban.assert_not_awaited()
        create_link.assert_not_awaited()
        send_message.assert_not_awaited()

    async def test_out_of_band_invoice_future_expiry_kicked_user_gets_rejoin_task_and_link(self):
        enqueue_params = await self.run_invoice_webhook_with_future_expiry(
            "evt_invoice_oob_kicked",
            paid_out_of_band=True,
        )
        payload_json = enqueue_params[3]

        with patch.object(self.main, "get_db_conn", side_effect=[FakeConnection(), FakeConnection(), FakeConnection()]), \
             patch.object(self.main, "claim_pending_message_deliveries", return_value=[
                 ("stripe:evt_invoice_oob_kicked:rejoin_invite", 123, "stripe_rejoin_check", payload_json, 1, None, 1)
             ]), \
             patch.object(self.main.bot, "get_chat_member", AsyncMock(return_value=SimpleNamespace(status="kicked"))), \
             patch.object(self.main.bot, "unban_chat_member", AsyncMock()) as unban, \
             patch.object(self.main.bot, "create_chat_invite_link", AsyncMock(return_value=SimpleNamespace(invite_link="https://t.me/+oob"))), \
             patch.object(self.main.bot, "send_message", AsyncMock()) as send_message:
            result = await self.main.process_pending_message_deliveries()

        self.assertEqual(result["sent"], 1)
        unban.assert_awaited_once()
        send_message.assert_awaited_once()
        self.assertIn("https://t.me/+oob", send_message.await_args.args[1])

    async def test_out_of_band_invoice_without_linked_user_does_not_enqueue_rejoin_task(self):
        payload, event, subscription = self.invoice_payment_event(
            "evt_invoice_oob_unlinked",
            paid_out_of_band=True,
        )
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection(fetches=[None, None, None, None, None, None])

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()), \
             patch.object(self.main, "notify_admins", AsyncMock()), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        self.assertTrue(conn.closed)
        queries = [query for query, _ in conn.cursor_obj.queries]
        self.assertFalse(any(params[2] != "stripe_admin_message" for params in self.delivery_inserts(conn)))
        self.assertEqual(len(self.admin_delivery_inserts(conn)), len(self.main.ADMIN_IDS))
        self.assertTrue(any("INSERT INTO unlinked_stripe_events" in query for query in queries))

    async def test_invoice_missing_period_does_not_update_access_or_enqueue_user_delivery(self):
        payload, event, subscription = self.invoice_payment_event(
            "evt_invoice_missing_period",
            period_end=None,
            include_line_period=False,
        )
        subscription.current_period_end = None
        request = FakeStripeRequest(payload, {"Stripe-Signature": "sig", "Content-Type": "application/json"})
        conn = FakeConnection()
        notify_admins = AsyncMock()

        with patch.object(self.main, "construct_verified_stripe_event", return_value=event), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("claimed", 1))), \
             patch.object(self.main, "mark_event_processed", AsyncMock()) as mark_processed, \
             patch.object(self.main, "notify_admins", notify_admins), \
             patch.object(self.main.asyncio, "to_thread", AsyncMock(return_value=subscription)), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            response = await self.main.stripe_webhook(request)

        self.assertEqual(response.status, 200)
        mark_processed.assert_awaited_once_with("evt_invoice_missing_period", 1)
        notify_admins.assert_not_awaited()
        queries = [query for query, _ in conn.cursor_obj.queries]
        self.assertFalse(any("UPDATE users" in query for query in queries))
        self.assertFalse(any("INSERT INTO access_events" in query for query in queries))
        self.assertFalse(any(params[2] != "stripe_admin_message" for params in self.delivery_inserts(conn)))
        admin_payloads = [json.loads(adapted_json_value(params[3])) for params in self.admin_delivery_inserts(conn)]
        self.assertEqual(len(admin_payloads), len(self.main.ADMIN_IDS))
        self.assertTrue(any("не найден срок периода подписки" in payload["text"] for payload in admin_payloads))
        self.assertTrue(all("None" not in payload["text"] for payload in admin_payloads))
        self.assertEqual(conn.commits, 1)

    async def test_webhook_and_stripe_routes_are_registered_once_and_do_not_conflict(self):
        app = self.main.create_app()
        telegram_path = self.main.get_telegram_webhook_path()

        self.assertEqual(self.route_count(app, "POST", telegram_path), 1)
        self.assertEqual(self.route_count(app, "POST", "/stripe-payment"), 1)
        self.assertNotEqual(telegram_path, "/stripe-payment")

    async def test_mark_event_processed_lost_generation_fails_closed(self):
        conn = FakeConnection()
        with patch.object(self.main, "get_db_conn", return_value=conn), \
             patch.object(
                 self.main,
                 "mark_stripe_event_processed",
                 return_value="not_owner",
             ) as mark:
            with self.assertRaisesRegex(RuntimeError, "claim ownership lost"):
                await self.main.mark_event_processed("evt_lost_owner", 7)

        mark.assert_called_once_with(conn.cursor_obj, "evt_lost_owner", 7)
        self.assertEqual(conn.commits, 1)

    async def test_startup_sets_db_commands_webhook_verifies_url_and_registers_jobs_once(self):
        fake_scheduler = FakeScheduler()
        app = self.main.create_app()
        with patch.object(self.main, "scheduler", fake_scheduler), \
             patch.object(self.main, "SCHEDULER_JOBS_REGISTERED", False), \
             patch.object(self.main, "init_db") as init_db, \
             patch.object(self.main.bot, "set_my_commands", AsyncMock()) as set_commands, \
             patch.object(self.main.bot, "set_webhook", AsyncMock()) as set_webhook, \
             patch.object(self.main.bot, "get_webhook_info", AsyncMock(return_value=FakeWebhookInfo())) as get_info:
            await self.main.on_startup(app)
            await self.main.on_startup(app)

        self.assertEqual(init_db.call_count, 2)
        self.assertEqual(set_commands.await_count, 2)
        self.assertEqual(set_webhook.await_count, 2)
        for call in set_webhook.await_args_list:
            self.assertEqual(call.kwargs["secret_token"], TEST_ENV["WEBHOOK_SECRET"])
        self.assertEqual(get_info.await_count, 2)
        self.assertEqual(len(fake_scheduler.jobs), 8)
        self.assertFalse(hasattr(self.main, "scheduled_apply_reserved_gifts"))
        self.assertEqual(fake_scheduler.start_calls, 1)

    async def test_telegram_webhook_uses_same_validated_secret_for_setup_and_handler(self):
        handler_instance = Mock()
        handler_class = Mock(return_value=handler_instance)

        with patch.object(self.main, "SimpleRequestHandler", handler_class):
            app = self.main.create_app()

        handler_class.assert_called_once_with(
            dispatcher=self.main.dp,
            bot=self.main.bot,
            secret_token=self.main.WEBHOOK_SECRET,
        )
        handler_instance.register.assert_called_once_with(
            app,
            path=f"/webhook/{self.main.WEBHOOK_SECRET}",
        )
        self.assertEqual(self.main.WEBHOOK_SECRET, TEST_ENV["WEBHOOK_SECRET"])

    async def test_invalid_character_in_telegram_webhook_secret_fails_closed_without_leak(self):
        self.assert_invalid_telegram_webhook_secret_fails_closed("invalid.secret/value")

    async def test_overlong_telegram_webhook_secret_fails_closed_without_leak(self):
        self.assert_invalid_telegram_webhook_secret_fails_closed("a" * 257)

    async def test_shutdown_closes_bot_session_and_is_repeatable(self):
        fake_scheduler = FakeScheduler()
        fake_scheduler.running = True
        close_db_pool = Mock()
        close_session = AsyncMock()
        with patch.object(self.main, "scheduler", fake_scheduler), \
             patch.object(self.main.bot.session, "close", close_session), \
             patch.object(self.main, "close_db_pool", close_db_pool):
            await self.main.on_shutdown(self.main.create_app())
            await self.main.on_shutdown(self.main.create_app())

        self.assertEqual(close_session.await_count, 2)
        self.assertEqual(close_db_pool.call_count, 2)
        self.assertEqual(fake_scheduler.shutdown_calls, 1)

    async def test_main_and_question_fsm_handlers_use_aiogram3_state_api(self):
        state = FakeState()
        message = FakeMessage()

        await self.main.show_menu(message, state)
        self.assertEqual(state.clear_calls, 1)
        self.assertEqual(message.answers[-1][0], "Главное меню\n\nВыберите нужный раздел:")
        menu_keyboard = message.answers[-1][1]["reply_markup"]
        self.assertEqual(
            [[button.text for button in row] for row in menu_keyboard.keyboard],
            [
                ["🧘 Бесплатный урок"],
                ["👤 Профиль и подписка", "📅 Расписание"],
                ["💬 Задать вопрос", "🚨 Правила клуба"],
                ["🎁 Подарить доступ в клуб"],
            ],
        )

        admin_keyboard = self.main.get_main_keyboard(1)
        self.assertEqual(
            [[button.text for button in row] for row in admin_keyboard.keyboard],
            [
                ["🧘 Бесплатный урок"],
                ["👤 Профиль и подписка", "📅 Расписание"],
                ["💬 Задать вопрос", "🚨 Правила клуба"],
                ["🎁 Подарить доступ в клуб"],
                ["🛠 Управление подарками"],
                ["📅 Управление расписанием"],
            ],
        )

        await self.main.ask_question_button(message, state)
        self.assertEqual(state.clear_calls, 2)
        self.assertEqual(state.states[-1], self.main.ContactState.waiting_for_message)
        question_keyboard = message.answers[-1][1]["reply_markup"]
        self.assertEqual(question_keyboard.keyboard[0][0].text, "❌ Отмена")

    async def test_main_menu_buttons_keep_their_navigation_handlers(self):
        user = SimpleNamespace(id=123, full_name="Test User", username="test_user")

        profile_message = SimpleNamespace(from_user=user, answer=AsyncMock())
        profile_state = FakeState()
        with patch.object(self.main, "profile", AsyncMock()) as profile:
            await self.main.profile_button_handler(profile_message, profile_state)
        profile.assert_awaited_once_with(profile_message)
        self.assertEqual(profile_state.clear_calls, 1)

        question_message = SimpleNamespace(from_user=user, answer=AsyncMock())
        question_state = FakeState()
        await self.main.ask_question_button(question_message, question_state)
        self.assertEqual(question_state.states[-1], self.main.ContactState.waiting_for_message)
        self.assertIn("Напишите ваш вопрос", question_message.answer.await_args.args[0])

        rules_message = SimpleNamespace(from_user=user, answer=AsyncMock())
        rules_state = FakeState()
        await self.main.rules_button_handler(rules_message, rules_state)
        self.assertIn("Правила и регламент", rules_message.answer.await_args.args[0])

        gift_message = SimpleNamespace(from_user=user, answer=AsyncMock())
        gift_state = FakeState()
        configured = {
            "configured": True,
            "missing_prices": [],
            "template_count": 3,
            "required_template_count": 3,
        }
        with patch.object(self.main, "get_db_conn", return_value=FakeConnection()), \
             patch.object(self.main, "gift_configuration_status", return_value=configured), \
             patch.object(self.main, "save_telegram_user_profile"):
            await self.main.gift_access_button_handler(gift_message, gift_state)
        self.assertEqual(gift_state.states[-1], self.main.GiftPurchaseStates.tariff)
        self.assertIn("На какой срок подарить доступ", gift_message.answer.await_args.args[0])

        lesson_message = SimpleNamespace(from_user=user, answer=AsyncMock())
        lesson_state = FakeState()
        with patch.object(self.main, "get_db_conn", return_value=FakeConnection(fetches=[(False, False, False)])), \
             patch.object(self.main, "process_claimed_delivery", AsyncMock(return_value="sent")) as delivery:
            await self.main.free_lesson_button(lesson_message, lesson_state)
        delivery.assert_awaited_once()
        self.assertEqual(lesson_state.clear_calls, 1)

        source = Path("main.py").read_text(encoding="utf-8")
        self.assertIn('@router.message(F.text == "🧘 Бесплатный урок", StateFilter(\'*\'))', source)
        self.assertIn('@router.message(F.text == "🎁 Подарить доступ в клуб", StateFilter(\'*\'))', source)
        self.assertIn('@router.message(F.text == "🚨 Правила клуба", StateFilter(\'*\'))', source)
        self.assertNotIn('F.text == "🎁 Бесплатный урок"', source)
        self.assertNotIn('F.text == "🎁 Подарить доступ"', source)
        self.assertNotIn('F.text == "🆘 Правила клуба"', source)

    async def test_gift_certificate_name_step_accepts_name_or_without_name(self):
        callback = FakeCallback()
        callback.data = "gift_tariff:gift_1m"
        state = FakeState()
        await self.main.gift_tariff_selected(callback, state)
        self.assertEqual(state.states[-1], self.main.GiftPurchaseStates.certificate_name)
        prompt, kwargs = callback.message.answers[-1]
        self.assertIn("Как подписать сертификат?", prompt)
        self.assertEqual(kwargs["reply_markup"].inline_keyboard[0][0].text, "Без имени")

        named_state = FakeState()
        named_message = FakeMessage()
        named_message.text = "  Анна   Мария  "
        await self.main.gift_certificate_name_received(named_message, named_state)
        self.assertEqual(named_state.data["certificate_name"], "Анна Мария")
        self.assertEqual(named_state.states[-1], self.main.GiftPurchaseStates.recipient_name)

        unnamed_state = FakeState()
        unnamed_callback = FakeCallback()
        await self.main.gift_certificate_without_name_callback(unnamed_callback, unnamed_state)
        self.assertIsNone(unnamed_state.data["certificate_name"])
        self.assertEqual(unnamed_state.states[-1], self.main.GiftPurchaseStates.recipient_name)

    async def test_gift_certificate_name_rejects_too_long_before_checkout(self):
        state = FakeState()
        message = FakeMessage()
        message.text = "А" * 51
        await self.main.gift_certificate_name_received(message, state)
        self.assertEqual(state.states, [])
        self.assertEqual(state.data, {})
        self.assertEqual(
            message.answers[-1][0],
            "Подпись слишком длинная для сертификата. "
            "Попробуйте указать имя или более короткий вариант имени и фамилии.",
        )

    async def test_schedule_uses_current_moscow_month_and_has_no_previous_month_fallback(self):
        august = datetime(2026, 8, 17, 12, tzinfo=timezone.utc)
        with patch.object(self.main, "load_club_schedule", return_value={"telegram_file_id": "current-file"}) as load:
            key, file_id, caption = self.main.current_schedule(august)
        self.assertEqual((key, file_id), ("2026-08", "current-file"))
        self.assertEqual(caption, "📅 Расписание на август 2026")
        load.assert_called_once_with("2026-08")

        with patch.object(self.main, "load_club_schedule", return_value=None) as load:
            key, file_id, _ = self.main.current_schedule(august)
        self.assertEqual(key, "2026-08")
        self.assertIsNone(file_id)
        load.assert_called_once_with("2026-08")

        moscow_september = datetime(2026, 8, 31, 21, 30, tzinfo=timezone.utc)
        with patch.object(self.main, "load_club_schedule", return_value=None) as load:
            key, _, caption = self.main.current_schedule(moscow_september)
        self.assertEqual(key, "2026-09")
        self.assertEqual(caption, "📅 Расписание на сентябрь 2026")
        load.assert_called_once_with("2026-09")

    async def test_schedule_handler_sends_photo_or_missing_message_and_clears_state(self):
        state = FakeState()
        message = SimpleNamespace(answer=AsyncMock(), answer_photo=AsyncMock())
        with patch.object(self.main, "current_schedule", return_value=("2026-08", "file-id", "📅 Расписание на август 2026")):
            await self.main.schedule_button_handler(message, state)
        message.answer_photo.assert_awaited_once_with(photo="file-id", caption="📅 Расписание на август 2026")
        message.answer.assert_not_awaited()
        self.assertEqual(state.clear_calls, 1)

        state = FakeState()
        message = SimpleNamespace(answer=AsyncMock(), answer_photo=AsyncMock())
        with patch.object(self.main, "current_schedule", return_value=("2026-08", None, "unused")):
            await self.main.schedule_button_handler(message, state)
        message.answer.assert_awaited_once_with("📅 Расписание на этот месяц скоро появится.")
        message.answer_photo.assert_not_awaited()
        self.assertEqual(state.clear_calls, 1)

    async def test_invalid_schedule_file_has_safe_user_and_admin_fallback(self):
        state = FakeState()
        raw_file_id = "AgAC_secret_schedule_file"
        raw_error = f"wrong file identifier {raw_file_id}"
        message = SimpleNamespace(
            answer=AsyncMock(),
            answer_photo=AsyncMock(side_effect=RuntimeError(raw_error)),
        )
        with patch.object(
            self.main,
            "current_schedule",
            return_value=("2026-08", raw_file_id, "📅 Расписание на август 2026"),
        ), patch.object(self.main, "notify_admins", AsyncMock()) as notify, self.assertLogs(level="ERROR") as logs:
            await self.main.schedule_button_handler(message, state)

        message.answer.assert_awaited_once_with("📅 Не получилось загрузить расписание. Мы уже проверяем файл.")
        notify.assert_awaited_once()
        notified_text = notify.await_args.args[0]
        self.assertIn("2026-08", notified_text)
        self.assertIn("ref:", notified_text)
        self.assertNotIn(raw_file_id, notified_text)
        self.assertNotIn(raw_file_id, "\n".join(logs.output))

    async def test_admin_schedule_controls_are_private_admin_only(self):
        admin = FakeIncomingMessage(user_id=1)
        with patch.object(
            self.main,
            "load_admin_schedule_view",
            return_value=("schedule panel", self.main.inline_keyboard([])),
        ):
            await self.main.admin_schedule_button_handler(admin, FakeState())
        self.assertEqual(admin.answers[0][0], "schedule panel")

        non_admin = FakeIncomingMessage(user_id=777)
        with patch.object(self.main, "load_admin_schedule_view") as load:
            await self.main.admin_schedule_button_handler(non_admin, FakeState())
        load.assert_not_called()
        self.assertEqual(non_admin.answers, [])

        forged = FakeCallback(user_id=777)
        forged.data = "admin_schedule_upload:2026-09"
        with patch.object(self.main, "get_db_conn") as get_conn:
            await self.main.admin_schedule_upload_callback(forged, FakeState())
        get_conn.assert_not_called()
        self.assertEqual(forged.answers, [])

        group_admin = FakeCallback(user_id=1)
        group_admin.data = "admin_schedule_upload:2026-09"
        group_admin.message.chat.type = "group"
        await self.main.admin_schedule_upload_callback(group_admin, FakeState())
        self.assertIn("личном чате", group_admin.answers[0][0])

    async def test_admin_schedule_upload_saves_photo_owner_and_clears_fsm(self):
        callback = FakeCallback(user_id=1)
        callback.data = "admin_schedule_upload:2026-09"
        state = FakeState()
        await self.main.admin_schedule_upload_callback(callback, state)
        self.assertEqual(state.states[-1], self.main.ScheduleAdminStates.waiting_for_photo)
        self.assertEqual(state.data["schedule_month"], "2026-09")

        conn = FakeConnection(fetches=[("2026-09",)])
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=1),
            chat=SimpleNamespace(type="private"),
            photo=[SimpleNamespace(file_id="small"), SimpleNamespace(file_id="large-file-id")],
            answer=AsyncMock(),
            answer_photo=AsyncMock(),
        )
        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.admin_schedule_photo_received(message, state)

        insert = next((query, params) for query, params in conn.cursor_obj.queries if "INSERT INTO club_schedules" in query)
        self.assertIn("ON CONFLICT (schedule_month) DO UPDATE", insert[0])
        self.assertEqual(insert[1], ("2026-09", "large-file-id", 1))
        self.assertEqual(conn.commits, 1)
        self.assertEqual(state.clear_calls, 1)
        message.answer_photo.assert_awaited_once_with(
            photo="large-file-id",
            caption="✅ Расписание на сентябрь 2026 сохранено.",
        )

    async def test_admin_schedule_cancel_and_delete_are_safe(self):
        cancel = FakeCallback(user_id=1)
        cancel.data = "admin_schedule_upload_cancel"
        cancel_state = FakeState()
        await self.main.admin_schedule_upload_cancel_callback(cancel, cancel_state)
        self.assertEqual(cancel_state.clear_calls, 1)

        request = FakeCallback(user_id=1)
        request.data = "admin_schedule_remove:2026-09"
        with patch.object(self.main, "get_db_conn") as get_conn:
            await self.main.admin_schedule_delete_callback(request, FakeState())
        get_conn.assert_not_called()
        keyboard = request.message.edits[0][1]["reply_markup"]
        self.assertEqual(keyboard.inline_keyboard[0][0].callback_data, "admin_schedule_delete_confirm:2026-09")

        delete_conn = FakeConnection(fetches=[("2026-09",)])
        confirm = FakeCallback(user_id=1)
        confirm.data = "admin_schedule_delete_confirm:2026-09"
        with patch.object(self.main, "get_db_conn", return_value=delete_conn), \
             patch.object(self.main, "load_admin_schedule_view", return_value=("empty", self.main.inline_keyboard([]))):
            await self.main.admin_schedule_delete_confirm_callback(confirm, FakeState())
        delete_query = next((query, params) for query, params in delete_conn.cursor_obj.queries if "DELETE FROM club_schedules" in query)
        self.assertEqual(delete_query[1], ("2026-09",))
        self.assertEqual(delete_conn.commits, 1)
        self.assertIn("Расписание удалено", confirm.message.edits[0][0])

    async def test_admin_gift_center_is_private_admin_only_and_lists_other_buyers(self):
        gift = {
            "public_reference": "GIFT-OTHERBUYER0001",
            "purchaser_telegram_id": 777,
            "status": "paid_unclaimed",
        }
        admin = FakeIncomingMessage(user_id=1)
        admin_state = FakeState()
        with patch.object(self.main, "load_admin_gifts", return_value=[gift]):
            await self.main.admin_gift_center_button_handler(admin, admin_state)
        self.assertEqual(admin_state.clear_calls, 1)
        keyboard = admin.answers[0][1]["reply_markup"]
        self.assertEqual(keyboard.inline_keyboard[0][0].callback_data, "admin_gift_open:GIFT-OTHERBUYER0001")

        non_admin = FakeIncomingMessage(user_id=777)
        with patch.object(self.main, "load_admin_gifts") as load_gifts:
            await self.main.admin_gift_center_button_handler(non_admin, FakeState())
        load_gifts.assert_not_called()
        self.assertEqual(non_admin.answers, [])

    def test_admin_gift_actions_match_status_safety_policy(self):
        base = {"public_reference": "GIFT-SAFE000000001"}
        for status in ("checkout_pending", "checkout_open"):
            keyboard = self.main.admin_gift_detail_keyboard({**base, "status": status})
            callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
            self.assertIn("admin_gift_cancel:GIFT-SAFE000000001", callbacks)

        for status in ("payment_pending", "paid_unclaimed", "reserved", "redeemed", "refunded", "cancelled"):
            gift = {**base, "status": status}
            keyboard = self.main.admin_gift_detail_keyboard(gift)
            callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
            self.assertFalse(any(value.startswith("admin_gift_cancel:") for value in callbacks))

        paid = {
            **base, "status": "paid_unclaimed", "purchaser_telegram_id": 777,
            "purchaser_username": "buyer", "recipient_telegram_id": None,
            "recipient_username": None, "tariff_code": "gift_1m", "created_at": datetime(2026, 8, 1),
            "paid_at": datetime(2026, 8, 1), "applied_expiry": None,
        }
        self.assertIn("полный refund в Stripe", self.main.admin_gift_detail_text(paid))
        redeemed = {**paid, "status": "redeemed"}
        self.assertIn("не отзывает доступ автоматически", self.main.admin_gift_detail_text(redeemed))

    async def test_admin_unpaid_cancel_creates_existing_confirmed_gift_cancel_action(self):
        callback = FakeCallback(user_id=1)
        callback.data = "admin_gift_cancel:GIFT-CANCEL0000001"
        gift = {
            "public_reference": "GIFT-CANCEL0000001", "status": "checkout_open",
            "purchaser_telegram_id": 777, "recipient_telegram_id": None, "tariff_code": "gift_1m",
        }
        connection = FakeConnection()
        with patch.object(self.main, "get_db_conn", return_value=connection), \
             patch.object(self.main, "fetch_gift_by_public_reference", return_value=gift), \
             patch.object(self.main, "make_action_request", return_value="action-1") as make_request, \
             patch.object(self.main, "send_admin_action_confirmation", AsyncMock()) as confirm:
            await self.main.admin_gift_cancel_callback(callback)
        make_request.assert_called_once_with(
            connection.cursor_obj, 1, "gift_cancel",
            {"public_reference": "GIFT-CANCEL0000001", "admin_id": 1},
        )
        self.assertEqual(connection.commits, 1)
        confirm.assert_awaited_once()

    def test_purchaser_cannot_cancel_created_gift_checkout(self):
        checkout_keyboard = self.main.gift_active_checkout_conflict_keyboard({
            "public_reference": "GIFT-CREATED000001",
            "checkout_url": "https://checkout.example/gift",
        })
        buttons = [button for row in checkout_keyboard.inline_keyboard for button in row]
        self.assertEqual([button.text for button in buttons], ["💳 Вернуться к оплате"])
        self.assertTrue(all(button.callback_data is None for button in buttons))

        source = Path(self.main.__file__).read_text(encoding="utf-8")
        self.assertNotIn("gift_cancel_checkout:", source)
        self.assertNotIn("gift_user_cancel", source)
        self.assertNotIn("❌ Отменить прежнюю оплату", source)
        self.assertIn('"gift_cancel"', source)
        self.assertIn("send_admin_action_confirmation", source)

    async def test_paid_gift_cancel_callback_never_creates_action_request(self):
        callback = FakeCallback(user_id=1)
        callback.data = "admin_gift_cancel:GIFT-PAID000000001"
        with patch.object(self.main, "get_db_conn", return_value=FakeConnection()), \
             patch.object(self.main, "fetch_gift_by_public_reference", return_value={"status": "paid_unclaimed"}), \
             patch.object(self.main, "make_action_request") as make_request:
            await self.main.admin_gift_cancel_callback(callback)
        make_request.assert_not_called()
        self.assertIn("нельзя отменить локально", callback.answers[0][0])

    async def test_user_gift_status_query_remains_scoped_to_actual_purchaser(self):
        message = FakeIncomingMessage(user_id=777)
        connection = FakeConnection(fetches=[[]])
        with patch.object(self.main, "get_db_conn", return_value=connection):
            await self.main.gift_status_command(message)
        query, params = connection.cursor_obj.queries[0]
        self.assertIn("WHERE purchaser_telegram_id = %s", query)
        self.assertEqual(params, (777,))
        self.assertEqual(message.answers[0][0], "У вас пока нет подарочных сертификатов.")

    async def test_inline_keyboards_keep_expected_callback_data_and_urls(self):
        tariffs = self.main.get_tariffs_keyboard(show_trial=True)
        self.assertEqual(
            [row[0].callback_data for row in tariffs.inline_keyboard],
            ["sub_trial", "sub_1", "sub_6", "sub_12"],
        )

        no_trial = self.main.get_tariffs_keyboard(show_trial=False)
        self.assertEqual(
            [row[0].callback_data for row in no_trial.inline_keyboard],
            ["sub_1", "sub_6", "sub_12"],
        )

        feedback = self.main.get_free_lesson_feedback_keyboard()
        self.assertEqual(
            [row[0].callback_data for row in feedback.inline_keyboard],
            ["feedback_join", "feedback_question", "feedback_think"],
        )

        callback = FakeCallback()
        await self.main.send_checkout_open_instruction(
            callback,
            "https://checkout.example/session",
            42,
            "cs_test_123",
            "sub_1",
            "subscription",
        )
        payment_keyboard = callback.message.answers[-1][1]["reply_markup"]
        self.assertEqual(payment_keyboard.inline_keyboard[0][0].url, "https://checkout.example/session")
        self.assertEqual(payment_keyboard.inline_keyboard[1][0].callback_data, "back_to_tariffs")

    async def test_telegram_webhook_request_passes_through_aiogram3_handler(self):
        app = self.main.create_app()
        app.on_startup.clear()
        app.on_shutdown.clear()
        update = {
            "update_id": 1001,
            "message": {
                "message_id": 1,
                "date": int(time.time()),
                "chat": {"id": 2002, "type": "private"},
                "from": {"id": 2002, "is_bot": False, "first_name": "Test"},
                "text": "/not_registered_for_bootstrap",
            },
        }
        handler = self.route_handler(app, "POST", self.main.get_telegram_webhook_path())
        with patch.object(self.main.dp, "feed_raw_update", AsyncMock(return_value=None)) as dispatch:
            response = await handler(FakeTelegramRequest(update))
            await asyncio.sleep(0)

        self.assertEqual(response.status, 200)
        dispatch.assert_awaited_once_with(bot=self.main.bot, update=update)

    async def test_telegram_webhook_rejects_missing_secret_before_dispatch(self):
        app = self.main.create_app()
        app.on_startup.clear()
        app.on_shutdown.clear()
        handler = self.route_handler(app, "POST", self.main.get_telegram_webhook_path())
        request = FakeTelegramRequest({"update_id": 1003}, secret_token=None)

        with patch.object(self.main.dp, "feed_raw_update", AsyncMock()) as dispatch:
            response = await handler(request)

        self.assertEqual(response.status, 401)
        dispatch.assert_not_awaited()

    async def test_telegram_webhook_rejects_wrong_secret_before_dispatch_without_leaking_secret(self):
        app = self.main.create_app()
        app.on_startup.clear()
        app.on_shutdown.clear()
        handler = self.route_handler(app, "POST", self.main.get_telegram_webhook_path())
        raw_secret = TEST_ENV["WEBHOOK_SECRET"]
        request = FakeTelegramRequest({"update_id": 1004}, secret_token="wrong-token")

        log_stream = io.StringIO()
        log_handler = logging.StreamHandler(log_stream)
        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)
        try:
            with patch.object(self.main.dp, "feed_raw_update", AsyncMock()) as dispatch:
                response = await handler(request)
        finally:
            root_logger.removeHandler(log_handler)

        self.assertEqual(response.status, 401)
        dispatch.assert_not_awaited()
        self.assertNotIn(raw_secret, response.text)
        self.assertNotIn(raw_secret, log_stream.getvalue())

    async def test_unknown_callback_passes_through_dispatcher_without_payment_or_admin_action(self):
        app = self.main.create_app()
        app.on_startup.clear()
        app.on_shutdown.clear()
        update = {
            "update_id": 1002,
            "callback_query": {
                "id": "unknown-callback",
                "from": {"id": 2002, "is_bot": False, "first_name": "Test"},
                "chat_instance": "ci",
                "data": "unknown_callback",
                "message": {
                    "message_id": 2,
                    "date": int(time.time()),
                    "chat": {"id": 2002, "type": "private"},
                },
            },
        }
        handler = self.route_handler(app, "POST", self.main.get_telegram_webhook_path())
        with patch.object(self.main, "process_payment", AsyncMock()) as process_payment, \
             patch.object(self.main, "admin_action_confirm_callback", AsyncMock()) as confirm_action:
            response = await handler(FakeTelegramRequest(update))

        self.assertEqual(response.status, 200)
        process_payment.assert_not_awaited()
        confirm_action.assert_not_awaited()

    async def test_signed_stripe_webhook_route_still_returns_expected_response(self):
        app = self.main.create_app()
        app.on_startup.clear()
        app.on_shutdown.clear()
        payload = json.dumps(
            {
                "id": "evt_bootstrap_duplicate",
                "object": "event",
                "type": "checkout.session.completed",
                "created": 1720000000,
                "data": {"object": {"id": "cs_bootstrap", "object": "checkout.session"}},
            },
            separators=(",", ":"),
        ).encode("utf-8")
        headers = {
            "Stripe-Signature": signed_header(payload, TEST_ENV["STRIPE_WEBHOOK_SECRET"]),
            "Content-Type": "application/json",
        }
        with patch.dict(os.environ, TEST_ENV, clear=False), \
             patch.object(self.main, "claim_normalized_stripe_event", AsyncMock(return_value=("duplicate_processing", None))):
            handler = self.route_handler(app, "POST", "/stripe-payment")
            response = await handler(FakeStripeRequest(payload, headers))

        self.assertEqual(response.status, 200)

    def refund_payment_row(self, *, payment_id=501, telegram_id=123, amount=5000, currency="usd", subscription_id="sub_refund", period_end=None, created_at=None):
        return (
            payment_id,
            telegram_id,
            amount,
            currency,
            "cus_refund",
            subscription_id,
            "recurring",
            "succeeded",
            period_end or datetime.utcnow() + timedelta(days=30),
            created_at or datetime.utcnow() - timedelta(hours=1),
        )

    def refund_proof(self, **overrides):
        proof = {
            "refund_id": "re_full",
            "charge_id": "ch_full",
            "payment_intent_id": "pi_full",
            "invoice_id": "in_full",
            "customer_id": "cus_refund",
            "subscription_id": "sub_refund",
            "amount_refunded": 5000,
            "refund_status": "succeeded",
            "currency": "usd",
        }
        proof.update(overrides)
        return proof

    def test_subscription_refund_full_latest_payment_revokes_access_and_queues_removal(self):
        old_expiry = datetime.utcnow() + timedelta(days=20)
        conn = FakeConnection(fetches=[
            None,
            [self.refund_payment_row()],
            None,
            (True, old_expiry, "sub_refund", "cus_refund"),
            None,
            None,
            None,
            (self.main.SUBSCRIPTION_REFUND_ACCESS_REVOKED,),
        ])

        result = self.main.apply_subscription_refund_reconciliation(
            conn.cursor_obj,
            "evt_refund_full",
            self.refund_proof(),
        )

        self.assertEqual(result["result"], self.main.SUBSCRIPTION_REFUND_ACCESS_REVOKED)
        queries = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertIn("UPDATE users", queries)
        self.assertIn("stripe_refund_access_revoked", str(conn.cursor_obj.queries))
        self.assertIn("UPDATE message_delivery_events", queries)
        self.assertIn("INSERT INTO subscription_removal_events", queries)
        self.assertIn("INSERT INTO subscription_refund_reconciliations", queries)

    def test_subscription_refund_partial_or_pending_requires_review_without_revoke(self):
        cases = (
            (self.refund_proof(amount_refunded=1000), [None, [self.refund_payment_row()], (self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED,)]),
            (self.refund_proof(refund_id="re_pending", refund_status="pending"), [None, (self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED,)]),
        )
        for proof, fetches in cases:
            with self.subTest(refund_id=proof["refund_id"]):
                conn = FakeConnection(fetches=fetches)

                result = self.main.apply_subscription_refund_reconciliation(
                    conn.cursor_obj,
                    "evt_" + proof["refund_id"],
                    proof,
                )

                self.assertEqual(result["result"], self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED)
                queries = "\n".join(query for query, _ in conn.cursor_obj.queries)
                self.assertIn("INSERT INTO subscription_refund_reconciliations", queries)
                self.assertNotIn("UPDATE users", queries)
                self.assertIn("stripe_admin_message", str(conn.cursor_obj.queries))

    def test_subscription_refund_duplicate_is_idempotent(self):
        conn = FakeConnection(fetches=[(self.main.SUBSCRIPTION_REFUND_ACCESS_REVOKED,)])

        result = self.main.apply_subscription_refund_reconciliation(
            conn.cursor_obj,
            "evt_refund_duplicate",
            self.refund_proof(),
        )

        self.assertEqual(result["result"], self.main.SUBSCRIPTION_REFUND_ALREADY_RECONCILED)
        queries = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertNotIn("UPDATE users", queries)
        self.assertNotIn("INSERT INTO access_events", queries)

    def test_subscription_refund_charge_and_refund_events_share_revoked_payment(self):
        cases = (
            (
                self.refund_proof(refund_id="re_after_charge", charge_id="ch_same"),
                "charge:ch_same",
                "evt_refund_after_charge",
                "refund.updated",
            ),
            (
                self.refund_proof(refund_id=None, charge_id="ch_after_refund"),
                "refund:re_same",
                "evt_charge_after_refund",
                "charge.refunded",
            ),
        )
        for proof, existing_key, event_id, event_type in cases:
            with self.subTest(event_type=event_type):
                conn = FakeConnection(fetches=[
                    None,
                    [self.refund_payment_row()],
                    (77, existing_key, self.main.SUBSCRIPTION_REFUND_ACCESS_REVOKED),
                ])

                result = self.main.apply_subscription_refund_reconciliation(
                    conn.cursor_obj,
                    event_id,
                    proof,
                    event_type=event_type,
                )

                self.assertEqual(result["result"], self.main.SUBSCRIPTION_REFUND_ALREADY_RECONCILED)
                self.assertEqual(result["reconciliation_key"], existing_key)
                queries = "\n".join(query for query, _ in conn.cursor_obj.queries)
                self.assertIn("FROM subscription_refund_reconciliations", queries)
                self.assertIn("INSERT INTO subscription_refund_events", queries)
                self.assertNotIn("UPDATE users", queries)
                self.assertNotIn("INSERT INTO access_events", queries)
                self.assertNotIn("INSERT INTO subscription_removal_events", queries)

    def test_subscription_refund_review_required_can_be_reprocessed_to_revoke(self):
        pending = self.refund_proof(refund_status="pending")
        pending_conn = FakeConnection(fetches=[None, (1, self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED)])
        pending_result = self.main.apply_subscription_refund_reconciliation(
            pending_conn.cursor_obj,
            "evt_refund_pending",
            pending,
            event_type="refund.created",
        )
        self.assertEqual(pending_result["result"], self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED)

        period_end = datetime.utcnow() + timedelta(days=30)
        succeeded_conn = FakeConnection(fetches=[
            (self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED,),
            [self.refund_payment_row(period_end=period_end)],
            None,
            (True, period_end, "sub_refund", "cus_refund"),
            None,
            None,
            None,
            (1, self.main.SUBSCRIPTION_REFUND_ACCESS_REVOKED),
        ])
        succeeded_result = self.main.apply_subscription_refund_reconciliation(
            succeeded_conn.cursor_obj,
            "evt_refund_succeeded",
            self.refund_proof(),
            event_type="refund.updated",
        )

        self.assertEqual(succeeded_result["result"], self.main.SUBSCRIPTION_REFUND_ACCESS_REVOKED)
        self.assertIn("UPDATE users", "\n".join(query for query, _ in succeeded_conn.cursor_obj.queries))

    def test_subscription_refund_partial_then_full_revokes_once(self):
        partial_conn = FakeConnection(fetches=[
            None,
            [self.refund_payment_row()],
            (1, self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED),
        ])
        partial_result = self.main.apply_subscription_refund_reconciliation(
            partial_conn.cursor_obj,
            "evt_refund_partial",
            self.refund_proof(amount_refunded=1000),
            event_type="refund.created",
        )
        self.assertEqual(partial_result["result"], self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED)

        period_end = datetime.utcnow() + timedelta(days=30)
        full_conn = FakeConnection(fetches=[
            (self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED,),
            [self.refund_payment_row(period_end=period_end)],
            None,
            (True, period_end, "sub_refund", "cus_refund"),
            None,
            None,
            None,
            (1, self.main.SUBSCRIPTION_REFUND_ACCESS_REVOKED),
        ])
        full_result = self.main.apply_subscription_refund_reconciliation(
            full_conn.cursor_obj,
            "evt_refund_full_after_partial",
            self.refund_proof(),
            event_type="refund.updated",
        )

        self.assertEqual(full_result["result"], self.main.SUBSCRIPTION_REFUND_ACCESS_REVOKED)

    def test_charge_refunded_multiple_refunds_uses_charge_level_cumulative_key(self):
        proof = self.main.subscription_refund_proof_from_event(
            "charge.refunded",
            {
                "id": "ch_multi",
                "payment_intent": "pi_multi",
                "invoice": "in_multi",
                "customer": "cus_multi",
                "amount_refunded": 5000,
                "refunded": True,
                "currency": "usd",
                "refunds": {
                    "data": [
                        {"id": "re_part_1", "amount": 2000, "status": "succeeded"},
                        {"id": "re_part_2", "amount": 3000, "status": "succeeded"},
                    ]
                },
            },
        )

        self.assertIsNone(proof["refund_id"])
        self.assertEqual(proof["reconciliation_key"], "charge:ch_multi")
        self.assertEqual(proof["amount_refunded"], 5000)
        self.assertEqual(proof["proof_model"], "charge")

    def test_subscription_refund_newer_access_change_requires_review(self):
        period_end = datetime.utcnow() + timedelta(days=30)
        conn = FakeConnection(fetches=[
            None,
            [self.refund_payment_row(period_end=period_end)],
            None,
            (True, period_end, "sub_refund", "cus_refund"),
            None,
            (1,),
            (1, self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED),
        ])

        result = self.main.apply_subscription_refund_reconciliation(
            conn.cursor_obj,
            "evt_refund_newer_access",
            self.refund_proof(),
        )

        self.assertEqual(result["result"], self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED)
        self.assertEqual(result["reason"], "newer_access_change_exists")
        self.assertNotIn("UPDATE users", "\n".join(query for query, _ in conn.cursor_obj.queries))

    def test_subscription_refund_current_identity_or_expiry_mismatch_requires_review(self):
        period_end = datetime.utcnow() + timedelta(days=30)
        cases = (
            ((True, period_end, "sub_other", "cus_refund"), "current_subscription_mismatch"),
            ((True, period_end, "sub_refund", "cus_other"), "current_customer_mismatch"),
            ((True, period_end + timedelta(hours=1), "sub_refund", "cus_refund"), "current_expiry_exceeds_refunded_period"),
        )
        for user_row, expected_reason in cases:
            with self.subTest(expected_reason=expected_reason):
                conn = FakeConnection(fetches=[
                    None,
                    [self.refund_payment_row(period_end=period_end)],
                    None,
                    user_row,
                    (1, self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED),
                ])

                result = self.main.apply_subscription_refund_reconciliation(
                    conn.cursor_obj,
                    "evt_" + expected_reason,
                    self.refund_proof(),
                )

                self.assertEqual(result["result"], self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED)
                self.assertEqual(result["reason"], expected_reason)

    def test_subscription_refund_paid_already_false_does_not_queue_second_removal(self):
        period_end = datetime.utcnow() + timedelta(days=30)
        conn = FakeConnection(fetches=[
            None,
            [self.refund_payment_row(period_end=period_end)],
            None,
            (False, period_end, "sub_refund", "cus_refund"),
            (1, self.main.SUBSCRIPTION_REFUND_ALREADY_INACTIVE),
        ])

        result = self.main.apply_subscription_refund_reconciliation(
            conn.cursor_obj,
            "evt_refund_already_inactive",
            self.refund_proof(),
        )

        self.assertEqual(result["result"], self.main.SUBSCRIPTION_REFUND_ALREADY_INACTIVE)
        queries = "\n".join(query for query, _ in conn.cursor_obj.queries)
        self.assertNotIn("INSERT INTO subscription_removal_events", queries)
        self.assertNotIn("UPDATE users", queries)

    def test_revoke_access_command_is_confirm_only_and_refund_info_is_read_only_source(self):
        source = Path(self.main.__file__).read_text(encoding="utf-8")
        revoke_source = source[source.index("async def revoke_access_command"):source.index("@router.message(Command('refund_info')")]
        refund_source = source[source.index("async def refund_info_command"):source.index("@router.message(Command('sync_stripe_user')")]
        self.assertIn('"revoke_access"', revoke_source)
        self.assertIn("make_action_request", revoke_source)
        self.assertNotIn("UPDATE users", revoke_source)
        self.assertNotIn("stripe.Subscription.modify", revoke_source)
        self.assertNotIn("INSERT INTO", refund_source)
        self.assertNotIn("UPDATE ", refund_source)
        self.assertIn("read_only: true", refund_source)

    async def test_refund_info_uses_reconciliation_original_payment_for_refund_charge_and_pi(self):
        reconciliation = (
            "re_info",
            "evt_refund_info",
            "ch_info",
            "pi_info",
            "in_info",
            "cus_info",
            "sub_info",
            123,
            501,
            5000,
            5000,
            "usd",
            "succeeded",
            True,
            self.main.SUBSCRIPTION_REFUND_ACCESS_REVOKED,
            None,
            datetime.utcnow(),
        )
        payment = (
            501,
            123,
            "in_paid",
            "cus_info",
            "sub_info",
            "recurring",
            5000,
            "usd",
            datetime.utcnow() + timedelta(days=30),
            datetime.utcnow() - timedelta(days=1),
        )
        for query in ("re_info", "ch_info", "pi_info"):
            with self.subTest(query=query):
                conn = FakeConnection(fetches=[[reconciliation], [payment]])
                message = FakeIncomingMessage(user_id=1)
                command = SimpleNamespace(args=query)

                with patch.object(self.main, "get_db_conn", return_value=conn), \
                     patch.object(self.main.stripe.Refund, "retrieve", side_effect=AssertionError("Stripe API must not be called")), \
                     patch.object(self.main.stripe.Charge, "retrieve", side_effect=AssertionError("Stripe API must not be called")):
                    await self.main.refund_info_command(message, command)

                answer = message.answers[-1][0]
                self.assertIn("original_payment_event_id=501", answer)
                self.assertIn("Original payments:", answer)
                self.assertIn("payment_event_id=501", answer)
                self.assertIn("kind=recurring", answer)
                self.assertIn("auto_revoke_safe=True", answer)
                self.assertIn("review_required=False", answer)
                self.assertIn("read_only: true", answer)
                queries = "\n".join(sql for sql, _ in conn.cursor_obj.queries)
                self.assertIn("WHERE id = ANY", queries)
                self.assertNotIn("OR stripe_customer_id = %s", queries)
                self.assertNotIn("INSERT", queries)
                self.assertNotIn("UPDATE", queries)
                self.assertNotIn("DELETE", queries)

    async def test_refund_info_without_original_payment_reports_unavailable_locally(self):
        reconciliation = (
            "re_no_payment",
            "evt_refund_info",
            "ch_no_payment",
            "pi_no_payment",
            "in_info",
            "cus_info",
            "sub_info",
            123,
            None,
            5000,
            5000,
            "usd",
            "succeeded",
            False,
            self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED,
            "manual_review",
            None,
        )
        conn = FakeConnection(fetches=[[reconciliation]])
        message = FakeIncomingMessage(user_id=1)

        with patch.object(self.main, "get_db_conn", return_value=conn), \
             patch.object(self.main.stripe.Refund, "retrieve", side_effect=AssertionError("Stripe API must not be called")), \
             patch.object(self.main.stripe.Charge, "retrieve", side_effect=AssertionError("Stripe API must not be called")):
            await self.main.refund_info_command(message, SimpleNamespace(args="re_no_payment"))

        answer = message.answers[-1][0]
        self.assertIn("original_payment_event_id=none", answer)
        self.assertIn("payment match unavailable locally", answer)
        self.assertIn("review_required=True", answer)
        queries = "\n".join(sql for sql, _ in conn.cursor_obj.queries)
        self.assertNotIn("WHERE id = ANY", queries)
        self.assertNotIn("INSERT", queries)
        self.assertNotIn("UPDATE", queries)
        self.assertNotIn("DELETE", queries)

    async def test_refund_info_accepts_full_identifier_without_echoing_it(self):
        raw_refund = "re_privacy_full_identifier_123456"
        reconciliation = (
            raw_refund, "evt_privacy_123456", "ch_privacy_123456", "pi_privacy_123456",
            "in_privacy_123456", "cus_privacy_123456", "sub_privacy_123456", 123, None,
            5000, 5000, "usd", "succeeded", True,
            self.main.SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "manual_review", None,
        )
        conn = FakeConnection(fetches=[[reconciliation]])
        message = FakeIncomingMessage(user_id=1)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.refund_info_command(message, SimpleNamespace(args=raw_refund))

        answer = message.answers[-1][0]
        self.assertNotIn(raw_refund, answer)
        self.assertIn(self.main.safe_log_id(raw_refund), answer)
        self.assertEqual(conn.cursor_obj.queries[0][1], (raw_refund, raw_refund, raw_refund))

    async def test_unlinked_stripe_admin_output_redacts_ids_email_and_command_echo(self):
        raw_values = {
            "event": "evt_privacy_event_123456",
            "invoice": "in_privacy_invoice_123456",
            "customer": "cus_privacy_customer_123456",
            "subscription": "sub_privacy_subscription_123456",
            "email": "private.person@example.com",
        }
        row = (
            raw_values["event"], "invoice.payment_succeeded", raw_values["invoice"],
            raw_values["customer"], raw_values["subscription"], raw_values["email"],
            5000, "usd", "subscription_cycle", datetime.utcnow(), datetime.utcnow(),
        )
        conn = FakeConnection(fetches=[[row]])
        message = FakeIncomingMessage(user_id=1)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.unlinked_stripe_command(message)

        answer = message.replies[-1][0]
        for raw in raw_values.values():
            self.assertNotIn(raw, answer)
        self.assertIn(self.main.safe_log_id(raw_values["customer"]), answer)
        self.assertIn(self.main.safe_log_email(raw_values["email"]), answer)
        self.assertIn("/link_stripe_user <telegram_id> <customer_id> <subscription_id>", answer)

    async def test_stripe_links_admin_output_redacts_identity_and_email(self):
        customer = "cus_privacy_links_123456"
        subscription = "sub_privacy_links_123456"
        email = "stripe.links@example.com"
        row = (customer, subscription, email, "active", datetime.utcnow(), True, "test", datetime.utcnow(), datetime.utcnow())
        conn = FakeConnection(fetches=[[row]])
        message = FakeIncomingMessage(user_id=1)

        with patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.stripe_links_command(message, SimpleNamespace(args="123"))

        answer = message.replies[-1][0]
        self.assertNotIn(customer, answer)
        self.assertNotIn(subscription, answer)
        self.assertNotIn(email, answer)
        self.assertIn(self.main.safe_log_id(customer), answer)
        self.assertIn(self.main.safe_log_email(email), answer)

    async def test_checkout_failure_admin_alert_redacts_stripe_identity(self):
        customer = "cus_checkout_alert_private_123456"
        subscription = "sub_checkout_alert_private_123456"
        callback = SimpleNamespace(message=SimpleNamespace(answer=AsyncMock()))

        with patch.object(self.main, "get_open_invoice_url_for_subscription", new=AsyncMock(return_value=(None, None))), \
             patch.object(self.main, "create_billing_portal_url", new=AsyncMock(return_value=None)), \
             patch.object(self.main, "notify_admins", new=AsyncMock()) as notify:
            result = await self.main.send_existing_subscription_action(
                callback, 123, subscription, customer, "active"
            )

        self.assertTrue(result)
        alert = notify.await_args.args[0]
        self.assertNotIn(customer, alert)
        self.assertNotIn(subscription, alert)
        self.assertNotIn("https://checkout.stripe.com/", alert)
        self.assertIn(self.main.safe_log_id(customer), alert)

    def test_restore_access_summary_redacts_stripe_identity(self):
        customer = "cus_restore_private_123456"
        subscription = "sub_restore_private_123456"
        summary = self.main.restore_access_user_summary(
            123,
            (True, datetime.utcnow(), False, None, False, subscription, customer, True),
        )
        self.assertNotIn(customer, summary)
        self.assertNotIn(subscription, summary)
        self.assertIn(self.main.safe_log_id(customer), summary)

    async def test_support_reply_button_enters_waiting_state_without_target_payload(self):
        callback = FakeCallback(user_id=777)
        state = FakeState()

        await self.main.start_support_reply(callback, state)

        self.assertEqual(state.states, [self.main.SupportReplyState.waiting_for_message])
        self.assertEqual(state.data, {})
        self.assertIn("фото/видео", callback.message.answers[-1][0])

    async def test_admin_reply_includes_safe_support_reply_button(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=1),
            chat=SimpleNamespace(id=1),
            message_id=88,
            text="Ответ",
            answer=AsyncMock(),
        )
        state = FakeState()
        state.data["reply_to_user"] = 777

        with patch.object(self.main.bot, "send_message", new=AsyncMock()) as send_message, \
             patch.object(self.main.bot, "copy_message", new=AsyncMock()):
            await self.main.send_admin_reply(message, state)

        markup = send_message.await_args.kwargs["reply_markup"]
        self.assertEqual(markup.inline_keyboard[0][0].callback_data, "support_reply")
        self.assertNotIn("777", markup.inline_keyboard[0][0].callback_data)

    def support_message(self, **kwargs):
        defaults = {
            "from_user": SimpleNamespace(id=777, username="member", full_name="Member Name"),
            "text": None,
            "photo": None,
            "video": None,
            "caption": None,
            "document": None,
            "answer": AsyncMock(),
        }
        defaults.update(kwargs)
        return SimpleNamespace(**defaults)

    async def test_support_text_reply_delivers_actual_user_context_and_clears_state(self):
        message = self.support_message(text="Мой ID 999, но это только текст")
        state = FakeState()

        with patch.object(self.main, "ADMIN_IDS", [1]), \
             patch.object(self.main.bot, "send_message", new=AsyncMock()) as send_message:
            await self.main.send_support_reply(message, state)

        self.assertEqual(send_message.await_args_list[0].args[0], 1)
        self.assertIn("ID: 777", send_message.await_args_list[0].args[1])
        admin_button = send_message.await_args_list[0].kwargs["reply_markup"].inline_keyboard[0][0]
        self.assertEqual(admin_button.callback_data, "reply_to_777")
        self.assertEqual(send_message.await_args_list[1].args, (1, message.text))
        self.assertEqual(state.clear_calls, 1)
        self.assertIn("передано", message.answer.await_args.args[0])

    async def test_support_photo_uses_highest_file_id_and_preserves_caption(self):
        message = self.support_message(
            photo=[SimpleNamespace(file_id="small"), SimpleNamespace(file_id="largest")],
            caption="photo caption",
        )
        state = FakeState()

        with patch.object(self.main, "ADMIN_IDS", [1]), \
             patch.object(self.main.bot, "send_message", new=AsyncMock()) as send_message, \
             patch.object(self.main.bot, "send_photo", new=AsyncMock()) as send_photo:
            await self.main.send_support_reply(message, state)

        admin_button = send_message.await_args.kwargs["reply_markup"].inline_keyboard[0][0]
        self.assertEqual(admin_button.callback_data, "reply_to_777")
        send_photo.assert_awaited_once_with(1, "largest", caption="photo caption")
        self.assertEqual(state.clear_calls, 1)

    async def test_support_photo_without_caption_is_delivered(self):
        message = self.support_message(photo=[SimpleNamespace(file_id="photo-id")])
        state = FakeState()

        with patch.object(self.main, "ADMIN_IDS", [1]), \
             patch.object(self.main.bot, "send_message", new=AsyncMock()), \
             patch.object(self.main.bot, "send_photo", new=AsyncMock()) as send_photo:
            await self.main.send_support_reply(message, state)

        send_photo.assert_awaited_once_with(1, "photo-id", caption=None)

    async def test_support_video_uses_file_id_and_preserves_caption(self):
        message = self.support_message(video=SimpleNamespace(file_id="video-id"), caption="video caption")
        state = FakeState()

        with patch.object(self.main, "ADMIN_IDS", [1]), \
             patch.object(self.main.bot, "send_message", new=AsyncMock()) as send_message, \
             patch.object(self.main.bot, "send_video", new=AsyncMock()) as send_video:
            await self.main.send_support_reply(message, state)

        admin_button = send_message.await_args.kwargs["reply_markup"].inline_keyboard[0][0]
        self.assertEqual(admin_button.callback_data, "reply_to_777")
        send_video.assert_awaited_once_with(1, "video-id", caption="video caption")

    async def test_support_video_without_caption_is_delivered(self):
        message = self.support_message(video=SimpleNamespace(file_id="video-id"))
        state = FakeState()

        with patch.object(self.main, "ADMIN_IDS", [1]), \
             patch.object(self.main.bot, "send_message", new=AsyncMock()), \
             patch.object(self.main.bot, "send_video", new=AsyncMock()) as send_video:
            await self.main.send_support_reply(message, state)

        send_video.assert_awaited_once_with(1, "video-id", caption=None)

    async def test_support_document_is_rejected_without_admin_delivery_or_state_clear(self):
        message = self.support_message(document=SimpleNamespace(file_id="document-id"))
        state = FakeState()

        with patch.object(self.main.bot, "send_message", new=AsyncMock()) as send_message:
            await self.main.send_support_reply(message, state)

        send_message.assert_not_awaited()
        self.assertEqual(state.clear_calls, 0)
        self.assertIn("только текст, фото или видео", message.answer.await_args.args[0])

    async def test_non_admin_cannot_start_admin_reply(self):
        callback = FakeCallback(user_id=777)
        callback.data = "reply_to_999"
        state = FakeState()

        await self.main.start_admin_reply(callback, state)

        self.assertEqual(state.states, [])
        self.assertEqual(state.data, {})
        self.assertTrue(callback.answers[-1][1]["show_alert"])

    async def test_support_text_cannot_inject_admin_reply_callback_target(self):
        message = self.support_message(text="reply_to_999999")
        state = FakeState()

        with patch.object(self.main, "ADMIN_IDS", [1]), \
             patch.object(self.main.bot, "send_message", new=AsyncMock()) as send_message:
            await self.main.send_support_reply(message, state)

        admin_button = send_message.await_args_list[0].kwargs["reply_markup"].inline_keyboard[0][0]
        self.assertEqual(admin_button.callback_data, "reply_to_777")
        self.assertNotEqual(admin_button.callback_data, message.text)

    async def test_support_round_trip_returns_admin_reply_action(self):
        admin_message = SimpleNamespace(
            from_user=SimpleNamespace(id=1),
            chat=SimpleNamespace(id=1),
            message_id=88,
            text="admin answer",
            answer=AsyncMock(),
        )
        admin_state = FakeState()
        admin_state.data["reply_to_user"] = 777
        user_state = FakeState()
        callback = FakeCallback(user_id=777)
        user_message = self.support_message(text="user follows up")

        with patch.object(self.main.bot, "send_message", new=AsyncMock()) as send_message, \
             patch.object(self.main.bot, "copy_message", new=AsyncMock()), \
             patch.object(self.main, "ADMIN_IDS", [1]):
            await self.main.send_admin_reply(admin_message, admin_state)
            user_button = send_message.await_args_list[0].kwargs["reply_markup"].inline_keyboard[0][0]
            self.assertEqual(user_button.callback_data, "support_reply")
            self.assertNotIn("777", user_button.callback_data)

            await self.main.start_support_reply(callback, user_state)
            await self.main.send_support_reply(user_message, user_state)

        context_call = next(
            call for call in send_message.await_args_list
            if len(call.args) > 1 and "Ответ пользователя" in call.args[1]
        )
        admin_button = context_call.kwargs["reply_markup"].inline_keyboard[0][0]
        self.assertEqual(admin_button.callback_data, "reply_to_777")
        self.assertEqual(user_state.clear_calls, 2)

    async def test_support_total_admin_failure_keeps_state_and_no_false_confirmation(self):
        message = self.support_message(text="retry me")
        state = FakeState()

        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(
                 self.main, "deliver_support_reply_to_admin",
                 new=AsyncMock(side_effect=RuntimeError("delivery failed")),
             ):
            await self.main.send_support_reply(message, state)

        self.assertEqual(state.clear_calls, 0)
        self.assertIn("не удалось", message.answer.await_args.args[0].lower())
        self.assertNotIn("спасибо", message.answer.await_args.args[0].lower())

    async def test_support_partial_admin_delivery_succeeds_and_clears_state(self):
        message = self.support_message(text="partial")
        state = FakeState()
        delivery = AsyncMock(side_effect=[None, RuntimeError("second admin failed")])

        with patch.object(self.main, "ADMIN_IDS", [1, 2]), \
             patch.object(self.main, "deliver_support_reply_to_admin", new=delivery):
            await self.main.send_support_reply(message, state)

        self.assertEqual([call.args[0] for call in delivery.await_args_list], [1, 2])
        self.assertEqual(state.clear_calls, 1)
        self.assertIn("передано", message.answer.await_args.args[0])

    async def test_existing_support_question_text_workflow_still_forwards(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=777, username="member", full_name="Member Name"),
            chat=SimpleNamespace(id=777),
            message_id=55,
            text="existing question",
            answer=AsyncMock(),
        )
        state = FakeState()
        conn = FakeConnection()

        with patch.object(self.main, "ADMIN_IDS", [1]), \
             patch.object(self.main.bot, "forward_message", new=AsyncMock()) as forward_message, \
             patch.object(self.main.bot, "send_message", new=AsyncMock()), \
             patch.object(self.main, "get_db_conn", return_value=conn):
            await self.main.forward_question_to_admin(message, state)

        forward_message.assert_awaited_once_with(chat_id=1, from_chat_id=777, message_id=55)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(state.clear_calls, 1)


if __name__ == "__main__":
    unittest.main()
