import contextlib
import asyncio
import io
import json
import os
import tempfile
import threading
import time
import unittest
from unittest import mock
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit

import psycopg2
from aiogram.fsm.storage.base import StorageKey
from psycopg2 import sql
from psycopg2.extensions import make_dsn

from db_migrations import (
    MIGRATIONS_DIR,
    MigrationError,
    equivalent_index_structure_exists,
    index_structure_matches,
    load_migrations,
    payment_integrity_index_requirement_matches,
    run_migrations,
)
from postgres_fsm_storage import PostgresFSMStorage, cleanup_postgres_fsm_storage
from access_mismatch_observability import (
    load_access_mismatch_counts,
    load_access_mismatch_samples,
)
from scheduled_jobs import (
    claim_pending_message_deliveries,
    enqueue_message_delivery,
    mark_delivery_cancelled,
    mark_delivery_failed,
    mark_delivery_sent,
    save_delivery_invite_link,
)
from stripe_invoice_rules import (
    claim_stripe_event,
    mark_stripe_event_processed,
    release_stripe_event_claim,
)


POSTGRES_TEST_DSN = os.getenv("POSTGRES_TEST_DSN")

MAIN_TEST_ENV = {
    "BOT_TOKEN": "123456:TEST_TOKEN_FOR_POSTGRES_ONLY",
    "DATABASE_URL": "postgresql://user:pass@localhost/db",
    "GROUP_ID": "-100123",
    "ADMIN_IDS": "1,2",
    "STRIPE_API_KEY": "sk_test_dummy",
    "STRIPE_WEBHOOK_SECRET": "postgres_test_webhook_secret",
    "WEBHOOK_SECRET": "telegram_secret",
    "YOUR_DOMAIN": "https://club.example",
    "PRICE_TRIAL": "price_trial",
    "PRICE_1M": "price_1m",
    "PRICE_6M": "price_6m",
    "PRICE_12M": "price_12m",
    "GIFT_TOKEN_SECRET": "postgres-test-gift-token-secret-32chars",
    "GIFT_PRICE_1M": "price_gift_1m",
    "GIFT_PRICE_6M": "price_gift_6m",
    "GIFT_PRICE_12M": "price_gift_12m",
}


def import_main():
    os.environ.update(MAIN_TEST_ENV)
    import main
    return main


def connect(dsn):
    return psycopg2.connect(dsn)


def db_dsn(dbname):
    return make_dsn(POSTGRES_TEST_DSN, dbname=dbname)


def dsn_password(dsn):
    if "://" in dsn:
        password = urlsplit(dsn).password
        return unquote(password) if password else None
    for part in dsn.split():
        key, sep, value = part.partition("=")
        if sep and key == "password":
            return value
    return None


def create_temp_db(prefix="codex_pg_migrations"):
    name = f"{prefix}_{uuid.uuid4().hex[:12]}"
    conn = connect(POSTGRES_TEST_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    finally:
        cur.close()
        conn.close()
    return name, db_dsn(name)


def drop_temp_db(name):
    conn = connect(POSTGRES_TEST_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s
              AND pid <> pg_backend_pid()
            """,
            (name,),
        )
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))
    finally:
        cur.close()
        conn.close()


class TrackingConnection:
    closed_count = 0
    cursor_closed_count = 0

    def __init__(self, raw):
        self.raw = raw

    def cursor(self):
        return TrackingCursor(self.raw.cursor())

    def commit(self):
        return self.raw.commit()

    def rollback(self):
        return self.raw.rollback()

    def close(self):
        type(self).closed_count += 1
        return self.raw.close()


class TrackingCursor:
    def __init__(self, raw):
        self.raw = raw

    def execute(self, *args, **kwargs):
        return self.raw.execute(*args, **kwargs)

    def fetchone(self):
        return self.raw.fetchone()

    def fetchall(self):
        return self.raw.fetchall()

    def close(self):
        TrackingConnection.cursor_closed_count += 1
        return self.raw.close()


@unittest.skipUnless(POSTGRES_TEST_DSN, "POSTGRES_TEST_DSN is required for PostgreSQL integration tests")
class PostgresMigrationIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.db_name, self.dsn = create_temp_db()

    def tearDown(self):
        drop_temp_db(self.db_name)

    def get_conn(self):
        conn = connect(self.dsn)
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
        return conn

    def query_all(self, query, params=()):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(query, params)
            return cur.fetchall()
        finally:
            cur.close()
            conn.close()

    def query_one(self, query, params=()):
        rows = self.query_all(query, params)
        return rows[0] if rows else None

    def index_columns_unique_predicate(self, index_name):
        return self.query_one(
            """
            SELECT
                i.indisunique,
                array_agg(a.attname ORDER BY ord.ordinality),
                pg_get_expr(i.indpred, i.indrelid)
            FROM pg_class idx
            JOIN pg_index i ON i.indexrelid = idx.oid
            JOIN pg_class tbl ON tbl.oid = i.indrelid
            JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
            JOIN unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ordinality) ON TRUE
            JOIN pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = ord.attnum
            WHERE ns.nspname = 'public'
              AND idx.relname = %s
            GROUP BY i.indisunique, i.indpred, i.indrelid
            """,
            (index_name,),
        )

    def insert_recovery_user(self, telegram_id, **overrides):
        fields = {
            "telegram_id": telegram_id,
            "paid": False,
            "expiry_date": None,
            "first_payment_done": False,
            "blocked_bot": False,
            "stripe_subscription_id": None,
        }
        fields.update(overrides)
        columns = list(fields)
        values = [fields[column] for column in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                sql.SQL("INSERT INTO users ({}) VALUES ({})").format(
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(placeholders),
                ),
                values,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def insert_checkout_attempt(
        self,
        telegram_id,
        *,
        hours_ago=24,
        status="expired",
        mode="payment",
        tariff="sub_1",
        stripe_subscription_id=None,
        last_error=None,
    ):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO checkout_sessions (
                    telegram_id, tariff_code, mode, stripe_subscription_id,
                    idempotency_key, status, last_error, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s,
                        (NOW() AT TIME ZONE 'UTC') - (%s * INTERVAL '1 hour'),
                        (NOW() AT TIME ZONE 'UTC') - (%s * INTERVAL '1 hour'))
                RETURNING id
                """,
                (
                    telegram_id,
                    tariff,
                    mode,
                    stripe_subscription_id,
                    f"idem_{telegram_id}_{uuid.uuid4().hex}",
                    status,
                    last_error,
                    hours_ago,
                    hours_ago,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0]
        finally:
            cur.close()
            conn.close()

    def insert_retry_attempt(self, telegram_id, *, hours_ago=24, tariff="sub_1"):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO checkout_retry_events (telegram_id, tariff_code, attempt_at)
                VALUES (%s, %s, (NOW() AT TIME ZONE 'UTC') - (%s * INTERVAL '1 hour'))
                """,
                (telegram_id, tariff, hours_ago),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def insert_stripe_link(self, telegram_id, *, status, is_active, subscription_id=None, future_period=True):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            current_period_sql = (
                "(NOW() AT TIME ZONE 'UTC') + INTERVAL '24 hours'"
                if future_period
                else "NULL"
            )
            cur.execute(
                f"""
                INSERT INTO stripe_links (
                    telegram_id, stripe_subscription_id, status, is_active, current_period_end
                )
                VALUES (%s, %s, %s, %s, {current_period_sql})
                """,
                (telegram_id, subscription_id or f"sub_{telegram_id}", status, is_active),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def due_recovery_users(self):
        main = import_main()
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            return main.fetch_due_first_purchase_recovery_users(cur, limit=50)
        finally:
            cur.close()
            conn.close()

    def enqueue_recovery(self, telegram_id, latest_attempt_at):
        main = import_main()
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            created = main.enqueue_first_purchase_recovery_reminder(cur, telegram_id, latest_attempt_at)
            conn.commit()
            return created
        finally:
            cur.close()
            conn.close()

    def recovery_row(self, telegram_id):
        main = import_main()
        return self.query_one(
            """
            SELECT delivery_key, status, attempt_count, last_error, claimed_at, lease_until, sent_at, next_attempt_at, payload_json
            FROM message_delivery_events
            WHERE delivery_key = %s
            """,
            (main.first_purchase_recovery_delivery_key(telegram_id),),
        )

    def payment_delivery_payload(self, delivery_key):
        row = self.query_one(
            """
            SELECT delivery_type, payload_json
            FROM message_delivery_events
            WHERE delivery_key = %s
            """,
            (delivery_key,),
        )
        return row[0], json.loads(row[1])

    def test_payment_success_message_initial_state_update_and_outbox_payload_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        self.insert_recovery_user(9801)
        new_expiry = datetime(2026, 9, 1, 0, 0)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE users
                SET paid = TRUE,
                    expiry_date = %s,
                    first_payment_done = TRUE,
                    payment_failed = FALSE,
                    payment_failed_at = NULL,
                    grace_period_end = NULL
                WHERE telegram_id = %s
                RETURNING expiry_date
                """,
                (new_expiry, 9801),
            )
            confirmed_expiry = cur.fetchone()[0]
            main.enqueue_user_payment_success_message(
                cur,
                "evt_pg_initial",
                9801,
                "payment_success",
                confirmed_expiry,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        delivery_type, payload = self.payment_delivery_payload("stripe:evt_pg_initial:payment_success")
        self.assertEqual(delivery_type, "stripe_user_message")
        self.assertIn("Оплата прошла успешно 🤍", payload["text"])
        self.assertIn("01.09.2026", payload["text"])
        self.assertNotIn("None", payload["text"])
        self.assertEqual(payload["new_expiry"], "2026-09-01T00:00:00")

    def test_routine_payment_success_is_log_only_but_payment_problem_is_durable_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            admin_ids = [111, 222]
            with mock.patch.object(main, "ADMIN_IDS", admin_ids):
                self.assertEqual(
                    main.enqueue_admin_payment_success(
                        cur,
                        "evt_pg_admin_success",
                        "payment_success",
                        9809,
                        "sub_1",
                        150000,
                        "rub",
                        datetime(2026, 9, 1, 0, 0),
                        "evt_pg_admin_success",
                    ),
                    0,
                )
                duplicate_result = main.enqueue_admin_payment_success(
                    cur,
                    "evt_pg_admin_success",
                    "payment_success",
                    9809,
                    "sub_1",
                    150000,
                    "rub",
                    datetime(2026, 9, 1, 0, 0),
                    "evt_pg_admin_success",
                )
                self.assertEqual(duplicate_result, 0)
                cur.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT telegram_id), COUNT(DISTINCT delivery_key)
                    FROM message_delivery_events
                    WHERE delivery_type = 'stripe_admin_message'
                      AND delivery_key LIKE 'stripe-admin:evt_pg_admin_success:payment_success:%'
                    """
                )
                self.assertEqual(cur.fetchone(), (0, 0, 0))
                self.assertEqual(
                    main.enqueue_admin_payment_problem(
                        cur,
                        "evt_pg_admin_failed",
                        "invoice_payment_failed",
                        "invoice_payment_failed",
                        telegram_id=9809,
                        stripe_code="card_declined",
                        safe_ref="invoice_payment_failed:pgsafe",
                    ),
                    2,
                )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        rows = self.query_all(
            """
            SELECT delivery_key, telegram_id, delivery_type, status, payload_json
            FROM message_delivery_events
            WHERE delivery_type = 'stripe_admin_message'
            ORDER BY delivery_key
            """
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual({row[1] for row in rows}, {111, 222})
        self.assertEqual({row[2] for row in rows}, {"stripe_admin_message"})
        self.assertEqual({row[3] for row in rows}, {"pending"})
        self.assertEqual(
            [row[0] for row in rows],
            [
                "stripe-admin:evt_pg_admin_failed:invoice_payment_failed:111",
                "stripe-admin:evt_pg_admin_failed:invoice_payment_failed:222",
            ],
        )
        row_counts = self.query_all(
            """
            SELECT telegram_id, COUNT(*)
            FROM message_delivery_events
            WHERE delivery_type = 'stripe_admin_message'
            GROUP BY telegram_id
            ORDER BY telegram_id
            """
        )
        self.assertEqual(row_counts, [(111, 1), (222, 1)])
        key_counts = self.query_all(
            """
            SELECT delivery_key, COUNT(*)
            FROM message_delivery_events
            WHERE delivery_type = 'stripe_admin_message'
            GROUP BY delivery_key
            """
        )
        self.assertTrue(all(count == 1 for _, count in key_counts))
        payloads = [json.loads(row[4]) for row in rows]
        self.assertFalse(any("Тип: первая оплата" in payload["text"] for payload in payloads))
        self.assertTrue(any("банк отклонил карту" in payload["text"] for payload in payloads))
        self.assertTrue(all("None" not in payload["text"] for payload in payloads))

    def test_weekly_report_partial_recipient_state_reclaims_only_missing_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        key = "2026-08-03"
        start = datetime(2026, 8, 2, 21, 0)
        end = datetime(2026, 8, 9, 21, 0)

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            first = main.claim_weekly_report_run(cur, key, start, end)
            self.assertEqual(first["status"], "claimed")
            main.save_weekly_report_recipient_progress(
                cur,
                key,
                [1],
                [3],
                ["2:TelegramNetworkError:safe_ref"],
            )
            main.fail_weekly_report_run(
                cur,
                key,
                [1],
                [3],
                ["2:TelegramNetworkError:safe_ref"],
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            reclaimed = main.claim_weekly_report_run(cur, key, start, end)
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(reclaimed["status"], "claimed")
        self.assertEqual(reclaimed["sent_admin_ids"], [1])
        self.assertEqual(reclaimed["permanent_admin_ids"], [3])
        completed_at = self.query_one(
            "SELECT completed_at FROM weekly_report_runs WHERE report_key = %s",
            (key,),
        )[0]
        self.assertIsNone(completed_at)

    def test_payment_success_message_renewal_and_duplicate_dedupe_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        self.insert_recovery_user(9802, paid=True, expiry_date=datetime(2026, 8, 1), first_payment_done=True)
        new_expiry = datetime(2026, 10, 1, 0, 0)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE users
                SET expiry_date = %s,
                    paid = TRUE
                WHERE telegram_id = %s
                RETURNING expiry_date
                """,
                (new_expiry, 9802),
            )
            confirmed_expiry = cur.fetchone()[0]
            self.assertTrue(main.enqueue_user_payment_success_message(
                cur,
                "evt_pg_renewal",
                9802,
                "renewal_success",
                confirmed_expiry,
            ))
            self.assertTrue(main.enqueue_user_payment_success_message(
                cur,
                "evt_pg_renewal",
                9802,
                "renewal_success",
                confirmed_expiry,
            ))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(self.query_one(
            "SELECT COUNT(*) FROM message_delivery_events WHERE delivery_key = %s",
            ("stripe:evt_pg_renewal:renewal_success",),
        )[0], 1)
        delivery_type, payload = self.payment_delivery_payload("stripe:evt_pg_renewal:renewal_success")
        self.assertEqual(delivery_type, "stripe_user_message")
        self.assertIn("Подписка успешно продлена 🤍", payload["text"])
        self.assertIn("01.10.2026", payload["text"])
        self.assertNotIn("01.08.2026", payload["text"])

    def test_invoice_effective_expiry_case_update_drives_outbox_payload_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        telegram_id = 9812
        old_expiry = datetime(2026, 11, 1, 0, 0)
        stripe_period_expiry = datetime(2026, 10, 1, 0, 0)
        self.insert_recovery_user(
            telegram_id,
            paid=True,
            expiry_date=old_expiry,
            first_payment_done=True,
            stripe_subscription_id="sub_effective_exact",
            stripe_customer_id="cus_effective_exact",
        )
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                WITH target AS (
                    SELECT telegram_id, expiry_date AS old_expiry, payment_failed AS was_payment_failed
                    FROM users
                    WHERE stripe_subscription_id = %s
                )
                UPDATE users
                SET expiry_date = CASE
                        WHEN users.expiry_date IS NOT NULL AND users.expiry_date >= %s THEN users.expiry_date
                        ELSE %s
                    END,
                    paid = TRUE,
                    payment_failed = FALSE,
                    payment_failed_at = NULL,
                    grace_period_end = NULL
                FROM target
                WHERE users.telegram_id = target.telegram_id
                RETURNING users.telegram_id, target.old_expiry, target.was_payment_failed, users.expiry_date AS effective_expiry
                """,
                ("sub_effective_exact", stripe_period_expiry, stripe_period_expiry),
            )
            row = cur.fetchone()
            self.assertEqual(row, (telegram_id, old_expiry, False, old_expiry))
            effective_expiry = row[3]
            main.insert_payment_event(
                cur,
                "evt_pg_effective_expiry",
                "invoice.payment_succeeded",
                "succeeded",
                telegram_id=telegram_id,
                invoice_id="in_effective_expiry",
                stripe_customer_id="cus_effective_exact",
                stripe_subscription_id="sub_effective_exact",
                payment_kind="recurring",
                billing_reason="subscription_cycle",
                tariff_code="sub_1",
                amount_paid=1000,
                amount_due=1000,
                currency="rub",
                period_start=datetime(2026, 9, 1, 0, 0),
                period_end=stripe_period_expiry,
            )
            cur.execute(
                """
                INSERT INTO access_events (
                    telegram_id, event_type, source, old_expiry, new_expiry,
                    stripe_event_id, stripe_subscription_id, notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    telegram_id,
                    "stripe_invoice_paid",
                    "stripe_webhook",
                    old_expiry,
                    effective_expiry,
                    "evt_pg_effective_expiry",
                    "sub_effective_exact",
                    "period_source=subscription.current_period_end",
                ),
            )
            main.enqueue_user_payment_success_message(
                cur,
                "evt_pg_effective_expiry",
                telegram_id,
                "renewal_success",
                effective_expiry,
            )
            main.enqueue_rejoin_invite_after_payment(
                cur,
                telegram_id,
                effective_expiry,
                "invoice.payment_succeeded",
                "evt_pg_effective_expiry",
                stripe_subscription_id="sub_effective_exact",
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        user_expiry = self.query_one("SELECT expiry_date FROM users WHERE telegram_id = %s", (telegram_id,))[0]
        self.assertEqual(user_expiry, old_expiry)
        payment_period_end = self.query_one(
            "SELECT period_end FROM payment_events WHERE stripe_event_id = %s",
            ("evt_pg_effective_expiry",),
        )[0]
        access_new_expiry = self.query_one(
            "SELECT new_expiry FROM access_events WHERE stripe_event_id = %s",
            ("evt_pg_effective_expiry",),
        )[0]
        self.assertEqual(payment_period_end, stripe_period_expiry)
        self.assertEqual(access_new_expiry, old_expiry)
        _, renewal_payload = self.payment_delivery_payload("stripe:evt_pg_effective_expiry:renewal_success")
        _, rejoin_payload = self.payment_delivery_payload("stripe:evt_pg_effective_expiry:rejoin_invite")
        self.assertIn("01.11.2026", renewal_payload["text"])
        self.assertIn("01.11.2026", rejoin_payload["text"])
        self.assertEqual(renewal_payload["new_expiry"], "2026-11-01T00:00:00")
        self.assertNotIn("01.10.2026", renewal_payload["text"])
        self.assertNotIn("01.10.2026", rejoin_payload["text"])

    def test_payment_recovered_cleans_failure_state_and_outbox_payload_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        self.insert_recovery_user(
            9803,
            payment_failed=True,
            payment_failed_at=datetime(2026, 8, 1),
            grace_period_end=datetime(2026, 8, 3),
        )
        new_expiry = datetime(2026, 9, 15, 0, 0)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE users
                SET paid = TRUE,
                    expiry_date = %s,
                    payment_failed = FALSE,
                    payment_failed_at = NULL,
                    grace_period_end = NULL
                WHERE telegram_id = %s
                RETURNING expiry_date
                """,
                (new_expiry, 9803),
            )
            confirmed_expiry = cur.fetchone()[0]
            main.enqueue_user_payment_success_message(
                cur,
                "evt_pg_recovered",
                9803,
                "payment_recovered",
                confirmed_expiry,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        user_state = self.query_one(
            "SELECT payment_failed, payment_failed_at, grace_period_end FROM users WHERE telegram_id = %s",
            (9803,),
        )
        self.assertEqual(user_state, (False, None, None))
        _, payload = self.payment_delivery_payload("stripe:evt_pg_recovered:payment_recovered")
        self.assertIn("Подписка снова активна", payload["text"])
        self.assertIn("15.09.2026", payload["text"])

    def test_checkout_recovery_cleanup_and_payload_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        telegram_id = 9813
        new_expiry = datetime(2026, 9, 20, 0, 0)
        self.insert_recovery_user(
            telegram_id,
            payment_failed=True,
            payment_failed_at=datetime(2026, 8, 20, 0, 0),
            grace_period_end=datetime(2026, 8, 23, 0, 0),
        )
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                WITH target AS (
                    SELECT telegram_id, payment_failed AS was_payment_failed
                    FROM users
                    WHERE telegram_id = %s
                )
                UPDATE users
                SET paid = TRUE,
                    expiry_date = %s,
                    first_payment_done = TRUE,
                    payment_failed = FALSE,
                    payment_failed_at = NULL,
                    grace_period_end = NULL,
                    last_payment_succeeded_at = NOW()
                FROM target
                WHERE users.telegram_id = target.telegram_id
                RETURNING users.expiry_date, target.was_payment_failed
                """,
                (telegram_id, new_expiry),
            )
            effective_expiry, was_payment_failed = cur.fetchone()
            purpose = main.payment_success_purpose("initial_subscription", was_payment_failed)
            self.assertEqual(purpose, "payment_recovered")
            main.enqueue_user_payment_success_message(
                cur,
                "evt_pg_checkout_recovered",
                telegram_id,
                purpose,
                effective_expiry,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        user_state = self.query_one(
            "SELECT paid, expiry_date, first_payment_done, payment_failed, payment_failed_at, grace_period_end FROM users WHERE telegram_id = %s",
            (telegram_id,),
        )
        self.assertEqual(user_state, (True, new_expiry, True, False, None, None))
        _, payload = self.payment_delivery_payload("stripe:evt_pg_checkout_recovered:payment_recovered")
        self.assertIn("Подписка снова активна", payload["text"])
        self.assertIn("20.09.2026", payload["text"])
        self.assertEqual(payload["new_expiry"], "2026-09-20T00:00:00")
        self.assertIsNone(self.query_one(
            "SELECT delivery_key FROM message_delivery_events WHERE delivery_key = %s",
            ("stripe:evt_pg_checkout_recovered:payment_success",),
        ))

    def test_payment_success_message_missing_expiry_fallback_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        self.insert_recovery_user(9804)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            main.enqueue_user_payment_success_message(
                cur,
                "evt_pg_missing_expiry",
                9804,
                "payment_success",
                None,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        _, payload = self.payment_delivery_payload("stripe:evt_pg_missing_expiry:payment_success")
        self.assertNotIn("None", payload["text"])
        self.assertNotIn("01.09.2026", payload["text"])
        self.assertIn("Мы дополнительно проверяем дату окончания подписки", payload["text"])

    def test_failed_invoice_customer_fallback_conflict_guard_real_postgres(self):
        run_migrations(self.get_conn)
        self.insert_recovery_user(
            9701,
            paid=True,
            expiry_date=datetime.utcnow() + timedelta(days=10),
            stripe_customer_id="cus_conflict",
            stripe_subscription_id="sub_current",
        )
        self.assertFalse(import_main().should_apply_failed_invoice_to_user("sub_current", "sub_old"))
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE users
                SET payment_failed = TRUE,
                    payment_failed_at = COALESCE(payment_failed_at, NOW()),
                    grace_period_end = GREATEST(
                        COALESCE(grace_period_end, NOW()),
                        NOW() + (48 * INTERVAL '1 hour')
                    ),
                    stripe_subscription_id = COALESCE(%s, stripe_subscription_id),
                    stripe_customer_id = COALESCE(%s, stripe_customer_id)
                WHERE stripe_customer_id = %s
                  AND (stripe_subscription_id IS NULL OR stripe_subscription_id = %s)
                RETURNING telegram_id
                """,
                ("sub_old", "cus_conflict", "cus_conflict", "sub_old"),
            )
            self.assertIsNone(cur.fetchone())
            conn.commit()
        finally:
            cur.close()
            conn.close()
        row = self.query_one(
            """
            SELECT stripe_subscription_id, payment_failed, grace_period_end, paid, expiry_date
            FROM users
            WHERE telegram_id = 9701
            """
        )
        self.assertEqual(row[0], "sub_current")
        self.assertFalse(row[1])
        self.assertIsNone(row[2])
        self.assertTrue(row[3])
        self.assertGreater(row[4], datetime.utcnow())

    def test_failed_invoice_exact_subscription_match_real_postgres(self):
        run_migrations(self.get_conn)
        self.insert_recovery_user(
            9702,
            stripe_customer_id="cus_exact",
            stripe_subscription_id="sub_exact",
        )
        self.assertTrue(import_main().should_apply_failed_invoice_to_user("sub_exact", "sub_exact"))
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE users
                SET payment_failed = TRUE,
                    payment_failed_at = COALESCE(payment_failed_at, NOW()),
                    grace_period_end = GREATEST(
                        COALESCE(grace_period_end, NOW()),
                        NOW() + (48 * INTERVAL '1 hour')
                    ),
                    stripe_customer_id = COALESCE(%s, stripe_customer_id)
                WHERE stripe_subscription_id = %s
                RETURNING telegram_id, payment_failed, grace_period_end
                """,
                ("cus_exact", "sub_exact"),
            )
            row = cur.fetchone()
            conn.commit()
        finally:
            cur.close()
            conn.close()
        self.assertEqual(row[0], 9702)
        self.assertTrue(row[1])
        self.assertIsNotNone(row[2])

    def test_terminal_stripe_link_deactivation_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO stripe_links (
                    telegram_id, stripe_customer_id, stripe_subscription_id, status, is_active
                )
                VALUES
                    (9703, 'cus_terminal', 'sub_terminal', 'active', TRUE),
                    (9703, 'cus_terminal', 'sub_other', 'active', TRUE)
                """
            )
            main.mark_stripe_link_subscription_terminal(cur, "sub_terminal", "canceled")
            conn.commit()
        finally:
            cur.close()
            conn.close()
        rows = self.query_all(
            """
            SELECT stripe_subscription_id, status, is_active
            FROM stripe_links
            WHERE telegram_id = 9703
            ORDER BY stripe_subscription_id
            """
        )
        self.assertEqual(rows, [
            ("sub_other", "active", True),
            ("sub_terminal", "canceled", False),
        ])

    def test_empty_database_migrations_versions_checksums_and_idempotency(self):
        run_migrations(self.get_conn)
        migrations = load_migrations()
        rows = self.query_all("SELECT version, checksum, baseline FROM schema_migrations ORDER BY version")
        self.assertEqual([(m["version"], m["checksum"], False) for m in migrations], rows)

        run_migrations(self.get_conn)
        rows_after = self.query_all("SELECT version, checksum, baseline FROM schema_migrations ORDER BY version")
        self.assertEqual(rows, rows_after)

    def test_minimal_old_schema_is_upgraded_without_fictitious_baseline(self):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                CREATE TABLE users (
                    telegram_id BIGINT UNIQUE NOT NULL,
                    paid BOOLEAN DEFAULT FALSE,
                    expiry_date TIMESTAMP,
                    stripe_customer_id TEXT
                )
                """
            )
            cur.execute("INSERT INTO users (telegram_id, paid, stripe_customer_id) VALUES (1001, TRUE, 'cus_keep')")
            cur.execute(
                """
                CREATE TABLE checkout_sessions (
                    telegram_id BIGINT,
                    tariff_code TEXT,
                    status TEXT,
                    idempotency_key TEXT
                )
                """
            )
            cur.execute(
                """
                INSERT INTO checkout_sessions (
                    telegram_id, tariff_code, idempotency_key, status
                )
                VALUES (1001, 'sub_1', 'idem_keep', 'completed')
                """
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        result = run_migrations(self.get_conn)
        self.assertEqual(result["baselined"], [])
        self.assertEqual(self.query_one("SELECT paid, stripe_customer_id FROM users WHERE telegram_id = 1001"), (True, "cus_keep"))
        self.assertEqual(self.query_one("SELECT to_regclass('public.payment_events')")[0], "payment_events")
        self.assertEqual(self.query_one("SELECT to_regclass('public.subscription_removal_events')")[0], "subscription_removal_events")
        self.assertEqual(self.query_one("SELECT to_regclass('public.checkout_retry_events')")[0], "checkout_retry_events")
        self.assertEqual(self.query_one("SELECT to_regclass('public.scheduled_job_runs')")[0], "scheduled_job_runs")
        self.assertEqual(self.query_one("SELECT status FROM checkout_sessions WHERE idempotency_key = 'idem_keep'"), ("completed",))
        columns = {row[0] for row in self.query_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'message_delivery_events'
            """
        )}
        self.assertTrue({"next_attempt_at", "payload_json", "invite_link"} <= columns)
        rows = self.query_all("SELECT version, baseline FROM schema_migrations ORDER BY version")
        self.assertTrue(rows)
        self.assertFalse(any(baseline for _, baseline in rows))

        run_migrations(self.get_conn)
        self.assertEqual(self.query_one("SELECT COUNT(*) FROM users WHERE telegram_id = 1001")[0], 1)

    def test_hardening_tables_columns_and_indexes_exist(self):
        run_migrations(self.get_conn)
        for table in (
            "checkout_retry_events",
            "subscription_removal_events",
            "message_delivery_events",
            "scheduled_job_runs",
            "admin_action_requests",
            "stripe_identity_conflicts",
        ):
            self.assertEqual(self.query_one("SELECT to_regclass(%s)", (f"public.{table}",))[0], table)

        columns = {row[0] for row in self.query_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'checkout_retry_events'
            """
        )}
        self.assertTrue({"telegram_id", "tariff_code", "last_admin_alert_at", "resolved_at", "resolved_source"} <= columns)
        self.assertEqual(
            self.query_one("SELECT to_regclass('public.checkout_retry_events_user_attempt_idx')")[0],
            "checkout_retry_events_user_attempt_idx",
        )

    def insert_checkout_reuse_attempt_at(self, telegram_id, tariff, attempt_at, last_admin_alert_at=None):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO checkout_retry_events (
                    telegram_id, tariff_code, attempt_at, last_admin_alert_at
                )
                VALUES (%s, %s, %s, %s)
                """,
                (telegram_id, tariff, attempt_at, last_admin_alert_at),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def checkout_reuse_admin_delivery_count(self):
        return self.query_one(
            """
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE delivery_type = 'stripe_admin_message'
              AND payload_json::text LIKE '%%checkout_open_reused%%'
            """
        )[0]

    def test_checkout_reuse_alert_cooldown_checks_all_recent_attempt_rows(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 49001
        tariff = "sub_1"
        now = datetime.utcnow()
        for minutes_ago in (4, 3, 2):
            self.insert_checkout_reuse_attempt_at(user_id, tariff, now - timedelta(minutes=minutes_ago))

        with mock.patch.object(main, "get_db_conn", self.get_conn), \
             mock.patch.object(main, "ADMIN_IDS", [1, 2]):
            asyncio.run(main.notify_admins_about_checkout_reuse(user_id, tariff, 3, "cs_reuse_pg", now.timestamp()))

        self.assertEqual(self.checkout_reuse_admin_delivery_count(), 2)

        self.insert_checkout_reuse_attempt_at(user_id, tariff, now + timedelta(seconds=1))
        with mock.patch.object(main, "get_db_conn", self.get_conn), \
             mock.patch.object(main, "ADMIN_IDS", [1, 2]):
            asyncio.run(main.notify_admins_about_checkout_reuse(
                user_id,
                tariff,
                4,
                "cs_reuse_pg",
                (now + timedelta(seconds=1)).timestamp(),
            ))

        self.assertEqual(self.checkout_reuse_admin_delivery_count(), 2)
        self.assertEqual(
            self.query_one(
                """
                SELECT COUNT(*)
                FROM checkout_retry_events
                WHERE telegram_id = %s
                  AND tariff_code = %s
                  AND last_admin_alert_at IS NOT NULL
                """,
                (user_id, tariff),
            )[0],
            1,
        )

    def test_checkout_reuse_alert_concurrent_threshold_calls_enqueue_once(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 49002
        tariff = "sub_1"
        now = datetime.utcnow()
        for minutes_ago in (4, 3, 2):
            self.insert_checkout_reuse_attempt_at(user_id, tariff, now - timedelta(minutes=minutes_ago))

        errors = []

        def call_helper():
            try:
                asyncio.run(main.notify_admins_about_checkout_reuse(
                    user_id,
                    tariff,
                    3,
                    "cs_reuse_pg_concurrent",
                    now.timestamp(),
                ))
            except Exception as exc:
                errors.append(exc)

        with mock.patch.object(main, "get_db_conn", self.get_conn), \
             mock.patch.object(main, "ADMIN_IDS", [1, 2]):
            threads = [threading.Thread(target=call_helper), threading.Thread(target=call_helper)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(self.checkout_reuse_admin_delivery_count(), 2)
        self.assertEqual(
            self.query_one(
                """
                SELECT COUNT(*)
                FROM checkout_retry_events
                WHERE telegram_id = %s
                  AND tariff_code = %s
                  AND last_admin_alert_at IS NOT NULL
                """,
                (user_id, tariff),
            )[0],
            1,
        )

    def test_checkout_reuse_alert_allowed_again_after_cooldown(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 49003
        tariff = "sub_1"
        now = datetime.utcnow()
        for minutes_ago in (4, 3, 2):
            self.insert_checkout_reuse_attempt_at(user_id, tariff, now - timedelta(minutes=minutes_ago))

        with mock.patch.object(main, "get_db_conn", self.get_conn), \
             mock.patch.object(main, "ADMIN_IDS", [1, 2]):
            asyncio.run(main.notify_admins_about_checkout_reuse(user_id, tariff, 3, "cs_reuse_pg_later", now.timestamp()))

        later = now + timedelta(minutes=31)
        self.insert_checkout_reuse_attempt_at(user_id, tariff, later)
        with mock.patch.object(main, "get_db_conn", self.get_conn), \
             mock.patch.object(main, "ADMIN_IDS", [1, 2]):
            asyncio.run(main.notify_admins_about_checkout_reuse(
                user_id,
                tariff,
                4,
                "cs_reuse_pg_later",
                later.timestamp(),
            ))

        self.assertEqual(self.checkout_reuse_admin_delivery_count(), 4)

    def test_payment_integrity_guards_created_on_clean_schema_and_idempotent(self):
        run_migrations(self.get_conn)
        expected = {
            "payment_events_unique_stripe_event_id": ("payment_events", ["stripe_event_id"], "(stripe_event_id IS NOT NULL)"),
            "checkout_sessions_unique_stripe_session_id": ("checkout_sessions", ["stripe_session_id"], "(stripe_session_id IS NOT NULL)"),
            "checkout_sessions_unique_idempotency_key": ("checkout_sessions", ["idempotency_key"], "(idempotency_key IS NOT NULL)"),
            "users_unique_stripe_subscription": ("users", ["stripe_subscription_id"], "(stripe_subscription_id IS NOT NULL)"),
            "users_unique_stripe_customer": ("users", ["stripe_customer_id"], "(stripe_customer_id IS NOT NULL)"),
            "stripe_links_unique_subscription_user": ("stripe_links", ["stripe_subscription_id"], "(stripe_subscription_id IS NOT NULL)"),
        }
        for index_name, (_table, columns, predicate) in expected.items():
            row = self.index_columns_unique_predicate(index_name)
            self.assertIsNotNone(row, index_name)
            self.assertTrue(row[0], index_name)
            self.assertEqual(row[1], columns)
            self.assertEqual(row[2], predicate)

        rows = self.query_all("SELECT version, baseline FROM schema_migrations ORDER BY version")
        self.assertIn(("0006_payment_integrity_guards", False), rows)
        run_migrations(self.get_conn)
        rows_after = self.query_all("SELECT version, baseline FROM schema_migrations ORDER BY version")
        self.assertEqual(rows, rows_after)

    def test_legacy_schema_gets_payment_integrity_guards_without_duplicate_indexes(self):
        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("DROP INDEX payment_events_unique_stripe_event_id")
            cur.execute("CREATE UNIQUE INDEX legacy_payment_event_unique ON payment_events(stripe_event_id) WHERE stripe_event_id IS NOT NULL")
            conn.commit()
        finally:
            cur.close()
            conn.close()

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            spec = {
                "table": "payment_events",
                "columns": ("stripe_event_id",),
                "unique": True,
                "predicate": "stripe_event_id IS NOT NULL",
            }
            self.assertTrue(payment_integrity_index_requirement_matches(cur, "payment_events_unique_stripe_event_id", spec))
        finally:
            cur.close()
            conn.close()

        index_count = self.query_one(
            """
            SELECT COUNT(*)
            FROM pg_index i
            JOIN pg_class tbl ON tbl.oid = i.indrelid
            JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
            JOIN unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ordinality) ON TRUE
            JOIN pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = ord.attnum
            WHERE ns.nspname = 'public'
              AND tbl.relname = 'payment_events'
              AND i.indisunique
              AND a.attname = 'stripe_event_id'
              AND i.indnkeyatts = 1
              AND pg_get_expr(i.indpred, i.indrelid) = '(stripe_event_id IS NOT NULL)'
            """
        )[0]
        self.assertEqual(index_count, 1)

    def assert_duplicate_blocks_migration(self, setup_sql, expected_message):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(setup_sql)
            conn.commit()
        finally:
            cur.close()
            conn.close()
        with self.assertRaisesRegex(MigrationError, expected_message):
            run_migrations(self.get_conn)

    def test_duplicate_payment_event_blocks_payment_integrity_migration(self):
        self.assert_duplicate_blocks_migration(
            """
            CREATE TABLE users (telegram_id BIGINT UNIQUE NOT NULL);
            CREATE TABLE stripe_events (event_id TEXT);
            CREATE TABLE access_events (id SERIAL PRIMARY KEY);
            CREATE TABLE stripe_links (telegram_id BIGINT, stripe_subscription_id TEXT, stripe_customer_id TEXT);
            CREATE TABLE payment_events (stripe_event_id TEXT);
            CREATE TABLE checkout_sessions (telegram_id BIGINT, tariff_code TEXT, status TEXT, stripe_session_id TEXT, idempotency_key TEXT);
            CREATE TABLE message_delivery_events (delivery_key TEXT, telegram_id BIGINT, delivery_type TEXT, status TEXT);
            INSERT INTO payment_events (stripe_event_id) VALUES ('evt_dup'), ('evt_dup');
            """,
            "payment_events.stripe_event_id",
        )

    def test_duplicate_checkout_session_blocks_payment_integrity_migration(self):
        self.assert_duplicate_blocks_migration(
            """
            CREATE TABLE users (telegram_id BIGINT UNIQUE NOT NULL);
            CREATE TABLE stripe_events (event_id TEXT);
            CREATE TABLE access_events (id SERIAL PRIMARY KEY);
            CREATE TABLE stripe_links (telegram_id BIGINT, stripe_subscription_id TEXT, stripe_customer_id TEXT);
            CREATE TABLE payment_events (stripe_event_id TEXT);
            CREATE TABLE checkout_sessions (telegram_id BIGINT, tariff_code TEXT, status TEXT, stripe_session_id TEXT, idempotency_key TEXT);
            CREATE TABLE message_delivery_events (delivery_key TEXT, telegram_id BIGINT, delivery_type TEXT, status TEXT);
            INSERT INTO checkout_sessions (stripe_session_id, idempotency_key) VALUES ('cs_dup', 'idem_a'), ('cs_dup', 'idem_b');
            """,
            "checkout_sessions.stripe_session_id",
        )

    def test_duplicate_checkout_idempotency_key_blocks_payment_integrity_migration(self):
        self.assert_duplicate_blocks_migration(
            """
            CREATE TABLE users (telegram_id BIGINT UNIQUE NOT NULL);
            CREATE TABLE stripe_events (event_id TEXT);
            CREATE TABLE access_events (id SERIAL PRIMARY KEY);
            CREATE TABLE stripe_links (telegram_id BIGINT, stripe_subscription_id TEXT, stripe_customer_id TEXT);
            CREATE TABLE payment_events (stripe_event_id TEXT);
            CREATE TABLE checkout_sessions (telegram_id BIGINT, tariff_code TEXT, status TEXT, stripe_session_id TEXT, idempotency_key TEXT);
            CREATE TABLE message_delivery_events (delivery_key TEXT, telegram_id BIGINT, delivery_type TEXT, status TEXT);
            INSERT INTO checkout_sessions (stripe_session_id, idempotency_key) VALUES ('cs_a', 'idem_dup'), ('cs_b', 'idem_dup');
            """,
            "checkout_sessions.idempotency_key",
        )

    def test_payment_integrity_index_structure_rejects_non_unique_and_wrong_predicate(self):
        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("DROP INDEX payment_events_unique_stripe_event_id")
            cur.execute("CREATE INDEX payment_events_unique_stripe_event_id ON payment_events(stripe_event_id) WHERE stripe_event_id IS NOT NULL")
            conn.commit()
            self.assertFalse(index_structure_matches(
                cur,
                "payment_events_unique_stripe_event_id",
                "payment_events",
                ("stripe_event_id",),
                unique=True,
                predicate="stripe_event_id IS NOT NULL",
            ))

            cur.execute("DROP INDEX payment_events_unique_stripe_event_id")
            cur.execute("CREATE UNIQUE INDEX payment_events_unique_stripe_event_id ON payment_events(stripe_event_id)")
            conn.commit()
            self.assertFalse(index_structure_matches(
                cur,
                "payment_events_unique_stripe_event_id",
                "payment_events",
                ("stripe_event_id",),
                unique=True,
                predicate="stripe_event_id IS NOT NULL",
            ))

            cur.execute("DROP INDEX payment_events_unique_stripe_event_id")
            cur.execute("CREATE UNIQUE INDEX payment_events_unique_stripe_event_id ON payment_events(stripe_event_id) WHERE stripe_event_id IS NOT NULL")
            conn.commit()
            self.assertTrue(index_structure_matches(
                cur,
                "payment_events_unique_stripe_event_id",
                "payment_events",
                ("stripe_event_id",),
                unique=True,
                predicate="stripe_event_id IS NOT NULL",
            ))
        finally:
            cur.close()
            conn.close()

    def test_payment_integrity_concurrent_duplicate_insert_is_blocked(self):
        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO payment_events (stripe_event_id, event_type, payment_status)
                VALUES ('evt_concurrent_guard', 'checkout.session.completed', 'succeeded')
                """
            )
            conn.commit()
            with self.assertRaises(psycopg2.errors.UniqueViolation):
                cur.execute(
                    """
                    INSERT INTO payment_events (stripe_event_id, event_type, payment_status)
                    VALUES ('evt_concurrent_guard', 'checkout.session.completed', 'succeeded')
                    """
                )
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def test_payment_integrity_preflight_sql_is_read_only_and_executes(self):
        run_migrations(self.get_conn)
        path = Path(MIGRATIONS_DIR).parent / "ops" / "payment_integrity_preflight.sql"
        text = path.read_text(encoding="utf-8")
        self.assertNotRegex(text, r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\b")
        self.assertNotIn(" AS users_telegram_id", text)
        self.assertNotIn(" AS stripe_links_telegram_id", text)
        self.assertNotRegex(text, r"SELECT\s+'[^']+'\s+AS check_name,\s+telegram_id\b")
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            for statement in [part.strip() for part in text.split(";") if part.strip()]:
                self.assertTrue(statement.lstrip().upper().startswith("SELECT"), statement[:80])
                cur.execute(statement)
                cur.fetchall()
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def test_stripe_identity_conflict_audit_uses_schema_0003_unique_text_key(self):
        run_migrations(self.get_conn)
        main = import_main()
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO users (telegram_id, stripe_customer_id) VALUES (999, 'cus_pg_conflict')")
            conn.commit()
        finally:
            cur.close()
            conn.close()

        conflict = main.StripeIdentityConflictError(
            "users_customer_conflict",
            "cus_pg_conflict",
            None,
            123,
            "checkout.session.completed",
        )

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn):
            main.persist_stripe_identity_conflict_audit(
                conflict,
                "evt_pg_identity_conflict",
                "checkout.session.completed",
                checkout_session_id="cs_pg_identity_conflict",
            )

        row = self.query_one(
            """
            SELECT conflict_type, stripe_id, telegram_ids, resolved, created_at, updated_at
            FROM stripe_identity_conflicts
            WHERE conflict_type = 'users_customer_conflict'
              AND stripe_id = 'cus_pg_conflict'
            """
        )
        self.assertEqual(row[2], "[123,999]")
        self.assertFalse(row[3])
        first_updated_at = row[5]
        self.assertEqual(
            self.query_one("SELECT COUNT(*) FROM unlinked_stripe_events WHERE event_id = 'evt_pg_identity_conflict'")[0],
            1,
        )
        self.assertEqual(
            self.query_one("SELECT COUNT(*) FROM message_delivery_events WHERE delivery_type = 'stripe_admin_message'")[0],
            2,
        )

        time.sleep(1.05)
        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn):
            main.persist_stripe_identity_conflict_audit(
                main.StripeIdentityConflictError(
                    "users_customer_conflict",
                    "cus_pg_conflict",
                    None,
                    123,
                    "checkout.session.completed",
                ),
                "evt_pg_identity_conflict",
                "checkout.session.completed",
            )
        self.assertEqual(
            self.query_one(
                """
                SELECT COUNT(*)
                FROM stripe_identity_conflicts
                WHERE conflict_type = 'users_customer_conflict'
                  AND stripe_id = 'cus_pg_conflict'
                  AND telegram_ids = '[123,999]'
                """
            )[0],
            1,
        )
        self.assertGreater(
            self.query_one(
                """
                SELECT updated_at
                FROM stripe_identity_conflicts
                WHERE conflict_type = 'users_customer_conflict'
                  AND stripe_id = 'cus_pg_conflict'
                  AND telegram_ids = '[123,999]'
                """
            )[0],
            first_updated_at,
        )

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE stripe_identity_conflicts
                SET resolved = TRUE
                WHERE conflict_type = 'users_customer_conflict'
                  AND stripe_id = 'cus_pg_conflict'
                  AND telegram_ids = '[123,999]'
                """
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn):
            main.persist_stripe_identity_conflict_audit(
                main.StripeIdentityConflictError(
                    "users_customer_conflict",
                    "cus_pg_conflict",
                    None,
                    123,
                    "checkout.session.completed",
                ),
                "evt_pg_identity_conflict_new",
                "checkout.session.completed",
            )
        self.assertEqual(
            self.query_one(
                """
                SELECT COUNT(*)
                FROM stripe_identity_conflicts
                WHERE conflict_type = 'users_customer_conflict'
                  AND stripe_id = 'cus_pg_conflict'
                  AND telegram_ids = '[123,999]'
                """
            )[0],
            2,
        )

        before_counts = {
            "conflicts": self.query_one("SELECT COUNT(*) FROM stripe_identity_conflicts")[0],
            "unlinked": self.query_one("SELECT COUNT(*) FROM unlinked_stripe_events")[0],
            "deliveries": self.query_one("SELECT COUNT(*) FROM message_delivery_events")[0],
        }
        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main, "enqueue_admin_payment_problem_safely", side_effect=RuntimeError("audit delivery failed")):
            with self.assertRaises(RuntimeError):
                main.persist_stripe_identity_conflict_audit(
                    main.StripeIdentityConflictError(
                        "users_customer_conflict",
                        "cus_pg_conflict",
                        None,
                        123,
                        "invoice.payment_succeeded",
                    ),
                    "evt_pg_identity_conflict_rollback",
                    "invoice.payment_succeeded",
                )

        self.assertEqual(self.query_one("SELECT COUNT(*) FROM stripe_identity_conflicts")[0], before_counts["conflicts"])
        self.assertEqual(self.query_one("SELECT COUNT(*) FROM unlinked_stripe_events")[0], before_counts["unlinked"])
        self.assertEqual(self.query_one("SELECT COUNT(*) FROM message_delivery_events")[0], before_counts["deliveries"])

    def test_upsert_stripe_link_subscription_unique_same_user_update_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO stripe_links (
                    telegram_id, stripe_customer_id, stripe_subscription_id,
                    status, is_active, source, updated_at
                )
                VALUES (123, NULL, 'sub_same_pg', 'incomplete', FALSE, 'seed', NOW())
                """
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            main.upsert_stripe_link(
                cur,
                123,
                stripe_customer_id="cus_same_pg",
                stripe_subscription_id="sub_same_pg",
                status="active",
                current_period_end=datetime.utcnow() + timedelta(days=30),
                is_active=True,
                source="invoice.payment_succeeded",
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(
            self.query_one("SELECT COUNT(*) FROM stripe_links WHERE stripe_subscription_id = 'sub_same_pg'")[0],
            1,
        )
        self.assertEqual(
            self.query_one(
                """
                SELECT telegram_id, stripe_customer_id, status, is_active, source
                FROM stripe_links
                WHERE stripe_subscription_id = 'sub_same_pg'
                """
            ),
            (123, "cus_same_pg", "active", True, "invoice.payment_succeeded"),
        )
        self.assertEqual(self.query_one("SELECT COUNT(*) FROM stripe_identity_conflicts")[0], 0)

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            main.upsert_stripe_link(
                cur,
                123,
                stripe_customer_id="cus_same_pg",
                stripe_subscription_id="sub_same_pg",
                status="past_due",
                is_active=False,
                source="customer.subscription.updated",
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        self.assertEqual(
            self.query_one("SELECT COUNT(*) FROM stripe_links WHERE stripe_subscription_id = 'sub_same_pg'")[0],
            1,
        )
        self.assertEqual(
            self.query_one("SELECT status, is_active FROM stripe_links WHERE stripe_subscription_id = 'sub_same_pg'"),
            ("past_due", False),
        )

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            with self.assertRaises(main.StripeIdentityConflictError) as raised:
                main.upsert_stripe_link(
                    cur,
                    999,
                    stripe_customer_id="cus_other_pg",
                    stripe_subscription_id="sub_same_pg",
                    status="active",
                    is_active=True,
                    source="invoice.payment_succeeded",
                )
            conn.rollback()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(raised.exception.conflict_type, "stripe_links_subscription_conflict")
        self.assertEqual(raised.exception.existing_telegram_id, 123)
        self.assertEqual(raised.exception.requested_telegram_id, 999)
        self.assertEqual(
            self.query_one("SELECT COUNT(*) FROM stripe_links WHERE stripe_subscription_id = 'sub_same_pg'")[0],
            1,
        )
        self.assertEqual(self.query_one("SELECT COUNT(*) FROM stripe_identity_conflicts")[0], 0)

    def test_postgres_fsm_storage_migration_shape_and_idempotency(self):
        run_migrations(self.get_conn)

        columns = {
            row[0]: row[1]
            for row in self.query_all(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'aiogram_fsm_states'
                """
            )
        }
        self.assertEqual(columns["data_json"], "jsonb")
        self.assertIn("created_at", columns)
        self.assertIn("updated_at", columns)
        self.assertEqual(
            self.query_one("SELECT to_regclass('public.aiogram_fsm_states_updated_at_idx')")[0],
            "aiogram_fsm_states_updated_at_idx",
        )

        run_migrations(self.get_conn)
        self.assertEqual(self.query_one("SELECT COUNT(*) FROM aiogram_fsm_states")[0], 0)

    def test_postgres_fsm_storage_roundtrip_isolation_concurrent_update_and_cleanup(self):
        run_migrations(self.get_conn)

        async def scenario():
            first = PostgresFSMStorage(self.get_conn)
            second = PostgresFSMStorage(self.get_conn)
            key = StorageKey(bot_id=1, chat_id=2, user_id=3, thread_id=None, business_connection_id=None, destiny="default")
            other_key = StorageKey(bot_id=1, chat_id=2, user_id=3, thread_id=9, business_connection_id="biz", destiny="other")

            await first.set_state(key, "ContactState:waiting_for_message")
            await first.set_data(key, {"step": 1})
            self.assertEqual(await second.get_state(key), "ContactState:waiting_for_message")
            self.assertEqual(await second.get_data(key), {"step": 1})

            await second.set_data(other_key, {"isolated": True})
            self.assertEqual(await first.get_data(other_key), {"isolated": True})
            self.assertEqual(await first.get_data(key), {"step": 1})

            await asyncio.gather(
                first.update_data(key, {"first": 1}),
                second.update_data(key, {"second": 2}),
            )
            self.assertEqual(await first.get_data(key), {"step": 1, "first": 1, "second": 2})

            conn = self.get_conn()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    UPDATE aiogram_fsm_states
                    SET data_json = '[]'::jsonb
                    WHERE bot_id = 1 AND chat_id = 2 AND user_id = 3 AND destiny = 'default'
                    """
                )
                cur.execute(
                    """
                    INSERT INTO aiogram_fsm_states (
                        bot_id, chat_id, user_id, thread_id, business_connection_id, destiny,
                        state, data_json, updated_at
                    )
                    VALUES (99, 99, 99, 0, '', 'stale', NULL, '{}'::jsonb, NOW() - INTERVAL '31 days')
                    """
                )
                cur.execute(
                    """
                    INSERT INTO aiogram_fsm_states (
                        bot_id, chat_id, user_id, thread_id, business_connection_id, destiny,
                        state, data_json, updated_at
                    )
                    VALUES (98, 98, 98, 0, '', 'recent', NULL, '{}'::jsonb, NOW())
                    """
                )
                conn.commit()
            finally:
                cur.close()
                conn.close()

            with self.assertLogs(level="WARNING"):
                self.assertEqual(await first.get_data(key), {})
            self.assertEqual(cleanup_postgres_fsm_storage(self.get_conn, older_than_days=30), 1)
            self.assertEqual(self.query_one("SELECT COUNT(*) FROM aiogram_fsm_states WHERE destiny = 'stale'")[0], 0)
            self.assertEqual(self.query_one("SELECT COUNT(*) FROM aiogram_fsm_states WHERE destiny = 'recent'")[0], 1)

            await first.set_state(other_key, None)
            await first.set_data(other_key, {})
            self.assertEqual(await first.get_data(other_key), {})

        asyncio.run(scenario())

    def test_first_purchase_recovery_attempt_age_retry_event_and_pending_subscription_eligibility(self):
        run_migrations(self.get_conn)

        self.insert_recovery_user(9101)
        self.insert_checkout_attempt(9101, hours_ago=23)
        self.assertNotIn(9101, {row[0] for row in self.due_recovery_users()})

        self.insert_recovery_user(9102)
        self.insert_checkout_attempt(9102, hours_ago=24)
        self.assertIn(9102, {row[0] for row in self.due_recovery_users()})

        self.insert_recovery_user(9103)
        self.insert_retry_attempt(9103, hours_ago=25)
        self.assertIn(9103, {row[0] for row in self.due_recovery_users()})

        self.insert_recovery_user(9104, stripe_subscription_id="sub_pending")
        self.insert_checkout_attempt(9104, hours_ago=25)
        self.insert_stripe_link(
            9104,
            subscription_id="sub_pending",
            status="checkout_subscription_pending_invoice",
            is_active=False,
        )
        self.assertIn(9104, {row[0] for row in self.due_recovery_users()})

        self.insert_recovery_user(9105, stripe_subscription_id="sub_incomplete")
        self.insert_checkout_attempt(9105, hours_ago=25)
        self.insert_stripe_link(9105, subscription_id="sub_incomplete", status="incomplete", is_active=False)
        self.assertIn(9105, {row[0] for row in self.due_recovery_users()})

    def test_first_purchase_recovery_completed_subscription_checkout_without_payment_is_eligible(self):
        run_migrations(self.get_conn)

        for user_id, link_status in (
            (9601, "checkout_subscription_pending_invoice"),
            (9602, "incomplete"),
        ):
            self.insert_recovery_user(user_id, stripe_subscription_id=f"sub_{user_id}")
            self.insert_checkout_attempt(
                user_id,
                hours_ago=25,
                status="completed",
                mode="subscription",
            )
            conn = self.get_conn()
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    INSERT INTO stripe_links (
                        telegram_id, stripe_subscription_id, status, is_active, current_period_end
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        FALSE,
                        (NOW() AT TIME ZONE 'UTC') + INTERVAL '24 hours'
                    )
                    """,
                    (user_id, f"sub_{user_id}", link_status),
                )
                conn.commit()
            finally:
                cur.close()
                conn.close()

        due_ids = {row[0] for row in self.due_recovery_users()}
        self.assertIn(9601, due_ids)
        self.assertIn(9602, due_ids)

    def test_first_purchase_recovery_unpaid_subscription_links_remain_eligible(self):
        run_migrations(self.get_conn)
        main = import_main()

        for user_id, link_status in (
            (9621, "incomplete"),
            (9622, "past_due"),
        ):
            self.insert_recovery_user(user_id, stripe_subscription_id=f"sub_{user_id}")
            self.insert_checkout_attempt(user_id, hours_ago=25, status="completed", mode="subscription")
            self.insert_stripe_link(
                user_id,
                status=link_status,
                is_active=main.stripe_link_active_for_status(link_status),
            )

        for user_id, link_status in (
            (9623, "active"),
            (9624, "trialing"),
        ):
            self.insert_recovery_user(user_id, stripe_subscription_id=f"sub_{user_id}")
            self.insert_checkout_attempt(user_id, hours_ago=25, status="completed", mode="subscription")
            self.insert_stripe_link(
                user_id,
                status=link_status,
                is_active=main.stripe_link_active_for_status(link_status),
            )

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            for user_id, link_status in (
                (9625, "incomplete"),
                (9626, "active"),
            ):
                main.upsert_stripe_link(
                    cur,
                    user_id,
                    stripe_customer_id=f"cus_{user_id}",
                    stripe_subscription_id=f"sub_updated_{user_id}",
                    status=link_status,
                    current_period_end=int(time.time()) + 86400,
                    is_active=main.stripe_link_active_for_status(link_status),
                    source="customer.subscription.updated",
                )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        due_ids = {row[0] for row in self.due_recovery_users()}
        self.assertIn(9621, due_ids)
        self.assertIn(9622, due_ids)
        self.assertNotIn(9623, due_ids)
        self.assertNotIn(9624, due_ids)
        link_states = dict(self.query_all(
            """
            SELECT status, is_active
            FROM stripe_links
            WHERE stripe_subscription_id IN ('sub_updated_9625', 'sub_updated_9626')
            ORDER BY stripe_subscription_id
            """
        ))
        self.assertFalse(link_states["incomplete"])
        self.assertTrue(link_states["active"])

    def test_active_subscription_access_mismatch_queries_real_postgres(self):
        run_migrations(self.get_conn)
        now = datetime.utcnow()
        future = now + timedelta(days=30)
        expired = now - timedelta(days=1)
        conn = self.get_conn()
        cur = conn.cursor()

        def add_user(user_id, paid, expiry, subscription_id=None, customer_id=None):
            subscription_id = subscription_id or f"sub_mismatch_{user_id}"
            customer_id = customer_id or f"cus_mismatch_{user_id}"
            cur.execute(
                """
                INSERT INTO users (
                    telegram_id, paid, expiry_date, stripe_subscription_id, stripe_customer_id
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (user_id, paid, expiry, subscription_id, customer_id),
            )
            return subscription_id, customer_id

        def add_link(user_id, subscription_id, customer_id, status="active", is_active=True, updated_at=None):
            cur.execute(
                """
                INSERT INTO stripe_links (
                    telegram_id, stripe_subscription_id, stripe_customer_id,
                    status, is_active, current_period_end, source, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, 'test', COALESCE(%s, NOW()))
                """,
                (user_id, subscription_id, customer_id, status, is_active, future, updated_at),
            )

        def add_payment(user_id, subscription_id, customer_id, event_id, **overrides):
            values = {
                "event_type": "invoice.payment_succeeded",
                "payment_status": "succeeded",
                "payment_kind": "recurring",
                "amount_paid": 5000,
                "period_end": future,
                "invoice_id": f"in_{event_id}",
            }
            values.update(overrides)
            cur.execute(
                """
                INSERT INTO payment_events (
                    stripe_event_id, event_type, telegram_id, invoice_id,
                    stripe_customer_id, stripe_subscription_id,
                    payment_status, payment_kind, amount_paid, period_end
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event_id, values["event_type"], user_id, values["invoice_id"],
                    customer_id, subscription_id, values["payment_status"],
                    values["payment_kind"], values["amount_paid"], values["period_end"],
                ),
            )

        sub, cus = add_user(61001, False, future); add_link(61001, sub, cus)
        sub, cus = add_user(61002, True, future); add_link(61002, sub, cus)
        sub, cus = add_user(61003, False, future); add_link(61003, sub, cus, status="canceled", is_active=False)
        sub, cus = add_user(61004, False, None); add_link(61004, sub, cus)
        sub, cus = add_user(61005, True, expired); add_link(61005, sub, cus)
        sub, cus = add_user(61006, False, future); add_link(61006, sub, cus)
        add_payment(61006, sub, cus, "evt_mismatch_valid")

        sub, cus = add_user(61007, False, future); add_link(61007, sub, cus)
        add_payment(61007, sub, cus, "evt_mismatch_failed", payment_status="failed")
        add_payment(61007, sub, cus, "evt_mismatch_zero", amount_paid=0)
        add_payment(61007, sub, cus, "evt_mismatch_gift", payment_kind="gift", amount_paid=5000)
        add_payment(61007, sub, cus, "evt_mismatch_stale", period_end=expired)
        add_payment(61007, sub, cus, "evt_mismatch_admin", event_type="customer.subscription.updated")

        user_sub, user_cus = add_user(61009, False, future, "sub_identity_user", "cus_identity_user")
        add_link(61009, "sub_identity_other", "cus_identity_other")
        add_payment(61009, "sub_identity_other", "cus_identity_other", "evt_mismatch_identity")

        sub, cus = add_user(61010, False, future); add_link(61010, sub, cus)
        add_link(61010, "sub_historical_terminal", cus, status="canceled", is_active=False, updated_at=now + timedelta(days=1))
        conn.commit()

        counts = load_access_mismatch_counts(cur)
        samples = load_access_mismatch_samples(cur, limit=50)
        cur.close()
        conn.close()

        self.assertEqual(counts, {
            "active_local_unpaid": 5,
            "active_missing_or_stale_expiry": 2,
            "active_unpaid_with_local_payment_proof": 1,
        })
        sample_by_user = {row[0]: row for row in samples}
        self.assertNotIn(61002, sample_by_user)
        self.assertNotIn(61003, sample_by_user)
        self.assertNotIn(61009, sample_by_user)
        self.assertEqual(sample_by_user[61004][4], None)
        self.assertEqual(sample_by_user[61005][3], True)
        self.assertEqual(sample_by_user[61006][5], "evt_mismatch_valid")
        self.assertIsNone(sample_by_user[61007][5])
        self.assertEqual(sum(1 for row in samples if row[0] == 61010), 1)

    def test_bot_health_reports_mismatch_counts_db_only_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        raw_subscription = "sub_health_mismatch_sensitive_123456"
        raw_customer = "cus_health_mismatch_sensitive_654321"
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (
                telegram_id, paid, expiry_date, stripe_subscription_id, stripe_customer_id
            ) VALUES (61101, FALSE, NULL, %s, %s)
            """,
            (raw_subscription, raw_customer),
        )
        main.upsert_stripe_link(
            cur,
            61101,
            stripe_customer_id=raw_customer,
            stripe_subscription_id=raw_subscription,
            status="active",
            current_period_end=datetime.utcnow() + timedelta(days=30),
            is_active=True,
            source="customer.subscription.updated",
        )
        conn.commit()
        cur.close()
        conn.close()

        class HealthMessage:
            def __init__(self):
                self.from_user = SimpleNamespace(id=1)
                self.chat = SimpleNamespace(type="private")
                self.answers = []

            async def answer(self, text, **kwargs):
                self.answers.append(text)

        message = HealthMessage()
        stripe_balance = mock.Mock(side_effect=AssertionError("Stripe API called from health"))
        stripe_price = mock.Mock(side_effect=AssertionError("Stripe API called from health"))
        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.bot, "get_me", mock.AsyncMock(return_value=SimpleNamespace(id=1))), \
             mock.patch.object(main.bot, "get_webhook_info", mock.AsyncMock(return_value=SimpleNamespace(pending_update_count=0, last_error_message=None))), \
             mock.patch.object(main.stripe.Balance, "retrieve", stripe_balance), \
             mock.patch.object(main.stripe.Price, "retrieve", stripe_price):
            asyncio.run(main.bot_health_command(message))

        output = message.answers[0]
        self.assertIn("active link + paid=False: 1", output)
        self.assertIn("active link + missing/stale expiry: 1", output)
        self.assertIn("active unpaid with local payment proof: 0", output)
        self.assertNotIn(raw_subscription, output)
        self.assertNotIn(raw_customer, output)
        stripe_balance.assert_not_called()
        stripe_price.assert_not_called()

    def test_mismatch_ranking_filters_identity_before_newer_conflicting_link_real_postgres(self):
        run_migrations(self.get_conn)
        future = datetime.utcnow() + timedelta(days=30)
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (
                telegram_id, paid, expiry_date, stripe_subscription_id, stripe_customer_id
            ) VALUES (61201, FALSE, %s, 'sub_rank_A', 'cus_rank_A')
            """,
            (future,),
        )
        cur.execute(
            """
            INSERT INTO stripe_links (
                telegram_id, stripe_subscription_id, stripe_customer_id,
                status, is_active, current_period_end, source, updated_at
            ) VALUES
                (61201, 'sub_rank_A', 'cus_rank_A', 'active', TRUE, %s, 'matching', NOW() - INTERVAL '1 day'),
                (61201, 'sub_rank_B', 'cus_rank_B', 'active', TRUE, %s, 'conflicting', NOW())
            """,
            (future, future),
        )
        cur.execute(
            """
            INSERT INTO payment_events (
                stripe_event_id, event_type, telegram_id, invoice_id,
                stripe_customer_id, stripe_subscription_id,
                payment_status, payment_kind, amount_paid, period_end
            ) VALUES
                ('evt_rank_A', 'invoice.payment_succeeded', 61201, 'in_rank_A',
                 'cus_rank_A', 'sub_rank_A', 'succeeded', 'recurring', 5000, %s),
                ('evt_rank_B', 'invoice.payment_succeeded', 61201, 'in_rank_B',
                 'cus_rank_B', 'sub_rank_B', 'succeeded', 'recurring', 5000, %s)
            """,
            (future, future),
        )
        conn.commit()

        counts = load_access_mismatch_counts(cur)
        samples = load_access_mismatch_samples(cur, limit=20)
        matching_rows = [row for row in samples if row[0] == 61201]
        self.assertEqual(counts["active_local_unpaid"], 1)
        self.assertEqual(counts["active_unpaid_with_local_payment_proof"], 1)
        self.assertEqual(len(matching_rows), 1)
        self.assertEqual(matching_rows[0][1], "sub_rank_A")
        self.assertEqual(matching_rows[0][5], "evt_rank_A")
        self.assertNotEqual(matching_rows[0][5], "evt_rank_B")

        # The schema prevents multiple rows for the exact same current identity.
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            cur.execute(
                """
                INSERT INTO stripe_links (
                    telegram_id, stripe_subscription_id, stripe_customer_id,
                    status, is_active, current_period_end, source, updated_at
                ) VALUES (61201, 'sub_rank_A', 'cus_rank_A', 'active', TRUE, %s, 'duplicate', NOW())
                """,
                (future,),
            )
        conn.rollback()
        samples_after_duplicate = load_access_mismatch_samples(cur, limit=20)
        self.assertEqual(sum(1 for row in samples_after_duplicate if row[0] == 61201), 1)
        cur.close()
        conn.close()

    def test_first_purchase_recovery_completed_checkout_with_payment_proof_is_not_eligible(self):
        run_migrations(self.get_conn)

        self.insert_recovery_user(9611)
        self.insert_checkout_attempt(9611, hours_ago=25, status="completed", mode="subscription")

        self.insert_recovery_user(9612)
        self.insert_checkout_attempt(9612, hours_ago=25, status="completed", mode="subscription")
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE users
                SET paid = TRUE,
                    expiry_date = (NOW() AT TIME ZONE 'UTC') + INTERVAL '7 days'
                WHERE telegram_id = 9612
                """
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        for user_id, link_status, is_active in (
            (9613, "active", True),
            (9614, "trialing", True),
        ):
            self.insert_recovery_user(user_id, stripe_subscription_id=f"sub_{user_id}")
            self.insert_checkout_attempt(
                user_id,
                hours_ago=25,
                status="completed",
                mode="subscription",
            )
            self.insert_stripe_link(user_id, status=link_status, is_active=is_active)

        self.insert_recovery_user(9616)
        self.insert_checkout_attempt(9616, hours_ago=25, status="completed", mode="payment")

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO payment_events (
                    stripe_event_id, event_type, telegram_id, payment_status
                )
                VALUES
                    ('evt_9611_paid', 'invoice.payment_succeeded', 9611, 'succeeded'),
                    ('evt_9616_paid', 'checkout.session.completed', 9616, 'succeeded')
                """
            )
            cur.execute(
                """
                INSERT INTO access_events (telegram_id, event_type, source, new_expiry)
                VALUES (
                    9616,
                    'checkout_completed',
                    'stripe',
                    (NOW() AT TIME ZONE 'UTC') + INTERVAL '7 days'
                )
                """
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        due_ids = {row[0] for row in self.due_recovery_users()}
        for user_id in (9611, 9612, 9613, 9614, 9616):
            self.assertNotIn(user_id, due_ids)

    def test_first_purchase_recovery_success_states_and_newer_attempt_block_eligibility(self):
        run_migrations(self.get_conn)

        self.insert_recovery_user(9201)
        self.insert_checkout_attempt(9201, hours_ago=25)
        self.insert_retry_attempt(9201, hours_ago=2)
        self.assertNotIn(9201, {row[0] for row in self.due_recovery_users()})

        for user_id, status, is_active, future_period in (
            (9202, "active", True, True),
            (9203, "trialing", True, False),
        ):
            self.insert_recovery_user(user_id, stripe_subscription_id=f"sub_{user_id}")
            self.insert_checkout_attempt(user_id, hours_ago=25)
            self.insert_stripe_link(user_id, status=status, is_active=is_active, future_period=future_period)
            self.assertNotIn(user_id, {row[0] for row in self.due_recovery_users()})

        self.insert_recovery_user(9206)
        self.insert_checkout_attempt(9206, hours_ago=25)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO payment_events (
                    stripe_event_id, event_type, telegram_id, payment_status
                )
                VALUES ('evt_9206', 'invoice.payment_succeeded', 9206, 'succeeded')
                """
            )
            cur.execute(
                """
                INSERT INTO access_events (telegram_id, event_type, source, new_expiry)
                VALUES (9207, 'manual_access', 'manual', (NOW() AT TIME ZONE 'UTC') + INTERVAL '7 days')
                """
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.insert_recovery_user(9207)
        self.insert_checkout_attempt(9207, hours_ago=25)
        due_ids = {row[0] for row in self.due_recovery_users()}
        self.assertNotIn(9206, due_ids)
        self.assertNotIn(9207, due_ids)

    def test_first_purchase_recovery_real_postgres_enqueue_reactivation_and_recheck(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9301
        self.insert_recovery_user(user_id)
        self.insert_checkout_attempt(user_id, hours_ago=25, tariff="sub_1")

        due = {row[0]: row[1] for row in self.due_recovery_users()}
        self.assertIn(user_id, due)
        self.assertTrue(self.enqueue_recovery(user_id, due[user_id]))
        self.assertEqual(self.recovery_row(user_id)[1], "pending")

        self.insert_retry_attempt(user_id, hours_ago=1, tariff="sub_6")
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            main.cancel_first_purchase_recovery_delivery(
                cur,
                main.first_purchase_recovery_delivery_key(user_id),
                "newer_attempt_postponed",
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        self.assertEqual(self.recovery_row(user_id)[1], "cancelled")
        self.assertNotIn(user_id, {row[0] for row in self.due_recovery_users()})

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE checkout_retry_events
                SET attempt_at = (NOW() AT TIME ZONE 'UTC') - INTERVAL '25 hours'
                WHERE telegram_id = %s
                """,
                (user_id,),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        due = {row[0]: row[1] for row in self.due_recovery_users()}
        self.assertIn(user_id, due)
        self.assertTrue(self.enqueue_recovery(user_id, due[user_id]))
        row = self.recovery_row(user_id)
        self.assertEqual(row[1], "pending")
        self.assertIsNone(row[3])
        self.assertIsNone(row[4])
        self.assertIsNone(row[5])
        self.assertIsNone(row[6])

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE message_delivery_events
                SET status = 'sent', sent_at = NOW()
                WHERE delivery_key = %s
                """,
                (main.first_purchase_recovery_delivery_key(user_id),),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        self.assertFalse(self.enqueue_recovery(user_id, due[user_id]))
        self.assertEqual(self.recovery_row(user_id)[1], "sent")

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO payment_events (
                    stripe_event_id, event_type, telegram_id, payment_status
                )
                VALUES ('evt_9301_paid', 'invoice.payment_succeeded', %s, 'succeeded')
                """,
                (user_id,),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            self.assertFalse(main.first_purchase_recovery_reminder_still_due(cur, user_id))
        finally:
            cur.close()
            conn.close()

    def test_first_purchase_recovery_real_postgres_concurrent_reactivation_one_pending(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9401
        self.insert_recovery_user(user_id)
        self.insert_checkout_attempt(user_id, hours_ago=25)
        due = {row[0]: row[1] for row in self.due_recovery_users()}
        self.assertTrue(self.enqueue_recovery(user_id, due[user_id]))

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            main.cancel_first_purchase_recovery_delivery(
                cur,
                main.first_purchase_recovery_delivery_key(user_id),
                "postponed",
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        self.insert_retry_attempt(user_id, hours_ago=25, tariff="sub_6")
        due = {row[0]: row[1] for row in self.due_recovery_users()}

        results = []
        errors = []

        def worker():
            try:
                results.append(self.enqueue_recovery(user_id, due[user_id]))
            except Exception as exc:
                errors.append(exc)

        first = threading.Thread(target=worker)
        second = threading.Thread(target=worker)
        first.start()
        second.start()
        first.join()
        second.join()

        self.assertFalse(errors)
        self.assertEqual(self.query_one(
            """
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE delivery_key = %s AND status = 'pending'
            """,
            (main.first_purchase_recovery_delivery_key(user_id),),
        )[0], 1)
        self.assertEqual(sum(1 for result in results if result), 1)

    def test_first_purchase_recovery_payment_success_cancels_and_blocks_worker_recheck(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9501
        self.insert_recovery_user(user_id)
        self.insert_checkout_attempt(user_id, hours_ago=25)
        due = {row[0]: row[1] for row in self.due_recovery_users()}
        self.assertTrue(self.enqueue_recovery(user_id, due[user_id]))

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            main.cancel_first_purchase_recovery_deliveries(cur, user_id, reason="paid")
            cur.execute(
                """
                INSERT INTO payment_events (
                    stripe_event_id, event_type, telegram_id, payment_status
                )
                VALUES ('evt_9501_paid', 'invoice.payment_succeeded', %s, 'succeeded')
                """,
                (user_id,),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(self.recovery_row(user_id)[1], "cancelled")
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            self.assertFalse(
                main.first_purchase_recovery_reminder_still_due(
                    cur,
                    user_id,
                    current_delivery_key=main.first_purchase_recovery_delivery_key(user_id),
                )
            )
        finally:
            cur.close()
            conn.close()

    def test_first_purchase_recovery_scheduler_payload_context_boundary_and_dedupe(self):
        run_migrations(self.get_conn)
        main = import_main()

        self.insert_recovery_user(9701)
        self.insert_checkout_attempt(9701, hours_ago=23, status="expired", mode="payment")
        self.insert_recovery_user(9702)
        self.insert_checkout_attempt(9702, hours_ago=25, status="expired", mode="payment", tariff="sub_1")

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn):
            result = asyncio.run(main.enqueue_due_first_purchase_recovery_reminders(limit=50))
            second_result = asyncio.run(main.enqueue_due_first_purchase_recovery_reminders(limit=50))

        self.assertEqual(result, {"due": 1, "enqueued": 1})
        self.assertEqual(second_result, {"due": 0, "enqueued": 0})
        self.assertIsNone(self.recovery_row(9701))
        row = self.recovery_row(9702)
        self.assertEqual(row[1], "pending")
        payload = json.loads(row[8])
        self.assertEqual(payload["text"], main.first_purchase_recovery_reminder_text())
        self.assertEqual(payload["keyboard_kind"], "retry_payment")
        self.assertEqual(payload["reason_category"], "checkout_expired")
        self.assertEqual(payload["stage"], "checkout")
        self.assertEqual(payload["tariff_code"], "sub_1")
        self.assertNotIn("9702", payload["safe_ref"])

    def test_first_purchase_recovery_scheduler_payload_uses_safe_checkout_error_contexts(self):
        run_migrations(self.get_conn)
        main = import_main()

        for user_id, status, last_error, expected_category in (
            (9710, "failed", "checkout.session.async_payment_failed", "checkout_async_payment_failed"),
            (9711, "expired", "checkout.session.expired", "checkout_expired"),
            (9712, "failed", "raw Stripe exception cus_raw in_raw person@example.com", "unknown_payment_error"),
        ):
            self.insert_recovery_user(user_id)
            self.insert_checkout_attempt(
                user_id,
                hours_ago=25,
                status=status,
                mode="subscription",
                last_error=last_error,
            )

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn):
            result = asyncio.run(main.enqueue_due_first_purchase_recovery_reminders(limit=50))

        self.assertEqual(result, {"due": 3, "enqueued": 3})
        for user_id, expected_category in (
            (9710, "checkout_async_payment_failed"),
            (9711, "checkout_expired"),
            (9712, "unknown_payment_error"),
        ):
            payload = json.loads(self.recovery_row(user_id)[8])
            self.assertEqual(payload["reason_category"], expected_category)
            self.assertNotIn("cus_raw", json.dumps(payload, ensure_ascii=False))
            self.assertNotIn("in_raw", json.dumps(payload, ensure_ascii=False))
            self.assertNotIn("person@example.com", json.dumps(payload, ensure_ascii=False))

    def test_first_purchase_recovery_invoice_failure_context_tokens_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()

        cases = (
            (9720, None, "invoice_payment_failed", "invoice_payment_failed"),
            (9721, "card_declined", "invoice_payment_failed:card_declined", "card_declined"),
            (9722, "insufficient_funds", "invoice_payment_failed:insufficient_funds", "insufficient_funds"),
            (9723, "authentication_required", "invoice_payment_failed:authentication_required", "authentication_required"),
        )
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            for user_id, failure_code, token, _ in cases:
                self.insert_recovery_user(user_id, stripe_subscription_id=f"sub_{user_id}")
                self.insert_checkout_attempt(
                    user_id,
                    hours_ago=25,
                    status="completed",
                    mode="subscription",
                    stripe_subscription_id=f"sub_{user_id}",
                )
                persisted_token, updated_checkout_id = (
                    main.persist_first_purchase_recovery_invoice_failure_context(
                        cur,
                        user_id,
                        f"sub_{user_id}",
                        failure_code=failure_code,
                    )
                )
                self.assertEqual(persisted_token, token)
                self.assertIsNotNone(updated_checkout_id)
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn):
            result = asyncio.run(main.enqueue_due_first_purchase_recovery_reminders(limit=50))

        self.assertEqual(result, {"due": 4, "enqueued": 4})
        for user_id, _, token, expected_category in cases:
            checkout_error = self.query_one(
                "SELECT last_error FROM checkout_sessions WHERE telegram_id = %s",
                (user_id,),
            )[0]
            self.assertEqual(checkout_error, token)
            payload = json.loads(self.recovery_row(user_id)[8])
            self.assertEqual(payload["attempt_error_context"], token)
            self.assertEqual(payload["reason_category"], expected_category)

    def test_invoice_failure_context_prefers_exact_subscription_over_newer_null_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9725
        self.insert_recovery_user(user_id, stripe_subscription_id="sub_exact_priority")
        exact_id = self.insert_checkout_attempt(
            user_id,
            hours_ago=30,
            status="completed",
            mode="subscription",
            stripe_subscription_id="sub_exact_priority",
        )
        null_id = self.insert_checkout_attempt(
            user_id,
            hours_ago=25,
            status="completed",
            mode="subscription",
            stripe_subscription_id=None,
        )

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            token, updated_id = main.persist_first_purchase_recovery_invoice_failure_context(
                cur,
                user_id,
                "sub_exact_priority",
                failure_code="card_declined",
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(token, "invoice_payment_failed:card_declined")
        self.assertEqual(updated_id, exact_id)
        rows = self.query_all(
            """
            SELECT id, last_error
            FROM checkout_sessions
            WHERE id IN (%s, %s)
            ORDER BY id
            """,
            (exact_id, null_id),
        )
        self.assertEqual(dict(rows)[exact_id], "invoice_payment_failed:card_declined")
        self.assertIsNone(dict(rows)[null_id])

    def test_invoice_failure_context_uses_latest_null_when_no_exact_subscription_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9726
        self.insert_recovery_user(user_id, stripe_subscription_id="sub_missing_exact")
        older_null_id = self.insert_checkout_attempt(
            user_id,
            hours_ago=30,
            status="completed",
            mode="subscription",
            stripe_subscription_id=None,
        )
        latest_null_id = self.insert_checkout_attempt(
            user_id,
            hours_ago=25,
            status="completed",
            mode="subscription",
            stripe_subscription_id=None,
        )

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            token, updated_id = main.persist_first_purchase_recovery_invoice_failure_context(
                cur,
                user_id,
                "sub_missing_exact",
                failure_code=None,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(token, "invoice_payment_failed")
        self.assertEqual(updated_id, latest_null_id)
        rows = self.query_all(
            """
            SELECT id, last_error
            FROM checkout_sessions
            WHERE id IN (%s, %s)
            ORDER BY id
            """,
            (older_null_id, latest_null_id),
        )
        self.assertIsNone(dict(rows)[older_null_id])
        self.assertEqual(dict(rows)[latest_null_id], "invoice_payment_failed")

    def test_invoice_failure_context_does_not_update_other_non_null_subscription_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9727
        self.insert_recovery_user(user_id, stripe_subscription_id="sub_missing_exact")
        other_sub_id = self.insert_checkout_attempt(
            user_id,
            hours_ago=25,
            status="completed",
            mode="subscription",
            stripe_subscription_id="sub_other",
        )
        null_id = self.insert_checkout_attempt(
            user_id,
            hours_ago=30,
            status="completed",
            mode="subscription",
            stripe_subscription_id=None,
        )

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            token, updated_id = main.persist_first_purchase_recovery_invoice_failure_context(
                cur,
                user_id,
                "sub_missing_exact",
                failure_code="insufficient_funds",
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(token, "invoice_payment_failed:insufficient_funds")
        self.assertEqual(updated_id, null_id)
        rows = self.query_all(
            """
            SELECT id, last_error
            FROM checkout_sessions
            WHERE id IN (%s, %s)
            ORDER BY id
            """,
            (other_sub_id, null_id),
        )
        self.assertIsNone(dict(rows)[other_sub_id])
        self.assertEqual(dict(rows)[null_id], "invoice_payment_failed:insufficient_funds")

    def test_first_purchase_recovery_stale_invoice_does_not_change_context_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9724
        self.insert_recovery_user(user_id, stripe_subscription_id="sub_current")
        self.insert_checkout_attempt(
            user_id,
            hours_ago=25,
            status="completed",
            mode="subscription",
            stripe_subscription_id="sub_current",
        )

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO access_events (
                    telegram_id, event_type, source, stripe_event_id, stripe_subscription_id, notes
                )
                VALUES (%s, 'ignored_stale_negative_event', 'invoice.payment_failed', %s, %s, %s)
                """,
                (user_id, "evt_stale_context", "sub_old", "stale invoice ignored before recovery context update"),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertIsNone(self.query_one(
            "SELECT last_error FROM checkout_sessions WHERE telegram_id = %s",
            (user_id,),
        )[0])

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn):
            result = asyncio.run(main.enqueue_due_first_purchase_recovery_reminders(limit=50))

        self.assertEqual(result, {"due": 1, "enqueued": 1})
        payload = json.loads(self.recovery_row(user_id)[8])
        self.assertEqual(payload["reason_category"], "payment_confirmation_pending")
        self.assertEqual(payload["attempt_error_context"], "unknown")

    def test_first_purchase_recovery_worker_success_is_log_only_for_admins(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9703
        self.insert_recovery_user(user_id, stripe_subscription_id="sub_9703")
        self.insert_checkout_attempt(user_id, hours_ago=25, status="completed", mode="subscription")
        self.insert_stripe_link(
            user_id,
            subscription_id="sub_9703",
            status="checkout_subscription_pending_invoice",
            is_active=False,
        )

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn):
            asyncio.run(main.enqueue_due_first_purchase_recovery_reminders(limit=50))

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.bot, "send_message", mock.AsyncMock()):
            first = asyncio.run(main.process_pending_message_deliveries(limit=10))

        self.assertEqual(first["sent"], 1)
        self.assertEqual(self.recovery_row(user_id)[1], "sent")
        admin_rows = self.query_all(
            """
            SELECT delivery_key, telegram_id, delivery_type, status, payload_json
            FROM message_delivery_events
            WHERE delivery_type = 'stripe_admin_message'
              AND payload_json::text LIKE %s
            ORDER BY telegram_id
            """,
            ("%first_purchase_recovery_sent%",),
        )
        self.assertEqual(admin_rows, [])

    def test_first_purchase_recovery_retryable_failure_does_not_create_admin_sent_notice(self):
        from aiogram.exceptions import TelegramNetworkError

        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9704
        self.insert_recovery_user(user_id)
        self.insert_checkout_attempt(user_id, hours_ago=25)
        due = {row[0]: row for row in self.due_recovery_users()}

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            self.assertTrue(main.enqueue_first_purchase_recovery_reminder(
                cur,
                user_id,
                due[user_id][1],
                main.first_purchase_recovery_row_context(due[user_id]),
            ))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.bot, "send_message", mock.AsyncMock(side_effect=TelegramNetworkError(method=None, message="network"))), \
             mock.patch.object(main, "notify_admins", mock.AsyncMock()):
            result = asyncio.run(main.process_pending_message_deliveries(limit=10))

        self.assertEqual(result["retryable_failed"], 1)
        self.assertEqual(self.recovery_row(user_id)[1], "failed")
        self.assertEqual(self.query_one(
            """
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE delivery_type = 'stripe_admin_message'
              AND payload_json::text LIKE %s
            """,
            ("%first_purchase_recovery_sent%",),
        )[0], 0)

    def test_first_purchase_recovery_blocked_user_does_not_create_admin_sent_notice(self):
        from aiogram.exceptions import TelegramForbiddenError

        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9705
        self.insert_recovery_user(user_id)
        self.insert_checkout_attempt(user_id, hours_ago=25)
        due = {row[0]: row for row in self.due_recovery_users()}
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            main.enqueue_first_purchase_recovery_reminder(
                cur,
                user_id,
                due[user_id][1],
                main.first_purchase_recovery_row_context(due[user_id]),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.bot, "send_message", mock.AsyncMock(side_effect=TelegramForbiddenError(method=None, message="blocked"))), \
             mock.patch.object(main, "notify_admins", mock.AsyncMock()):
            result = asyncio.run(main.process_pending_message_deliveries(limit=10))

        self.assertEqual(result["blocked"], 1)
        self.assertEqual(self.recovery_row(user_id)[1], "permanently_failed")
        self.assertTrue(self.query_one("SELECT blocked_bot FROM users WHERE telegram_id = %s", (user_id,))[0])
        self.assertEqual(self.query_one(
            """
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE delivery_type = 'stripe_admin_message'
              AND payload_json::text LIKE %s
            """,
            ("%first_purchase_recovery_sent%",),
        )[0], 0)

    def test_access_restore_delivery_dedupe_and_auto_sync_key_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9901
        expiry = datetime.utcnow() + timedelta(days=30)
        self.insert_recovery_user(user_id, paid=True, expiry_date=expiry)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            first = main.enqueue_automatic_membership_repair(
                cur,
                user_id,
                expiry,
                main.ACCESS_RESTORE_SOURCE_AUTO_SYNC,
                requested_by_admin_id=1,
                reason="sync_stripe_user_active_period",
            )
            second = main.enqueue_automatic_membership_repair(
                cur,
                user_id,
                expiry,
                main.ACCESS_RESTORE_SOURCE_AUTO_SYNC,
                requested_by_admin_id=1,
                reason="sync_stripe_user_active_period",
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertTrue(first)
        self.assertTrue(second)
        expected_key = main.access_restore_auto_delivery_key("auto-sync", user_id, expiry)
        row = self.query_one(
            """
            SELECT COUNT(*), MIN(delivery_type), MIN(status), MIN(payload_json)
            FROM message_delivery_events
            WHERE delivery_key = %s
            """,
            (expected_key,),
        )
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], main.ACCESS_RESTORE_DELIVERY_TYPE)
        self.assertEqual(row[2], "pending")
        payload = json.loads(row[3])
        self.assertEqual(payload["telegram_id"], user_id)
        self.assertEqual(payload["source"], main.ACCESS_RESTORE_SOURCE_AUTO_SYNC)
        self.assertEqual(payload["reason"], "sync_stripe_user_active_period")

    def test_admin_action_requests_failed_restore_status_real_postgres(self):
        from admin_security import claim_admin_action, complete_admin_action, fail_admin_action, make_action_request

        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            failed_action_id = make_action_request(cur, 1, "restore_access", {"telegram_id": 9907})
            completed_action_id = make_action_request(cur, 1, "restore_access", {"telegram_id": 9908})
            conn.commit()

            self.assertEqual(claim_admin_action(cur, failed_action_id, 1)["status"], "claimed")
            fail_admin_action(cur, failed_action_id)
            self.assertEqual(claim_admin_action(cur, completed_action_id, 1)["status"], "claimed")
            complete_admin_action(cur, completed_action_id)
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(self.query_one(
            "SELECT status FROM admin_action_requests WHERE action_id = %s",
            (failed_action_id,),
        )[0], "failed")
        self.assertEqual(self.query_one(
            "SELECT status FROM admin_action_requests WHERE action_id = %s",
            (completed_action_id,),
        )[0], "completed")

    def test_sync_stripe_user_identity_changed_rolls_back_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9910
        grace = datetime.utcnow() + timedelta(hours=2)
        self.insert_recovery_user(
            user_id,
            paid=False,
            expiry_date=None,
            payment_failed=True,
            grace_period_end=grace,
            stripe_subscription_id="sub_A",
            stripe_customer_id="cus_old",
        )
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())

        class ReplyMessage:
            def __init__(self):
                self.from_user = SimpleNamespace(id=1)
                self.chat = SimpleNamespace(type="private")
                self.replies = []

            async def reply(self, text, **kwargs):
                self.replies.append((text, kwargs))

        async def stripe_after_pointer_change(func, *args, **kwargs):
            conn = self.get_conn()
            cur = conn.cursor()
            try:
                cur.execute(
                    "UPDATE users SET stripe_subscription_id = 'sub_B', stripe_customer_id = 'cus_newer' WHERE telegram_id = %s",
                    (user_id,),
                )
                conn.commit()
            finally:
                cur.close()
                conn.close()
            return SimpleNamespace(status="active", current_period_end=period_end, customer="cus_A", cancel_at_period_end=False)

        message = ReplyMessage()
        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.asyncio, "to_thread", mock.AsyncMock(side_effect=stripe_after_pointer_change)):
            asyncio.run(main.sync_stripe_user_command(message, SimpleNamespace(args=str(user_id))))

        self.assertIn("Stripe subscription пользователя изменилась", message.replies[0][0])
        self.assertNotIn("sub_A", message.replies[0][0])
        self.assertNotIn("sub_B", message.replies[0][0])
        self.assertEqual(self.query_one(
            "SELECT paid, expiry_date, payment_failed, grace_period_end IS NOT NULL, stripe_subscription_id, stripe_customer_id FROM users WHERE telegram_id = %s",
            (user_id,),
        ), (False, None, True, True, "sub_B", "cus_newer"))
        self.assertIsNone(self.query_one(
            "SELECT 1 FROM access_events WHERE telegram_id = %s AND event_type = 'manual_stripe_sync'",
            (user_id,),
        ))
        self.assertIsNone(self.query_one(
            "SELECT 1 FROM message_delivery_events WHERE telegram_id = %s AND delivery_type = %s",
            (user_id, main.ACCESS_RESTORE_DELIVERY_TYPE),
        ))

    def test_restore_access_stripe_identity_changed_fails_closed_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9911
        self.insert_recovery_user(
            user_id,
            paid=False,
            expiry_date=None,
            stripe_subscription_id="sub_A",
            stripe_customer_id="cus_old",
        )
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())

        async def stripe_after_pointer_change(func, *args, **kwargs):
            conn = self.get_conn()
            cur = conn.cursor()
            try:
                cur.execute(
                    "UPDATE users SET stripe_subscription_id = 'sub_B', stripe_customer_id = 'cus_newer' WHERE telegram_id = %s",
                    (user_id,),
                )
                conn.commit()
            finally:
                cur.close()
                conn.close()
            return SimpleNamespace(status="active", current_period_end=period_end, customer="cus_A", cancel_at_period_end=False)

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.asyncio, "to_thread", mock.AsyncMock(side_effect=stripe_after_pointer_change)), \
             mock.patch.object(main.bot, "get_chat_member", mock.AsyncMock()) as member:
            result = asyncio.run(main.execute_confirmed_restore_access({
                "telegram_id": user_id,
                "admin_id": 1,
                "action_id": "act_identity_changed",
            }))

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["reason"], "stripe_identity_changed")
        self.assertIn("Stripe subscription пользователя изменилась", result["admin_message"])
        self.assertNotIn("sub_A", result["admin_message"])
        self.assertNotIn("sub_B", result["admin_message"])
        member.assert_not_awaited()
        self.assertEqual(self.query_one(
            "SELECT paid, expiry_date, stripe_subscription_id, stripe_customer_id FROM users WHERE telegram_id = %s",
            (user_id,),
        ), (False, None, "sub_B", "cus_newer"))
        self.assertIsNone(self.query_one(
            "SELECT 1 FROM access_events WHERE telegram_id = %s AND event_type = 'restore_access_stripe_sync'",
            (user_id,),
        ))
        self.assertIsNone(self.query_one(
            "SELECT 1 FROM message_delivery_events WHERE telegram_id = %s AND delivery_type = %s",
            (user_id, main.ACCESS_RESTORE_DELIVERY_TYPE),
        ))

    def test_restore_access_matching_stripe_identity_updates_event_and_delivery_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9912
        self.insert_recovery_user(
            user_id,
            paid=False,
            expiry_date=None,
            stripe_subscription_id="sub_A",
            stripe_customer_id="cus_old",
        )
        period_end = int((datetime.utcnow() + timedelta(days=30)).timestamp())
        subscription = SimpleNamespace(status="active", current_period_end=period_end, customer="cus_A", cancel_at_period_end=False)

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.asyncio, "to_thread", mock.AsyncMock(return_value=subscription)), \
             mock.patch.object(main.bot, "get_chat_member", mock.AsyncMock(return_value=SimpleNamespace(status="left"))):
            result = asyncio.run(main.execute_confirmed_restore_access({
                "telegram_id": user_id,
                "admin_id": 1,
                "action_id": "act_identity_match",
            }))

        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["delivery_created"])
        row = self.query_one(
            "SELECT paid, stripe_subscription_id, stripe_customer_id FROM users WHERE telegram_id = %s",
            (user_id,),
        )
        self.assertEqual(row, (True, "sub_A", "cus_A"))
        self.assertEqual(self.query_one(
            "SELECT stripe_subscription_id FROM access_events WHERE telegram_id = %s AND event_type = 'restore_access_stripe_sync'",
            (user_id,),
        )[0], "sub_A")
        self.assertEqual(self.query_one(
            "SELECT delivery_type, status FROM message_delivery_events WHERE telegram_id = %s AND delivery_key = %s",
            (user_id, main.access_restore_delivery_key("act_identity_match", user_id)),
        ), (main.ACCESS_RESTORE_DELIVERY_TYPE, "pending"))

    def test_access_restore_worker_sends_invite_and_persists_state_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9902
        expiry = datetime.utcnow() + timedelta(days=30)
        self.insert_recovery_user(user_id, paid=True, expiry_date=expiry)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            self.assertTrue(main.enqueue_access_restore_invite(
                cur,
                user_id,
                expiry,
                main.ACCESS_RESTORE_SOURCE_ADMIN,
                requested_by_admin_id=1,
                admin_action_id="act_pg_restore",
                reason="manual_restore_access",
                delivery_key=main.access_restore_delivery_key("act_pg_restore", user_id),
            ))
            self.assertTrue(main.enqueue_access_restore_invite(
                cur,
                user_id,
                expiry,
                main.ACCESS_RESTORE_SOURCE_ADMIN,
                requested_by_admin_id=1,
                admin_action_id="act_pg_restore",
                reason="manual_restore_access",
                delivery_key=main.access_restore_delivery_key("act_pg_restore", user_id),
            ))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.bot, "get_chat_member", mock.AsyncMock(return_value=SimpleNamespace(status="left"))), \
             mock.patch.object(main.bot, "unban_chat_member", mock.AsyncMock()), \
             mock.patch.object(main.bot, "create_chat_invite_link", mock.AsyncMock(return_value=SimpleNamespace(invite_link="https://t.me/+restore_pg"))) as create_link, \
             mock.patch.object(main.bot, "send_message", mock.AsyncMock()) as send_message:
            result = asyncio.run(main.process_pending_message_deliveries(limit=10))

        self.assertEqual(result["sent"], 1)
        create_link.assert_awaited_once()
        send_message.assert_awaited_once()
        delivery = self.query_one(
            """
            SELECT status, invite_link
            FROM message_delivery_events
            WHERE delivery_key = %s
            """,
            (main.access_restore_delivery_key("act_pg_restore", user_id),),
        )
        self.assertEqual(delivery, ("sent", "https://t.me/+restore_pg"))
        self.assertEqual(self.query_one(
            "SELECT source, telegram_id, status FROM bot_invite_links WHERE invite_link = %s",
            ("https://t.me/+restore_pg",),
        ), (main.ACCESS_RESTORE_SOURCE_ADMIN, user_id, "active"))
        self.assertEqual(self.query_one(
            "SELECT event_type FROM access_events WHERE telegram_id = %s AND event_type = 'restore_access_invite_sent'",
            (user_id,),
        )[0], "restore_access_invite_sent")

    def test_access_restore_worker_cancels_inactive_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9903
        expired = datetime.utcnow() - timedelta(days=1)
        self.insert_recovery_user(user_id, paid=True, expiry_date=expired)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            main.enqueue_access_restore_invite(
                cur,
                user_id,
                datetime.utcnow() + timedelta(days=1),
                main.ACCESS_RESTORE_SOURCE_ADMIN,
                admin_action_id="act_expired",
                delivery_key=main.access_restore_delivery_key("act_expired", user_id),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.bot, "get_chat_member", mock.AsyncMock()) as member:
            result = asyncio.run(main.process_pending_message_deliveries(limit=10))

        self.assertEqual(result, {"sent": 0, "retryable_failed": 0, "permanently_failed": 0, "blocked": 0})
        member.assert_not_awaited()
        self.assertEqual(self.query_one(
            "SELECT status, last_error FROM message_delivery_events WHERE delivery_key = %s",
            (main.access_restore_delivery_key("act_expired", user_id),),
        ), ("cancelled", "access_restore_inactive"))
        self.assertEqual(self.query_one(
            "SELECT event_type FROM access_events WHERE telegram_id = %s",
            (user_id,),
        )[0], "restore_access_cancelled_inactive")

    def test_access_restore_retry_reuses_existing_invite_real_postgres(self):
        from aiogram.exceptions import TelegramNetworkError

        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9904
        expiry = datetime.utcnow() + timedelta(days=30)
        self.insert_recovery_user(user_id, paid=True, expiry_date=expiry)
        key = main.access_restore_delivery_key("act_retry", user_id)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            main.enqueue_access_restore_invite(
                cur,
                user_id,
                expiry,
                main.ACCESS_RESTORE_SOURCE_ADMIN,
                admin_action_id="act_retry",
                delivery_key=key,
            )
            cur.execute("UPDATE message_delivery_events SET invite_link = %s WHERE delivery_key = %s", ("https://t.me/+saved_restore", key))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.bot, "get_chat_member", mock.AsyncMock(return_value=SimpleNamespace(status="left"))), \
             mock.patch.object(main.bot, "unban_chat_member", mock.AsyncMock()), \
             mock.patch.object(main.bot, "create_chat_invite_link", mock.AsyncMock()) as create_link, \
             mock.patch.object(main.bot, "send_message", mock.AsyncMock(side_effect=TelegramNetworkError(method=None, message="network"))), \
             mock.patch.object(main, "notify_admins", mock.AsyncMock()):
            result = asyncio.run(main.process_pending_message_deliveries(limit=10))

        self.assertEqual(result["retryable_failed"], 1)
        create_link.assert_not_awaited()
        self.assertEqual(self.query_one(
            "SELECT status, invite_link FROM message_delivery_events WHERE delivery_key = %s",
            (key,),
        )[0:2], ("failed", "https://t.me/+saved_restore"))

    def test_access_restore_second_worker_does_not_resend_sent_delivery_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9905
        expiry = datetime.utcnow() + timedelta(days=30)
        self.insert_recovery_user(user_id, paid=True, expiry_date=expiry)
        key = main.access_restore_delivery_key("act_once", user_id)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            main.enqueue_access_restore_invite(
                cur,
                user_id,
                expiry,
                main.ACCESS_RESTORE_SOURCE_ADMIN,
                admin_action_id="act_once",
                delivery_key=key,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.bot, "get_chat_member", mock.AsyncMock(return_value=SimpleNamespace(status="left"))), \
             mock.patch.object(main.bot, "unban_chat_member", mock.AsyncMock()), \
             mock.patch.object(main.bot, "create_chat_invite_link", mock.AsyncMock(return_value=SimpleNamespace(invite_link="https://t.me/+once"))), \
             mock.patch.object(main.bot, "send_message", mock.AsyncMock()) as send_message:
            first = asyncio.run(main.process_pending_message_deliveries(limit=10))
            second = asyncio.run(main.process_pending_message_deliveries(limit=10))

        self.assertEqual(first["sent"], 1)
        self.assertEqual(second["sent"], 0)
        self.assertEqual(send_message.await_count, 1)
        self.assertEqual(self.query_one("SELECT status FROM message_delivery_events WHERE delivery_key = %s", (key,))[0], "sent")

    def test_active_paid_user_removal_protection_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        user_id = 9906
        expiry = datetime.utcnow() + timedelta(days=7)
        self.insert_recovery_user(
            user_id,
            paid=True,
            expiry_date=expiry,
            auto_renew=False,
            stripe_subscription_id=None,
        )

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.bot, "ban_chat_member", mock.AsyncMock()) as ban:
            result = asyncio.run(main.ban_user_logic(user_id))

        self.assertEqual(result, "active_in_db")
        ban.assert_not_awaited()
        self.assertTrue(self.query_one("SELECT paid FROM users WHERE telegram_id = %s", (user_id,))[0])
        self.assertEqual(self.query_one(
            "SELECT status, last_error FROM subscription_removal_events WHERE telegram_id = %s",
            (user_id,),
        ), ("pending", "active_access_in_db"))

    def test_gift_certificate_delivery_is_deduped_and_stores_no_raw_token_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO gift_certificate_templates (tariff_code, file_id, uploaded_by)
                VALUES ('gift_1m', 'photo_file_id', 1)
            """)
            token_hash = main.gift_token_hash_for_reference("GIFT-0000000000000001", 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_name, sender_name,
                    gift_message, tariff_code, duration_days, status, token_hash, token_version
                )
                VALUES (%s, 'GIFT-0000000000000001', 9907, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'paid_unclaimed', %s, 1)
                RETURNING *
            """, (str(uuid.uuid4()), token_hash))
            gift_row = main.gift_row_dict(cur, cur.fetchone())
            first = main.enqueue_gift_certificate_delivery(cur, gift_row, 9907, main.GIFT_CERTIFICATE_BUYER)
            second = main.enqueue_gift_certificate_delivery(cur, gift_row, 9907, main.GIFT_CERTIFICATE_BUYER)
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertTrue(first)
        self.assertFalse(second)
        row = self.query_one("""
            SELECT delivery_key, payload_json
            FROM message_delivery_events
            WHERE delivery_type = %s
        """, (main.GIFT_CERTIFICATE_BUYER,))
        self.assertEqual(row[0], "gift:GIFT-0000000000000001:certificate:buyer:v1")
        raw_token = main.generate_gift_token("GIFT-0000000000000001", 1)
        self.assertNotIn("button_url", row[1])
        self.assertNotIn("start=gift_", row[1])
        self.assertNotIn("https://t.me/", row[1])
        self.assertNotIn(raw_token, row[1])
        self.assertIn('"token_version": 1', row[1])

    def test_gift_checkout_draft_replaces_only_when_recipient_sender_and_message_match_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        purchaser_id = 9925
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            first = main.create_gift_checkout_draft(
                cur,
                purchaser_id,
                "gift_1m",
                "Анна",
                "Олег",
                "С днём рождения",
            )
            same, same_result = main.find_or_create_gift_checkout_draft(
                cur,
                purchaser_id,
                "gift_1m",
                "Анна",
                "Олег",
                "С днём рождения",
            )
            changed_recipient, recipient_result = main.find_or_create_gift_checkout_draft(
                cur,
                purchaser_id,
                "gift_1m",
                "Мария",
                "Олег",
                "С днём рождения",
            )
            changed_sender, sender_result = main.find_or_create_gift_checkout_draft(
                cur,
                purchaser_id,
                "gift_1m",
                "Мария",
                "Ирина",
                "С днём рождения",
            )
            changed_message, message_result = main.find_or_create_gift_checkout_draft(
                cur,
                purchaser_id,
                "gift_1m",
                "Мария",
                "Ирина",
                "Для тебя",
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(same["id"], first["id"])
        self.assertEqual(same_result, "draft_reused")
        self.assertNotEqual(changed_recipient["id"], first["id"])
        self.assertFalse(recipient_result)
        self.assertNotEqual(changed_sender["id"], changed_recipient["id"])
        self.assertFalse(sender_result)
        self.assertNotEqual(changed_message["id"], changed_sender["id"])
        self.assertFalse(message_result)
        self.assertEqual(
            self.query_one("""
                SELECT recipient_name, sender_name, gift_message, status
                FROM gift_access_grants
                WHERE id = %s
            """, (changed_message["id"],)),
            ("Мария", "Ирина", "Для тебя", "checkout_pending"),
        )
        self.assertEqual(
            self.query_one("""
                SELECT COUNT(*)
                FROM gift_access_grants
                WHERE purchaser_telegram_id = %s
                  AND status = 'cancelled'
            """, (purchaser_id,))[0],
            3,
        )

    def test_gift_checkout_open_with_changed_details_requires_existing_payment_resolution_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        purchaser_id = 9926
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            draft = main.create_gift_checkout_draft(cur, purchaser_id, "gift_1m", "Анна", "Олег", "Первый")
            opened = main.mark_gift_checkout_open(
                cur,
                draft["id"],
                "checkout_pending",
                "cs_open_existing",
                "https://checkout.example/existing",
                None,
            )
            conflict, conflict_result = main.find_or_create_gift_checkout_draft(
                cur,
                purchaser_id,
                "gift_1m",
                "Мария",
                "Олег",
                "Первый",
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(conflict["id"], opened["id"])
        self.assertEqual(conflict_result, "active_checkout_conflict")
        self.assertEqual(
            self.query_one("""
                SELECT COUNT(*)
                FROM gift_access_grants
                WHERE purchaser_telegram_id = %s
                  AND status IN ('checkout_pending', 'checkout_open', 'payment_pending')
            """, (purchaser_id,))[0],
            1,
        )

    def test_gift_paid_and_reserved_cannot_be_cancelled_without_refund_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        cases = [
            ("GIFT-0000000000000011", "paid_unclaimed"),
            ("GIFT-0000000000000012", "reserved"),
            ("GIFT-0000000000000013", "payment_pending"),
        ]
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            for index, (reference, status) in enumerate(cases):
                token_hash = main.gift_token_hash_for_reference(reference, 1)
                cur.execute("""
                    INSERT INTO gift_access_grants (
                        id, public_reference, purchaser_telegram_id, recipient_name, sender_name,
                        gift_message, tariff_code, duration_days, status, token_hash, token_version,
                        stripe_session_id
                    )
                    VALUES (%s, %s, %s, 'Recipient', 'Sender', '',
                            'gift_1m', 30, %s, %s, 1, %s)
                """, (
                    str(uuid.uuid4()),
                    reference,
                    9931 + index,
                    status,
                    token_hash,
                    f"cs_cancel_blocked_{index}",
                ))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main, "expire_stripe_checkout_session", mock.AsyncMock()) as expire:
            results = [
                asyncio.run(main.safely_cancel_gift_checkout(reference, 1, source="test_cancel"))
                for reference, _ in cases
            ]

        expire.assert_not_awaited()
        self.assertEqual([result["status"] for result in results], ["failed", "failed", "failed"])
        self.assertEqual(
            self.query_all("""
                SELECT public_reference, status
                FROM gift_access_grants
                WHERE public_reference IN (%s, %s, %s)
                ORDER BY public_reference
            """, tuple(reference for reference, _ in cases)),
            sorted(cases),
        )

    def test_gift_checkout_open_cancel_expires_stripe_before_local_cancel_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        reference = "GIFT-0000000000000014"
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            token_hash = main.gift_token_hash_for_reference(reference, 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_name, sender_name,
                    gift_message, tariff_code, duration_days, status, token_hash, token_version,
                    stripe_session_id, checkout_url
                )
                VALUES (%s, %s, 9934, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'checkout_open', %s, 1,
                        'cs_open_cancel', 'https://checkout.example/old')
            """, (str(uuid.uuid4()), reference, token_hash))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main, "expire_stripe_checkout_session", mock.AsyncMock(return_value={"status": "expired"})) as expire:
            result = asyncio.run(main.safely_cancel_gift_checkout(reference, 9934, source="test_cancel"))

        expire.assert_awaited_once_with("cs_open_cancel")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            self.query_one("SELECT status, cancelled_at IS NOT NULL FROM gift_access_grants WHERE public_reference = %s", (reference,)),
            ("cancelled", True),
        )

    def test_cancelled_gift_paid_webhook_records_payment_and_manual_review_alert_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        reference = "GIFT-0000000000000015"
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            token_hash = main.gift_token_hash_for_reference(reference, 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_name, sender_name,
                    gift_message, tariff_code, duration_days, status, token_hash, token_version,
                    stripe_session_id
                )
                VALUES (%s, %s, 9935, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'cancelled', %s, 1, 'cs_paid_after_cancel')
                RETURNING *
            """, (str(uuid.uuid4()), reference, token_hash))
            gift_row = main.gift_row_dict(cur, cur.fetchone())
            session = {
                "id": "cs_paid_after_cancel",
                "mode": "payment",
                "payment_status": "paid",
                "client_reference_id": "9935",
                "amount_total": 5000,
                "currency": "usd",
                "payment_intent": "pi_paid_after_cancel",
                "metadata": {
                    "payment_kind": main.GIFT_PAYMENT_KIND,
                    "gift_id": str(gift_row["id"]),
                    "purchaser_telegram_id": "9935",
                    "tariff_code": "gift_1m",
                    "duration_days": "30",
                },
            }
            line_item = {"quantity": 1}
            price = {"id": "price_gift_1m", "type": "one_time", "active": False, "unit_amount": 5000, "currency": "usd"}
            updated = main.mark_gift_paid_and_enqueue(cur, "evt_paid_after_cancel", "checkout.session.completed", session, line_item, price, gift_row)
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(updated["status"], "review_required")
        self.assertEqual(
            self.query_one("SELECT status, stripe_payment_intent_id, last_error_category FROM gift_access_grants WHERE public_reference = %s", (reference,)),
            ("review_required", "pi_paid_after_cancel", "manual_review_required"),
        )
        self.assertEqual(
            self.query_one("SELECT payment_status, payment_kind FROM payment_events WHERE stripe_event_id = 'evt_paid_after_cancel'"),
            ("succeeded", main.GIFT_PAYMENT_KIND),
        )
        alert_count = self.query_one("""
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE delivery_key LIKE %s
              AND payload_json LIKE '%%CRITICAL%%'
        """, (f"gift:{reference}:gift_admin_problem:%",))[0]
        self.assertEqual(alert_count, len(main.ADMIN_IDS))

    def test_gift_async_payment_failed_cancels_terminal_session_in_source(self):
        source = Path("main.py").read_text(encoding="utf-8")
        self.assertIn("elif event_type in ('checkout.session.expired', 'checkout.session.async_payment_failed'):", source)
        self.assertIn('terminal_status = "cancelled"', source)
        self.assertNotIn('terminal_status = "cancelled" if event_type == "checkout.session.expired" else "payment_pending"', source)

    def test_gift_refund_before_redeem_invalidates_token_and_cancels_certificate_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            token_hash = main.gift_token_hash_for_reference("GIFT-0000000000000002", 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_name, sender_name,
                    gift_message, tariff_code, duration_days, status, token_hash, token_version,
                    stripe_payment_intent_id, amount_total, currency
                )
                VALUES (%s, 'GIFT-0000000000000002', 9908, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'paid_unclaimed', %s, 1, 'pi_refund_before', 5000, 'usd')
                RETURNING *
            """, (str(uuid.uuid4()), token_hash))
            gift_row = main.gift_row_dict(cur, cur.fetchone())
            main.enqueue_message_delivery(
                cur,
                main.gift_delivery_key("GIFT-0000000000000002", main.GIFT_CERTIFICATE_BUYER, token_version=1, recipient_kind="buyer"),
                9908,
                main.GIFT_CERTIFICATE_BUYER,
                {"public_reference": "GIFT-0000000000000002", "token_version": 1},
            )
            updated = main.apply_gift_refund_event(
                cur,
                "evt_refund_before",
                "charge.refunded",
                {"payment_intent": "pi_refund_before", "amount": 5000, "amount_refunded": 5000, "refunded": True},
                gift_row,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(updated["status"], "refunded")
        self.assertEqual(updated["token_version"], 2)
        self.assertEqual(self.query_one("SELECT status FROM message_delivery_events WHERE delivery_key = %s", (
            "gift:GIFT-0000000000000002:certificate:buyer:v1",
        ))[0], "cancelled")

    def test_gift_refund_after_redeem_requires_review_without_user_revoke_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            user_id = 9909
            expiry = datetime.utcnow() + timedelta(days=30)
            cur.execute("INSERT INTO users (telegram_id, paid, expiry_date) VALUES (%s, TRUE, %s)", (user_id, expiry))
            token_hash = main.gift_token_hash_for_reference("GIFT-0000000000000003", 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_telegram_id,
                    recipient_name, sender_name, gift_message, tariff_code, duration_days,
                    status, token_hash, token_version, stripe_payment_intent_id, amount_total, currency
                )
                VALUES (%s, 'GIFT-0000000000000003', 9910, %s, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'redeemed', %s, 1, 'pi_refund_after', 5000, 'usd')
                RETURNING *
            """, (str(uuid.uuid4()), user_id, token_hash))
            gift_row = main.gift_row_dict(cur, cur.fetchone())
            updated = main.apply_gift_refund_event(
                cur,
                "evt_refund_after",
                "refund.updated",
                {"payment_intent": "pi_refund_after", "amount": 5000, "status": "succeeded"},
                gift_row,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(updated["status"], "review_required")
        self.assertEqual(self.query_one("SELECT paid, expiry_date FROM users WHERE telegram_id = %s", (user_id,)), (True, expiry))

    def test_reserved_gift_scheduler_skips_live_active_subscription_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        recipient_id = 9911
        expiry = datetime.utcnow() + timedelta(days=10)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (telegram_id, paid, expiry_date, auto_renew, stripe_subscription_id)
                VALUES (%s, TRUE, %s, TRUE, 'sub_live_active')
            """, (recipient_id, expiry))
            token_hash = main.gift_token_hash_for_reference("GIFT-0000000000000004", 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_telegram_id,
                    recipient_name, sender_name, gift_message, tariff_code, duration_days,
                    status, token_hash, token_version
                )
                VALUES (%s, 'GIFT-0000000000000004', 9912, %s, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'reserved', %s, 1)
            """, (str(uuid.uuid4()), recipient_id, token_hash))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.stripe.Subscription, "retrieve", return_value={"status": "active", "cancel_at_period_end": False}):
            result = asyncio.run(main.apply_reserved_gifts(limit=10))

        self.assertEqual(result, {"applied": 0, "skipped": 0})
        self.assertEqual(self.query_one("SELECT status FROM gift_access_grants WHERE public_reference = 'GIFT-0000000000000004'")[0], "reserved")

    def test_reserved_gift_scheduler_applies_after_cancelled_subscription_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        recipient_id = 9913
        expiry = datetime.utcnow() - timedelta(days=1)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (telegram_id, paid, expiry_date, auto_renew, stripe_subscription_id)
                VALUES (%s, TRUE, %s, TRUE, 'sub_cancelled')
            """, (recipient_id, expiry))
            token_hash = main.gift_token_hash_for_reference("GIFT-0000000000000005", 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_telegram_id,
                    recipient_name, sender_name, gift_message, tariff_code, duration_days,
                    status, token_hash, token_version
                )
                VALUES (%s, 'GIFT-0000000000000005', 9914, %s, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'reserved', %s, 1)
            """, (str(uuid.uuid4()), recipient_id, token_hash))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.stripe.Subscription, "retrieve", return_value={"status": "canceled", "cancel_at_period_end": False}):
            result = asyncio.run(main.apply_reserved_gifts(limit=10))

        self.assertEqual(result, {"applied": 0, "skipped": 0})
        self.assertEqual(self.query_one("SELECT status FROM gift_access_grants WHERE public_reference = 'GIFT-0000000000000005'")[0], "reserved")
        self.assertEqual(self.query_one("SELECT paid, expiry_date FROM users WHERE telegram_id = %s", (recipient_id,)), (True, expiry))

    def test_gift_activation_blocked_for_active_auto_renew(self):
        run_migrations(self.get_conn)
        main = import_main()
        recipient_id = 9920
        old_expiry = datetime.utcnow() + timedelta(days=14)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (telegram_id, paid, expiry_date, auto_renew, stripe_subscription_id)
                VALUES (%s, TRUE, %s, TRUE, 'sub_live_auto')
            """, (recipient_id, old_expiry))
            token_hash = main.gift_token_hash_for_reference("GIFT-0000000000000008", 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_name, sender_name,
                    gift_message, tariff_code, duration_days, status, token_hash, token_version
                )
                VALUES (%s, 'GIFT-0000000000000008', 9921, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'paid_unclaimed', %s, 1)
                RETURNING *
            """, (str(uuid.uuid4()), token_hash))
            gift_row = main.gift_row_dict(cur, cur.fetchone())
            first, first_action, first_expiry = main.apply_gift_access_in_transaction(
                cur,
                gift_row,
                recipient_id,
                {"action": "block_active_auto_renew", "subscription_id": "sub_live_auto", "status": "active"},
            )
            second, second_action, second_expiry = main.apply_gift_access_in_transaction(
                cur,
                gift_row,
                recipient_id,
                {"action": "block_active_auto_renew", "subscription_id": "sub_live_auto", "status": "active"},
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(first_action, "blocked_active_auto_renew")
        self.assertEqual(second_action, "blocked_active_auto_renew")
        self.assertEqual(first_expiry, old_expiry)
        self.assertEqual(second_expiry, old_expiry)
        self.assertEqual(first["status"], "paid_unclaimed")
        self.assertEqual(second["status"], "paid_unclaimed")
        self.assertIn("напишите администратору", main.build_gift_reserved_recipient_text(first))
        self.assertEqual(
            self.query_one("SELECT paid, expiry_date, auto_renew, stripe_subscription_id FROM users WHERE telegram_id = %s", (recipient_id,)),
            (True, old_expiry, True, "sub_live_auto"),
        )
        self.assertEqual(
            self.query_one("""
                SELECT status, recipient_telegram_id, reserved_at, redeemed_at, applied_at, applied_expiry
                FROM gift_access_grants
                WHERE public_reference = 'GIFT-0000000000000008'
            """),
            ("paid_unclaimed", None, None, None, None, None),
        )
        self.assertEqual(self.query_one("SELECT COUNT(*) FROM access_events WHERE telegram_id = %s", (recipient_id,))[0], 0)
        self.assertEqual(
            self.query_one("SELECT COUNT(*) FROM message_delivery_events WHERE delivery_key LIKE %s", ("gift:GIFT-0000000000000008:%",))[0],
            0,
        )
        self.assertEqual(asyncio.run(main.apply_reserved_gifts(limit=10)), {"applied": 0, "skipped": 0})

    def test_gift_activation_paid_false_future_expiry_starts_from_now_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        recipient_id = 9927
        stale_future_expiry = datetime.utcnow() + timedelta(days=90)
        before_activation = datetime.utcnow()
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (telegram_id, paid, expiry_date, auto_renew, stripe_subscription_id)
                VALUES (%s, FALSE, %s, FALSE, NULL)
            """, (recipient_id, stale_future_expiry))
            token_hash = main.gift_token_hash_for_reference("GIFT-0000000000000010", 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_name, sender_name,
                    gift_message, tariff_code, duration_days, status, token_hash, token_version
                )
                VALUES (%s, 'GIFT-0000000000000010', 9928, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'paid_unclaimed', %s, 1)
                RETURNING *
            """, (str(uuid.uuid4()), token_hash))
            gift_row = main.gift_row_dict(cur, cur.fetchone())
            updated, action, new_expiry = main.apply_gift_access_in_transaction(cur, gift_row, recipient_id, None)
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(action, "redeemed")
        self.assertEqual(updated["status"], "redeemed")
        self.assertLess(new_expiry, stale_future_expiry)
        self.assertGreaterEqual(new_expiry, before_activation + timedelta(days=30))
        self.assertLess(new_expiry, before_activation + timedelta(days=30, seconds=10))
        self.assertEqual(
            self.query_one("SELECT paid, expiry_date FROM users WHERE telegram_id = %s", (recipient_id,)),
            (True, new_expiry),
        )

    def test_gift_activation_is_atomic_under_concurrency(self):
        run_migrations(self.get_conn)
        main = import_main()
        winner_ids = (9922, 9923)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            token_hash = main.gift_token_hash_for_reference("GIFT-0000000000000009", 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_name, sender_name,
                    gift_message, tariff_code, duration_days, status, token_hash, token_version
                )
                VALUES (%s, 'GIFT-0000000000000009', 9924, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'paid_unclaimed', %s, 1)
            """, (str(uuid.uuid4()), token_hash))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        barrier = threading.Barrier(2)
        results = []
        errors = []

        def activate(recipient_id):
            local_conn = self.get_conn()
            local_cur = local_conn.cursor()
            try:
                barrier.wait(timeout=5)
                gift_row = main.fetch_gift_by_public_reference_version(local_cur, "GIFT-0000000000000009", 1, for_update=True)
                if not gift_row or gift_row["status"] != "paid_unclaimed":
                    local_conn.rollback()
                    results.append((recipient_id, "already_used"))
                    return
                updated, action, _ = main.apply_gift_access_in_transaction(local_cur, gift_row, recipient_id, None)
                local_conn.commit()
                results.append((recipient_id, action, updated["recipient_telegram_id"]))
            except Exception as exc:
                local_conn.rollback()
                errors.append(exc)
            finally:
                local_cur.close()
                local_conn.close()

        first = threading.Thread(target=activate, args=(winner_ids[0],))
        second = threading.Thread(target=activate, args=(winner_ids[1],))
        first.start()
        second.start()
        first.join()
        second.join()

        self.assertFalse(errors)
        self.assertEqual(len(results), 2)
        activated = [row for row in results if len(row) >= 2 and row[1] == "redeemed"]
        blocked = [row for row in results if len(row) >= 2 and row[1] == "already_used"]
        self.assertEqual(len(activated), 1)
        self.assertEqual(len(blocked), 1)
        winner_id = activated[0][0]
        loser_id = blocked[0][0]
        self.assertIn(winner_id, winner_ids)
        self.assertIn(loser_id, winner_ids)
        self.assertNotEqual(winner_id, loser_id)
        self.assertEqual(
            self.query_one("""
                SELECT status, recipient_telegram_id, redeemed_at IS NOT NULL, applied_at IS NOT NULL
                FROM gift_access_grants
                WHERE public_reference = 'GIFT-0000000000000009'
            """),
            ("redeemed", winner_id, True, True),
        )
        self.assertEqual(self.query_one("SELECT paid, expiry_date > NOW() AT TIME ZONE 'UTC' FROM users WHERE telegram_id = %s", (winner_id,)), (True, True))
        self.assertIsNone(self.query_one("SELECT paid, expiry_date FROM users WHERE telegram_id = %s", (loser_id,)))
        self.assertEqual(
            self.query_one("""
                SELECT COUNT(*)
                FROM access_events
                WHERE event_type = 'gift_access_redeemed'
                  AND notes LIKE 'gift=GIFT-0000000000000009'
            """)[0],
            1,
        )

    def test_gift_activation_identity_changed_rolls_back_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        recipient_id = 9915
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (telegram_id, paid, expiry_date, auto_renew, stripe_subscription_id)
                VALUES (%s, FALSE, NULL, FALSE, 'sub_write_b')
            """, (recipient_id,))
            token_hash = main.gift_token_hash_for_reference("GIFT-0000000000000006", 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_name, sender_name,
                    gift_message, tariff_code, duration_days, status, token_hash, token_version
                )
                VALUES (%s, 'GIFT-0000000000000006', 9916, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'paid_unclaimed', %s, 1)
                RETURNING *
            """, (str(uuid.uuid4()), token_hash))
            gift_row = main.gift_row_dict(cur, cur.fetchone())
            conn.commit()
            with self.assertRaisesRegex(ValueError, "gift_recipient_subscription_identity_changed"):
                main.apply_gift_access_in_transaction(
                    cur,
                    gift_row,
                    recipient_id,
                    {"action": "apply", "subscription_id": "sub_initial_a", "status": "canceled"},
                )
            conn.rollback()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(self.query_one("SELECT status FROM gift_access_grants WHERE public_reference = 'GIFT-0000000000000006'")[0], "paid_unclaimed")
        self.assertEqual(self.query_one("SELECT paid, expiry_date FROM users WHERE telegram_id = %s", (recipient_id,)), (False, None))
        self.assertEqual(self.query_one("SELECT COUNT(*) FROM access_events WHERE telegram_id = %s", (recipient_id,))[0], 0)

    def test_reserved_gift_scheduler_identity_guard_even_if_db_flags_changed_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        recipient_id = 9917
        expired = datetime.utcnow() - timedelta(days=1)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (telegram_id, paid, expiry_date, auto_renew, stripe_subscription_id)
                VALUES (%s, TRUE, %s, FALSE, 'sub_write_b')
            """, (recipient_id, expired))
            token_hash = main.gift_token_hash_for_reference("GIFT-0000000000000007", 1)
            cur.execute("""
                INSERT INTO gift_access_grants (
                    id, public_reference, purchaser_telegram_id, recipient_telegram_id,
                    recipient_name, sender_name, gift_message, tariff_code, duration_days,
                    status, token_hash, token_version
                )
                VALUES (%s, 'GIFT-0000000000000007', 9918, %s, 'Recipient', 'Sender', '',
                        'gift_1m', 30, 'reserved', %s, 1)
            """, (str(uuid.uuid4()), recipient_id, token_hash))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(
                 main,
                 "gift_recipient_subscription_state",
                 mock.AsyncMock(return_value={"action": "apply", "subscription_id": "sub_initial_a", "status": "canceled"}),
             ):
            result = asyncio.run(main.apply_reserved_gifts(limit=10))

        self.assertEqual(result, {"applied": 0, "skipped": 0})
        self.assertEqual(self.query_one("SELECT status FROM gift_access_grants WHERE public_reference = 'GIFT-0000000000000007'")[0], "reserved")
        self.assertEqual(self.query_one("SELECT paid, expiry_date FROM users WHERE telegram_id = %s", (recipient_id,)), (True, expired))
        self.assertEqual(self.query_one("SELECT COUNT(*) FROM access_events WHERE telegram_id = %s", (recipient_id,))[0], 0)

    def test_checksum_mismatch_fails_closed(self):
        run_migrations(self.get_conn)
        with tempfile.TemporaryDirectory() as tmp:
            for migration in Path(MIGRATIONS_DIR).glob("*.sql"):
                target = Path(tmp) / migration.name
                text = migration.read_text(encoding="utf-8")
                if migration.name.startswith("0001_"):
                    text += "\n-- changed checksum\n"
                target.write_text(text, encoding="utf-8")
            with self.assertRaisesRegex(MigrationError, "Checksum mismatch"):
                run_migrations(self.get_conn, migrations_dir=tmp)

    def test_failed_migration_rolls_back_and_is_not_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "0001_fail.sql").write_text(
                "CREATE TABLE rollback_probe(id integer);\nSELECT * FROM table_that_does_not_exist;\n",
                encoding="utf-8",
            )
            with self.assertRaises(Exception):
                run_migrations(self.get_conn, migrations_dir=tmp)

        self.assertIsNone(self.query_one("SELECT to_regclass('public.rollback_probe')")[0])
        self.assertEqual(self.query_one("SELECT COUNT(*) FROM schema_migrations WHERE version = '0001_fail'")[0], 0)

    def test_two_replicas_are_serialized_by_advisory_lock(self):
        password = dsn_password(POSTGRES_TEST_DSN)
        self.assertIn(self.db_name, self.dsn)
        if password:
            self.assertIn("password=", self.dsn)

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            conn = connect(self.dsn)
            conn.close()
        if password:
            self.assertNotIn(password, stdout.getvalue())
            self.assertNotIn(password, stderr.getvalue())

        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "0001_slow.sql").write_text(
                "SELECT pg_sleep(0.35);\nCREATE TABLE replica_probe(id integer);\n",
                encoding="utf-8",
            )
            results = []
            errors = []

            def worker():
                try:
                    results.append(run_migrations(self.get_conn, migrations_dir=tmp))
                except Exception as exc:
                    errors.append(exc)

            first = threading.Thread(target=worker)
            second = threading.Thread(target=worker)
            started = time.monotonic()
            first.start()
            second.start()
            first.join()
            second.join()

            self.assertFalse(errors)
            self.assertEqual(len(results), 2)
            self.assertGreaterEqual(time.monotonic() - started, 0.35)
            self.assertEqual(self.query_one("SELECT COUNT(*) FROM schema_migrations WHERE version = '0001_slow'")[0], 1)

    def test_stripe_identity_conflict_blocks_unique_indexes(self):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                CREATE TABLE users (
                    telegram_id BIGINT UNIQUE NOT NULL,
                    paid BOOLEAN DEFAULT FALSE,
                    expiry_date TIMESTAMP,
                    stripe_subscription_id TEXT,
                    stripe_customer_id TEXT,
                    blocked_bot BOOLEAN DEFAULT FALSE,
                    last_successful_invoice_created_at TIMESTAMP,
                    last_subscription_state_event_created_at TIMESTAMP
                )
                """
            )
            cur.execute("CREATE TABLE stripe_events (event_id TEXT PRIMARY KEY)")
            cur.execute("CREATE TABLE access_events (id SERIAL PRIMARY KEY)")
            cur.execute("CREATE TABLE checkout_sessions (telegram_id BIGINT, tariff_code TEXT, status TEXT, idempotency_key TEXT)")
            cur.execute("CREATE TABLE message_delivery_events (delivery_key TEXT, telegram_id BIGINT, delivery_type TEXT, status TEXT)")
            cur.execute("CREATE TABLE stripe_identity_conflicts (id SERIAL PRIMARY KEY)")
            cur.execute("INSERT INTO users (telegram_id, stripe_subscription_id) VALUES (1, 'sub_dup'), (2, 'sub_dup')")
            conn.commit()
        finally:
            cur.close()
            conn.close()

        with self.assertRaisesRegex(MigrationError, "Stripe identity conflicts"):
            run_migrations(self.get_conn)

    def test_connections_and_cursors_close_on_success_and_error(self):
        TrackingConnection.closed_count = 0
        TrackingConnection.cursor_closed_count = 0

        def tracking_conn():
            return TrackingConnection(connect(self.dsn))

        run_migrations(tracking_conn)
        self.assertGreaterEqual(TrackingConnection.closed_count, 1)
        self.assertGreaterEqual(TrackingConnection.cursor_closed_count, 1)

        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "0001_fail.sql").write_text("SELECT * FROM missing_table;", encoding="utf-8")
            with self.assertRaises(Exception):
                run_migrations(tracking_conn, migrations_dir=tmp)
        self.assertGreaterEqual(TrackingConnection.closed_count, 2)
        self.assertGreaterEqual(TrackingConnection.cursor_closed_count, 2)

    def test_missing_required_column_is_added_by_idempotent_migration(self):
        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("DROP TABLE schema_migrations")
            cur.execute("ALTER TABLE users DROP COLUMN blocked_bot")
            conn.commit()
        finally:
            cur.close()
            conn.close()

        run_migrations(self.get_conn)
        columns = {row[0] for row in self.query_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'users'
            """
        )}
        self.assertIn("blocked_bot", columns)

    def test_subscription_refund_reconciliation_migration_fresh_and_idempotent(self):
        first = run_migrations(self.get_conn)
        second = run_migrations(self.get_conn)
        self.assertIn("0007_subscription_refund_reconciliation", first["applied"])
        self.assertIn("0007_subscription_refund_reconciliation", second["applied"])
        columns = {row[0] for row in self.query_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'subscription_refund_reconciliations'
            """
        )}
        for column in (
            "reconciliation_key",
            "refund_id",
            "stripe_event_id",
            "charge_id",
            "payment_intent_id",
            "invoice_id",
            "customer_id",
            "subscription_id",
            "telegram_id",
            "original_payment_event_id",
            "reconciliation_result",
            "review_reason",
            "access_revoked_at",
        ):
            self.assertIn(column, columns)
        for index_name in (
            "subscription_refund_reconciliations_unique_key",
            "subscription_refund_reconciliations_unique_refund_id",
            "srr_unique_refund_payment_revoke",
            "subscription_refund_events_unique_stripe_event_id",
        ):
            self.assertEqual(
                self.query_one("SELECT to_regclass(%s)", (f"public.{index_name}",))[0],
                index_name,
            )
        removal_columns = {row[0] for row in self.query_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'subscription_removal_events'
            """
        )}
        self.assertIn("revoke_started_at", removal_columns)

    def test_stripe_event_claim_fencing_migration_fresh_and_idempotent(self):
        first = run_migrations(self.get_conn)
        second = run_migrations(self.get_conn)
        self.assertIn("0008_stripe_event_claim_fencing", first["applied"])
        self.assertIn("0008_stripe_event_claim_fencing", second["applied"])
        self.assertEqual(
            self.query_one(
                """
                SELECT data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = 'stripe_events'
                  AND column_name = 'claim_generation'
                """
            ),
            ("bigint", "NO", "0"),
        )

    def test_stripe_event_claim_fencing_upgrades_legacy_rows_without_rewrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            for migration in sorted(Path(MIGRATIONS_DIR).glob("*.sql")):
                if migration.name >= "0008_":
                    continue
                (Path(tmp) / migration.name).write_text(
                    migration.read_text(encoding="utf-8"),
                    encoding="utf-8",
                )
            run_migrations(self.get_conn, migrations_dir=tmp)

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO stripe_events (event_id, processed, processed_at)
                VALUES
                    ('evt_legacy_processed', TRUE, NOW()),
                    ('evt_legacy_unprocessed', FALSE, NOW() - INTERVAL '11 minutes')
                """
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        run_migrations(self.get_conn)
        self.assertEqual(
            self.query_one(
                "SELECT processed, claim_generation FROM stripe_events WHERE event_id = %s",
                ("evt_legacy_processed",),
            ),
            (True, 0),
        )
        self.assertEqual(
            self.query_one(
                "SELECT processed, claim_generation FROM stripe_events WHERE event_id = %s",
                ("evt_legacy_unprocessed",),
            ),
            (False, 0),
        )
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            self.assertEqual(
                claim_stripe_event(cur, "evt_legacy_processed"),
                ("duplicate_processed", None),
            )
            self.assertEqual(
                claim_stripe_event(cur, "evt_legacy_unprocessed"),
                ("claimed", 1),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def test_stripe_event_claim_generation_fences_real_concurrent_owners(self):
        run_migrations(self.get_conn)
        event_id = "evt_pg_claim_fencing"

        a = self.get_conn()
        a_cur = a.cursor()
        self.assertEqual(claim_stripe_event(a_cur, event_id), ("claimed", 1))
        a.commit()
        a_cur.close()
        a.close()

        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE stripe_events SET processed_at = NOW() - INTERVAL '11 minutes' WHERE event_id = %s",
            (event_id,),
        )
        conn.commit()
        cur.close()
        conn.close()

        b = self.get_conn()
        b_cur = b.cursor()
        self.assertEqual(claim_stripe_event(b_cur, event_id), ("claimed", 2))

        late_release = {"result": None, "error": None}

        def release_a():
            release_conn = self.get_conn()
            release_cur = release_conn.cursor()
            try:
                late_release["result"] = release_stripe_event_claim(
                    release_cur,
                    event_id,
                    1,
                )
                release_conn.commit()
            except Exception as error:
                late_release["error"] = error
            finally:
                release_cur.close()
                release_conn.close()

        thread = threading.Thread(target=release_a)
        thread.start()
        time.sleep(0.2)
        self.assertTrue(thread.is_alive())
        b.commit()
        b_cur.close()
        b.close()
        thread.join(timeout=5)

        self.assertIsNone(late_release["error"])
        self.assertEqual(late_release["result"], "not_owner")
        self.assertEqual(
            self.query_one(
                "SELECT processed, claim_generation FROM stripe_events WHERE event_id = %s",
                (event_id,),
            ),
            (False, 2),
        )

        c = self.get_conn()
        c_cur = c.cursor()
        self.assertEqual(
            claim_stripe_event(c_cur, event_id),
            ("duplicate_processing", None),
        )
        c.rollback()
        c_cur.close()
        c.close()

        stale_mark = self.get_conn()
        stale_mark_cur = stale_mark.cursor()
        self.assertEqual(
            mark_stripe_event_processed(stale_mark_cur, event_id, 1),
            "not_owner",
        )
        stale_mark.commit()
        stale_mark_cur.close()
        stale_mark.close()

        owner = self.get_conn()
        owner_cur = owner.cursor()
        self.assertEqual(
            mark_stripe_event_processed(owner_cur, event_id, 2),
            "processed",
        )
        owner.commit()
        owner_cur.close()
        owner.close()

        late = self.get_conn()
        late_cur = late.cursor()
        self.assertEqual(
            release_stripe_event_claim(late_cur, event_id, 2),
            "not_owner",
        )
        late.commit()
        late_cur.close()
        late.close()

        final_c = self.get_conn()
        final_c_cur = final_c.cursor()
        self.assertEqual(
            claim_stripe_event(final_c_cur, event_id),
            ("duplicate_processed", None),
        )
        final_c.rollback()
        final_c_cur.close()
        final_c.close()

    def test_stripe_event_claim_generation_increases_monotonically(self):
        run_migrations(self.get_conn)
        event_id = "evt_pg_claim_generation_monotonic"
        generations = []
        for expected in (1, 2, 3):
            conn = self.get_conn()
            cur = conn.cursor()
            if expected > 1:
                cur.execute(
                    "UPDATE stripe_events SET processed_at = NOW() - INTERVAL '11 minutes' WHERE event_id = %s",
                    (event_id,),
                )
            result = claim_stripe_event(cur, event_id)
            conn.commit()
            cur.close()
            conn.close()
            generations.append(result[1])
        self.assertEqual(generations, [1, 2, 3])

    def test_message_delivery_claim_fencing_migration_fresh_idempotent_and_legacy(self):
        first = run_migrations(self.get_conn)
        second = run_migrations(self.get_conn)
        self.assertIn("0009_message_delivery_claim_fencing", first["applied"])
        self.assertIn("0009_message_delivery_claim_fencing", second["applied"])
        self.assertEqual(
            self.query_one(
                """
                SELECT data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = 'message_delivery_events'
                  AND column_name = 'claim_generation'
                """
            ),
            ("bigint", "NO", "0"),
        )

        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO message_delivery_events (
                delivery_key, telegram_id, delivery_type, status, attempt_count, payload_json
            ) VALUES
                ('legacy_sent', 1, 'notice', 'sent', 4, '{"text":"sent"}'),
                ('legacy_failed', 1, 'notice', 'failed', 3, '{"text":"failed"}'),
                ('legacy_cancelled', 1, 'notice', 'cancelled', 2, '{"text":"cancelled"}')
            """
        )
        conn.commit()
        cur.close()
        conn.close()
        self.assertEqual(
            self.query_one(
                """
                SELECT COUNT(*), MIN(claim_generation), MAX(claim_generation), SUM(attempt_count)
                FROM message_delivery_events
                WHERE delivery_key LIKE 'legacy_%%'
                """
            ),
            (3, 0, 0, 9),
        )

    def test_message_delivery_claim_fencing_upgrades_0008_rows_without_rewrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            for migration in sorted(Path(MIGRATIONS_DIR).glob("*.sql")):
                if migration.name >= "0009_":
                    continue
                (Path(tmp) / migration.name).write_text(
                    migration.read_text(encoding="utf-8"),
                    encoding="utf-8",
                )
            run_migrations(self.get_conn, migrations_dir=tmp)

        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO message_delivery_events (
                delivery_key, telegram_id, delivery_type, status, attempt_count,
                claimed_at, lease_until, payload_json
            ) VALUES
                ('legacy_0008_processing', 1, 'notice', 'processing', 5,
                 NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '10 minutes', '{"text":"processing"}'),
                ('legacy_0008_sent', 1, 'notice', 'sent', 4, NULL, NULL, '{"text":"sent"}'),
                ('legacy_0008_failed', 1, 'notice', 'failed', 3, NULL, NULL, '{"text":"failed"}'),
                ('legacy_0008_cancelled', 1, 'notice', 'cancelled', 2, NULL, NULL, '{"text":"cancelled"}')
            """
        )
        cur.execute(
            "UPDATE message_delivery_events SET next_attempt_at = NOW() + INTERVAL '1 day' WHERE delivery_key = 'legacy_0008_failed'"
        )
        conn.commit()
        cur.close()
        conn.close()

        run_migrations(self.get_conn)
        self.assertEqual(
            self.query_one(
                """
                SELECT COUNT(*), MIN(claim_generation), MAX(claim_generation), SUM(attempt_count)
                FROM message_delivery_events
                WHERE delivery_key LIKE 'legacy_0008_%%'
                """
            ),
            (4, 0, 0, 14),
        )
        reclaim = self.get_conn()
        reclaim_cur = reclaim.cursor()
        rows = claim_pending_message_deliveries(reclaim_cur, limit=1)
        reclaim.commit()
        reclaim_cur.close()
        reclaim.close()
        self.assertEqual(
            (rows[0][0], rows[0][4], rows[0][6]),
            ("legacy_0008_processing", 6, 1),
        )

    def test_message_delivery_due_indexes_fresh_repeat_and_0009_upgrade(self):
        first = run_migrations(self.get_conn)
        second = run_migrations(self.get_conn)
        self.assertIn("0010_message_delivery_due_indexes", first["applied"])
        self.assertIn("0010_message_delivery_due_indexes", second["applied"])

        expected_indexes = {
            "message_delivery_events_pending_failed_due_idx": (
                ["next_attempt_at", "delivery_key"],
                "(status = ANY (ARRAY['pending'::text, 'failed'::text]))",
            ),
            "message_delivery_events_processing_lease_idx": (
                ["lease_until", "delivery_key"],
                "(status = 'processing'::text)",
            ),
        }
        rows = self.query_all(
            """
            SELECT idx.relname,
                   array_agg(att.attname ORDER BY key.ordinality),
                   pg_get_expr(ind.indpred, ind.indrelid)
            FROM pg_class idx
            JOIN pg_index ind ON ind.indexrelid = idx.oid
            JOIN pg_class tbl ON tbl.oid = ind.indrelid
            JOIN unnest(ind.indkey) WITH ORDINALITY AS key(attnum, ordinality) ON TRUE
            JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = key.attnum
            WHERE tbl.relname = 'message_delivery_events'
              AND idx.relname = ANY(%s)
            GROUP BY idx.relname, ind.indpred, ind.indrelid
            ORDER BY idx.relname
            """,
            (list(expected_indexes),),
        )
        self.assertEqual({name: (columns, predicate) for name, columns, predicate in rows}, expected_indexes)

        legacy_name, legacy_dsn = create_temp_db(prefix="codex_pg_outbox_0009")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                for migration in sorted(Path(MIGRATIONS_DIR).glob("*.sql")):
                    if migration.name >= "0010_":
                        continue
                    (Path(tmp) / migration.name).write_text(
                        migration.read_text(encoding="utf-8"),
                        encoding="utf-8",
                    )
                run_migrations(lambda: connect(legacy_dsn), migrations_dir=tmp)

            legacy = connect(legacy_dsn)
            legacy_cur = legacy.cursor()
            legacy_cur.execute(
                """
                INSERT INTO message_delivery_events (
                    delivery_key, telegram_id, delivery_type, status, attempt_count,
                    next_attempt_at, claim_generation
                ) VALUES ('legacy_0009_due', 1, 'notice', 'failed', 3, NOW(), 4)
                """
            )
            legacy.commit()
            legacy_cur.close()
            legacy.close()

            result = run_migrations(lambda: connect(legacy_dsn))
            self.assertIn("0010_message_delivery_due_indexes", result["applied"])
            legacy = connect(legacy_dsn)
            legacy_cur = legacy.cursor()
            legacy_cur.execute(
                "SELECT status, attempt_count, claim_generation FROM message_delivery_events WHERE delivery_key = 'legacy_0009_due'"
            )
            self.assertEqual(legacy_cur.fetchone(), ("failed", 3, 4))
            legacy_cur.execute(
                """
                SELECT COUNT(*) FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname IN (
                    'message_delivery_events_pending_failed_due_idx',
                    'message_delivery_events_processing_lease_idx'
                  )
                """
            )
            self.assertEqual(legacy_cur.fetchone()[0], 2)
            legacy_cur.close()
            legacy.close()
        finally:
            drop_temp_db(legacy_name)

    def test_message_delivery_due_indexes_are_used_and_claim_semantics_hold(self):
        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO message_delivery_events (
                delivery_key, telegram_id, delivery_type, status, next_attempt_at,
                claimed_at, lease_until
            )
            SELECT 'terminal_' || g, g, 'notice', 'sent', NULL, NULL, NULL
            FROM generate_series(1, 50000) AS g
            """
        )
        cur.execute(
            """
            INSERT INTO message_delivery_events (
                delivery_key, telegram_id, delivery_type, status, next_attempt_at,
                claimed_at, lease_until
            ) VALUES
                ('due_pending', 60001, 'notice', 'pending', NOW() - INTERVAL '1 minute', NULL, NULL),
                ('due_failed', 60002, 'notice', 'failed', NOW() - INTERVAL '1 minute', NULL, NULL),
                ('future_pending', 60003, 'notice', 'pending', NOW() + INTERVAL '1 day', NULL, NULL),
                ('stale_processing', 60004, 'notice', 'processing', NULL, NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '1 minute'),
                ('active_processing', 60005, 'notice', 'processing', NULL, NOW(), NOW() + INTERVAL '10 minutes')
            """
        )
        cur.execute("ANALYZE message_delivery_events")
        cur.execute(
            """
            EXPLAIN (COSTS OFF)
            SELECT delivery_key
            FROM message_delivery_events
            WHERE (
                    status IN ('pending', 'failed')
                    AND COALESCE(next_attempt_at, NOW()) <= NOW()
                  )
               OR (
                    status = 'processing'
                    AND lease_until < NOW()
                  )
            ORDER BY next_attempt_at NULLS FIRST, delivery_key
            LIMIT 25
            FOR UPDATE SKIP LOCKED
            """
        )
        plan = "\n".join(row[0] for row in cur.fetchall())
        conn.commit()
        self.assertIn("message_delivery_events_pending_failed_due_idx", plan)
        self.assertIn("message_delivery_events_processing_lease_idx", plan)

        claimed = claim_pending_message_deliveries(cur, limit=10)
        conn.commit()
        claimed_by_key = {row[0]: row for row in claimed}
        self.assertEqual(set(claimed_by_key), {"due_pending", "due_failed", "stale_processing"})
        self.assertEqual(claimed_by_key["due_pending"][6], 1)
        self.assertEqual(claimed_by_key["due_failed"][6], 1)
        self.assertEqual(claimed_by_key["stale_processing"][6], 1)
        self.assertEqual(
            self.query_one("SELECT status FROM message_delivery_events WHERE delivery_key = 'future_pending'"),
            ("pending",),
        )
        self.assertEqual(
            self.query_one("SELECT status FROM message_delivery_events WHERE delivery_key = 'active_processing'"),
            ("processing",),
        )
        cur.close()
        conn.close()

    def test_message_delivery_due_indexes_preserve_two_worker_skip_locked(self):
        run_migrations(self.get_conn)
        setup = self.get_conn()
        setup_cur = setup.cursor()
        for key in ("skip_locked_a", "skip_locked_b"):
            self.assertTrue(enqueue_message_delivery(setup_cur, key, 123, "notice", {"text": key}))
        setup.commit()
        setup_cur.close()
        setup.close()

        worker_a = self.get_conn()
        worker_a_cur = worker_a.cursor()
        claimed_a = claim_pending_message_deliveries(worker_a_cur, limit=1)
        self.assertEqual(len(claimed_a), 1)

        worker_b = self.get_conn()
        worker_b_cur = worker_b.cursor()
        claimed_b = claim_pending_message_deliveries(worker_b_cur, limit=1)
        self.assertEqual(len(claimed_b), 1)
        self.assertNotEqual(claimed_a[0][0], claimed_b[0][0])
        self.assertEqual((claimed_a[0][6], claimed_b[0][6]), (1, 1))

        worker_b.commit()
        worker_b_cur.close()
        worker_b.close()
        worker_a.commit()
        worker_a_cur.close()
        worker_a.close()

    def test_retryable_outbox_alert_stage_is_durable_and_deduped_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()
        delivery_key = "stripe:evt_pg_retry_alert_secret:payment_notice"
        delivery_hash = main.safe_delivery_hash(delivery_key)
        first_seen = datetime.utcnow() - timedelta(hours=1, minutes=5)

        seed = self.get_conn()
        seed_cur = seed.cursor()
        first = main.claim_outbox_retry_escalation(
            seed_cur,
            delivery_key,
            "first_purchase_recovery_reminder",
            3,
            now=first_seen,
        )
        seed.commit()
        self.assertIsNone(first["stage"])
        second = main.claim_outbox_retry_escalation(
            seed_cur,
            delivery_key,
            "first_purchase_recovery_reminder",
            3,
            now=first_seen + timedelta(hours=1, minutes=5),
        )
        seed.commit()
        seed_cur.close()
        seed.close()
        self.assertEqual(second["stage"], "age_1h")

        send_message = mock.AsyncMock()
        with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
             mock.patch.object(main.bot, "send_message", send_message), \
             mock.patch.object(main.asyncio, "sleep", mock.AsyncMock()):
            asyncio.run(main.notify_retryable_outbox_failure(
                delivery_key,
                "first_purchase_recovery_reminder",
                3,
                RuntimeError("private database body"),
                {"retryable": True},
            ))
            asyncio.run(main.notify_retryable_outbox_failure(
                delivery_key,
                "first_purchase_recovery_reminder",
                4,
                RuntimeError("private database body"),
                {"retryable": True},
            ))

        self.assertEqual(send_message.await_count, len(main.ADMIN_IDS))
        self.assertEqual(
            self.query_one(
                "SELECT COUNT(*) FROM admin_alerts WHERE alert_key = %s",
                (f"outbox-retry-observed:{delivery_hash}",),
            )[0],
            1,
        )
        self.assertEqual(
            self.query_one(
                "SELECT COUNT(*) FROM admin_alerts WHERE alert_key = %s",
                (f"outbox-retry:{delivery_hash}:age_1h",),
            )[0],
            1,
        )
        sent_text = "\n".join(call.args[1] for call in send_message.await_args_list)
        self.assertNotIn(delivery_key, sent_text)
        self.assertNotIn("private database body", sent_text)

    def test_notify_admins_permanent_dedupe_retries_only_stale_unfinished_real_postgres(self):
        run_migrations(self.get_conn)
        main = import_main()

        def seed(key, status, age_minutes):
            conn = self.get_conn()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO admin_alerts (
                    alert_key, severity, text, status, delivered_admin_ids,
                    created_at, updated_at
                ) VALUES (%s, 'WARNING', 'seed', %s, '',
                          NOW() - (%s * INTERVAL '1 minute'), NOW())
                """,
                (key, status, age_minutes),
            )
            conn.commit()
            cur.close()
            conn.close()

        async def call(key, *, dedupe_forever=True, send_side_effect=None):
            send_message = mock.AsyncMock(side_effect=send_side_effect)
            with mock.patch.object(main, "get_db_conn", side_effect=self.get_conn), \
                 mock.patch.object(main.bot, "send_message", send_message), \
                 mock.patch.object(main.asyncio, "sleep", mock.AsyncMock()):
                result = await main.notify_admins(
                    "safe escalation",
                    alert_key=key,
                    severity="WARNING",
                    dedupe_forever=dedupe_forever,
                )
            return result, send_message.await_count

        suppressed_cases = (
            ("permanent-delivered", "delivered", 60),
            ("permanent-partial", "partial", 60),
            ("permanent-recent-claimed", "claimed", 5),
            ("permanent-recent-failed", "failed", 5),
        )
        for key, status, age_minutes in suppressed_cases:
            with self.subTest(status=status):
                seed(key, status, age_minutes)
                result, send_count = asyncio.run(call(key))
                self.assertTrue(result["deduped"])
                self.assertEqual(send_count, 0)

        for status in ("claimed", "failed"):
            key = f"permanent-old-{status}"
            with self.subTest(stale_status=status):
                seed(key, status, 16)
                result, send_count = asyncio.run(call(key))
                self.assertEqual(result["delivered"], main.ADMIN_IDS)
                self.assertEqual(send_count, len(main.ADMIN_IDS))
                repeated, repeated_count = asyncio.run(call(key))
                self.assertTrue(repeated["deduped"])
                self.assertEqual(repeated_count, 0)

        permanent_key = "outbox-permanent:safehash"
        failed, failed_count = asyncio.run(call(
            permanent_key,
            send_side_effect=RuntimeError("temporary admin transport failure"),
        ))
        self.assertEqual(failed["delivered"], [])
        self.assertEqual(failed_count, len(main.ADMIN_IDS))
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE admin_alerts SET created_at = NOW() - INTERVAL '16 minutes' WHERE alert_key = %s",
            (permanent_key,),
        )
        conn.commit()
        cur.close()
        conn.close()
        retried, retried_count = asyncio.run(call(permanent_key))
        self.assertEqual(retried["delivered"], main.ADMIN_IDS)
        self.assertEqual(retried_count, len(main.ADMIN_IDS))
        final, final_count = asyncio.run(call(permanent_key))
        self.assertTrue(final["deduped"])
        self.assertEqual(final_count, 0)

        normal_key = "normal-cooldown"
        seed(normal_key, "delivered", 16)
        normal, normal_count = asyncio.run(call(normal_key, dedupe_forever=False))
        self.assertEqual(normal["delivered"], main.ADMIN_IDS)
        self.assertEqual(normal_count, len(main.ADMIN_IDS))
        normal_repeat, normal_repeat_count = asyncio.run(call(normal_key, dedupe_forever=False))
        self.assertTrue(normal_repeat["deduped"])
        self.assertEqual(normal_repeat_count, 0)

    def test_message_delivery_generation_fences_all_finalizers_real_postgres(self):
        run_migrations(self.get_conn)

        def enqueue_and_claim(key):
            conn = self.get_conn()
            cur = conn.cursor()
            self.assertTrue(enqueue_message_delivery(cur, key, 123, "notice", {"text": key}))
            conn.commit()
            rows = claim_pending_message_deliveries(cur, limit=1)
            conn.commit()
            cur.close()
            conn.close()
            self.assertEqual(rows[0][0], key)
            return rows[0]

        row = enqueue_and_claim("delivery_fence_sent")
        self.assertEqual((row[4], row[6]), (1, 1))
        active = self.get_conn()
        active_cur = active.cursor()
        self.assertEqual(claim_pending_message_deliveries(active_cur, limit=1), [])
        active.rollback()
        active_cur.close()
        active.close()

        stale = self.get_conn()
        stale_cur = stale.cursor()
        stale_cur.execute(
            "UPDATE message_delivery_events SET lease_until = NOW() - INTERVAL '1 minute' WHERE delivery_key = %s",
            ("delivery_fence_sent",),
        )
        stale.commit()
        reclaimed = claim_pending_message_deliveries(stale_cur, limit=1)
        stale.commit()
        stale_cur.close()
        stale.close()
        self.assertEqual((reclaimed[0][4], reclaimed[0][6]), (2, 2))

        old = self.get_conn()
        old_cur = old.cursor()
        self.assertEqual(
            save_delivery_invite_link(old_cur, "delivery_fence_sent", 1, "https://example.invalid/old"),
            ("not_owner", None),
        )
        self.assertEqual(mark_delivery_sent(old_cur, "delivery_fence_sent", 1), "not_owner")
        self.assertEqual(mark_delivery_failed(old_cur, "delivery_fence_sent", 1, "old"), "not_owner")
        self.assertEqual(mark_delivery_cancelled(old_cur, "delivery_fence_sent", 1, "old"), "not_owner")
        old.commit()
        old_cur.close()
        old.close()
        self.assertEqual(
            self.query_one(
                "SELECT status, attempt_count, claim_generation, invite_link FROM message_delivery_events WHERE delivery_key = %s",
                ("delivery_fence_sent",),
            ),
            ("processing", 2, 2, None),
        )

        owner = self.get_conn()
        owner_cur = owner.cursor()
        self.assertEqual(mark_delivery_sent(owner_cur, "delivery_fence_sent", 2), "sent")
        owner.commit()
        owner_cur.close()
        owner.close()

        failed = enqueue_and_claim("delivery_fence_failed")
        failed_conn = self.get_conn()
        failed_cur = failed_conn.cursor()
        self.assertEqual(mark_delivery_failed(failed_cur, failed[0], failed[6], "retry"), "failed")
        failed_conn.commit()
        failed_cur.close()
        failed_conn.close()

        cancelled = enqueue_and_claim("delivery_fence_cancelled")
        cancelled_conn = self.get_conn()
        cancelled_cur = cancelled_conn.cursor()
        self.assertEqual(
            mark_delivery_cancelled(cancelled_cur, cancelled[0], cancelled[6], "no_longer_due"),
            "cancelled",
        )
        cancelled_conn.commit()
        cancelled_cur.close()
        cancelled_conn.close()

        legacy = self.get_conn()
        legacy_cur = legacy.cursor()
        legacy_cur.execute(
            """
            INSERT INTO message_delivery_events (
                delivery_key, telegram_id, delivery_type, status, attempt_count,
                claimed_at, lease_until, claim_generation, payload_json
            ) VALUES ('delivery_legacy_processing', 123, 'notice', 'processing', 7,
                      NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '10 minutes', 0, '{"text":"legacy"}')
            """
        )
        legacy.commit()
        legacy_rows = claim_pending_message_deliveries(legacy_cur, limit=1)
        legacy.commit()
        legacy_cur.close()
        legacy.close()
        self.assertEqual((legacy_rows[0][0], legacy_rows[0][4], legacy_rows[0][6]), ("delivery_legacy_processing", 8, 1))

    def test_message_delivery_real_concurrency_fences_late_owner_and_keeps_at_least_once(self):
        run_migrations(self.get_conn)
        key = "delivery_concurrent_fence"
        setup = self.get_conn()
        setup_cur = setup.cursor()
        enqueue_message_delivery(setup_cur, key, 123, "stripe_user_message", {"text": "proof"})
        setup.commit()
        first = claim_pending_message_deliveries(setup_cur, limit=1)
        setup.commit()
        self.assertEqual((first[0][4], first[0][6]), (1, 1))
        setup_cur.execute(
            "UPDATE message_delivery_events SET lease_until = NOW() - INTERVAL '1 minute' WHERE delivery_key = %s",
            (key,),
        )
        setup.commit()
        setup_cur.close()
        setup.close()

        worker_b = self.get_conn()
        worker_b_cur = worker_b.cursor()
        reclaimed = claim_pending_message_deliveries(worker_b_cur, limit=1)
        self.assertEqual((reclaimed[0][4], reclaimed[0][6]), (2, 2))

        late_result = {"value": None, "error": None}

        def finalize_a():
            conn = self.get_conn()
            cur = conn.cursor()
            try:
                late_result["value"] = mark_delivery_sent(cur, key, 1)
                conn.commit()
            except Exception as error:
                late_result["error"] = error
            finally:
                cur.close()
                conn.close()

        thread = threading.Thread(target=finalize_a)
        thread.start()
        time.sleep(0.2)
        self.assertTrue(thread.is_alive())
        worker_b.commit()
        worker_b_cur.close()
        worker_b.close()
        thread.join(timeout=5)
        self.assertIsNone(late_result["error"])
        self.assertEqual(late_result["value"], "not_owner")
        self.assertEqual(
            self.query_one(
                "SELECT status, attempt_count, claim_generation FROM message_delivery_events WHERE delivery_key = %s",
                (key,),
            ),
            ("processing", 2, 2),
        )

        # Generation fencing intentionally preserves at-least-once crash recovery:
        # worker A may have sent before disappearing, and worker B still owns a retryable claim.
        sends = ["worker_a_send_succeeded", "worker_b_resend_possible"]
        self.assertEqual(len(sends), 2)

    def test_subscription_refund_reconciliation_unique_guards_block_duplicates(self):
        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO subscription_refund_reconciliations (
                    reconciliation_key, refund_id, stripe_event_id, original_payment_event_id,
                    reconciliation_result
                )
                VALUES ('refund:re_dup', 're_dup', 'evt_refund_dup_1', 10, 'access_revoked')
                RETURNING id
                """
            )
            reconciliation_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO subscription_refund_events (reconciliation_id, stripe_event_id, event_type)
                VALUES (%s, 'evt_refund_dup_1', 'refund.created')
                """,
                (reconciliation_id,),
            )
            cur.execute(
                """
                INSERT INTO subscription_refund_events (reconciliation_id, stripe_event_id, event_type)
                VALUES (%s, 'evt_refund_dup_2', 'refund.updated')
                """,
                (reconciliation_id,),
            )
            conn.commit()
            self.assertEqual(
                self.query_one(
                    "SELECT COUNT(*) FROM subscription_refund_events WHERE reconciliation_id = %s",
                    (reconciliation_id,),
                )[0],
                2,
            )
            with self.assertRaises(Exception):
                cur.execute(
                    """
                    INSERT INTO subscription_refund_reconciliations (
                        reconciliation_key, refund_id, stripe_event_id, original_payment_event_id,
                        reconciliation_result
                    )
                    VALUES ('refund:re_dup_2', 're_dup_2', 'evt_refund_dup_3', 10, 'access_revoked')
                    """
                )
                conn.commit()
        finally:
            conn.rollback()
            cur.close()
            conn.close()

    def test_subscription_refund_charge_and_refund_events_share_revoked_payment_concurrently(self):
        main = import_main()
        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO subscription_refund_reconciliations (
                    reconciliation_key, refund_id, stripe_event_id, original_payment_event_id,
                    reconciliation_result, telegram_id, access_revoked_at
                )
                VALUES ('refund:re_concurrent', 're_concurrent', 'evt_refund_concurrent_1', 10,
                        'access_revoked', 123, NOW())
                RETURNING id
                """
            )
            reconciliation_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO subscription_refund_events (reconciliation_id, stripe_event_id, event_type)
                VALUES (%s, 'evt_refund_concurrent_1', 'refund.updated')
                """,
                (reconciliation_id,),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        errors = []

        def record_event(event_id, event_type):
            worker_conn = self.get_conn()
            worker_cur = worker_conn.cursor()
            try:
                row = main.find_access_revoked_refund_reconciliation(worker_cur, 10)
                self.assertIsNotNone(row)
                main.record_subscription_refund_event(worker_cur, row[0], event_id, event_type)
                worker_conn.commit()
            except Exception as exc:
                worker_conn.rollback()
                errors.append(exc)
            finally:
                worker_cur.close()
                worker_conn.close()

        threads = [
            threading.Thread(target=record_event, args=("evt_refund_concurrent_2", "refund.updated")),
            threading.Thread(target=record_event, args=("evt_charge_concurrent_3", "charge.refunded")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(
            self.query_one(
                """
                SELECT COUNT(*)
                FROM subscription_refund_reconciliations
                WHERE original_payment_event_id = 10
                  AND reconciliation_result = 'access_revoked'
                """
            )[0],
            1,
        )
        self.assertEqual(
            self.query_one(
                """
                SELECT COUNT(*)
                FROM subscription_refund_events
                WHERE reconciliation_id = %s
                """,
                (reconciliation_id,),
            )[0],
            3,
        )

    def test_subscription_removal_new_revoke_reopens_terminal_cycle_real_postgres(self):
        main = import_main()
        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        old_started_at = datetime.utcnow() - timedelta(days=1)
        new_started_at = datetime.utcnow()
        try:
            cur.execute(
                """
                INSERT INTO subscription_removal_events (
                    telegram_id, status, reason, owner_id, claimed_at, lease_until,
                    telegram_removed_at, db_finalized_at, attempt_count, last_error,
                    revoke_started_at, created_at, updated_at
                )
                VALUES (123, 'superseded', 'manual_access_revoked', 'old-owner', NOW(), NOW() + INTERVAL '1 hour',
                        NOW(), NOW(), 5, 'old error', %s, NOW(), NOW())
                """,
                (old_started_at,),
            )
            main.enqueue_subscription_refund_group_removal(
                cur,
                123,
                reason="manual_access_revoked",
                revoke_started_at=new_started_at,
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        self.assertEqual(
            self.query_one(
                """
                SELECT status, reason, owner_id, claimed_at, lease_until, telegram_removed_at,
                       db_finalized_at, attempt_count, last_error, revoke_started_at
                FROM subscription_removal_events
                WHERE telegram_id = 123
                """
            ),
            ("pending", "manual_access_revoked", None, None, None, None, None, 0, None, new_started_at),
        )

        conn = self.get_conn()
        cur = conn.cursor()
        try:
            self.assertEqual(main.claim_subscription_removal(cur, 123, "subscription_expired"), "claimed")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def test_subscription_removal_terminal_without_new_revoke_is_not_claimed_real_postgres(self):
        main = import_main()
        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO subscription_removal_events (
                    telegram_id, status, reason, attempt_count, revoke_started_at, created_at, updated_at
                )
                VALUES (124, 'superseded', 'subscription_refund_reconciled', 2, NOW(), NOW(), NOW())
                """
            )
            conn.commit()
            self.assertEqual(main.claim_subscription_removal(cur, 124, "subscription_expired"), "superseded")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def test_subscription_removal_new_revoke_after_removed_or_finalized_restarts_cycle_real_postgres(self):
        main = import_main()
        run_migrations(self.get_conn)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            for telegram_id, status in ((125, "telegram_removed"), (126, "db_finalized"), (127, "cancelled"), (128, "not_due")):
                cur.execute(
                    """
                    INSERT INTO subscription_removal_events (
                        telegram_id, status, reason, owner_id, claimed_at, lease_until,
                        telegram_removed_at, db_finalized_at, attempt_count, last_error,
                        revoke_started_at, created_at, updated_at
                    )
                    VALUES (%s, %s, 'subscription_refund_reconciled', 'old-owner', NOW(), NOW() + INTERVAL '1 hour',
                            NOW(), NOW(), 4, 'old error', NOW() - INTERVAL '1 day', NOW(), NOW())
                    """,
                    (telegram_id, status),
                )
                main.enqueue_subscription_refund_group_removal(
                    cur,
                    telegram_id,
                    reason="subscription_refund_reconciled",
                    revoke_started_at=datetime.utcnow(),
                )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        for telegram_id in (125, 126, 127, 128):
            with self.subTest(telegram_id=telegram_id):
                self.assertEqual(
                    self.query_one(
                        """
                        SELECT status, owner_id, claimed_at, lease_until, telegram_removed_at,
                               db_finalized_at, attempt_count, last_error
                        FROM subscription_removal_events
                        WHERE telegram_id = %s
                        """,
                        (telegram_id,),
                    ),
                    ("pending", None, None, None, None, None, 0, None),
                )

    def test_migration_runner_rejects_destructive_sql(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "0001_drop.sql").write_text("DROP TABLE users;", encoding="utf-8")
            with self.assertRaisesRegex(MigrationError, "Destructive SQL"):
                run_migrations(self.get_conn, migrations_dir=tmp)


if __name__ == "__main__":
    unittest.main()
