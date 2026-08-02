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
from urllib.parse import unquote, urlsplit

import psycopg2
from aiogram.fsm.storage.base import StorageKey
from psycopg2 import sql
from psycopg2.extensions import make_dsn

from db_migrations import MIGRATIONS_DIR, MigrationError, load_migrations, run_migrations
from postgres_fsm_storage import PostgresFSMStorage, cleanup_postgres_fsm_storage


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
        return connect(self.dsn)

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
            conn.commit()
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

    def test_admin_payment_notifications_are_durable_and_deduplicated_real_postgres(self):
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
                    2,
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
                self.assertEqual(duplicate_result, len(admin_ids))
                cur.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT telegram_id), COUNT(DISTINCT delivery_key)
                    FROM message_delivery_events
                    WHERE delivery_type = 'stripe_admin_message'
                      AND delivery_key LIKE 'stripe-admin:evt_pg_admin_success:payment_success:%'
                    """
                )
                self.assertEqual(cur.fetchone(), (2, 2, 2))
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
        self.assertEqual(len(rows), 4)
        self.assertEqual({row[1] for row in rows}, {111, 222})
        self.assertEqual({row[2] for row in rows}, {"stripe_admin_message"})
        self.assertEqual({row[3] for row in rows}, {"pending"})
        self.assertEqual(
            [row[0] for row in rows],
            [
                "stripe-admin:evt_pg_admin_failed:invoice_payment_failed:111",
                "stripe-admin:evt_pg_admin_failed:invoice_payment_failed:222",
                "stripe-admin:evt_pg_admin_success:payment_success:111",
                "stripe-admin:evt_pg_admin_success:payment_success:222",
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
        self.assertEqual(row_counts, [(111, 2), (222, 2)])
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
        self.assertTrue(any("Тип: первая оплата" in payload["text"] for payload in payloads))
        self.assertTrue(any("банк отклонил карту" in payload["text"] for payload in payloads))
        self.assertTrue(all("None" not in payload["text"] for payload in payloads))

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
                self.assertEqual(
                    main.persist_first_purchase_recovery_invoice_failure_context(
                        cur,
                        user_id,
                        f"sub_{user_id}",
                        failure_code=failure_code,
                    ),
                    token,
                )
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

    def test_first_purchase_recovery_worker_success_creates_user_and_admin_deliveries_once(self):
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
        self.assertEqual(len(admin_rows), 2)
        self.assertEqual([row[1] for row in admin_rows], [1, 2])
        self.assertTrue(all(row[3] == "pending" for row in admin_rows))
        self.assertEqual(len({row[0] for row in admin_rows}), 2)
        for delivery_key, _, delivery_type, _, payload_json in admin_rows:
            self.assertEqual(delivery_type, "stripe_admin_message")
            payload = json.loads(payload_json)
            self.assertEqual(payload["category"], "first_purchase_recovery_sent")
            self.assertEqual(payload["severity"], "INFO")
            self.assertIn("🔁 Повторная попытка оплаты предложена", payload["text"])
            self.assertIn("Последняя попытка:", payload["text"])
            self.assertIn("Напоминание: отправлено", payload["text"])
            self.assertNotIn(str(user_id), delivery_key)

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

    def test_migration_runner_rejects_destructive_sql(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "0001_drop.sql").write_text("DROP TABLE users;", encoding="utf-8")
            with self.assertRaisesRegex(MigrationError, "Destructive SQL"):
                run_migrations(self.get_conn, migrations_dir=tmp)


if __name__ == "__main__":
    unittest.main()
