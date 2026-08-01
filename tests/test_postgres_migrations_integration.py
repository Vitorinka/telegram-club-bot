import contextlib
import asyncio
import io
import os
import tempfile
import threading
import time
import unittest
import uuid
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
    "STRIPE_WEBHOOK_SECRET": "whsec_postgres_test",
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

    def insert_checkout_attempt(self, telegram_id, *, hours_ago=24, status="expired", tariff="sub_1"):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO checkout_sessions (
                    telegram_id, tariff_code, mode, idempotency_key, status, created_at, updated_at
                )
                VALUES (%s, %s, 'payment', %s, %s,
                        (NOW() AT TIME ZONE 'UTC') - (%s * INTERVAL '1 hour'),
                        (NOW() AT TIME ZONE 'UTC') - (%s * INTERVAL '1 hour'))
                """,
                (telegram_id, tariff, f"idem_{telegram_id}_{uuid.uuid4().hex}", status, hours_ago, hours_ago),
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
                    'sub_pending',
                    'checkout_subscription_pending_invoice',
                    FALSE,
                    (NOW() AT TIME ZONE 'UTC') + INTERVAL '24 hours'
                )
                """,
                (9104,),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        self.assertIn(9104, {row[0] for row in self.due_recovery_users()})

        self.insert_recovery_user(9105, stripe_subscription_id="sub_incomplete")
        self.insert_checkout_attempt(9105, hours_ago=25)
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
                    'sub_incomplete',
                    'incomplete',
                    FALSE,
                    (NOW() AT TIME ZONE 'UTC') + INTERVAL '24 hours'
                )
                """,
                (9105,),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        self.assertIn(9105, {row[0] for row in self.due_recovery_users()})

    def test_first_purchase_recovery_success_states_and_newer_attempt_block_eligibility(self):
        run_migrations(self.get_conn)

        self.insert_recovery_user(9201)
        self.insert_checkout_attempt(9201, hours_ago=25)
        self.insert_retry_attempt(9201, hours_ago=2)
        self.assertNotIn(9201, {row[0] for row in self.due_recovery_users()})

        for user_id, status, is_active, future_period in (
            (9202, "active", False, True),
            (9203, "trialing", False, False),
            (9204, "past_due", True, True),
        ):
            self.insert_recovery_user(user_id, stripe_subscription_id=f"sub_{user_id}")
            self.insert_checkout_attempt(user_id, hours_ago=25)
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
                    (user_id, f"sub_{user_id}", status, is_active),
                )
                conn.commit()
            finally:
                cur.close()
                conn.close()
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
