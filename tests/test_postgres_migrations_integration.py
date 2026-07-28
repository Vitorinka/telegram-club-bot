import contextlib
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
from psycopg2 import sql
from psycopg2.extensions import make_dsn

from db_migrations import MIGRATIONS_DIR, MigrationError, load_migrations, run_migrations


POSTGRES_TEST_DSN = os.getenv("POSTGRES_TEST_DSN")


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
        self.assertTrue(password)
        self.assertIn(self.db_name, self.dsn)
        self.assertIn("password=", self.dsn)

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            conn = connect(self.dsn)
            conn.close()
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
