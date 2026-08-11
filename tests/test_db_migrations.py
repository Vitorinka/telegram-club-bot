import tempfile
import unittest
from pathlib import Path

from db_migrations import (
    BASELINE_REQUIRED_COLUMNS,
    BASELINE_REQUIRED_TABLES,
    MIGRATION_BASELINE_REQUIREMENTS,
    MigrationError,
    load_migrations,
    run_migrations,
)


class FakeMigrationCursor:
    def __init__(self, db):
        self.db = db
        self.queries = []
        self.last_query = ""
        self.last_params = ()
        self.closed = False

    def execute(self, query, params=()):
        self.last_query = query
        self.last_params = params
        self.queries.append((query, params))
        if self.db.fail_on_sql and self.db.fail_on_sql in query:
            raise RuntimeError("migration failed")
        if query.strip().startswith("INSERT INTO schema_migrations"):
            version, checksum = params[:2]
            self.db.applied[version] = checksum
        if "pg_advisory_lock" in query:
            self.db.lock_calls += 1
        if "pg_advisory_unlock" in query:
            self.db.unlock_calls += 1

    def fetchone(self):
        if "SELECT to_regclass('public.users')" in self.last_query:
            return ("users",) if self.db.existing_schema else (None,)
        if "SELECT to_regclass(%s)" in self.last_query:
            table = self.last_params[0].replace("public.", "")
            return (table,) if table in self.db.tables else (None,)
        if "SELECT COUNT(*)" in self.last_query and "conflicts" in self.last_query:
            return (self.db.conflict_count,)
        return None

    def fetchall(self):
        if "SELECT version, checksum FROM schema_migrations" in self.last_query:
            return sorted(self.db.applied.items())
        if "information_schema.columns" in self.last_query:
            table = self.last_params[0]
            return [(column,) for column in self.db.columns.get(table, ())]
        if "FROM pg_indexes" in self.last_query:
            return [(index,) for index in self.db.indexes]
        return []

    def close(self):
        self.closed = True


class FakeMigrationConnection:
    def __init__(self, db):
        self.db = db
        self.cursor_obj = FakeMigrationCursor(db)
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


class FakeMigrationDb:
    def __init__(self, existing_schema=False, applied=None, conflict_count=0, fail_on_sql=None):
        self.existing_schema = existing_schema
        self.applied = dict(applied or {})
        self.conflict_count = conflict_count
        self.fail_on_sql = fail_on_sql
        self.tables = set(BASELINE_REQUIRED_TABLES)
        self.tables.update({"users", "stripe_links"})
        self.columns = {
            table: set(columns)
            for table, columns in BASELINE_REQUIRED_COLUMNS.items()
        }
        self.indexes = set()
        self.connections = []
        self.lock_calls = 0
        self.unlock_calls = 0

    def get_conn(self):
        conn = FakeMigrationConnection(self)
        self.connections.append(conn)
        return conn


def write_migration(directory, name, sql):
    path = Path(directory) / name
    path.write_text(sql, encoding="utf-8")
    return path


class DbMigrationTests(unittest.TestCase):
    def test_message_delivery_due_indexes_migration_is_required(self):
        migration = MIGRATION_BASELINE_REQUIREMENTS["0010_message_delivery_due_indexes"]
        self.assertEqual(migration["tables"], ("message_delivery_events",))
        self.assertEqual(
            migration["indexes"],
            (
                "message_delivery_events_pending_failed_due_idx",
                "message_delivery_events_processing_lease_idx",
            ),
        )

    def test_message_delivery_due_indexes_migration_has_exact_sql(self):
        root = Path(__file__).resolve().parents[1]
        migration_sql = (root / "migrations" / "0010_message_delivery_due_indexes.sql").read_text()
        self.assertEqual(
            migration_sql,
            """CREATE INDEX IF NOT EXISTS message_delivery_events_pending_failed_due_idx
ON message_delivery_events
(next_attempt_at ASC NULLS FIRST, delivery_key)
WHERE status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS message_delivery_events_processing_lease_idx
ON message_delivery_events
(lease_until, delivery_key)
WHERE status = 'processing';
""",
        )

    def test_postgres_fsm_storage_migration_is_required(self):
        migration = MIGRATION_BASELINE_REQUIREMENTS["0004_postgres_fsm_storage"]
        self.assertIn("aiogram_fsm_states", migration["tables"])
        columns = migration["columns"]["aiogram_fsm_states"]
        for column in (
            "bot_id",
            "chat_id",
            "user_id",
            "thread_id",
            "business_connection_id",
            "destiny",
            "state",
            "data_json",
            "created_at",
            "updated_at",
        ):
            self.assertIn(column, columns)
        self.assertIn("aiogram_fsm_states_updated_at_idx", migration["indexes"])

    def test_postgres_fsm_storage_migration_has_expected_filename(self):
        root = Path(__file__).resolve().parents[1]
        self.assertTrue((root / "migrations" / "0004_postgres_fsm_storage.sql").exists())
        old_name = "0004_" + "aiogram_fsm_storage.sql"
        self.assertFalse((root / "migrations" / old_name).exists())

    def test_postgres_fsm_storage_migration_uses_jsonb_and_cleanup_columns(self):
        root = Path(__file__).resolve().parents[1]
        migration_sql = (root / "migrations" / "0004_postgres_fsm_storage.sql").read_text()

        self.assertIn("data_json JSONB NOT NULL DEFAULT '{}'::jsonb", migration_sql)
        self.assertIn("created_at TIMESTAMP NOT NULL DEFAULT NOW()", migration_sql)
        self.assertIn("updated_at TIMESTAMP NOT NULL DEFAULT NOW()", migration_sql)
        self.assertIn("aiogram_fsm_states_updated_at_idx", migration_sql)

    def test_empty_schema_applies_numbered_migrations(self):
        with tempfile.TemporaryDirectory() as tmp:
            migrations = [
                write_migration(tmp, "0001_initial.sql", "CREATE TABLE users(id int);"),
                write_migration(tmp, "0002_more.sql", "CREATE TABLE checkout_sessions(id int);"),
            ]
            db = FakeMigrationDb(existing_schema=False)
            result = run_migrations(db.get_conn, migrations_dir=tmp)

            self.assertEqual(result["baselined"], [])
            self.assertEqual(set(db.applied), {"0001_initial", "0002_more"})
            queries = "\n".join(query for query, _ in db.connections[0].cursor_obj.queries)
            self.assertIn(migrations[0].read_text(), queries)
            self.assertIn("pg_advisory_lock", queries)
            self.assertIn("pg_advisory_unlock", queries)

    def test_existing_schema_runs_unproven_migration_instead_of_baselining(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_migration(tmp, "0001_initial.sql", "CREATE TABLE users(id int);")
            db = FakeMigrationDb(existing_schema=True)
            result = run_migrations(db.get_conn, migrations_dir=tmp)

            self.assertEqual(result["baselined"], [])
            self.assertIn("0001_initial", db.applied)

    def test_existing_schema_with_identity_conflicts_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_migration(tmp, "0001_initial_schema.sql", "CREATE TABLE users(id int);")
            write_migration(tmp, "0002_checkout_and_hardening_tables.sql", "CREATE TABLE stripe_links(id int);")
            write_migration(tmp, "0003_stripe_identity_guards.sql", "CREATE INDEX idx ON users(id);")
            db = FakeMigrationDb(existing_schema=True, conflict_count=1)

            with self.assertRaisesRegex(MigrationError, "Stripe identity conflicts"):
                run_migrations(db.get_conn, migrations_dir=tmp)

    def test_repeated_application_verifies_checksum_and_skips_sql(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write_migration(tmp, "0001_initial.sql", "CREATE TABLE users(id int);")
            checksum = load_migrations(tmp)[0]["checksum"]
            db = FakeMigrationDb(existing_schema=False, applied={"0001_initial": checksum})

            run_migrations(db.get_conn, migrations_dir=tmp)

            queries = [query for query, _ in db.connections[0].cursor_obj.queries]
            self.assertFalse(any(path.read_text() == query for query in queries))

    def test_checksum_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_migration(tmp, "0001_initial.sql", "CREATE TABLE users(id int);")
            db = FakeMigrationDb(existing_schema=False, applied={"0001_initial": "bad"})

            with self.assertRaisesRegex(MigrationError, "Checksum mismatch"):
                run_migrations(db.get_conn, migrations_dir=tmp)

    def test_failed_migration_rolls_back_and_does_not_record_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_migration(tmp, "0001_initial.sql", "CREATE TABLE fail_me(id int);")
            db = FakeMigrationDb(existing_schema=False, fail_on_sql="fail_me")

            with self.assertRaises(RuntimeError):
                run_migrations(db.get_conn, migrations_dir=tmp)

            self.assertGreater(db.connections[0].rollbacks, 0)
            self.assertNotIn("0001_initial", db.applied)

    def test_two_replicas_share_state_and_second_only_verifies(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_migration(tmp, "0001_initial.sql", "CREATE TABLE users(id int);")
            db = FakeMigrationDb(existing_schema=False)

            run_migrations(db.get_conn, migrations_dir=tmp)
            applied_after_first = dict(db.applied)
            run_migrations(db.get_conn, migrations_dir=tmp)

            self.assertEqual(db.applied, applied_after_first)
            self.assertEqual(db.lock_calls, 2)
            self.assertEqual(db.unlock_calls, 2)


if __name__ == "__main__":
    unittest.main()
