import hashlib
import logging
import re
import time
from pathlib import Path


MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
MIGRATION_ID_RE = re.compile(r"^(\d{4}_[a-z0-9_]+)\.sql$")
MIGRATION_LOCK_KEY = "telegram_club_bot:schema_migrations"
DESTRUCTIVE_SQL_RE = re.compile(
    r"\b(DROP\s+TABLE|DROP\s+SCHEMA|TRUNCATE|ALTER\s+TABLE\s+\S+\s+DROP\s+COLUMN|DELETE\s+FROM\s+(?!checkout_retry_events|checkout_sessions))\b",
    re.IGNORECASE,
)

BASELINE_REQUIRED_TABLES = (
    "users",
    "stripe_events",
    "access_events",
    "checkout_sessions",
    "message_delivery_events",
    "stripe_identity_conflicts",
)

BASELINE_REQUIRED_COLUMNS = {
    "users": (
        "telegram_id",
        "paid",
        "expiry_date",
        "stripe_subscription_id",
        "stripe_customer_id",
        "blocked_bot",
        "last_successful_invoice_created_at",
        "last_subscription_state_event_created_at",
    ),
    "checkout_sessions": (
        "telegram_id",
        "tariff_code",
        "status",
        "idempotency_key",
    ),
    "message_delivery_events": (
        "delivery_key",
        "telegram_id",
        "delivery_type",
        "status",
    ),
}


class MigrationError(RuntimeError):
    pass


def load_migrations(migrations_dir=MIGRATIONS_DIR):
    migrations = []
    for path in sorted(Path(migrations_dir).glob("*.sql")):
        match = MIGRATION_ID_RE.match(path.name)
        if not match:
            raise MigrationError(f"Invalid migration filename: {path.name}")
        sql = path.read_text(encoding="utf-8")
        if DESTRUCTIVE_SQL_RE.search(sql):
            raise MigrationError(f"Destructive SQL is not allowed in migration: {path.name}")
        migrations.append({
            "version": match.group(1),
            "path": path,
            "sql": sql,
            "checksum": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        })
    if not migrations:
        raise MigrationError("No PostgreSQL migrations found")
    return migrations


def ensure_schema_migrations(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT NOW(),
            execution_ms INTEGER,
            baseline BOOLEAN DEFAULT FALSE
        );
        """
    )


def acquire_migration_lock(cur):
    cur.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (MIGRATION_LOCK_KEY,))


def release_migration_lock(cur):
    cur.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (MIGRATION_LOCK_KEY,))


def fetch_applied_migrations(cur):
    cur.execute("SELECT version, checksum FROM schema_migrations ORDER BY version")
    return dict(cur.fetchall())


def schema_has_existing_tables(cur):
    cur.execute("SELECT to_regclass('public.users')")
    row = cur.fetchone()
    return bool(row and row[0])


def assert_required_baseline_schema(cur):
    for table in BASELINE_REQUIRED_TABLES:
        cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
        row = cur.fetchone()
        if not row or not row[0]:
            raise MigrationError(f"Existing database is missing required table: {table}")

    for table, columns in BASELINE_REQUIRED_COLUMNS.items():
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            """,
            (table,),
        )
        present = {row[0] for row in cur.fetchall()}
        missing = sorted(set(columns) - present)
        if missing:
            raise MigrationError(f"Existing database table {table} is missing columns: {', '.join(missing)}")

    cur.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT stripe_subscription_id
            FROM users
            WHERE stripe_subscription_id IS NOT NULL
            GROUP BY stripe_subscription_id
            HAVING COUNT(*) > 1
            UNION ALL
            SELECT stripe_customer_id
            FROM users
            WHERE stripe_customer_id IS NOT NULL
            GROUP BY stripe_customer_id
            HAVING COUNT(*) > 1
        ) conflicts
        """
    )
    row = cur.fetchone()
    if row and row[0]:
        raise MigrationError("Existing database has unresolved Stripe identity conflicts")


def baseline_existing_database(cur, migrations):
    assert_required_baseline_schema(cur)
    for migration in migrations:
        cur.execute(
            """
            INSERT INTO schema_migrations (version, checksum, baseline)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (version) DO NOTHING
            """,
            (migration["version"], migration["checksum"]),
        )


def apply_migration(cur, migration):
    started = time.monotonic()
    cur.execute(migration["sql"])
    execution_ms = int((time.monotonic() - started) * 1000)
    cur.execute(
        """
        INSERT INTO schema_migrations (version, checksum, execution_ms, baseline)
        VALUES (%s, %s, %s, FALSE)
        """,
        (migration["version"], migration["checksum"], execution_ms),
    )


def run_migrations(get_conn, migrations_dir=MIGRATIONS_DIR):
    migrations = load_migrations(migrations_dir)
    conn = get_conn()
    cur = conn.cursor()
    lock_held = False
    try:
        acquire_migration_lock(cur)
        lock_held = True
        conn.commit()

        ensure_schema_migrations(cur)
        conn.commit()

        applied = fetch_applied_migrations(cur)
        if not applied and schema_has_existing_tables(cur):
            baseline_existing_database(cur, migrations)
            conn.commit()
            logging.info("PostgreSQL migrations baselined existing schema: count=%s", len(migrations))
            return {"applied": [], "baselined": [migration["version"] for migration in migrations]}

        for migration in migrations:
            existing_checksum = applied.get(migration["version"])
            if existing_checksum:
                if existing_checksum != migration["checksum"]:
                    raise MigrationError(f"Checksum mismatch for migration {migration['version']}")
                continue
            try:
                apply_migration(cur, migration)
                conn.commit()
                applied[migration["version"]] = migration["checksum"]
            except Exception:
                conn.rollback()
                raise

        applied_versions = [migration["version"] for migration in migrations if migration["version"] in applied]
        logging.info("PostgreSQL migrations verified/applied: count=%s", len(applied_versions))
        return {"applied": applied_versions, "baselined": []}
    except Exception:
        conn.rollback()
        raise
    finally:
        if lock_held:
            try:
                release_migration_lock(cur)
                conn.commit()
            except Exception:
                conn.rollback()
                logging.exception("Failed to release PostgreSQL migration advisory lock")
        cur.close()
        conn.close()
