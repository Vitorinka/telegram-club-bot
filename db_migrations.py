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

MIGRATION_BASELINE_REQUIREMENTS = {
    "0001_initial_schema": {
        "tables": (
            "users",
            "stripe_events",
            "access_events",
            "stripe_links",
            "unlinked_stripe_events",
            "payment_events",
            "weekly_report_runs",
            "system_settings",
        ),
        "columns": {
            "users": (
                "id",
                "telegram_id",
                "paid",
                "expiry_date",
                "stripe_subscription_id",
                "stripe_customer_id",
                "reminder_sent",
                "payment_failed",
                "payment_failed_at",
                "last_payment_succeeded_at",
                "grace_period_end",
                "auto_renew",
                "trial_used",
                "first_payment_done",
                "registered_at",
                "blocked_bot",
                "video_sent",
                "video_sent_at",
                "feedback_sent",
                "feedback_sent_at",
                "feedback_received",
                "username",
                "first_name",
                "last_name",
                "profile_updated_at",
                "last_successful_invoice_created_at",
                "last_subscription_state_event_created_at",
                "last_payment_failure_event_created_at",
                "manual_sync_at",
            ),
            "stripe_events": ("event_id", "processed", "processed_at", "event_created_at", "event_type", "object_id"),
            "access_events": (
                "id",
                "telegram_id",
                "event_type",
                "source",
                "old_expiry",
                "new_expiry",
                "stripe_event_id",
                "stripe_subscription_id",
                "notes",
                "created_at",
            ),
            "stripe_links": (
                "id",
                "telegram_id",
                "stripe_customer_id",
                "stripe_subscription_id",
                "customer_email",
                "status",
                "current_period_end",
                "is_active",
                "source",
                "created_at",
                "updated_at",
            ),
            "unlinked_stripe_events": (
                "id",
                "event_id",
                "event_type",
                "invoice_id",
                "stripe_customer_id",
                "stripe_subscription_id",
                "customer_email",
                "amount_paid",
                "currency",
                "billing_reason",
                "period_end",
                "raw_summary",
                "resolved",
                "resolved_by",
                "resolved_telegram_id",
                "resolved_at",
                "created_at",
            ),
            "payment_events": (
                "id",
                "stripe_event_id",
                "event_type",
                "telegram_id",
                "invoice_id",
                "checkout_session_id",
                "stripe_customer_id",
                "stripe_subscription_id",
                "payment_status",
                "payment_kind",
                "billing_reason",
                "tariff_code",
                "amount_paid",
                "amount_due",
                "currency",
                "period_start",
                "period_end",
                "recovered_after_failure",
                "created_at",
            ),
            "weekly_report_runs": (
                "report_key",
                "period_start",
                "period_end",
                "status",
                "sent_admin_ids",
                "created_at",
                "updated_at",
                "completed_at",
                "error_text",
            ),
            "system_settings": ("key", "value_text", "created_at", "updated_at"),
        },
        "indexes": (
            "payment_events_created_at_idx",
            "payment_events_telegram_id_idx",
            "payment_events_status_kind_idx",
        ),
    },
    "0002_checkout_and_hardening_tables": {
        "tables": (
            "checkout_sessions",
            "checkout_retry_events",
            "trial_redemptions",
            "admin_action_requests",
            "scheduled_job_runs",
            "message_delivery_events",
            "subscription_removal_events",
            "bot_invite_links",
            "admin_alerts",
        ),
        "columns": {
            "checkout_sessions": (
                "id",
                "telegram_id",
                "tariff_code",
                "mode",
                "stripe_session_id",
                "stripe_customer_id",
                "stripe_subscription_id",
                "idempotency_key",
                "checkout_url",
                "status",
                "expires_at",
                "created_at",
                "updated_at",
                "completed_at",
                "last_error",
            ),
            "checkout_retry_events": (
                "id",
                "telegram_id",
                "tariff_code",
                "username",
                "first_name",
                "last_name",
                "attempt_at",
                "last_admin_alert_at",
                "resolved_at",
                "resolved_source",
            ),
            "trial_redemptions": ("telegram_id", "stripe_event_id", "checkout_session_id", "redeemed_at"),
            "admin_action_requests": ("action_id", "admin_id", "action_type", "payload_json", "status", "created_at", "expires_at", "completed_at"),
            "scheduled_job_runs": (
                "job_key",
                "job_name",
                "schedule_slot",
                "status",
                "owner_id",
                "lease_until",
                "started_at",
                "updated_at",
                "completed_at",
                "error_text",
            ),
            "message_delivery_events": (
                "delivery_key",
                "telegram_id",
                "delivery_type",
                "status",
                "attempt_count",
                "claimed_at",
                "lease_until",
                "sent_at",
                "next_attempt_at",
                "payload_json",
                "invite_link",
                "last_error",
            ),
            "subscription_removal_events": (
                "telegram_id",
                "status",
                "reason",
                "owner_id",
                "claimed_at",
                "lease_until",
                "telegram_removed_at",
                "db_finalized_at",
                "admin_notified_at",
                "attempt_count",
                "last_error",
                "created_at",
                "updated_at",
            ),
            "bot_invite_links": ("invite_link", "source", "telegram_id", "status", "created_at", "expires_at", "revoked_at"),
            "admin_alerts": ("id", "alert_key", "severity", "text", "status", "delivered_admin_ids", "created_at", "updated_at"),
        },
        "indexes": ("checkout_sessions_one_open_tariff", "checkout_retry_events_user_attempt_idx"),
    },
    "0003_stripe_identity_guards": {
        "tables": ("stripe_identity_conflicts",),
        "columns": {
            "stripe_identity_conflicts": (
                "id",
                "conflict_type",
                "stripe_id",
                "telegram_ids",
                "details",
                "resolved",
                "created_at",
                "updated_at",
            ),
        },
        "indexes": (
            "stripe_identity_conflicts_active_unique",
            "users_unique_stripe_subscription",
            "users_unique_stripe_customer",
            "stripe_links_unique_subscription_user",
        ),
        "requires_stripe_identity_clean": True,
    },
    "0004_postgres_fsm_storage": {
        "tables": ("aiogram_fsm_states",),
        "columns": {
            "aiogram_fsm_states": (
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
            ),
        },
        "indexes": ("aiogram_fsm_states_updated_at_idx",),
    },
    "0005_failed_subscription_terminations": {
        "tables": ("failed_subscription_terminations",),
        "columns": {
            "failed_subscription_terminations": (
                "operation_id", "telegram_id", "stripe_subscription_id", "failed_invoice_id",
                "reason", "status", "owner_id", "claim_generation", "lease_until",
                "access_expiry", "stripe_cancelled_at", "collection_stopped_at", "telegram_banned_at",
                "telegram_removed_at", "db_finalized_at", "completed_at", "attempt_count",
                "last_error_category", "created_at", "updated_at",
            ),
        },
        "indexes": (
            "failed_subscription_terminations_due_idx",
            "failed_subscription_terminations_subscription_uidx",
            "failed_subscription_terminations_user_idx",
        ),
    },
}

BASELINE_REQUIRED_TABLES = MIGRATION_BASELINE_REQUIREMENTS["0002_checkout_and_hardening_tables"]["tables"] + (
    "stripe_identity_conflicts",
    "aiogram_fsm_states",
    "failed_subscription_terminations",
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
    "aiogram_fsm_states": (
        "bot_id",
        "chat_id",
        "user_id",
        "state",
        "data_json",
        "created_at",
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


def table_exists(cur, table):
    cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
    row = cur.fetchone()
    return bool(row and row[0])


def present_columns(cur, table):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (table,),
    )
    return {row[0] for row in cur.fetchall()}


def present_indexes(cur):
    cur.execute(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
        """
    )
    return {row[0] for row in cur.fetchall()}


def assert_no_stripe_identity_conflicts(cur):
    for table in ("users", "stripe_links"):
        cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
        row = cur.fetchone()
        if not row or not row[0]:
            return

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


def migration_schema_matches(cur, version):
    requirements = MIGRATION_BASELINE_REQUIREMENTS.get(version)
    if not requirements:
        return False
    for table in requirements.get("tables", ()):
        if not table_exists(cur, table):
            return False
    for table, columns in requirements.get("columns", {}).items():
        if not set(columns) <= present_columns(cur, table):
            return False
    required_indexes = set(requirements.get("indexes", ()))
    if required_indexes and not required_indexes <= present_indexes(cur):
        return False
    if requirements.get("requires_stripe_identity_clean"):
        assert_no_stripe_identity_conflicts(cur)
    return True


def baseline_existing_database(cur, migrations):
    baselined = []
    for migration in migrations:
        if not migration_schema_matches(cur, migration["version"]):
            continue
        cur.execute(
            """
            INSERT INTO schema_migrations (version, checksum, baseline)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (version) DO NOTHING
            """,
            (migration["version"], migration["checksum"]),
        )
        baselined.append(migration["version"])
    return baselined


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
        baselined = []
        if not applied and schema_has_existing_tables(cur):
            baselined = baseline_existing_database(cur, migrations)
            conn.commit()
            applied = fetch_applied_migrations(cur)
            logging.info("PostgreSQL migrations baselined existing schema: count=%s", len(baselined))

        for migration in migrations:
            existing_checksum = applied.get(migration["version"])
            if existing_checksum:
                if existing_checksum != migration["checksum"]:
                    raise MigrationError(f"Checksum mismatch for migration {migration['version']}")
                continue
            if MIGRATION_BASELINE_REQUIREMENTS.get(migration["version"], {}).get("requires_stripe_identity_clean"):
                assert_no_stripe_identity_conflicts(cur)
            try:
                apply_migration(cur, migration)
                conn.commit()
                applied[migration["version"]] = migration["checksum"]
            except Exception:
                conn.rollback()
                raise

        applied_versions = [migration["version"] for migration in migrations if migration["version"] in applied]
        logging.info("PostgreSQL migrations verified/applied: count=%s", len(applied_versions))
        return {"applied": applied_versions, "baselined": baselined}
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
