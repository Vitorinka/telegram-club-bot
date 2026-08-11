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
                "revoke_started_at",
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
    "0005_gift_access": {
        "tables": ("gift_access_grants", "gift_certificate_templates", "gift_access_events"),
        "columns": {
            "gift_access_grants": (
                "id",
                "public_reference",
                "purchaser_telegram_id",
                "recipient_telegram_id",
                "recipient_name",
                "sender_name",
                "gift_message",
                "tariff_code",
                "duration_days",
                "status",
                "token_hash",
                "token_version",
                "stripe_session_id",
                "stripe_payment_intent_id",
                "checkout_url",
                "checkout_expires_at",
                "amount_total",
                "currency",
                "paid_at",
                "reserved_at",
                "redeemed_at",
                "applied_at",
                "applied_expiry",
                "refunded_at",
                "cancelled_at",
                "created_at",
                "updated_at",
                "last_error",
                "last_error_category",
            ),
            "gift_certificate_templates": (
                "tariff_code",
                "file_id",
                "uploaded_by",
                "active",
                "created_at",
                "updated_at",
            ),
            "gift_access_events": (
                "id",
                "gift_id",
                "public_reference",
                "telegram_id",
                "event_type",
                "source",
                "notes",
                "created_at",
            ),
        },
        "indexes": (
            "gift_access_grants_checkout_open_idx",
            "gift_access_grants_purchaser_idx",
            "gift_access_grants_recipient_idx",
            "gift_access_grants_status_idx",
            "gift_access_events_gift_idx",
            "gift_access_events_public_reference_idx",
        ),
    },
    "0006_payment_integrity_guards": {
        "indexes": {
            "payment_events_unique_stripe_event_id": {
                "table": "payment_events",
                "columns": ("stripe_event_id",),
                "unique": True,
                "predicate": "stripe_event_id IS NOT NULL",
            },
            "checkout_sessions_unique_stripe_session_id": {
                "table": "checkout_sessions",
                "columns": ("stripe_session_id",),
                "unique": True,
                "predicate": "stripe_session_id IS NOT NULL",
            },
            "checkout_sessions_unique_idempotency_key": {
                "table": "checkout_sessions",
                "columns": ("idempotency_key",),
                "unique": True,
                "predicate": "idempotency_key IS NOT NULL",
            },
            "users_unique_stripe_subscription": {
                "table": "users",
                "columns": ("stripe_subscription_id",),
                "unique": True,
                "predicate": "stripe_subscription_id IS NOT NULL",
            },
            "users_unique_stripe_customer": {
                "table": "users",
                "columns": ("stripe_customer_id",),
                "unique": True,
                "predicate": "stripe_customer_id IS NOT NULL",
            },
            "stripe_links_unique_subscription_user": {
                "table": "stripe_links",
                "columns": ("stripe_subscription_id",),
                "unique": True,
                "predicate": "stripe_subscription_id IS NOT NULL",
            },
        },
        "requires_payment_integrity_clean": True,
    },
    "0007_subscription_refund_reconciliation": {
        "tables": ("subscription_refund_reconciliations", "subscription_refund_events"),
        "columns": {
            "subscription_refund_reconciliations": (
                "id",
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
                "amount_refunded",
                "original_amount",
                "currency",
                "refund_status",
                "is_full_refund",
                "reconciliation_result",
                "review_reason",
                "access_revoked_at",
                "created_at",
                "updated_at",
            ),
            "subscription_refund_events": (
                "id",
                "reconciliation_id",
                "stripe_event_id",
                "event_type",
                "created_at",
            ),
        },
        "indexes": {
            "subscription_refund_reconciliations_unique_key": {
                "table": "subscription_refund_reconciliations",
                "columns": ("reconciliation_key",),
                "unique": True,
                "predicate": None,
            },
            "subscription_refund_reconciliations_unique_refund_id": {
                "table": "subscription_refund_reconciliations",
                "columns": ("refund_id",),
                "unique": True,
                "predicate": "refund_id IS NOT NULL",
            },
            "srr_unique_refund_payment_revoke": {
                "table": "subscription_refund_reconciliations",
                "columns": ("original_payment_event_id",),
                "unique": True,
                "predicate": "original_payment_event_id IS NOT NULL AND reconciliation_result = 'access_revoked'",
            },
            "subscription_refund_events_unique_stripe_event_id": {
                "table": "subscription_refund_events",
                "columns": ("stripe_event_id",),
                "unique": True,
                "predicate": None,
            },
        },
    },
    "0008_stripe_event_claim_fencing": {
        "tables": ("stripe_events",),
        "columns": {
            "stripe_events": ("claim_generation",),
        },
    },
}

BASELINE_REQUIRED_TABLES = MIGRATION_BASELINE_REQUIREMENTS["0002_checkout_and_hardening_tables"]["tables"] + (
    "stripe_identity_conflicts",
    "aiogram_fsm_states",
    "gift_access_grants",
    "gift_certificate_templates",
    "gift_access_events",
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


def normalize_index_predicate(predicate):
    if predicate is None:
        return None
    text = re.sub(r"\s+", " ", str(predicate)).strip().lower()
    while text.startswith("(") and text.endswith(")"):
        inner = text[1:-1].strip()
        if inner.count("(") != inner.count(")"):
            break
        text = inner
    return text


def index_structure_matches(cur, index_name, table, columns, unique=True, predicate=None):
    cur.execute(
        """
        SELECT
            i.indisunique,
            pg_get_expr(i.indpred, i.indrelid) AS predicate,
            array_agg(a.attname ORDER BY ord.ordinality) AS columns
        FROM pg_class idx
        JOIN pg_index i ON i.indexrelid = idx.oid
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        JOIN unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ordinality) ON TRUE
        JOIN pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = ord.attnum
        WHERE ns.nspname = 'public'
          AND idx.relname = %s
          AND tbl.relname = %s
        GROUP BY i.indisunique, i.indpred, i.indrelid
        """,
        (index_name, table),
    )
    row = cur.fetchone()
    if not row:
        return False
    is_unique, actual_predicate, actual_columns = row
    return (
        bool(is_unique) is bool(unique)
        and tuple(actual_columns or ()) == tuple(columns)
        and normalize_index_predicate(actual_predicate) == normalize_index_predicate(predicate)
    )


def equivalent_index_structure_exists(cur, table, columns, unique=True, predicate=None):
    cur.execute(
        """
        SELECT
            idx.relname,
            i.indisunique,
            pg_get_expr(i.indpred, i.indrelid) AS predicate,
            array_agg(a.attname ORDER BY ord.ordinality) AS columns
        FROM pg_class idx
        JOIN pg_index i ON i.indexrelid = idx.oid
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        JOIN unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ordinality) ON TRUE
        JOIN pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = ord.attnum
        WHERE ns.nspname = 'public'
          AND tbl.relname = %s
        GROUP BY idx.relname, i.indisunique, i.indpred, i.indrelid
        """,
        (table,),
    )
    expected_predicate = normalize_index_predicate(predicate)
    expected_columns = tuple(columns)
    for _name, is_unique, actual_predicate, actual_columns in cur.fetchall():
        if (
            bool(is_unique) is bool(unique)
            and tuple(actual_columns or ()) == expected_columns
            and normalize_index_predicate(actual_predicate) == expected_predicate
        ):
            return True
    return False


def payment_integrity_index_requirement_matches(cur, index_name, spec):
    if index_structure_matches(
        cur,
        index_name,
        spec["table"],
        spec["columns"],
        unique=spec.get("unique", True),
        predicate=spec.get("predicate"),
    ):
        return True
    return equivalent_index_structure_exists(
        cur,
        spec["table"],
        spec["columns"],
        unique=spec.get("unique", True),
        predicate=spec.get("predicate"),
    )


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


PAYMENT_INTEGRITY_DUPLICATE_CHECKS = (
    ("payment_events.stripe_event_id", "payment_events", "stripe_event_id", "COUNT(*)"),
    ("checkout_sessions.stripe_session_id", "checkout_sessions", "stripe_session_id", "COUNT(*)"),
    ("checkout_sessions.idempotency_key", "checkout_sessions", "idempotency_key", "COUNT(*)"),
    ("users.stripe_customer_id", "users", "stripe_customer_id", "COUNT(*)"),
    ("users.stripe_subscription_id", "users", "stripe_subscription_id", "COUNT(*)"),
    ("stripe_links.stripe_subscription_id", "stripe_links", "stripe_subscription_id", "COUNT(DISTINCT telegram_id)"),
    ("stripe_events.event_id", "stripe_events", "event_id", "COUNT(*)"),
)


def assert_no_payment_integrity_duplicates(cur):
    for label, table, column, count_expr in PAYMENT_INTEGRITY_DUPLICATE_CHECKS:
        if not table_exists(cur, table) or column not in present_columns(cur, table):
            continue
        cur.execute(
            f"""
            SELECT {column}
            FROM {table}
            WHERE {column} IS NOT NULL
            GROUP BY {column}
            HAVING {count_expr} > 1
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row:
            raise MigrationError(f"Payment integrity guard blocked by duplicate {label}")


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
    required_indexes = requirements.get("indexes", ())
    if isinstance(required_indexes, dict):
        for index_name, spec in required_indexes.items():
            if not payment_integrity_index_requirement_matches(cur, index_name, spec):
                return False
    else:
        required_indexes = set(required_indexes)
        if required_indexes and not required_indexes <= present_indexes(cur):
            return False
    if requirements.get("requires_stripe_identity_clean"):
        assert_no_stripe_identity_conflicts(cur)
    if requirements.get("requires_payment_integrity_clean"):
        assert_no_payment_integrity_duplicates(cur)
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
            if MIGRATION_BASELINE_REQUIREMENTS.get(migration["version"], {}).get("requires_payment_integrity_clean"):
                assert_no_payment_integrity_duplicates(cur)
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
