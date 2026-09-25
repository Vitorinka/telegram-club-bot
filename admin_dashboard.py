from datetime import datetime, timezone


DASHBOARD_STATEMENT_TIMEOUT_MS = 5000


USERS_METRICS_SQL = """
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE
        (paid IS TRUE AND expiry_date IS NOT NULL AND expiry_date > NOW())
        OR (payment_failed IS TRUE AND grace_period_end IS NOT NULL AND grace_period_end > NOW())
    ) AS active_access,
    COUNT(*) FILTER (WHERE
        paid IS TRUE AND expiry_date IS NOT NULL AND expiry_date <= NOW()
        AND NOT (
            payment_failed IS TRUE
            AND grace_period_end IS NOT NULL
            AND grace_period_end > NOW()
        )
    ) AS expired_paid,
    COUNT(*) FILTER (WHERE payment_failed IS TRUE) AS payment_failed,
    COUNT(*) FILTER (WHERE
        payment_failed IS TRUE AND grace_period_end IS NOT NULL AND grace_period_end > NOW()
    ) AS active_grace,
    COUNT(*) FILTER (WHERE
        trial_used IS TRUE
        AND first_payment_done IS NOT TRUE
        AND (
            (paid IS TRUE AND expiry_date IS NOT NULL AND expiry_date > NOW())
            OR (payment_failed IS TRUE AND grace_period_end IS NOT NULL AND grace_period_end > NOW())
        )
    ) AS trial,
    COUNT(*) FILTER (WHERE
        stripe_customer_id IS NOT NULL OR stripe_subscription_id IS NOT NULL
    ) AS stripe_linked,
    COUNT(*) FILTER (WHERE
        payment_failed IS TRUE AND grace_period_end IS NOT NULL AND grace_period_end <= NOW()
    ) AS expired_grace,
    COUNT(*) FILTER (WHERE paid IS TRUE AND auto_renew IS TRUE) AS auto_renew,
    COUNT(*) FILTER (WHERE paid IS TRUE AND auto_renew IS NOT TRUE) AS non_renewing
FROM users
"""


REMOVAL_METRICS_SQL = """
SELECT
    COUNT(*) FILTER (WHERE status = 'pending') AS pending_removals,
    COUNT(*) FILTER (WHERE status IN (
        'processing', 'stripe_canceled', 'telegram_failed', 'telegram_removed'
    )) AS retryable_removals,
    COUNT(*) FILTER (WHERE
        status = 'db_finalized'
        AND db_finalized_at >= NOW() - INTERVAL '24 hours'
    ) AS finalized_removals_recent
FROM subscription_removal_events
"""


DELIVERY_METRICS_SQL = """
SELECT
    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
    COUNT(*) FILTER (WHERE status = 'processing') AS processing,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed,
    COUNT(*) FILTER (WHERE status = 'permanently_failed') AS permanently_failed,
    COUNT(*) FILTER (WHERE status = 'sent' AND sent_at >= NOW() - INTERVAL '24 hours') AS sent_last_24h
FROM message_delivery_events
"""


MIGRATION_METRICS_SQL = """
SELECT COUNT(*), COALESCE(MAX(version), '')
FROM schema_migrations
"""


SCHEDULER_METRICS_SQL = """
SELECT
    COUNT(*) FILTER (WHERE status = 'failed' AND updated_at >= NOW() - INTERVAL '24 hours'),
    COUNT(*) FILTER (WHERE status = 'running'),
    COUNT(*) FILTER (WHERE
        status = 'running' AND (lease_until IS NULL OR lease_until <= NOW())
    )
FROM scheduled_job_runs
"""


DASHBOARD_METRICS_SQL = f"""
WITH users_metrics AS ({USERS_METRICS_SQL}),
removal_metrics AS ({REMOVAL_METRICS_SQL}),
delivery_metrics AS ({DELIVERY_METRICS_SQL}),
migration_metrics(migration_count, latest_version) AS ({MIGRATION_METRICS_SQL}),
scheduler_metrics(failed_last_24h, running, stale) AS ({SCHEDULER_METRICS_SQL})
SELECT
    u.total, u.active_access, u.expired_paid, u.payment_failed,
    u.active_grace, u.trial, u.stripe_linked, u.expired_grace,
    u.auto_renew, u.non_renewing,
    r.pending_removals, r.retryable_removals, r.finalized_removals_recent,
    d.pending, d.processing, d.failed, d.permanently_failed, d.sent_last_24h,
    m.migration_count, m.latest_version,
    s.failed_last_24h, s.running, s.stale
FROM users_metrics u, removal_metrics r, delivery_metrics d,
     migration_metrics m, scheduler_metrics s
"""


def _integer_dict(names, row):
    return {name: int(value or 0) for name, value in zip(names, row)}


def collect_admin_dashboard(get_connection, db_pool_health, scheduler_job_count):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute(f"SET LOCAL statement_timeout = {DASHBOARD_STATEMENT_TIMEOUT_MS}")

        cur.execute(DASHBOARD_METRICS_SQL)
        metrics_row = cur.fetchone()
        users_row = metrics_row[0:10]
        removal_row = metrics_row[10:13]
        delivery_row = metrics_row[13:18]
        migrations_row = metrics_row[18:20]
        scheduler_row = metrics_row[20:23]
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    user_values = _integer_dict(
        (
            "total", "active_access", "expired_paid", "payment_failed",
            "active_grace", "trial", "stripe_linked", "expired_grace",
            "auto_renew", "non_renewing",
        ),
        users_row,
    )
    pool = db_pool_health() or {}
    safe_pool = {
        key: pool.get(key)
        for key in (
            "pool_available", "pool_used", "connection_errors",
            "statement_timeout_ms",
        )
        if key in pool
    }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "users": {
            key: user_values[key]
            for key in (
                "total", "active_access", "expired_paid", "payment_failed",
                "active_grace", "trial", "stripe_linked",
            )
        },
        "billing": {
            "failed_payments": user_values["payment_failed"],
            "active_grace": user_values["active_grace"],
            "expired_grace": user_values["expired_grace"],
            "auto_renew": user_values["auto_renew"],
            "non_renewing": user_values["non_renewing"],
        },
        "access": _integer_dict(
            (
                "pending_removals", "retryable_removals",
                "finalized_removals_recent",
            ),
            removal_row,
        ),
        "deliveries": _integer_dict(
            ("pending", "processing", "failed", "permanently_failed", "sent_last_24h"),
            delivery_row,
        ),
        "system": {
            "db_pool": safe_pool,
            "migrations": {
                "count": int(migrations_row[0] or 0),
                "latest": migrations_row[1] or None,
            },
            "scheduler": {
                "known_jobs": int(scheduler_job_count),
                **_integer_dict(
                    ("failed_last_24h", "running", "stale"), scheduler_row
                ),
            },
        },
    }
