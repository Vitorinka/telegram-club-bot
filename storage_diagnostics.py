import logging


APPLICATION_TABLES = (
    "users", "stripe_events", "access_events", "stripe_links",
    "unlinked_stripe_events", "payment_events", "weekly_report_runs",
    "system_settings", "checkout_sessions", "checkout_retry_events",
    "trial_redemptions", "admin_action_requests", "scheduled_job_runs",
    "message_delivery_events", "subscription_removal_events",
    "bot_invite_links", "admin_alerts", "stripe_identity_conflicts",
    "aiogram_fsm_states", "gift_access_grants", "gift_certificate_templates",
    "gift_access_events", "subscription_refund_reconciliations",
    "subscription_refund_events", "content_items", "content_media",
    "content_media_uploads", "recipe_ingredients", "recipe_steps",
    "nutrition_material_bodies",
    "content_item_versions", "content_item_version_media",
    "content_item_version_recipe_ingredients", "content_item_version_recipe_steps",
    "content_item_version_nutrition",
    "content_categories", "content_item_categories", "content_item_version_categories",
    "schema_migrations",
)

CATALOG_SQL = """
SELECT requested.table_name,
       COALESCE(c.reltuples::bigint, 0) AS estimated_rows,
       COALESCE(pg_relation_size(c.oid), 0) AS table_bytes,
       COALESCE(pg_indexes_size(c.oid), 0) AS index_bytes,
       COALESCE(pg_total_relation_size(c.oid), 0) AS total_bytes,
       c.oid IS NOT NULL AS available
FROM unnest(%s::text[]) WITH ORDINALITY AS requested(table_name, position)
LEFT JOIN pg_namespace n ON n.nspname = 'public'
LEFT JOIN pg_class c ON c.relnamespace = n.oid
                    AND c.relname = requested.table_name
                    AND c.relkind IN ('r', 'p')
ORDER BY requested.position
"""

OPERATIONAL_QUERIES = {
    "outbox_states": """
        SELECT COUNT(*) FILTER (WHERE status IN ('pending', 'processing', 'failed')),
               COUNT(*) FILTER (WHERE status IN ('sent', 'cancelled', 'permanently_failed'))
        FROM message_delivery_events
    """,
    "stripe_processing": """
        SELECT COUNT(*) FILTER (WHERE processed IS NOT TRUE),
               COUNT(*) FILTER (WHERE processed IS NOT TRUE AND processed_at < NOW() - INTERVAL '10 minutes')
        FROM stripe_events
    """,
    "unlinked_unresolved": "SELECT COUNT(*) FROM unlinked_stripe_events WHERE resolved IS FALSE",
    "identity_conflicts": "SELECT COUNT(*) FROM stripe_identity_conflicts WHERE resolved IS NOT TRUE",
    "checkout_active": """
        SELECT COUNT(*) FROM checkout_sessions
        WHERE status IN ('creating', 'creation_unknown', 'open', 'payment_pending')
    """,
    "gift_states": "SELECT status, COUNT(*) FROM gift_access_grants GROUP BY status ORDER BY status",
    "scheduled_age": "SELECT MIN(started_at), MAX(started_at) FROM scheduled_job_runs",
    "payment_growth": """
        SELECT
          (SELECT COUNT(*) FROM payment_events WHERE created_at >= NOW() - INTERVAL '24 hours'),
          (SELECT COUNT(*) FROM payment_events WHERE created_at >= NOW() - INTERVAL '7 days'),
          (SELECT MIN(created_at) FROM payment_events),
          (SELECT MAX(created_at) FROM payment_events)
    """,
}

RETENTION_QUERIES = {
    "stale FSM (>30d)": "SELECT COUNT(*) FROM aiogram_fsm_states WHERE updated_at < NOW() - INTERVAL '30 days'",
    "resolved checkout retries (>30d)": """
        SELECT COUNT(*) FROM checkout_retry_events
        WHERE resolved_at IS NOT NULL AND resolved_at < NOW() - INTERVAL '30 days'
    """,
    "expired/revoked invite links (>90d)": """
        SELECT COUNT(*) FROM bot_invite_links
        WHERE (
                status = 'revoked'
                OR (
                    expires_at IS NOT NULL
                    AND expires_at < NOW()
                    AND status IS DISTINCT FROM 'active'
                )
              )
          AND COALESCE(revoked_at, expires_at, created_at) < NOW() - INTERVAL '90 days'
    """,
    "terminal/expired admin actions (>90d)": """
        SELECT COUNT(*) FROM admin_action_requests
        WHERE (status IN ('completed', 'failed', 'cancelled') OR expires_at < NOW())
          AND COALESCE(completed_at, expires_at, created_at) < NOW() - INTERVAL '90 days'
    """,
    "ordinary terminal admin alerts (>90d)": """
        SELECT COUNT(*) FROM admin_alerts
        WHERE status IN ('delivered', 'partial', 'failed')
          AND created_at < NOW() - INTERVAL '90 days'
          AND COALESCE(alert_key, '') NOT LIKE 'outbox-retry:%%'
          AND COALESCE(alert_key, '') NOT LIKE 'outbox-permanent:%%'
    """,
    "existing terminal checkout predicate (>30d)": """
        SELECT COUNT(*) FROM checkout_sessions cs
        WHERE cs.status IN ('completed', 'expired', 'failed')
          AND cs.updated_at < NOW() - INTERVAL '30 days'
          AND EXISTS (
              SELECT 1
              FROM payment_events pe
              WHERE pe.telegram_id = cs.telegram_id
                AND pe.payment_status = 'succeeded'
          )
    """,
}


def format_bytes(value):
    value = max(0, int(value or 0))
    units = ("B", "KB", "MB", "GB", "TB")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{int(amount)} {unit}" if unit == "B" or amount >= 100 else f"{amount:.1f} {unit}"
        amount /= 1024


def _safe_query(conn, sql, params=(), fetch="one", metric="metric"):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchall() if fetch == "all" else cur.fetchone()
    except Exception as error:
        conn.rollback()
        logging.warning("STORAGE_DIAGNOSTIC_UNAVAILABLE: metric=%s error_class=%s", metric, type(error).__name__)
        cur.close()
        cur = conn.cursor()
        cur.execute("SET TRANSACTION READ ONLY")
        return None
    finally:
        cur.close()


def collect_storage_diagnostics(conn):
    cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
    finally:
        cur.close()

    catalog_rows = _safe_query(conn, CATALOG_SQL, (list(APPLICATION_TABLES),), "all", "catalog") or []
    tables = [
        {
            "table": row[0], "estimated_rows": max(0, int(row[1])), "table_bytes": int(row[2]),
            "index_bytes": int(row[3]), "total_bytes": int(row[4]), "available": bool(row[5]),
        }
        for row in catalog_rows
    ]
    operational = {
        name: _safe_query(conn, sql, metric=name)
        for name, sql in OPERATIONAL_QUERIES.items()
    }
    retention = {
        name: (lambda row: int(row[0]) if row else None)(_safe_query(conn, sql, metric=name))
        for name, sql in RETENTION_QUERIES.items()
    }
    conn.rollback()
    return {"tables": tables, "operational": operational, "retention": retention}


def render_storage_diagnostics(data, message_limit=3900):
    tables = [row for row in data.get("tables", []) if row.get("available")]
    unavailable = [row["table"] for row in data.get("tables", []) if not row.get("available")]
    largest = sorted(tables, key=lambda row: row["total_bytes"], reverse=True)[:10]
    total_bytes = sum(row["total_bytes"] for row in tables)
    pages = [
        "🗄 Storage diagnostics\n\n"
        f"Application relations: {len(tables)}/{len(APPLICATION_TABLES)}\n"
        f"Total relation size: {format_bytes(total_bytes)}\n"
        "Rows: PostgreSQL estimates; sizes: exact catalog values.\n"
        + (f"Unavailable relations: {len(unavailable)}\n" if unavailable else "")
        + "Read-only: yes",
        "📊 Largest tables\n\n" + "\n".join(
            f"{row['table']}: ~{row['estimated_rows']} rows, total {format_bytes(row['total_bytes'])} "
            f"(table {format_bytes(row['table_bytes'])}, indexes {format_bytes(row['index_bytes'])})"
            for row in largest
        ),
    ]

    op = data.get("operational", {})
    lines = ["📚 Operational history"]
    if op.get("outbox_states"):
        lines.append(f"Outbox: active {op['outbox_states'][0]}, terminal {op['outbox_states'][1]}; cleanup NOT SAFE")
    if op.get("stripe_processing"):
        lines.append(f"Stripe events: unprocessed {op['stripe_processing'][0]}, stale {op['stripe_processing'][1]}; cleanup NOT SAFE")
    lines.append(f"Payment events: cleanup NOT SAFE")
    lines.append(f"Trial redemptions: cleanup NOT SAFE")
    if op.get("unlinked_unresolved"):
        lines.append(f"Unlinked Stripe unresolved: {op['unlinked_unresolved'][0]}")
    if op.get("identity_conflicts"):
        lines.append(f"Unresolved identity conflicts: {op['identity_conflicts'][0]}")
    if op.get("checkout_active"):
        lines.append(f"Active checkout sessions: {op['checkout_active'][0]}")
    if op.get("scheduled_age"):
        lines.append(f"Scheduled job range: {op['scheduled_age'][0] or 'n/a'} — {op['scheduled_age'][1] or 'n/a'}")
        lines.append("Scheduled job growth 24h/7d: not measured (no supporting timestamp index)")
    if op.get("payment_growth"):
        lines.append(
            f"Payment events growth: 24h {op['payment_growth'][0]}, 7d {op['payment_growth'][1]}; "
            f"range {op['payment_growth'][2] or 'n/a'} — {op['payment_growth'][3] or 'n/a'}"
        )
    pages.append("\n".join(lines))

    retention_lines = ["🧪 Retention dry-run (counts only; no rows changed)"]
    for label, count in data.get("retention", {}).items():
        retention_lines.append(f"{label}: {count if count is not None else 'unavailable'}")
    retention_lines.extend([
        "message_delivery_events terminal keys: MUST RETAIN",
        "stripe_events event IDs: MUST RETAIN",
        "payment/refund/gift identities: MUST RETAIN",
        "trial_redemptions identities: MUST RETAIN",
    ])
    pages.append("\n".join(retention_lines))

    result = []
    for page in pages:
        if len(page) <= message_limit:
            result.append(page)
        else:
            lines = page.splitlines()
            chunk = ""
            for line in lines:
                candidate = f"{chunk}\n{line}" if chunk else line
                if len(candidate) > message_limit:
                    result.append(chunk)
                    chunk = line
                else:
                    chunk = candidate
            if chunk:
                result.append(chunk)
    return result
