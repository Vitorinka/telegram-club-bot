import hashlib
import logging
import re


GROUPED_QUERIES = {
    "message_delivery_events.status": "SELECT status, COUNT(*) FROM message_delivery_events GROUP BY status ORDER BY status NULLS FIRST",
    "payment_events.payment_status": "SELECT payment_status, COUNT(*) FROM payment_events GROUP BY payment_status ORDER BY payment_status NULLS FIRST",
    "payment_events.payment_kind": "SELECT payment_kind, COUNT(*) FROM payment_events GROUP BY payment_kind ORDER BY payment_kind NULLS FIRST",
    "checkout_sessions.status": "SELECT status, COUNT(*) FROM checkout_sessions GROUP BY status ORDER BY status NULLS FIRST",
    "checkout_sessions.mode": "SELECT mode, COUNT(*) FROM checkout_sessions GROUP BY mode ORDER BY mode NULLS FIRST",
    "weekly_report_runs.status": "SELECT status, COUNT(*) FROM weekly_report_runs GROUP BY status ORDER BY status NULLS FIRST",
    "admin_action_requests.status": "SELECT status, COUNT(*) FROM admin_action_requests GROUP BY status ORDER BY status NULLS FIRST",
    "admin_action_requests.action_type": "SELECT action_type, COUNT(*) FROM admin_action_requests GROUP BY action_type ORDER BY action_type NULLS FIRST",
    "scheduled_job_runs.status": "SELECT status, COUNT(*) FROM scheduled_job_runs GROUP BY status ORDER BY status NULLS FIRST",
    "subscription_removal_events.status": "SELECT status, COUNT(*) FROM subscription_removal_events GROUP BY status ORDER BY status NULLS FIRST",
    "bot_invite_links.status": "SELECT status, COUNT(*) FROM bot_invite_links GROUP BY status ORDER BY status NULLS FIRST",
    "admin_alerts.status": "SELECT status, COUNT(*) FROM admin_alerts GROUP BY status ORDER BY status NULLS FIRST",
    "subscription_refund_reconciliations.reconciliation_result": "SELECT reconciliation_result, COUNT(*) FROM subscription_refund_reconciliations GROUP BY reconciliation_result ORDER BY reconciliation_result NULLS FIRST",
    "stripe_identity_conflicts.conflict_type": "SELECT conflict_type, COUNT(*) FROM stripe_identity_conflicts GROUP BY conflict_type ORDER BY conflict_type NULLS FIRST",
}

EXPECTED_VALUES = {
    "message_delivery_events.status": frozenset(("pending", "processing", "failed", "sent", "cancelled", "permanently_failed")),
    "payment_events.payment_status": frozenset(("succeeded", "failed")),
    "payment_events.payment_kind": frozenset((None, "trial", "initial_subscription", "recurring", "adjustment", "out_of_band", "gift_access", "unknown")),
    "checkout_sessions.status": frozenset(("creating", "creation_unknown", "open", "payment_pending", "completed", "expired", "failed", "manual_review_required")),
    "checkout_sessions.mode": frozenset(("payment", "subscription")),
    "weekly_report_runs.status": frozenset(("processing", "completed", "failed")),
    "admin_action_requests.status": frozenset(("pending", "processing", "completed", "failed", "cancelled")),
    "admin_action_requests.action_type": frozenset((
        "broadcast", "give_access", "set_expiry", "link_stripe_user",
        "resolve_checkout", "revoke_invite_links", "retry_delivery",
        "restore_access", "revoke_access", "gift_cancel", "gift_reissue",
        "gift_resend",
        "schedule_upload_replace",
        "billing_portal_resend",
    )),
    "scheduled_job_runs.status": frozenset(("running", "completed", "failed")),
    "subscription_removal_events.status": frozenset(("pending", "processing", "stripe_canceled", "telegram_failed", "telegram_removed", "db_finalized", "cancelled", "not_due", "superseded")),
    "bot_invite_links.status": frozenset((None, "active", "revoked")),
    "admin_alerts.status": frozenset((None, "claimed", "delivered", "partial", "failed", "observed")),
    "subscription_refund_reconciliations.reconciliation_result": frozenset(("review_required", "access_revoked", "already_reconciled", "already_inactive")),
    "stripe_identity_conflicts.conflict_type": frozenset((
        "users_subscription_conflict", "users_customer_conflict",
        "stripe_links_subscription_conflict", "stripe_links_customer_conflict",
    )),
}

LEGACY_ALLOWED_VALUES = {
    "stripe_identity_conflicts.conflict_type": frozenset((
        "users_duplicate_subscription",
    )),
}

STRUCTURAL_QUERIES = {
    "stripe_events": """
        SELECT
          COUNT(*) FILTER (WHERE claim_generation < 0),
          COUNT(*) FILTER (WHERE processed IS TRUE AND processed_at IS NULL)
        FROM stripe_events
    """,
    "message_delivery_events": """
        SELECT
          COUNT(*) FILTER (WHERE claim_generation < 0),
          COUNT(*) FILTER (WHERE attempt_count < 0),
          COUNT(*) FILTER (WHERE status = 'processing' AND lease_until IS NULL),
          COUNT(*) FILTER (WHERE status = 'sent' AND sent_at IS NULL)
        FROM message_delivery_events
    """,
    "subscription_removal_events": "SELECT COUNT(*) FILTER (WHERE attempt_count < 0) FROM subscription_removal_events",
    "weekly_report_runs": """
        SELECT
          COUNT(*) FILTER (WHERE period_end <= period_start),
          COUNT(*) FILTER (WHERE status IN ('completed', 'failed') AND completed_at IS NULL)
        FROM weekly_report_runs
    """,
    "scheduled_job_runs": "SELECT COUNT(*) FILTER (WHERE status = 'completed' AND completed_at IS NULL) FROM scheduled_job_runs",
    "admin_action_requests": "SELECT COUNT(*) FILTER (WHERE status = 'completed' AND completed_at IS NULL) FROM admin_action_requests",
    "payment_events": """
        SELECT
          COUNT(*) FILTER (WHERE amount_paid < 0),
          COUNT(*) FILTER (WHERE amount_due < 0),
          COUNT(*) FILTER (
            WHERE period_start IS NOT NULL AND period_end IS NOT NULL AND period_end < period_start
          )
        FROM payment_events
    """,
}

STRUCTURAL_LABELS = {
    "stripe_events": ("negative claim generation", "processed without processed_at"),
    "message_delivery_events": (
        "negative claim generation", "negative attempt count",
        "processing without lease", "sent without sent_at",
    ),
    "subscription_removal_events": ("negative attempt count",),
    "weekly_report_runs": ("invalid period", "terminal without completed_at"),
    "scheduled_job_runs": ("completed without completed_at",),
    "admin_action_requests": ("completed without completed_at",),
    "payment_events": ("negative amount_paid", "negative amount_due", "reversed period"),
}

OPEN_DOMAIN_QUERIES = {
    "stripe_links.status": "SELECT status, COUNT(*) FROM stripe_links GROUP BY status ORDER BY status NULLS FIRST",
    "stripe_events.event_type": "SELECT event_type, COUNT(*) FROM stripe_events GROUP BY event_type ORDER BY event_type NULLS FIRST",
    "subscription_refund_reconciliations.refund_status": "SELECT refund_status, COUNT(*) FROM subscription_refund_reconciliations GROUP BY refund_status ORDER BY refund_status NULLS FIRST",
}

_SAFE_VALUE_RE = re.compile(r"^[a-z][a-z0-9_.:-]{0,79}$")
_SENSITIVE_PREFIXES = ("cus_", "sub_", "evt_", "cs_", "pi_", "ch_", "re_", "sk_", "wh" + "sec_")


def _safe_query(conn, sql, fetch="all", metric="metric"):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall() if fetch == "all" else cur.fetchone()
    except Exception as error:
        conn.rollback()
        logging.warning(
            "CONSTRAINT_AUDIT_METRIC_UNAVAILABLE: metric=%s error_class=%s",
            metric,
            type(error).__name__,
        )
        cur.close()
        cur = conn.cursor()
        cur.execute("SET TRANSACTION READ ONLY")
        return None
    finally:
        cur.close()


def collect_constraint_audit(conn):
    cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
    finally:
        cur.close()

    grouped = {
        name: _safe_query(conn, sql, metric=name)
        for name, sql in GROUPED_QUERIES.items()
    }
    structural = {
        name: _safe_query(conn, sql, fetch="one", metric=name)
        for name, sql in STRUCTURAL_QUERIES.items()
    }
    external = {
        name: _safe_query(conn, sql, metric=name)
        for name, sql in OPEN_DOMAIN_QUERIES.items()
    }
    conn.rollback()
    return {"grouped": grouped, "structural": structural, "external": external}


def unexpected_values(data):
    result = {}
    for name, expected in EXPECTED_VALUES.items():
        rows = data.get("grouped", {}).get(name)
        if rows is None:
            continue
        allowed = expected | LEGACY_ALLOWED_VALUES.get(name, frozenset())
        unexpected = [(value, int(count)) for value, count in rows if value not in allowed]
        if unexpected:
            result[name] = unexpected
    return result


def _safe_value(value):
    if value is None:
        return "NULL"
    text = str(value)
    lowered = text.lower()
    sensitive = (
        lowered.startswith(_SENSITIVE_PREFIXES)
        or "://" in lowered
        or "@" in lowered
        or "secret" in lowered
        or "token" in lowered
    )
    if sensitive or not _SAFE_VALUE_RE.fullmatch(text):
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
        return f"<redacted:{digest}>"
    return text


def _split_pages(text, limit):
    pages = []
    current = ""
    for line in text.splitlines():
        pieces = [line[index:index + limit] for index in range(0, len(line), limit)] or [""]
        for piece in pieces:
            candidate = f"{current}\n{piece}" if current else piece
            if len(candidate) > limit:
                if current:
                    pages.append(current)
                current = piece
            else:
                current = candidate
    if current:
        pages.append(current)
    return pages


def render_constraint_audit(data, message_limit=3900):
    unexpected = unexpected_values(data)
    unexpected_count = sum(len(rows) for rows in unexpected.values())
    violation_count = sum(
        int(value or 0)
        for row in data.get("structural", {}).values()
        if row is not None
        for value in row
    )
    lines = [
        "🧪 DB constraint audit", "", "Summary:",
        f"- unexpected closed-set values: {unexpected_count}",
        f"- structural violations: {violation_count}",
        f"- candidate groups checked: {len(GROUPED_QUERIES)}",
        "- read-only: yes", "", "Closed state machines",
    ]
    for name in GROUPED_QUERIES:
        rows = data.get("grouped", {}).get(name)
        lines.append(f"- {name}")
        if name in EXPECTED_VALUES:
            expected = sorted(_safe_value(value) for value in EXPECTED_VALUES[name])
            lines.append(f"  current expected: {', '.join(expected)}")
            legacy = sorted(_safe_value(value) for value in LEGACY_ALLOWED_VALUES.get(name, ()))
            lines.append(f"  legacy allowed: {', '.join(legacy) if legacy else 'none'}")
        else:
            lines.append("  expected: observed-only candidate")
        if rows is None:
            lines.append("  observed: unavailable")
            lines.append("  unexpected: unavailable")
            continue
        lines.append("  observed:")
        lines.extend(f"    {_safe_value(value)}: {int(count)}" for value, count in rows)
        bad = unexpected.get(name, [])
        lines.append(
            "  unexpected: " + (
                ", ".join(f"{_safe_value(value)} ({count})" for value, count in bad)
                if bad else "none"
            )
        )

    lines.extend(["", "Structural violations"])
    for name in STRUCTURAL_QUERIES:
        row = data.get("structural", {}).get(name)
        if row is None:
            lines.append(f"- {name}: unavailable")
            continue
        for label, count in zip(STRUCTURAL_LABELS[name], row):
            lines.append(f"- {name} — {label}: {int(count or 0)}")

    lines.extend(["", "Open external domains", "OPEN EXTERNAL DOMAIN — DO NOT CHECK-CONSTRAIN"])
    for name in OPEN_DOMAIN_QUERIES:
        rows = data.get("external", {}).get(name)
        lines.append(f"- {name}")
        if rows is None:
            lines.append("  observed: unavailable")
        else:
            lines.extend(f"  {_safe_value(value)}: {int(count)}" for value, count in rows)
    return _split_pages("\n".join(lines), message_limit)
