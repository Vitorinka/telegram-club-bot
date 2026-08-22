import base64
import json

from admin_users import (
    _access_status,
    _access_type,
    _iso,
    _safe_label,
    escape_like,
    mask_stripe_identifier,
)


SUBSCRIPTION_STATES = frozenset({
    "all", "active", "auto_renew", "non_renewing", "failed_payment",
    "active_grace", "expired_grace", "expired", "stripe_linked",
    "no_stripe", "removal_retry", "completed",
})
DEFAULT_SUBSCRIPTIONS_LIMIT = 25
MAX_SUBSCRIPTIONS_LIMIT = 50
MAX_SUBSCRIPTIONS_QUERY_LENGTH = 64
SUBSCRIPTIONS_STATEMENT_TIMEOUT_MS = 5000
REMOVAL_RETRY_STATUSES = (
    "pending", "processing", "stripe_canceled", "telegram_failed",
    "telegram_removed",
)


class AdminSubscriptionsQueryError(ValueError):
    pass


def parse_subscriptions_limit(value):
    if value in (None, ""):
        return DEFAULT_SUBSCRIPTIONS_LIMIT
    try:
        limit = int(value)
    except (TypeError, ValueError):
        raise AdminSubscriptionsQueryError("invalid_limit") from None
    if limit < 1 or limit > MAX_SUBSCRIPTIONS_LIMIT:
        raise AdminSubscriptionsQueryError("invalid_limit")
    return limit


def normalize_subscriptions_query(value):
    query = str(value or "").strip()
    if len(query) > MAX_SUBSCRIPTIONS_QUERY_LENGTH:
        raise AdminSubscriptionsQueryError("query_too_long")
    return query


def encode_subscriptions_cursor(telegram_id):
    payload = json.dumps([int(telegram_id)], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_subscriptions_cursor(value):
    if not value:
        return None
    try:
        raw = str(value)
        decoded = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
        values = json.loads(decoded)
        if not isinstance(values, list) or len(values) != 1 or isinstance(values[0], bool):
            raise ValueError
        telegram_id = int(values[0])
    except (TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
        raise AdminSubscriptionsQueryError("invalid_cursor") from None
    if telegram_id <= 0:
        raise AdminSubscriptionsQueryError("invalid_cursor")
    return telegram_id


ACTIVE_GRACE_SQL = """(
    u.payment_failed IS TRUE
    AND u.grace_period_end IS NOT NULL
    AND u.grace_period_end > (NOW() AT TIME ZONE 'UTC')
)"""
EXPIRED_GRACE_SQL = """(
    u.payment_failed IS TRUE
    AND u.grace_period_end IS NOT NULL
    AND u.grace_period_end <= (NOW() AT TIME ZONE 'UTC')
)"""
ACTIVE_ACCESS_SQL = f"""(
    (u.paid IS TRUE AND u.expiry_date IS NOT NULL
     AND u.expiry_date > (NOW() AT TIME ZONE 'UTC'))
    OR {ACTIVE_GRACE_SQL}
)"""
REMOVAL_RETRY_SQL = "r.status IN ('pending', 'processing', 'stripe_canceled', 'telegram_failed', 'telegram_removed')"


STATE_SQL = {
    "all": None,
    "active": ACTIVE_ACCESS_SQL,
    "auto_renew": "u.paid IS TRUE AND u.auto_renew IS TRUE",
    "non_renewing": "u.paid IS TRUE AND u.auto_renew IS NOT TRUE",
    "failed_payment": "u.payment_failed IS TRUE",
    "active_grace": ACTIVE_GRACE_SQL,
    "expired_grace": EXPIRED_GRACE_SQL,
    "expired": f"u.paid IS TRUE AND u.expiry_date IS NOT NULL AND u.expiry_date <= (NOW() AT TIME ZONE 'UTC') AND NOT {ACTIVE_GRACE_SQL}",
    "stripe_linked": "(u.stripe_customer_id IS NOT NULL OR u.stripe_subscription_id IS NOT NULL)",
    "no_stripe": "(u.stripe_customer_id IS NULL AND u.stripe_subscription_id IS NULL)",
    "removal_retry": REMOVAL_RETRY_SQL,
    "completed": "r.status = 'db_finalized'",
}


def _begin_read_only(cur):
    cur.execute("SET TRANSACTION READ ONLY")
    cur.execute(f"SET LOCAL statement_timeout = {SUBSCRIPTIONS_STATEMENT_TIMEOUT_MS}")


def _subscription_state(paid, expiry_date, auto_renew, payment_failed, grace_period_end, now):
    if payment_failed and grace_period_end and grace_period_end > now:
        return "active_grace"
    if payment_failed and grace_period_end and grace_period_end <= now:
        return "expired_grace"
    if payment_failed:
        return "failed_payment"
    if paid and expiry_date and expiry_date > now:
        return "active_renewing" if auto_renew else "active_non_renewing"
    if paid and expiry_date and expiry_date <= now:
        return "expired"
    if paid:
        return "unknown"
    return "inactive"


def _needs_attention(payment_failed, grace_period_end, removal_status):
    return bool(
        payment_failed
        or removal_status in REMOVAL_RETRY_STATUSES
    )


def _projection(row):
    (
        telegram_id, username, first_name, paid, expiry_date, auto_renew,
        payment_failed, payment_failed_at, grace_period_end, trial_used,
        first_payment_done, stripe_linked, gift_current, removal_status, now,
    ) = row
    removal_status = _safe_label(removal_status) if removal_status else None
    return {
        "telegram_id": int(telegram_id),
        "username": username,
        "first_name": first_name,
        "access_status": _access_status(
            paid, expiry_date, payment_failed, grace_period_end, now
        ),
        "access_type": _access_type(
            trial_used, first_payment_done, gift_current, stripe_linked
        ),
        "expiry_date": _iso(expiry_date),
        "paid": bool(paid),
        "auto_renew": bool(auto_renew),
        "payment_failed": bool(payment_failed),
        "payment_failed_at": _iso(payment_failed_at),
        "grace_period_end": _iso(grace_period_end),
        "stripe_linked": bool(stripe_linked),
        "subscription_state": _subscription_state(
            paid, expiry_date, auto_renew, payment_failed, grace_period_end, now
        ),
        "removal_status": removal_status,
        "needs_attention": _needs_attention(
            payment_failed, grace_period_end, removal_status
        ),
    }


BASE_SELECT = """
    SELECT
        u.telegram_id, u.username, u.first_name, u.paid, u.expiry_date,
        u.auto_renew, u.payment_failed, u.payment_failed_at,
        u.grace_period_end, u.trial_used, u.first_payment_done,
        (u.stripe_customer_id IS NOT NULL OR u.stripe_subscription_id IS NOT NULL),
        EXISTS (
            SELECT 1 FROM gift_access_grants gift
            WHERE gift.recipient_telegram_id = u.telegram_id
              AND gift.status = 'redeemed'
              AND gift.applied_expiry = u.expiry_date
        ),
        r.status, (NOW() AT TIME ZONE 'UTC')
    FROM users u
    LEFT JOIN subscription_removal_events r ON r.telegram_id = u.telegram_id
"""


def list_admin_subscriptions(
    get_connection, *, limit=25, cursor=None, query="", state="all"
):
    limit = parse_subscriptions_limit(limit)
    query = normalize_subscriptions_query(query)
    if state not in SUBSCRIPTION_STATES:
        raise AdminSubscriptionsQueryError("invalid_state")
    cursor_id = decode_subscriptions_cursor(cursor)
    clauses = []
    params = []
    if STATE_SQL[state]:
        clauses.append(STATE_SQL[state])
    if query:
        pattern = f"%{escape_like(query)}%"
        if query.isdigit():
            clauses.append("(u.telegram_id = %s OR u.username ILIKE %s ESCAPE E'\\\\')")
            params.extend((int(query), pattern))
        else:
            clauses.append("u.username ILIKE %s ESCAPE E'\\\\'")
            params.append(pattern)
    if cursor_id:
        clauses.append("u.telegram_id < %s")
        params.append(cursor_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"{BASE_SELECT} {where} ORDER BY u.telegram_id DESC LIMIT %s"
    params.append(limit + 1)
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        cur.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE {ACTIVE_ACCESS_SQL}),
                COUNT(*) FILTER (WHERE {ACTIVE_GRACE_SQL}),
                COUNT(*) FILTER (WHERE u.payment_failed IS TRUE),
                COUNT(*) FILTER (WHERE u.paid IS TRUE AND u.auto_renew IS NOT TRUE),
                COUNT(*) FILTER (WHERE
                    u.payment_failed IS TRUE OR {REMOVAL_RETRY_SQL}
                )
            FROM users u
            LEFT JOIN subscription_removal_events r ON r.telegram_id = u.telegram_id
        """)
        summary_row = cur.fetchone()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    has_more = len(rows) > limit
    page = rows[:limit]
    return {
        "items": [_projection(row) for row in page],
        "next_cursor": encode_subscriptions_cursor(page[-1][0]) if has_more else None,
        "has_more": has_more,
        "summary": {
            "active": int(summary_row[0] or 0),
            "grace": int(summary_row[1] or 0),
            "failed_payment": int(summary_row[2] or 0),
            "non_renewing": int(summary_row[3] or 0),
            "needs_attention": int(summary_row[4] or 0),
        },
    }


def get_admin_subscription_details(get_connection, telegram_id):
    try:
        telegram_id = int(telegram_id)
    except (TypeError, ValueError):
        raise AdminSubscriptionsQueryError("invalid_telegram_id") from None
    if telegram_id <= 0:
        raise AdminSubscriptionsQueryError("invalid_telegram_id")
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute(
            BASE_SELECT.replace(
                "u.first_name, u.paid",
                "u.first_name, u.paid",
            ) + " WHERE u.telegram_id = %s",
            (telegram_id,),
        )
        projection_row = cur.fetchone()
        if projection_row is None:
            conn.rollback()
            return None
        cur.execute(
            """
            SELECT last_name, trial_used, first_payment_done,
                   stripe_customer_id, stripe_subscription_id
            FROM users WHERE telegram_id = %s
            """,
            (telegram_id,),
        )
        user_extra = cur.fetchone()
        cur.execute(
            """
            SELECT status, reason, access_expiry, stripe_canceled_at,
                   telegram_banned_at, updated_at
            FROM subscription_removal_events WHERE telegram_id = %s
            """,
            (telegram_id,),
        )
        removal = cur.fetchone()
        cur.execute(
            """
            SELECT event_type, source, old_expiry, new_expiry, created_at
            FROM access_events WHERE telegram_id = %s
            ORDER BY created_at DESC, id DESC LIMIT 10
            """,
            (telegram_id,),
        )
        access_events = cur.fetchall()
        cur.execute(
            """
            SELECT event_type, payment_status, payment_kind, tariff_code,
                   period_start, period_end, created_at
            FROM payment_events WHERE telegram_id = %s
            ORDER BY created_at DESC, id DESC LIMIT 10
            """,
            (telegram_id,),
        )
        payment_events = cur.fetchall()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    result = _projection(projection_row)
    last_name, trial_used, first_payment_done, customer_id, subscription_id = user_extra
    result.update({
        "last_name": last_name,
        "trial_used": bool(trial_used),
        "first_payment_done": bool(first_payment_done),
        "stripe": {
            "customer_id": mask_stripe_identifier(customer_id),
            "subscription_id": mask_stripe_identifier(subscription_id),
        },
        "removal": None if removal is None else {
            "status": _safe_label(removal[0]),
            "reason": _safe_label(removal[1]),
            "access_expiry": _iso(removal[2]),
            "stripe_canceled_at": _iso(removal[3]),
            "telegram_banned_at": _iso(removal[4]),
            "updated_at": _iso(removal[5]),
        },
        "access_history": [
            {
                "event_type": _safe_label(row[0]),
                "source": _safe_label(row[1]),
                "old_expiry": _iso(row[2]),
                "new_expiry": _iso(row[3]),
                "created_at": _iso(row[4]),
            }
            for row in access_events
        ],
        "payment_history": [
            {
                "event_type": _safe_label(row[0]),
                "payment_status": _safe_label(row[1]),
                "payment_kind": _safe_label(row[2]),
                "tariff_code": _safe_label(row[3]),
                "period_start": _iso(row[4]),
                "period_end": _iso(row[5]),
                "created_at": _iso(row[6]),
            }
            for row in payment_events
        ],
    })
    return result
