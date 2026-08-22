import base64
import json
import re
from datetime import datetime


USERS_STATUSES = frozenset({
    "all", "active", "trial", "failed_payment", "active_grace",
    "expired", "auto_renew", "non_renewing",
})
DEFAULT_USERS_LIMIT = 25
MAX_USERS_LIMIT = 50
MAX_USERS_QUERY_LENGTH = 64
USERS_STATEMENT_TIMEOUT_MS = 5000


class AdminUsersQueryError(ValueError):
    pass


def parse_users_limit(value):
    if value in (None, ""):
        return DEFAULT_USERS_LIMIT
    try:
        limit = int(value)
    except (TypeError, ValueError):
        raise AdminUsersQueryError("invalid_limit") from None
    if limit < 1 or limit > MAX_USERS_LIMIT:
        raise AdminUsersQueryError("invalid_limit")
    return limit


def normalize_users_query(value):
    query = str(value or "").strip()
    if len(query) > MAX_USERS_QUERY_LENGTH:
        raise AdminUsersQueryError("query_too_long")
    return query


def escape_like(value):
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def encode_users_cursor(registered_at, telegram_id):
    timestamp = registered_at.isoformat() if registered_at else "1970-01-01T00:00:00"
    payload = json.dumps([timestamp, int(telegram_id)], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_users_cursor(value):
    if not value:
        return None
    try:
        raw = str(value)
        decoded = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
        timestamp, telegram_id = json.loads(decoded)
        parsed_time = datetime.fromisoformat(timestamp)
        parsed_id = int(telegram_id)
    except (TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
        raise AdminUsersQueryError("invalid_cursor") from None
    if isinstance(telegram_id, bool) or parsed_id <= 0:
        raise AdminUsersQueryError("invalid_cursor")
    return parsed_time, parsed_id


ACTIVE_ACCESS_SQL = """(
    (u.paid IS TRUE AND u.expiry_date IS NOT NULL AND u.expiry_date > (NOW() AT TIME ZONE 'UTC'))
    OR (u.payment_failed IS TRUE AND u.grace_period_end IS NOT NULL AND u.grace_period_end > (NOW() AT TIME ZONE 'UTC'))
)"""
ACTIVE_GRACE_SQL = """(
    u.payment_failed IS TRUE
    AND u.grace_period_end IS NOT NULL
    AND u.grace_period_end > (NOW() AT TIME ZONE 'UTC')
)"""
CURRENT_GIFT_SQL = """EXISTS (
    SELECT 1 FROM gift_access_grants current_gift
    WHERE current_gift.recipient_telegram_id = u.telegram_id
      AND current_gift.status = 'redeemed'
      AND current_gift.applied_expiry = u.expiry_date
)"""


STATUS_SQL = {
    "all": None,
    "active": ACTIVE_ACCESS_SQL,
    "trial": f"u.trial_used IS TRUE AND u.first_payment_done IS NOT TRUE AND {ACTIVE_ACCESS_SQL} AND NOT {CURRENT_GIFT_SQL}",
    "failed_payment": "u.payment_failed IS TRUE",
    "active_grace": ACTIVE_GRACE_SQL,
    "expired": f"u.paid IS TRUE AND u.expiry_date IS NOT NULL AND u.expiry_date <= (NOW() AT TIME ZONE 'UTC') AND NOT {ACTIVE_GRACE_SQL}",
    "auto_renew": "u.paid IS TRUE AND u.auto_renew IS TRUE",
    "non_renewing": "u.paid IS TRUE AND u.auto_renew IS NOT TRUE",
}


def _iso(value):
    return value.isoformat() if value else None


def _access_status(paid, expiry_date, payment_failed, grace_period_end, now):
    if payment_failed and grace_period_end and grace_period_end > now:
        return "active_grace"
    if paid and expiry_date and expiry_date > now:
        return "active"
    if paid and expiry_date and expiry_date <= now:
        return "expired"
    return "inactive"


def _access_type(trial_used, first_payment_done, gift_current, stripe_linked):
    if gift_current:
        return "gift"
    if trial_used and not first_payment_done:
        return "trial"
    if first_payment_done or stripe_linked:
        return "paid"
    return "unknown"


def mask_stripe_identifier(value):
    if not value:
        return None
    text = str(value)
    prefix = text.split("_", 1)[0] if "_" in text else "id"
    return f"{prefix}_***{text[-6:]}" if len(text) > 6 else f"{prefix}_***"


def _begin_read_only(cur):
    cur.execute("SET TRANSACTION READ ONLY")
    cur.execute(f"SET LOCAL statement_timeout = {USERS_STATEMENT_TIMEOUT_MS}")


def list_admin_users(get_connection, *, limit=25, cursor=None, query="", status="all"):
    limit = parse_users_limit(limit)
    query = normalize_users_query(query)
    if status not in USERS_STATUSES:
        raise AdminUsersQueryError("invalid_status")
    cursor_value = decode_users_cursor(cursor)
    clauses = []
    params = []
    if STATUS_SQL[status]:
        clauses.append(STATUS_SQL[status])
    if query:
        username_pattern = f"%{escape_like(query)}%"
        if query.isdigit():
            clauses.append("(u.telegram_id = %s OR u.username ILIKE %s ESCAPE E'\\\\')")
            params.extend((int(query), username_pattern))
        else:
            clauses.append("u.username ILIKE %s ESCAPE E'\\\\'")
            params.append(username_pattern)
    if cursor_value:
        clauses.append(
            "(COALESCE(u.registered_at, TIMESTAMP 'epoch'), u.telegram_id) < (%s, %s)"
        )
        params.extend(cursor_value)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT
            u.telegram_id, u.username, u.first_name, u.paid, u.expiry_date,
            u.auto_renew, u.payment_failed, u.grace_period_end, u.trial_used,
            u.first_payment_done,
            (u.stripe_customer_id IS NOT NULL OR u.stripe_subscription_id IS NOT NULL) AS stripe_linked,
            EXISTS (
                SELECT 1 FROM gift_access_grants gift
                WHERE gift.recipient_telegram_id = u.telegram_id
                  AND gift.status = 'redeemed'
                  AND gift.applied_expiry = u.expiry_date
            ) AS gift_current,
            COALESCE(u.registered_at, TIMESTAMP 'epoch') AS ordering_time,
            (NOW() AT TIME ZONE 'UTC')
        FROM users u
        {where}
        ORDER BY COALESCE(u.registered_at, TIMESTAMP 'epoch') DESC, u.telegram_id DESC
        LIMIT %s
    """
    params.append(limit + 1)
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    has_more = len(rows) > limit
    page = rows[:limit]
    items = []
    for row in page:
        (
            telegram_id, username, first_name, paid, expiry_date, auto_renew,
            payment_failed, grace_period_end, trial_used, first_payment_done,
            stripe_linked, gift_current, ordering_time, now,
        ) = row
        items.append({
            "telegram_id": int(telegram_id),
            "username": username,
            "first_name": first_name,
            "paid": bool(paid),
            "expiry_date": _iso(expiry_date),
            "auto_renew": bool(auto_renew),
            "payment_failed": bool(payment_failed),
            "grace_period_end": _iso(grace_period_end),
            "trial_used": bool(trial_used),
            "first_payment_done": bool(first_payment_done),
            "stripe_linked": bool(stripe_linked),
            "access_status": _access_status(paid, expiry_date, payment_failed, grace_period_end, now),
            "access_type": _access_type(trial_used, first_payment_done, gift_current, stripe_linked),
        })
    next_cursor = None
    if has_more and page:
        next_cursor = encode_users_cursor(page[-1][12], page[-1][0])
    return {"items": items, "next_cursor": next_cursor, "has_more": has_more}


SAFE_INTERNAL_LABEL = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")


def _safe_label(value):
    text = str(value or "")
    return text if SAFE_INTERNAL_LABEL.fullmatch(text) else "other"


def get_admin_user_details(get_connection, telegram_id):
    try:
        telegram_id = int(telegram_id)
    except (TypeError, ValueError):
        raise AdminUsersQueryError("invalid_telegram_id") from None
    if telegram_id <= 0:
        raise AdminUsersQueryError("invalid_telegram_id")
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute(
            """
            SELECT u.telegram_id, u.username, u.first_name, u.last_name, u.paid,
                   u.expiry_date, u.auto_renew, u.payment_failed,
                   u.payment_failed_at, u.grace_period_end, u.trial_used,
                   u.first_payment_done, u.stripe_customer_id,
                   u.stripe_subscription_id,
                   EXISTS (
                       SELECT 1 FROM gift_access_grants gift
                       WHERE gift.recipient_telegram_id = u.telegram_id
                         AND gift.status = 'redeemed'
                         AND gift.applied_expiry = u.expiry_date
                   ), (NOW() AT TIME ZONE 'UTC')
            FROM users u WHERE u.telegram_id = %s
            """,
            (telegram_id,),
        )
        user = cur.fetchone()
        if user is None:
            conn.rollback()
            return None
        cur.execute(
            """
            SELECT status, reason, access_expiry, updated_at
            FROM subscription_removal_events
            WHERE telegram_id = %s
            """,
            (telegram_id,),
        )
        removal = cur.fetchone()
        cur.execute(
            """
            SELECT event_type, source, old_expiry, new_expiry, created_at
            FROM access_events
            WHERE telegram_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT 10
            """,
            (telegram_id,),
        )
        history = cur.fetchall()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    (
        user_id, username, first_name, last_name, paid, expiry_date, auto_renew,
        payment_failed, payment_failed_at, grace_period_end, trial_used,
        first_payment_done, customer_id, subscription_id, gift_current, now,
    ) = user
    stripe_linked = bool(customer_id or subscription_id)
    return {
        "telegram_id": int(user_id),
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "paid": bool(paid),
        "expiry_date": _iso(expiry_date),
        "auto_renew": bool(auto_renew),
        "payment_failed": bool(payment_failed),
        "payment_failed_at": _iso(payment_failed_at),
        "grace_period_end": _iso(grace_period_end),
        "trial_used": bool(trial_used),
        "first_payment_done": bool(first_payment_done),
        "access_status": _access_status(paid, expiry_date, payment_failed, grace_period_end, now),
        "access_type": _access_type(trial_used, first_payment_done, gift_current, stripe_linked),
        "stripe": {
            "customer_id": mask_stripe_identifier(customer_id),
            "subscription_id": mask_stripe_identifier(subscription_id),
        },
        "removal": None if removal is None else {
            "status": _safe_label(removal[0]),
            "reason": _safe_label(removal[1]),
            "access_expiry": _iso(removal[2]),
            "updated_at": _iso(removal[3]),
        },
        "access_history": [
            {
                "event_type": _safe_label(row[0]),
                "source": _safe_label(row[1]),
                "old_expiry": _iso(row[2]),
                "new_expiry": _iso(row[3]),
                "created_at": _iso(row[4]),
            }
            for row in history
        ],
    }
