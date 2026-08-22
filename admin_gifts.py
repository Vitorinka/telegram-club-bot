import base64
import json
import re
from datetime import datetime


DEFAULT_GIFTS_LIMIT = 25
MAX_GIFTS_LIMIT = 50
MAX_GIFTS_QUERY_LENGTH = 64
GIFTS_STATEMENT_TIMEOUT_MS = 5000
GIFT_REFERENCE_PATTERN = re.compile(r"^GIFT-[0-9A-F]{16}$")
SAFE_EVENT_VALUE_PATTERN = re.compile(r"^[a-z0-9_]{1,64}$")
GIFT_STATUSES = frozenset({
    "all", "checkout_pending", "checkout_open", "payment_pending",
    "paid_unclaimed", "reserved", "redeemed", "cancelled", "refunded",
    "review_required",
})
GIFT_DURATIONS = frozenset({"all", "gift_1m", "gift_6m", "gift_12m"})
GIFT_STATUS_LABELS = {
    "checkout_pending": "Ожидает создания оплаты",
    "checkout_open": "Ожидает оплаты",
    "payment_pending": "Оплата обрабатывается",
    "paid_unclaimed": "Оплачен, ожидает активации",
    "reserved": "Требует безопасного применения",
    "redeemed": "Активирован",
    "cancelled": "Отменён",
    "refunded": "Возвращён",
    "review_required": "Требует проверки администратора",
}
GIFT_DURATION_LABELS = {
    "gift_1m": "1 месяц",
    "gift_6m": "6 месяцев",
    "gift_12m": "12 месяцев",
}


class AdminGiftsQueryError(ValueError):
    pass


def _iso(value):
    return value.isoformat() if value else None


def _safe_event_value(value):
    text = str(value or "")
    return text if SAFE_EVENT_VALUE_PATTERN.fullmatch(text) else None


def parse_gifts_limit(value):
    if value in (None, ""):
        return DEFAULT_GIFTS_LIMIT
    try:
        limit = int(value)
    except (TypeError, ValueError):
        raise AdminGiftsQueryError("invalid_limit") from None
    if limit < 1 or limit > MAX_GIFTS_LIMIT:
        raise AdminGiftsQueryError("invalid_limit")
    return limit


def normalize_gifts_query(value):
    query = str(value or "").strip()
    if len(query) > MAX_GIFTS_QUERY_LENGTH:
        raise AdminGiftsQueryError("query_too_long")
    return query


def validate_gift_status(value):
    status = str(value or "all")
    if status not in GIFT_STATUSES:
        raise AdminGiftsQueryError("invalid_status")
    return status


def validate_gift_duration(value):
    duration = str(value or "all")
    if duration not in GIFT_DURATIONS:
        raise AdminGiftsQueryError("invalid_duration")
    return duration


def validate_gift_reference(value):
    reference = str(value or "")
    if not GIFT_REFERENCE_PATTERN.fullmatch(reference):
        raise AdminGiftsQueryError("invalid_gift_id")
    return reference


def escape_like(value):
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def encode_gifts_cursor(created_at, public_reference):
    payload = json.dumps(
        [created_at.isoformat(), public_reference], separators=(",", ":")
    ).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_gifts_cursor(value):
    if not value:
        return None
    try:
        raw = str(value)
        decoded = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
        timestamp, reference = json.loads(decoded)
        created_at = datetime.fromisoformat(timestamp)
        reference = validate_gift_reference(reference)
    except (TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError, AdminGiftsQueryError):
        raise AdminGiftsQueryError("invalid_cursor") from None
    return created_at, reference


def _begin_read_only(cur):
    cur.execute("SET TRANSACTION READ ONLY")
    cur.execute(f"SET LOCAL statement_timeout = {GIFTS_STATEMENT_TIMEOUT_MS}")


def _profile(telegram_id, username, first_name, last_name):
    if telegram_id is None:
        return None
    return {
        "telegram_id": int(telegram_id),
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "profile_available": any((username, first_name, last_name)),
    }


def _gift_projection(row):
    (
        public_reference, status, tariff_code, duration_days,
        purchaser_id, purchaser_username, purchaser_first, purchaser_last,
        recipient_id, recipient_username, recipient_first, recipient_last,
        recipient_name, certificate_name, created_at, paid_at, reserved_at,
        redeemed_at, applied_at, applied_expiry, refunded_at, cancelled_at,
        _ordering_created_at,
    ) = row
    return {
        "gift_id": public_reference,
        "public_reference": public_reference,
        "status": status,
        "status_label": GIFT_STATUS_LABELS.get(status, "Неизвестный статус"),
        "tariff_code": tariff_code,
        "duration_days": duration_days,
        "duration_label": GIFT_DURATION_LABELS.get(tariff_code, "Не определено"),
        "purchaser": _profile(
            purchaser_id, purchaser_username, purchaser_first, purchaser_last
        ),
        "recipient": _profile(
            recipient_id, recipient_username, recipient_first, recipient_last
        ),
        "recipient_name": recipient_name,
        "certificate_name": certificate_name,
        "certificate_personalized": bool(certificate_name),
        "created_at": _iso(created_at),
        "paid_at": _iso(paid_at),
        "reserved_at": _iso(reserved_at),
        "redeemed_at": _iso(redeemed_at),
        "applied_at": _iso(applied_at),
        "applied_expiry": _iso(applied_expiry),
        "refunded_at": _iso(refunded_at),
        "cancelled_at": _iso(cancelled_at),
        "requires_attention": status == "review_required",
    }


GIFT_SELECT = """
    SELECT
        gift.public_reference, gift.status, gift.tariff_code, gift.duration_days,
        gift.purchaser_telegram_id, purchaser.username, purchaser.first_name,
        purchaser.last_name, gift.recipient_telegram_id, recipient.username,
        recipient.first_name, recipient.last_name, gift.recipient_name,
        gift.certificate_name, gift.created_at, gift.paid_at, gift.reserved_at,
        gift.redeemed_at, gift.applied_at, gift.applied_expiry,
        gift.refunded_at, gift.cancelled_at,
        COALESCE(gift.created_at, TIMESTAMP 'epoch') AS ordering_created_at
    FROM gift_access_grants gift
    LEFT JOIN users purchaser
      ON purchaser.telegram_id = gift.purchaser_telegram_id
    LEFT JOIN users recipient
      ON recipient.telegram_id = gift.recipient_telegram_id
"""


def list_admin_gifts(
    get_connection, *, limit=25, cursor=None, query="", status="all",
    duration="all",
):
    limit = parse_gifts_limit(limit)
    query = normalize_gifts_query(query)
    status = validate_gift_status(status)
    duration = validate_gift_duration(duration)
    cursor_value = decode_gifts_cursor(cursor)
    clauses = []
    params = []
    if status != "all":
        clauses.append("gift.status = %s")
        params.append(status)
    if duration != "all":
        clauses.append("gift.tariff_code = %s")
        params.append(duration)
    if query:
        pattern = f"%{escape_like(query)}%"
        search_parts = [
            "gift.public_reference ILIKE %s ESCAPE E'\\\\'",
            "gift.certificate_name ILIKE %s ESCAPE E'\\\\'",
            "purchaser.username ILIKE %s ESCAPE E'\\\\'",
            "recipient.username ILIKE %s ESCAPE E'\\\\'",
        ]
        params.extend((pattern, pattern, pattern, pattern))
        if query.isdigit():
            search_parts.extend((
                "gift.purchaser_telegram_id = %s",
                "gift.recipient_telegram_id = %s",
            ))
            params.extend((int(query), int(query)))
        clauses.append(f"({' OR '.join(search_parts)})")
    if cursor_value:
        clauses.append(
            "(COALESCE(gift.created_at, TIMESTAMP 'epoch'), gift.public_reference) < (%s, %s)"
        )
        params.extend(cursor_value)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute(
            f"""
            {GIFT_SELECT}
            {where}
            ORDER BY COALESCE(gift.created_at, TIMESTAMP 'epoch') DESC,
                     gift.public_reference DESC
            LIMIT %s
            """,
            tuple(params + [limit + 1]),
        )
        rows = cur.fetchall()
        cur.execute("""
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE status IN ('paid_unclaimed', 'reserved')),
                COUNT(*) FILTER (WHERE status = 'redeemed'),
                COUNT(*) FILTER (WHERE status IN ('cancelled', 'refunded')),
                COUNT(*) FILTER (WHERE status = 'review_required')
            FROM gift_access_grants
        """)
        summary = cur.fetchone()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    has_more = len(rows) > limit
    page = rows[:limit]
    items = [_gift_projection(row) for row in page]
    return {
        "items": items,
        "next_cursor": (
            encode_gifts_cursor(page[-1][22], page[-1][0]) if has_more else None
        ),
        "has_more": has_more,
        "summary": {
            "total": int(summary[0] or 0),
            "awaiting_activation": int(summary[1] or 0),
            "redeemed": int(summary[2] or 0),
            "cancelled_or_refunded": int(summary[3] or 0),
            "requires_attention": int(summary[4] or 0),
        },
    }


def get_admin_gift_details(get_connection, gift_id):
    reference = validate_gift_reference(gift_id)
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute(
            f"{GIFT_SELECT} WHERE gift.public_reference = %s",
            (reference,),
        )
        row = cur.fetchone()
        if row:
            cur.execute("""
                SELECT event_type, source, telegram_id, created_at
                FROM gift_access_events
                WHERE public_reference = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 10
            """, (reference,))
            events = cur.fetchall()
        else:
            events = []
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    if row is None:
        return None
    result = _gift_projection(row)
    result["lifecycle_events"] = [
        {
            "event_type": _safe_event_value(event_type) or "unknown",
            "source": _safe_event_value(source),
            "telegram_id": int(telegram_id) if telegram_id is not None else None,
            "created_at": _iso(created_at),
        }
        for event_type, source, telegram_id, created_at in events
    ]
    return result
