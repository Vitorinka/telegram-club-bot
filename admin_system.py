import base64
import hashlib
import json
import re
from datetime import datetime, timezone


SYSTEM_STATEMENT_TIMEOUT_MS = 5000
DEFAULT_DELIVERIES_LIMIT = 25
MAX_DELIVERIES_LIMIT = 50
DELIVERY_STATUSES = frozenset({
    "all", "pending", "processing", "failed", "permanently_failed",
    "sent", "cancelled",
})
RETRYABLE_REMOVAL_STATUSES = (
    "processing", "stripe_canceled", "telegram_failed", "telegram_removed",
)


DELIVERY_TYPE_LABELS = {
    "subscription_expired_user": "Сообщение о завершении подписки",
    "failed_renewal": "Сообщение о неудачном продлении",
    "stripe_user_message": "Сообщение пользователю о подписке",
    "subscription_expiry_reminder": "Напоминание об окончании подписки",
    "grace_reminder": "Напоминание о grace-периоде",
    "payment_recovery_reminder": "Напоминание о восстановлении оплаты",
    "first_purchase_recovery_reminder": "Напоминание о первом платеже",
    "access_restore_invite": "Приглашение восстановить доступ",
    "telegram_unban_compensation": "Компенсация Telegram-блокировки",
    "stripe_admin_message": "Системное сообщение администратору",
    "stripe_rejoin_invite": "Приглашение вернуться в клуб",
    "stripe_rejoin_check": "Проверка возврата в клуб",
    "free_lesson": "Бесплатный урок",
    "free_lesson_followup": "Напоминание о бесплатном уроке",
    "gift_paid_buyer": "Сообщение покупателю подарка",
    "gift_checkout_expired_buyer": "Истечение оплаты подарка",
    "gift_checkout_failed_buyer": "Ошибка оплаты подарка",
    "gift_redeemed_buyer": "Активация подарка для покупателя",
    "gift_redeemed_recipient": "Активация подарка для получателя",
    "gift_reserved_buyer": "Резерв подарка для покупателя",
    "gift_reserved_recipient": "Резерв подарка для получателя",
    "gift_refunded_buyer": "Возврат подарка для покупателя",
    "gift_refunded_recipient": "Возврат подарка для получателя",
    "gift_certificate_buyer": "Сертификат покупателю подарка",
    "gift_certificate_recipient": "Сертификат получателю подарка",
    "gift_certificate_failed_buyer": "Ошибка сертификата подарка",
    "gift_admin_success": "Успешное системное событие подарка",
    "gift_admin_redeemed": "Активация подарка для администратора",
    "gift_admin_problem": "Проблема подарка",
    "gift_admin_refund": "Возврат подарка для администратора",
    "gift_admin_certificate_problem": "Проблема сертификата подарка",
}
DELIVERY_TYPES = frozenset(DELIVERY_TYPE_LABELS)
SAFE_LABEL = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")


class AdminSystemQueryError(ValueError):
    pass


def _iso(value):
    return value.isoformat() if value else None


def _safe_label(value, fallback="unknown"):
    text = str(value or "")
    return text if SAFE_LABEL.fullmatch(text) else fallback


def _safe_reference(namespace, value, length=16):
    digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:length]
    return f"{namespace}:{digest}"


def _delivery_id(delivery_key):
    return hashlib.md5(
        str(delivery_key).encode("utf-8"), usedforsecurity=False
    ).hexdigest()


def parse_deliveries_limit(value):
    if value in (None, ""):
        return DEFAULT_DELIVERIES_LIMIT
    try:
        limit = int(value)
    except (TypeError, ValueError):
        raise AdminSystemQueryError("invalid_limit") from None
    if limit < 1 or limit > MAX_DELIVERIES_LIMIT:
        raise AdminSystemQueryError("invalid_limit")
    return limit


def validate_delivery_status(value):
    status = str(value or "all")
    if status not in DELIVERY_STATUSES:
        raise AdminSystemQueryError("invalid_status")
    return status


def validate_delivery_type(value):
    if value in (None, ""):
        return None
    delivery_type = str(value)
    if delivery_type not in DELIVERY_TYPES:
        raise AdminSystemQueryError("invalid_type")
    return delivery_type


def encode_deliveries_cursor(ordering_time, delivery_id):
    timestamp = ordering_time.isoformat() if ordering_time else "1970-01-01T00:00:00"
    payload = json.dumps([timestamp, str(delivery_id)], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_deliveries_cursor(value):
    if not value:
        return None
    try:
        raw = str(value)
        decoded = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
        timestamp, delivery_id = json.loads(decoded)
        ordering_time = datetime.fromisoformat(timestamp)
        if not isinstance(delivery_id, str) or not re.fullmatch(r"[0-9a-f]{32}", delivery_id):
            raise ValueError
    except (TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
        raise AdminSystemQueryError("invalid_cursor") from None
    return ordering_time, delivery_id


def delivery_type_label(delivery_type):
    return DELIVERY_TYPE_LABELS.get(
        delivery_type, "Неизвестный тип доставки"
    )


def safe_error(last_error):
    if not last_error:
        return {"category": None, "reference": None}
    lowered = str(last_error).lower()
    if any(token in lowered for token in ("forbidden", "blocked", "chat not found")):
        category = "recipient_unavailable"
    elif any(token in lowered for token in ("timeout", "network", "retry", "temporar")):
        category = "transient_transport"
    elif any(token in lowered for token in ("manual", "unsafe", "mismatch", "legacy", "malformed")):
        category = "manual_intervention"
    else:
        category = "delivery_failed"
    return {
        "category": category,
        "reference": _safe_reference("delivery_error", last_error, 12),
    }


def _requires_attention(status, attempt_count, lease_until, delivery_type, now):
    if status == "permanently_failed":
        return True
    if status == "failed" and int(attempt_count or 0) >= 3:
        return True
    if status == "processing" and (lease_until is None or lease_until <= now):
        return True
    if delivery_type == "telegram_unban_compensation" and status in ("failed", "permanently_failed"):
        return True
    return False


def _explanation(status, requires_attention):
    if status == "permanently_failed":
        return "Не удалось доставить сообщение после максимального числа попыток."
    if status == "failed":
        return "Доставка завершилась ошибкой и ожидает следующей попытки."
    if status == "processing" and requires_attention:
        return "Обработка доставки просрочена и ожидает безопасного повторного claim."
    return None


def _delivery_projection(row):
    (
        delivery_key, telegram_id, delivery_type, status, attempt_count,
        claimed_at, lease_until, sent_at, next_attempt_at, last_error, now,
        ordering_time,
    ) = row
    delivery_type = _safe_label(delivery_type)
    status = _safe_label(status)
    attention = _requires_attention(
        status, attempt_count, lease_until, delivery_type, now
    )
    return {
        "delivery_id": _delivery_id(delivery_key),
        "delivery_reference": _safe_reference("delivery", delivery_key),
        "delivery_type": delivery_type,
        "delivery_label": delivery_type_label(delivery_type),
        "status": status,
        "telegram_id": int(telegram_id),
        "created_at": None,
        "updated_at": None,
        "claimed_at": _iso(claimed_at),
        "lease_until": _iso(lease_until),
        "next_attempt_at": _iso(next_attempt_at),
        "attempt_count": int(attempt_count or 0),
        "sent_at": _iso(sent_at),
        "last_error": safe_error(last_error),
        "requires_attention": attention,
        "explanation": _explanation(status, attention),
        "_ordering_time": ordering_time,
    }


DELIVERY_SELECT = """
    SELECT delivery_key, telegram_id, delivery_type, status, attempt_count,
           claimed_at, lease_until, sent_at, next_attempt_at, last_error,
           (NOW() AT TIME ZONE 'UTC'),
           COALESCE(sent_at, claimed_at, next_attempt_at, TIMESTAMP 'epoch') AS ordering_time
    FROM message_delivery_events
"""


def _begin_read_only(cur):
    cur.execute("SET TRANSACTION READ ONLY")
    cur.execute(f"SET LOCAL statement_timeout = {SYSTEM_STATEMENT_TIMEOUT_MS}")


def _public_delivery(item):
    item = dict(item)
    item.pop("_ordering_time", None)
    item.pop("_delivery_key", None)
    return item


def list_admin_deliveries(
    get_connection, *, status="all", limit=25, cursor=None, delivery_type=None
):
    status = validate_delivery_status(status)
    limit = parse_deliveries_limit(limit)
    delivery_type = validate_delivery_type(delivery_type)
    cursor_value = decode_deliveries_cursor(cursor)
    clauses = []
    params = []
    if status != "all":
        clauses.append("status = %s")
        params.append(status)
    if delivery_type:
        clauses.append("delivery_type = %s")
        params.append(delivery_type)
    if cursor_value:
        clauses.append(
            "(COALESCE(sent_at, claimed_at, next_attempt_at, TIMESTAMP 'epoch'), md5(delivery_key)) < (%s, %s)"
        )
        params.extend(cursor_value)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"{DELIVERY_SELECT} {where} ORDER BY ordering_time DESC, md5(delivery_key) DESC LIMIT %s"
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
    projected = [_delivery_projection(row) for row in rows[:limit]]
    next_cursor = None
    if has_more and projected:
        last = projected[-1]
        next_cursor = encode_deliveries_cursor(
            last["_ordering_time"], last["delivery_id"]
        )
    return {
        "items": [_public_delivery(item) for item in projected],
        "next_cursor": next_cursor,
        "has_more": has_more,
    }


def get_admin_delivery_details(get_connection, delivery_id):
    delivery_id = str(delivery_id or "")
    if not re.fullmatch(r"[0-9a-f]{32}", delivery_id):
        raise AdminSystemQueryError("invalid_delivery_id")
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute(
            f"{DELIVERY_SELECT} WHERE md5(delivery_key) = %s LIMIT 2",
            (delivery_id,),
        )
        rows = cur.fetchall()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    if not rows:
        return None
    if len(rows) != 1:
        raise AdminSystemQueryError("ambiguous_delivery_id")
    return _public_delivery(_delivery_projection(rows[0]))


def collect_admin_system(get_connection, db_pool_health, scheduler_job_count):
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending'),
                COUNT(*) FILTER (WHERE status = 'processing'),
                COUNT(*) FILTER (WHERE status = 'failed'),
                COUNT(*) FILTER (WHERE status = 'permanently_failed'),
                COUNT(*) FILTER (WHERE status = 'sent' AND sent_at >= NOW() - INTERVAL '24 hours')
            FROM message_delivery_events
        """)
        deliveries = cur.fetchone()
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending'),
                COUNT(*) FILTER (WHERE status IN ('processing', 'stripe_canceled', 'telegram_failed', 'telegram_removed')),
                COUNT(*) FILTER (WHERE status = 'db_finalized' AND db_finalized_at >= NOW() - INTERVAL '24 hours')
            FROM subscription_removal_events
        """)
        removals = cur.fetchone()
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'running'),
                COUNT(*) FILTER (WHERE status = 'failed' AND updated_at >= NOW() - INTERVAL '24 hours'),
                COUNT(*) FILTER (WHERE status = 'running' AND (lease_until IS NULL OR lease_until <= NOW()))
            FROM scheduled_job_runs
        """)
        scheduler = cur.fetchone()
        cur.execute("""
            SELECT job_key, job_name, status, started_at, completed_at,
                   updated_at, error_text, lease_until,
                   (NOW() AT TIME ZONE 'UTC')
            FROM scheduled_job_runs
            ORDER BY updated_at DESC, job_key DESC
            LIMIT 20
        """)
        recent_runs = cur.fetchall()
        cur.execute("""
            SELECT version, applied_at
            FROM schema_migrations
            ORDER BY version DESC LIMIT 1
        """)
        latest_migration = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM schema_migrations")
        migration_count = cur.fetchone()[0]
        cur.execute("""
            SELECT status, severity, COUNT(*)
            FROM admin_alerts
            GROUP BY status, severity
            ORDER BY status, severity
        """)
        alert_rows = cur.fetchall()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    pool = db_pool_health() or {}
    safe_pool = {
        key: pool.get(key)
        for key in (
            "pool_available", "pool_used", "connection_errors",
            "statement_timeout_ms",
        )
        if key in pool
    }
    safe_runs = []
    for job_key, job_name, status, started_at, completed_at, updated_at, error_text, lease_until, now in recent_runs:
        status = _safe_label(status)
        safe_runs.append({
            "job_reference": _safe_reference("job", job_key),
            "job_name": _safe_label(job_name),
            "status": status,
            "started_at": _iso(started_at),
            "completed_at": _iso(completed_at),
            "updated_at": _iso(updated_at),
            "error": safe_error(error_text),
            "stale": bool(status == "running" and (lease_until is None or lease_until <= now)),
        })
    alerts = [
        {
            "status": _safe_label(status),
            "severity": _safe_label(severity),
            "count": int(count or 0),
        }
        for status, severity, count in alert_rows
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database": safe_pool,
        "migrations": {
            "count": int(migration_count or 0),
            "latest": latest_migration[0] if latest_migration else None,
            "latest_applied_at": _iso(latest_migration[1]) if latest_migration else None,
        },
        "scheduler": {
            "known_jobs": int(scheduler_job_count),
            "running": int(scheduler[0] or 0),
            "failed_last_24h": int(scheduler[1] or 0),
            "stale": int(scheduler[2] or 0),
            "recent_runs": safe_runs,
        },
        "deliveries": {
            "pending": int(deliveries[0] or 0),
            "processing": int(deliveries[1] or 0),
            "failed": int(deliveries[2] or 0),
            "permanently_failed": int(deliveries[3] or 0),
            "sent_last_24h": int(deliveries[4] or 0),
        },
        "removals": {
            "pending": int(removals[0] or 0),
            "retryable": int(removals[1] or 0),
            "finalized_last_24h": int(removals[2] or 0),
        },
        "alerts": alerts,
    }
