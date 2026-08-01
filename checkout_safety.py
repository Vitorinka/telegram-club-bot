import hashlib
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote, urlsplit
from zoneinfo import ZoneInfo


CHECKOUT_CREATING_LEASE_SECONDS = 120
CHECKOUT_AMBIGUOUS_AUTO_RETRY_HOURS = 20
CHECKOUT_OPEN_STATUSES = ("creating", "creation_unknown", "open")
BLOCKING_SUBSCRIPTION_STATUSES = {
    "active",
    "trialing",
    "past_due",
    "unpaid",
    "incomplete",
    "paused",
}
TERMINAL_SUBSCRIPTION_STATUSES = {"canceled", "incomplete_expired"}
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def stable_checkout_idempotency_key(telegram_id, tariff_code, created_at=None):
    seed = f"{int(telegram_id)}:{tariff_code}:{created_at or 'initial'}".encode("utf-8")
    return "checkout_" + hashlib.sha256(seed).hexdigest()[:40]


def claim_checkout_session_record(cur, telegram_id, tariff_code, mode, now=None):
    """Atomically reserve one open Checkout per user/tariff."""
    now = now or datetime.utcnow()
    stale_before = now - timedelta(seconds=CHECKOUT_CREATING_LEASE_SECONDS)
    ambiguous_review_before = now - timedelta(hours=CHECKOUT_AMBIGUOUS_AUTO_RETRY_HOURS)
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"checkout:{int(telegram_id)}:{tariff_code}",),
    )
    cur.execute(
        """
        SELECT id, stripe_session_id, checkout_url, status, expires_at, idempotency_key, created_at
        FROM checkout_sessions
        WHERE telegram_id = %s
          AND tariff_code = %s
          AND status IN ('creating', 'creation_unknown', 'open')
        FOR UPDATE
        """,
        (int(telegram_id), tariff_code),
    )
    row = cur.fetchone()
    if row:
        record = checkout_row_to_dict(row)
        status = record["status"]
        expires_at = record["expires_at"]
        created_at = record["created_at"]
        if status == "open" and record["checkout_url"] and (expires_at is None or expires_at > now):
            return {"action": "reuse_open", "record": record}
        if status == "creation_unknown":
            if created_at and created_at < ambiguous_review_before:
                return {"action": "manual_review_required", "record": record}
            return {"action": "retry_create", "record": record}
        if status == "creating":
            if record["created_at"] and record["created_at"] >= stale_before:
                return {"action": "creating_in_progress", "record": record}
            if created_at and created_at < ambiguous_review_before:
                return {"action": "manual_review_required", "record": record}
            return {"action": "retry_create", "record": record}
        cur.execute(
            """
            UPDATE checkout_sessions
            SET status = CASE WHEN status = 'open' THEN 'expired' ELSE 'failed' END,
                updated_at = %s,
                last_error = CASE WHEN status = 'creating' THEN 'stale creating checkout reclaimed' ELSE last_error END
            WHERE id = %s
            """,
            (now, record["id"]),
        )

    created_at = now
    idempotency_key = stable_checkout_idempotency_key(telegram_id, tariff_code, created_at.isoformat())
    cur.execute(
        """
        INSERT INTO checkout_sessions (
            telegram_id, tariff_code, mode, idempotency_key, status, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, 'creating', %s, %s)
        RETURNING id, stripe_session_id, checkout_url, status, expires_at, idempotency_key, created_at
        """,
        (int(telegram_id), tariff_code, mode, idempotency_key, created_at, created_at),
    )
    return {"action": "create", "record": checkout_row_to_dict(cur.fetchone())}


def checkout_row_to_dict(row):
    keys = ("id", "stripe_session_id", "checkout_url", "status", "expires_at", "idempotency_key", "created_at")
    return dict(zip(keys, row))


def mark_checkout_open(cur, record_id, stripe_session_id, checkout_url, expires_at):
    cur.execute(
        """
        UPDATE checkout_sessions
        SET stripe_session_id = %s,
            checkout_url = %s,
            expires_at = %s,
            status = 'open',
            updated_at = NOW(),
            last_error = NULL
        WHERE id = %s
        """,
        (stripe_session_id, checkout_url, expires_at, record_id),
    )


def mark_checkout_failed(cur, record_id, error_text, status="failed"):
    cur.execute(
        """
        UPDATE checkout_sessions
        SET status = %s,
            updated_at = NOW(),
            last_error = LEFT(%s, 500)
        WHERE id = %s
        """,
        (status, str(error_text), record_id),
    )


def mark_checkout_completed(cur, session_id, customer_id=None, subscription_id=None):
    cur.execute(
        """
        UPDATE checkout_sessions
        SET status = 'completed',
            stripe_customer_id = COALESCE(%s, stripe_customer_id),
            stripe_subscription_id = COALESCE(%s, stripe_subscription_id),
            completed_at = NOW(),
            updated_at = NOW()
        WHERE stripe_session_id = %s
        """,
        (customer_id, subscription_id, session_id),
    )


def mark_checkout_terminal(cur, session_id, status, error_text=None):
    cur.execute(
        """
        UPDATE checkout_sessions
        SET status = %s,
            updated_at = NOW(),
            last_error = LEFT(COALESCE(%s, last_error), 500)
        WHERE stripe_session_id = %s
        """,
        (status, error_text, session_id),
    )


def active_or_resumable_subscriptions(subscriptions):
    data = getattr(subscriptions, "data", subscriptions or [])
    return [sub for sub in data if stripe_value(sub, "status") in BLOCKING_SUBSCRIPTION_STATUSES]


def is_terminal_subscription_status(status):
    return status in TERMINAL_SUBSCRIPTION_STATUSES


def stripe_link_active_for_status(status):
    return status in BLOCKING_SUBSCRIPTION_STATUSES and not is_terminal_subscription_status(status)


def subscription_status_action(status, count=1):
    if count > 1:
        return "duplicate_subscriptions"
    if status in ("active", "trialing"):
        return "already_active"
    if status in ("past_due", "unpaid", "incomplete"):
        return "open_invoice_or_portal"
    if status == "paused":
        return "billing_portal"
    return "allow_checkout"


def claim_trial_redemption(cur, telegram_id, stripe_event_id, checkout_session_id):
    cur.execute(
        """
        INSERT INTO trial_redemptions (telegram_id, stripe_event_id, checkout_session_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (telegram_id) DO NOTHING
        RETURNING telegram_id
        """,
        (int(telegram_id), stripe_event_id, checkout_session_id),
    )
    return cur.fetchone() is not None


def stripe_identity_conflict_queries():
    return [
        (
            "users_duplicate_subscription",
            """
            SELECT stripe_subscription_id, array_agg(telegram_id ORDER BY telegram_id), count(*)
            FROM users
            WHERE stripe_subscription_id IS NOT NULL
            GROUP BY stripe_subscription_id
            HAVING count(*) > 1
            """,
        ),
        (
            "users_duplicate_customer",
            """
            SELECT stripe_customer_id, array_agg(telegram_id ORDER BY telegram_id), count(*)
            FROM users
            WHERE stripe_customer_id IS NOT NULL
            GROUP BY stripe_customer_id
            HAVING count(*) > 1
            """,
        ),
        (
            "stripe_links_duplicate_subscription_user",
            """
            SELECT stripe_subscription_id, array_agg(DISTINCT telegram_id ORDER BY telegram_id), count(DISTINCT telegram_id)
            FROM stripe_links
            WHERE stripe_subscription_id IS NOT NULL
            GROUP BY stripe_subscription_id
            HAVING count(DISTINCT telegram_id) > 1
            """,
        ),
        (
            "stripe_links_duplicate_customer_user",
            """
            SELECT stripe_customer_id, array_agg(DISTINCT telegram_id ORDER BY telegram_id), count(DISTINCT telegram_id)
            FROM stripe_links
            WHERE stripe_customer_id IS NOT NULL
            GROUP BY stripe_customer_id
            HAVING count(DISTINCT telegram_id) > 1
            """,
        ),
    ]


def should_apply_negative_event(event_created_at, last_success_created_at):
    if not event_created_at or not last_success_created_at:
        return True
    return event_created_at > last_success_created_at


def live_subscription_is_paid(status, latest_invoice_status=None):
    return status in ("active", "trialing") and latest_invoice_status in (None, "paid")


def parse_moscow_expiry(date_text, time_text="23:59"):
    local_dt = datetime.strptime(f"{date_text} {time_text}", "%d.%m.%Y %H:%M").replace(tzinfo=MOSCOW_TZ)
    return local_dt, local_dt.astimezone(timezone.utc).replace(tzinfo=None)


def merge_expiry(current_expiry, stripe_expiry):
    if stripe_expiry is None:
        return current_expiry
    if current_expiry is None:
        return stripe_expiry
    return max(current_expiry, stripe_expiry)


def manual_link_access_decision(status, current_period_end, cancel_at_period_end=False, old_expiry=None, now=None):
    now = now or datetime.utcnow()
    stripe_expiry = datetime.utcfromtimestamp(current_period_end) if current_period_end else None
    grant_paid_access = status in ("active", "trialing") and stripe_expiry is not None and stripe_expiry > now
    effective_expiry = merge_expiry(old_expiry, stripe_expiry) if grant_paid_access else old_expiry
    auto_renew = status in ("active", "trialing", "past_due", "unpaid", "incomplete") and not cancel_at_period_end
    return {
        "grant_paid_access": bool(grant_paid_access),
        "effective_expiry": effective_expiry,
        "auto_renew": bool(auto_renew),
        "stripe_expiry": stripe_expiry,
    }


def is_out_of_band_access(payment_kind=None, access_source=None, manual_sync_at=None):
    del manual_sync_at
    return payment_kind == "out_of_band" or access_source in {
        "manual_give_access",
        "manual_set_expiry",
        "out_of_band",
    }


def classify_invoice_collection_risk(
    status,
    collection_method=None,
    amount_remaining=0,
    next_payment_attempt=None,
):
    if status in ("paid", "void", "uncollectible"):
        return status
    try:
        remaining = int(amount_remaining or 0)
    except (TypeError, ValueError):
        remaining = 0
    if status == "open" and remaining > 0 and next_payment_attempt:
        return "scheduled_retry"
    if status == "open" and remaining > 0 and collection_method == "send_invoice":
        return "manual_only_collectible"
    if status == "open" and remaining > 0:
        return "open_collectible"
    return "not_collectible"


def should_apply_failed_invoice_to_user(existing_subscription_id, event_subscription_id):
    if not event_subscription_id:
        return False
    return existing_subscription_id in (None, "", event_subscription_id)


def has_active_access(paid, expiry_date, payment_failed=False, grace_period_end=None, now=None):
    now = now or datetime.utcnow()
    if paid and expiry_date and expiry_date > now:
        return True
    return bool(payment_failed and grace_period_end and grace_period_end > now)


def backup_decision(env):
    enabled = str(env.get("BACKUP_TELEGRAM_ENABLED", "false")).lower() == "true"
    key = env.get("BACKUP_ENCRYPTION_KEY")
    if enabled and not key:
        return {"telegram_enabled": True, "allowed": False, "reason": "BACKUP_ENCRYPTION_KEY required"}
    return {"telegram_enabled": enabled, "allowed": True, "reason": None}


def build_pg_dump_command(database_url, base_env=None):
    parsed = urlsplit(database_url or "")
    if parsed.scheme not in ("postgres", "postgresql") or not parsed.hostname or not parsed.username:
        raise ValueError("Invalid PostgreSQL DATABASE_URL")
    dbname = unquote((parsed.path or "").lstrip("/"))
    if not dbname:
        raise ValueError("Invalid PostgreSQL DATABASE_URL")

    username = unquote(parsed.username)
    password = unquote(parsed.password or "")
    port = str(parsed.port or 5432)
    argv = [
        "pg_dump",
        "--host", parsed.hostname,
        "--port", port,
        "--username", username,
        "--dbname", dbname,
        "--no-owner",
        "--no-privileges",
    ]
    env = dict(base_env or {})
    env["PGPASSWORD"] = password
    env["PGSSLMODE"] = "require"
    return argv, env


def mask_secret_text(text):
    if not text:
        return text
    value = str(text)
    try:
        parsed = urlsplit(value)
        if parsed.password:
            value = value.replace(parsed.password, "***")
    except Exception:
        pass
    return value.replace("postgres://", "postgres://***@") if "@" in value else value


def stripe_value(obj, *path):
    current = obj
    for key in path:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(key)
        else:
            current = getattr(current, key, None)
    return current
