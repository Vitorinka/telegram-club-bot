import os
import logging
import asyncio
import base64
import io
import json
import hashlib
import hmac
import html
import re
import secrets
import shutil
import stripe
import psycopg2
import uuid
from psycopg2 import pool as psycopg2_pool
from psycopg2 import errors as psycopg2_errors
import subprocess
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.filters import Command, CommandObject, CommandStart, StateFilter
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from postgres_fsm_storage import PostgresFSMStorage, cleanup_postgres_fsm_storage
from storage_diagnostics import collect_storage_diagnostics, render_storage_diagnostics
from constraint_audit import collect_constraint_audit, render_constraint_audit
from stripe_invoice_rules import (
    checkout_completion_action,
    claim_stripe_event,
    invoice_payment_kind,
    is_zero_subscription_update_invoice,
    mark_stripe_event_processed,
    redact_email,
    redact_identifier,
    redact_url,
    release_stripe_event_claim,
    should_send_rejoin_invite,
    should_skip_invoice_notice_for_current_expiry,
    should_ignore_payment_failed_for_active_trial,
    subscription_update_period,
    successful_invoice_action,
)
from admin_security import (
    admin_action_confirmation_keyboard,
    admin_private_only,
    broadcast_preview,
    cancel_admin_action,
    claim_admin_action,
    complete_admin_action,
    fail_admin_action,
    make_action_request,
)
from checkout_safety import (
    active_or_resumable_subscriptions,
    backup_decision,
    build_pg_dump_command,
    claim_checkout_session_record,
    claim_trial_redemption,
    checkout_payment_access_decision,
    has_active_access,
    is_terminal_subscription_status,
    live_subscription_is_paid,
    manual_link_access_decision,
    mark_checkout_completed,
    mark_checkout_failed,
    mark_checkout_open,
    mark_checkout_terminal,
    mask_secret_text,
    parse_moscow_expiry,
    should_apply_failed_invoice_to_user,
    should_apply_negative_event,
    stripe_link_active_for_status,
    stripe_identity_conflict_queries,
    subscription_status_action,
)
from db_migrations import run_migrations
from access_mismatch_observability import (
    load_access_mismatch_counts,
    load_access_mismatch_samples,
)
from stripe_reconcile_audit import (
    load_reconcile_candidates,
    reconcile_candidates,
    render_reconcile_audit,
)
from delivery_failure_admin_ux import render_critical_delivery_alert
from gift_certificate import (
    CERTIFICATE_NAME_TOO_LONG_TEXT,
    CertificateNameError,
    certificate_assets_status,
    certificate_template_path,
    remove_generated_certificate,
    render_gift_certificate,
    validate_certificate_name_fits,
)
from group_access import (
    group_join_decision,
    invite_link_options,
    load_active_bot_invite_links,
    mark_bot_invite_link_revoked,
    save_bot_invite_link,
)
from scheduled_jobs import (
    OWNER_ID,
    cancel_message_delivery,
    claim_message_delivery,
    claim_pending_message_deliveries,
    claim_scheduled_job,
    complete_scheduled_job,
    enqueue_message_delivery,
    fail_scheduled_job,
    log_stale_delivery_claim,
    mark_delivery_cancelled,
    mark_delivery_failed,
    mark_delivery_sent,
    process_already_claimed_delivery,
    process_claimed_delivery,
    save_delivery_invite_link,
)
from weekly_report import (
    MOSCOW_TZ,
    build_payments_csv,
    build_weekly_report_text,
    claim_weekly_report_run_record,
    classify_manual_link_payment_kind,
    get_current_week_bounds,
    get_last_completed_week_bounds,
    parse_admin_ids,
    report_key as weekly_report_key,
    should_create_manual_link_payment_event,
    tariff_code_from_invoice,
    to_utc_naive,
)
from stripe_webhook_safety import (
    claim_normalized_stripe_event,
    construct_verified_stripe_event,
    normalize_stripe_event,
    require_normalized_stripe_event,
    stripe_signature_error_class,
    stripe_value,
    stripe_webhook_diagnostics,
)
class PromoStates(StatesGroup):
    waiting_for_media = State()
    waiting_for_text = State()

class ContactState(StatesGroup):
    waiting_for_message = State()


class ReplyState(StatesGroup):
    waiting_for_reply = State()


class SupportReplyState(StatesGroup):
    waiting_for_message = State()


class ScheduleAdminStates(StatesGroup):
    waiting_for_photo = State()

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("stripe").setLevel(logging.WARNING)
logging.info("Начинаю подключение к базе данных...")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GROUP_ID = os.getenv("GROUP_ID")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
stripe.api_key = os.getenv("STRIPE_API_KEY")

REQUIRED_ENV_VARS = (
    "BOT_TOKEN",
    "DATABASE_URL",
    "GROUP_ID",
    "ADMIN_IDS",
    "STRIPE_API_KEY",
    "STRIPE_WEBHOOK_SECRET",
    "WEBHOOK_SECRET",
    "YOUR_DOMAIN",
    "PRICE_TRIAL",
    "PRICE_1M",
    "PRICE_6M",
    "PRICE_12M",
)
missing_env_vars = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
if missing_env_vars:
    raise ValueError("Критическая ошибка: отсутствуют переменные окружения: " + ", ".join(missing_env_vars))

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
if (
    not WEBHOOK_SECRET
    or len(WEBHOOK_SECRET) > 256
    or re.fullmatch(r"[A-Za-z0-9_-]+", WEBHOOK_SECRET) is None
):
    raise ValueError(
        "WEBHOOK_SECRET is incompatible with Telegram webhook secret_token requirements"
    )

PHOTO_URL_INTRO = "AgACAgIAAxkBAAMPaee4TD_FGuIQ4LProdOdL5XV5EkAAiYRaxulqkBL5YKQtOj0fV4BAAMCAAN5AAM7BA"
PHOTO_URL_RULES = "AgACAgIAAxkBAAMSaee9wO7psIiqhOR3M52AQ_aRwPgAAjgRaxulqkBLRv00tJs-NW8BAAMCAAN5AAM7BA"


def is_telegram_forbidden_error(error):
    return isinstance(error, TelegramForbiddenError)


def is_telegram_bad_request_error(error):
    return isinstance(error, TelegramBadRequest)


def is_telegram_temporary_error(error):
    return isinstance(error, (TelegramNetworkError, TelegramRetryAfter))


def telegram_retry_delay_minutes(error, attempt_count=1):
    if isinstance(error, TelegramRetryAfter):
        retry_after = getattr(error, "retry_after", None)
        if retry_after is not None:
            return max(1, min(60, int((int(retry_after) + 59) / 60)))
    return min(60, max(5, int(attempt_count) * 5))


CHECKOUT_SESSION_COOLDOWN_SECONDS = 10 * 60
CHECKOUT_RETRY_WINDOW_SECONDS = 5 * 60
CHECKOUT_ADMIN_ALERT_COOLDOWN_SECONDS = 15 * 60
CHECKOUT_REUSE_ADMIN_ALERT_THRESHOLD = 3
CHECKOUT_REUSE_ADMIN_ALERT_COOLDOWN_SECONDS = 30 * 60
PAYMENT_RETRY_GRACE_HOURS = int(os.getenv("PAYMENT_RETRY_GRACE_HOURS", "48"))
FIRST_PURCHASE_RECOVERY_REMINDER_DELAY_HOURS = 24
FIRST_PURCHASE_RECOVERY_ATTEMPT_STATUSES = ("creating", "creation_unknown", "open", "expired", "failed", "completed")
ACCESS_RESTORE_DELIVERY_TYPE = "access_restore_invite"
ACCESS_RESTORE_SOURCE_ADMIN = "admin_restore_access"
ACCESS_RESTORE_SOURCE_AUTO_SYNC = "automatic_membership_repair"
SUBSCRIPTION_REFUND_REVOKE_EVENT = "stripe_refund_access_revoked"
MANUAL_ACCESS_REVOKED_EVENT = "manual_access_revoked"
SUBSCRIPTION_REFUND_REVIEW_REQUIRED = "review_required"
SUBSCRIPTION_REFUND_ACCESS_REVOKED = "access_revoked"
SUBSCRIPTION_REFUND_ALREADY_RECONCILED = "already_reconciled"
SUBSCRIPTION_REFUND_ALREADY_INACTIVE = "already_inactive"
SUBSCRIPTION_REFUND_EXPIRY_TOLERANCE_SECONDS = 120
SUBSCRIPTION_REFUND_DANGEROUS_DELIVERY_TYPES = (
    "stripe_rejoin_invite",
    "stripe_rejoin_check",
    ACCESS_RESTORE_DELIVERY_TYPE,
    "subscription_expiry_reminder",
    "grace_reminder",
    "payment_recovery_reminder",
    "first_purchase_recovery_reminder",
)
GIFT_PAYMENT_KIND = "gift_access"
GIFT_TARIFFS = {
    "gift_1m": {"duration_days": 30, "price_env": "GIFT_PRICE_1M", "label": "1 месяц"},
    "gift_6m": {"duration_days": 180, "price_env": "GIFT_PRICE_6M", "label": "6 месяцев"},
    "gift_12m": {"duration_days": 365, "price_env": "GIFT_PRICE_12M", "label": "12 месяцев"},
}
GIFT_NAME_LIMIT = 80
GIFT_MESSAGE_LIMIT = 300
GIFT_TOKEN_PREFIX = "gift_"
GIFT_TOKEN_SECRET_ENV = "GIFT_TOKEN_SECRET"
GIFT_CERTIFICATE_BUYER = "gift_certificate_buyer"
GIFT_CERTIFICATE_RECIPIENT = "gift_certificate_recipient"
GIFT_CERTIFICATE_CAPTION_LIMIT = 1024
GIFT_TEXT_DELIVERY_TYPES = {
    "gift_paid_buyer",
    "gift_checkout_expired_buyer",
    "gift_checkout_failed_buyer",
    "gift_redeemed_buyer",
    "gift_redeemed_recipient",
    "gift_reserved_buyer",
    "gift_reserved_recipient",
    "gift_refunded_buyer",
    "gift_refunded_recipient",
    "gift_certificate_failed_buyer",
    "gift_admin_success",
    "gift_admin_redeemed",
    "gift_admin_problem",
    "gift_admin_refund",
    "gift_admin_certificate_problem",
}
DB_CONNECT_TIMEOUT_SECONDS = int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "5"))
DB_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "15000"))
DB_POOL_MIN_CONN = int(os.getenv("DB_POOL_MIN_CONN", "1"))
DB_POOL_MAX_CONN = int(os.getenv("DB_POOL_MAX_CONN", "5"))
DB_POOL = None
DB_POOL_CONNECTION_ERRORS = 0
checkout_session_cache = {}
checkout_retry_state = {}
checkout_session_cache_lock = asyncio.Lock()
SCHEDULER_JOBS_REGISTERED = False


def inline_keyboard(rows):
    return InlineKeyboardMarkup(inline_keyboard=rows)


def reply_keyboard(rows, resize_keyboard=False, one_time_keyboard=None):
    kwargs = {"keyboard": rows, "resize_keyboard": resize_keyboard}
    if one_time_keyboard is not None:
        kwargs["one_time_keyboard"] = one_time_keyboard
    return ReplyKeyboardMarkup(**kwargs)


CHECKOUT_OPEN_INSTRUCTION = (
    "💳 Нажмите кнопку ниже, чтобы перейти к оплате.\n\n"
    "Если страница оплаты открылась внутри Telegram и сбрасывается, это может быть связано "
    "со встроенным браузером Telegram.\n\n"
    "Попробуйте открыть оплату во внешнем браузере Safari или Chrome: нажмите ⋯ в окне оплаты "
    "и выберите «Открыть в браузере»."
)

# --- СОСТОЯНИЯ FSM ---
class RegistrationStates(StatesGroup):
    intro = State()
    description = State()
    rules = State()
    choice = State()


class GiftPurchaseStates(StatesGroup):
    tariff = State()
    certificate_name = State()
    recipient_name = State()
    sender_name = State()
    message = State()
    preview = State()


bot = Bot(token=BOT_TOKEN)
storage = PostgresFSMStorage(lambda: get_db_conn())
router = Router()
dp = Dispatcher(storage=storage)
dp.include_router(router)
SCHEDULER_TZ = ZoneInfo("UTC")
scheduler = AsyncIOScheduler(timezone=SCHEDULER_TZ)

# --- ФУНКЦИИ БАЗЫ ДАННЫХ ---
class TrackedThreadedConnectionPool:
    def __init__(self, minconn, maxconn, dsn, **kwargs):
        self._pool = psycopg2_pool.ThreadedConnectionPool(minconn, maxconn, dsn, **kwargs)
        self._maxconn = maxconn
        self._lock = threading.Lock()
        self._checked_out = set()
        self._connection_errors = 0

    def getconn(self):
        try:
            conn = self._pool.getconn()
        except Exception:
            with self._lock:
                self._connection_errors += 1
            raise
        with self._lock:
            self._checked_out.add(id(conn))
        return conn

    def putconn(self, conn, close=False):
        conn_id = id(conn)
        with self._lock:
            if conn_id not in self._checked_out:
                logging.warning("DB_POOL_DOUBLE_PUT_IGNORED: conn_id=%s", conn_id)
                return
            self._checked_out.remove(conn_id)
        return self._pool.putconn(conn, close=close)

    def closeall(self):
        with self._lock:
            self._checked_out.clear()
        return self._pool.closeall()

    def health(self):
        with self._lock:
            used = len(self._checked_out)
            errors = self._connection_errors
        return {
            "pool_available": max(0, self._maxconn - used),
            "pool_used": used,
            "connection_errors": errors,
            "statement_timeout_ms": DB_STATEMENT_TIMEOUT_MS,
        }


class PooledDbConnection:
    def __init__(self, raw_conn, pool):
        self._raw_conn = raw_conn
        self._pool = pool
        self._closed = False

    def cursor(self, *args, **kwargs):
        return self._raw_conn.cursor(*args, **kwargs)

    def commit(self):
        return self._raw_conn.commit()

    def rollback(self):
        return self._raw_conn.rollback()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._pool.putconn(self._raw_conn)

    def close_broken(self):
        if self._closed:
            return
        self._closed = True
        self._pool.putconn(self._raw_conn, close=True)

    def __getattr__(self, name):
        return getattr(self._raw_conn, name)


def get_db_pool():
    global DB_POOL
    if DB_POOL is None:
        DB_POOL = TrackedThreadedConnectionPool(
            DB_POOL_MIN_CONN,
            DB_POOL_MAX_CONN,
            DATABASE_URL,
            sslmode='require',
            connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
            options=f"-c statement_timeout={DB_STATEMENT_TIMEOUT_MS}",
        )
    return DB_POOL


def get_db_conn():
    pool = get_db_pool()
    return PooledDbConnection(pool.getconn(), pool)


def close_db_pool():
    global DB_POOL
    if DB_POOL is not None:
        DB_POOL.closeall()
        DB_POOL = None


def db_pool_health():
    pool_obj = DB_POOL
    if pool_obj is None:
        return {
            "pool_available": "unknown",
            "pool_used": "unknown",
            "connection_errors": DB_POOL_CONNECTION_ERRORS,
            "statement_timeout_ms": DB_STATEMENT_TIMEOUT_MS,
        }
    return pool_obj.health()

def init_db():
    result = run_migrations(get_db_conn)
    logging.info("--- DB MIGRATIONS VERIFIED --- %s", result)

# Идемпотентность вебхуков
async def is_event_processed(event_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM stripe_events WHERE event_id = %s AND processed IS TRUE", (event_id,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists

async def claim_event_processing(event_id, event_created_at=None, event_type=None, object_id=None):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        claim_result = claim_stripe_event(
            cur,
            event_id,
            event_created_at=event_created_at,
            event_type=event_type,
            object_id=object_id,
        )
        conn.commit()
        return claim_result
    finally:
        cur.close()
        conn.close()

async def mark_event_processed(event_id, claim_generation):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        result = mark_stripe_event_processed(cur, event_id, claim_generation)
        conn.commit()
        if result != "processed":
            raise RuntimeError("Stripe event claim ownership lost before mark_processed")
        return result
    finally:
        cur.close()
        conn.close()

async def release_event_processing(event_id, claim_generation):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        result = release_stripe_event_claim(cur, event_id, claim_generation)
        conn.commit()
        return result
    finally:
        cur.close()
        conn.close()

async def log_access_event(
    telegram_id,
    event_type,
    source=None,
    old_expiry=None,
    new_expiry=None,
    stripe_event_id=None,
    stripe_subscription_id=None,
    notes=None
):
    conn = None
    cur = None

    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO access_events (
                telegram_id,
                event_type,
                source,
                old_expiry,
                new_expiry,
                stripe_event_id,
                stripe_subscription_id,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(telegram_id),
            event_type,
            source,
            old_expiry,
            new_expiry,
            stripe_event_id,
            stripe_subscription_id,
            notes
        ))
        conn.commit()
    except Exception as e:
        logging.error(f"Не удалось записать access_event для {telegram_id}: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_obj_value(obj, *path):
    current = obj
    for key in path:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(key)
        else:
            current = getattr(current, key, None)
    return current


def get_stripe_object_id(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return get_obj_value(value, "id")


def safe_log_id(value):
    return redact_identifier(value) or "нет"


def safe_log_email(value):
    return redact_email(value) or "нет"


def safe_log_url(value):
    return redact_url(value) or "нет"


def safe_delivery_hash(delivery_key):
    return hashlib.sha256(str(delivery_key).encode("utf-8")).hexdigest()[:16] if delivery_key else "нет"


def safe_admin_error_reference(context, exception):
    fingerprint = hashlib.sha256(f"{context}:{type(exception).__name__}:{exception}".encode("utf-8")).hexdigest()[:12]
    return f"{context}:{fingerprint}"


def safe_admin_context_reference(context, *parts):
    material = "|".join(str(part) for part in parts if part is not None)
    fingerprint = hashlib.sha256(f"{context}:{material}".encode("utf-8")).hexdigest()[:12]
    return f"{context}:{fingerprint}"


def non_decreasing_expiry(existing_expiry, candidate_expiry):
    """Return the later expiry without mixing aware and naive datetime comparisons."""
    def utc_naive(value):
        if value is None:
            return None
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    existing_utc = utc_naive(existing_expiry)
    candidate_utc = utc_naive(candidate_expiry)
    if existing_utc is None:
        return candidate_utc
    if candidate_utc is None:
        return existing_utc
    return max(existing_utc, candidate_utc)


def critical_alert_fingerprint(text):
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()[:16]


def safe_error_category(error_text):
    if not error_text:
        return "нет"
    text = str(error_text).lower()
    if "free_lesson_video_id" in text or "video_id" in text:
        return "free_lesson_video_missing"
    if "forbidden" in text or "blocked" in text:
        return "telegram_forbidden"
    if "retry" in text or "too many requests" in text:
        return "telegram_retry_after"
    if "network" in text or "timeout" in text:
        return "telegram_network"
    if "bad request" in text or "chat not found" in text:
        return "telegram_bad_request"
    if "delivery text missing" in text:
        return "payload_missing"
    return "other"


def fmt_outbox_dt(value):
    return value.strftime("%d.%m.%Y %H:%M") if value else "нет"


def normalize_utc_naive(value):
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def format_elapsed_duration(seconds):
    if seconds is None:
        return "нет"
    seconds = max(0, int(seconds))
    return f"{seconds // 60} мин."


def format_future_duration(seconds):
    if seconds is None:
        return "нет"
    seconds = max(0, int(seconds))
    minutes = (seconds + 59) // 60
    return f"{minutes} мин."


def format_nonnegative_duration(seconds):
    return format_elapsed_duration(seconds)


def outbox_unresolved_age_seconds(reference_time, oldest_unresolved_at):
    reference_time = normalize_utc_naive(reference_time)
    oldest_unresolved_at = normalize_utc_naive(oldest_unresolved_at)
    if not reference_time or not oldest_unresolved_at:
        return None
    return max(0, int((reference_time - oldest_unresolved_at).total_seconds()))


def outbox_next_retry_seconds(reference_time, next_attempt_at):
    reference_time = normalize_utc_naive(reference_time)
    next_attempt_at = normalize_utc_naive(next_attempt_at)
    if not reference_time or not next_attempt_at or next_attempt_at <= reference_time:
        return None
    return max(0, int((next_attempt_at - reference_time).total_seconds()))


def parse_checkout_days(metadata):
    if metadata is None:
        return None
    raw_days = metadata.get("days") if isinstance(metadata, dict) else getattr(metadata, "days", None)
    try:
        days = int(raw_days)
    except (TypeError, ValueError):
        return None
    return days if days > 0 else None


def positive_int_or_none(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def resolve_checkout_telegram_id(session):
    client_reference_id = positive_int_or_none(stripe_value(session, "client_reference_id"))
    metadata_telegram_id = positive_int_or_none(stripe_value(session, "metadata", "telegram_id"))

    if client_reference_id and metadata_telegram_id and client_reference_id != metadata_telegram_id:
        return None
    return client_reference_id or metadata_telegram_id


def stripe_period_to_datetime(period_end):
    return datetime.utcfromtimestamp(period_end) if period_end else None


def update_telegram_user_profile(cur, telegram_user):
    if not telegram_user:
        return
    telegram_id = getattr(telegram_user, "id", None)
    if not telegram_id:
        return
    cur.execute("""
        INSERT INTO users (telegram_id, paid, username, first_name, last_name, profile_updated_at)
        VALUES (%s, FALSE, %s, %s, %s, NOW())
        ON CONFLICT (telegram_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            profile_updated_at = NOW()
    """, (
        int(telegram_id),
        getattr(telegram_user, "username", None),
        getattr(telegram_user, "first_name", None),
        getattr(telegram_user, "last_name", None),
    ))


def save_telegram_user_profile(telegram_user):
    conn = None
    cur = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        update_telegram_user_profile(cur, telegram_user)
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logging.warning(
            "Не удалось обновить профиль Telegram user_id=%s: %s",
            getattr(telegram_user, "id", None),
            e,
        )
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def invoice_line_period_datetimes(invoice):
    lines_data = get_obj_value(invoice, "lines", "data") or []
    first_line = lines_data[0] if lines_data else None
    period_start = stripe_period_to_datetime(get_obj_value(first_line, "period", "start"))
    period_end = stripe_period_to_datetime(get_obj_value(first_line, "period", "end"))
    return period_start, period_end


def normalize_payment_kind(payment_kind):
    if payment_kind == "subscription_adjustment":
        return "adjustment"
    if payment_kind in ("trial", "initial_subscription", "recurring", "adjustment", "out_of_band", GIFT_PAYMENT_KIND):
        return payment_kind
    return "unknown"


def insert_payment_event(
    cur,
    stripe_event_id,
    event_type,
    payment_status,
    telegram_id=None,
    invoice_id=None,
    checkout_session_id=None,
    stripe_customer_id=None,
    stripe_subscription_id=None,
    payment_kind=None,
    billing_reason=None,
    tariff_code=None,
    amount_paid=0,
    amount_due=0,
    currency=None,
    period_start=None,
    period_end=None,
    recovered_after_failure=False,
    created_at=None,
):
    payment_status = payment_status if payment_status in ("succeeded", "failed") else "failed"
    payment_kind = normalize_payment_kind(payment_kind)
    cur.execute("""
        INSERT INTO payment_events (
            stripe_event_id,
            event_type,
            telegram_id,
            invoice_id,
            checkout_session_id,
            stripe_customer_id,
            stripe_subscription_id,
            payment_status,
            payment_kind,
            billing_reason,
            tariff_code,
            amount_paid,
            amount_due,
            currency,
            period_start,
            period_end,
            recovered_after_failure,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()))
        ON CONFLICT (stripe_event_id) DO NOTHING
    """, (
        stripe_event_id,
        event_type,
        int(telegram_id) if telegram_id is not None else None,
        invoice_id,
        checkout_session_id,
        stripe_customer_id,
        stripe_subscription_id,
        payment_status,
        payment_kind,
        billing_reason,
        tariff_code or "unknown",
        int(amount_paid or 0),
        int(amount_due or 0),
        currency,
        period_start,
        period_end,
        bool(recovered_after_failure),
        created_at,
    ))


STRIPE_IDENTITY_CONSTRAINT_MAP = {
    "users_unique_stripe_subscription": ("users_subscription_conflict", "stripe_subscription_id"),
    "users_unique_stripe_customer": ("users_customer_conflict", "stripe_customer_id"),
    "stripe_links_unique_subscription_user": ("stripe_links_subscription_conflict", "stripe_subscription_id"),
    "stripe_links_unique_customer_user": ("stripe_links_customer_conflict", "stripe_customer_id"),
}


class StripeIdentityConflictError(Exception):
    def __init__(
        self,
        conflict_type,
        stripe_id,
        existing_telegram_id,
        requested_telegram_id,
        source,
        constraint_name=None,
    ):
        super().__init__(conflict_type)
        self.conflict_type = conflict_type
        self.stripe_id = stripe_id
        self.safe_stripe_id = safe_log_id(stripe_id)
        self.existing_telegram_id = int(existing_telegram_id) if existing_telegram_id is not None else None
        self.requested_telegram_id = int(requested_telegram_id) if requested_telegram_id is not None else None
        self.source = source
        self.constraint_name = constraint_name


def stripe_identity_conflict_from_unique_violation(error, telegram_id, customer_id=None, subscription_id=None, source=None):
    constraint_name = getattr(getattr(error, "diag", None), "constraint_name", None)
    mapped = STRIPE_IDENTITY_CONSTRAINT_MAP.get(constraint_name)
    if not mapped:
        return None
    conflict_type, id_kind = mapped
    stripe_id = subscription_id if id_kind == "stripe_subscription_id" else customer_id
    return StripeIdentityConflictError(
        conflict_type,
        stripe_id,
        None,
        telegram_id,
        source or "unknown",
        constraint_name=constraint_name,
    )


def stripe_identity_conflict_telegram_ids_text(conflict):
    telegram_ids = sorted({
        int(value)
        for value in (conflict.existing_telegram_id, conflict.requested_telegram_id)
        if value is not None
    })
    return json.dumps(telegram_ids, separators=(",", ":"))


def populate_stripe_identity_conflict_owner(cur, conflict):
    if conflict.existing_telegram_id is not None or not conflict.stripe_id:
        return conflict
    if "subscription" in conflict.conflict_type:
        column = "stripe_subscription_id"
    elif "customer" in conflict.conflict_type:
        column = "stripe_customer_id"
    else:
        return conflict

    cur.execute(
        f"""
        SELECT telegram_id
        FROM users
        WHERE {column} = %s
        UNION
        SELECT telegram_id
        FROM stripe_links
        WHERE {column} = %s
        ORDER BY telegram_id
        LIMIT 1
        """,
        (conflict.stripe_id, conflict.stripe_id),
    )
    row = cur.fetchone()
    if row:
        conflict.existing_telegram_id = int(row[0])
    return conflict


def known_stripe_identity_unique_violation_is_same_user(conflict):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        populate_stripe_identity_conflict_owner(cur, conflict)
        return (
            conflict.existing_telegram_id is not None
            and conflict.requested_telegram_id is not None
            and int(conflict.existing_telegram_id) == int(conflict.requested_telegram_id)
        )
    finally:
        cur.close()
        conn.close()


def populate_known_stripe_identity_unique_violation_owner(conflict):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        populate_stripe_identity_conflict_owner(cur, conflict)
        return conflict
    finally:
        cur.close()
        conn.close()


def stripe_identity_already_linked_for_user(telegram_id, customer_id=None, subscription_id=None):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM users
            WHERE telegram_id = %s
              AND (%s IS NULL OR stripe_customer_id = %s)
              AND (%s IS NULL OR stripe_subscription_id = %s)
            UNION
            SELECT 1
            FROM stripe_links
            WHERE telegram_id = %s
              AND (%s IS NULL OR stripe_customer_id = %s)
              AND (%s IS NULL OR stripe_subscription_id = %s)
            LIMIT 1
            """,
            (
                telegram_id,
                customer_id,
                customer_id,
                subscription_id,
                subscription_id,
                telegram_id,
                customer_id,
                customer_id,
                subscription_id,
                subscription_id,
            ),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def raise_stripe_identity_conflict(conflict_type, stripe_id, existing_telegram_id, requested_telegram_id, source):
    if existing_telegram_id is None or int(existing_telegram_id) == int(requested_telegram_id):
        return
    raise StripeIdentityConflictError(
        conflict_type,
        stripe_id,
        existing_telegram_id,
        requested_telegram_id,
        source,
    )


def assert_stripe_identity_available(cur, telegram_id, customer_id=None, subscription_id=None, source=None):
    if not telegram_id:
        return
    telegram_id = int(telegram_id)
    checks = (
        ("users_subscription_conflict", "users", "stripe_subscription_id", subscription_id),
        ("users_customer_conflict", "users", "stripe_customer_id", customer_id),
        ("stripe_links_subscription_conflict", "stripe_links", "stripe_subscription_id", subscription_id),
        ("stripe_links_customer_conflict", "stripe_links", "stripe_customer_id", customer_id),
    )
    for conflict_type, table, column, stripe_id in checks:
        if not stripe_id:
            continue
        cur.execute(
            f"""
            SELECT telegram_id
            FROM {table}
            WHERE {column} = %s
            FOR UPDATE
            """,
            (stripe_id,),
        )
        for (existing_telegram_id,) in cur.fetchall():
            raise_stripe_identity_conflict(conflict_type, stripe_id, existing_telegram_id, telegram_id, source)

    cur.execute(
        """
        SELECT telegram_id, stripe_customer_id, stripe_subscription_id
        FROM users
        WHERE telegram_id = %s
        FOR UPDATE
        """,
        (telegram_id,),
    )
    user_row = cur.fetchone()

    cur.execute(
        """
        SELECT telegram_id, stripe_customer_id, stripe_subscription_id
        FROM stripe_links
        WHERE telegram_id = %s
        FOR UPDATE
        """,
        (telegram_id,),
    )
    cur.fetchall()


def insert_stripe_identity_conflict(cur, conflict):
    telegram_ids_text = stripe_identity_conflict_telegram_ids_text(conflict)
    details = json.dumps(
        {
            "source": conflict.source,
            "safe_stripe_id": conflict.safe_stripe_id,
            "existing_telegram_id": conflict.existing_telegram_id,
            "requested_telegram_id": conflict.requested_telegram_id,
            "constraint_name": conflict.constraint_name,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    cur.execute(
        """
        INSERT INTO stripe_identity_conflicts (
            conflict_type, stripe_id, telegram_ids, details, resolved, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, FALSE, NOW(), NOW())
        ON CONFLICT (conflict_type, stripe_id, telegram_ids) WHERE resolved IS NOT TRUE
        DO UPDATE SET
            details = COALESCE(stripe_identity_conflicts.details, EXCLUDED.details),
            updated_at = NOW()
        """,
        (
            conflict.conflict_type,
            conflict.stripe_id,
            telegram_ids_text,
            details,
        ),
    )


def persist_stripe_identity_conflict_audit(
    conflict,
    event_id,
    event_type,
    invoice_id=None,
    checkout_session_id=None,
    amount_paid=None,
    currency=None,
    billing_reason=None,
    period_end=None,
):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        populate_stripe_identity_conflict_owner(cur, conflict)
        insert_stripe_identity_conflict(cur, conflict)
        save_unlinked_stripe_event(
            cur,
            event_id,
            event_type,
            invoice_id=invoice_id,
            stripe_customer_id=conflict.stripe_id if "customer" in conflict.conflict_type else None,
            stripe_subscription_id=conflict.stripe_id if "subscription" in conflict.conflict_type else None,
            customer_email=None,
            amount_paid=amount_paid,
            currency=currency,
            billing_reason=billing_reason,
            period_end=period_end,
            raw_summary=(
                f"stripe_identity_conflict; type={conflict.conflict_type}; "
                f"safe_stripe_id={conflict.safe_stripe_id}; source={conflict.source}"
            ),
        )
        enqueue_admin_payment_problem_safely(
            cur,
            event_id=event_id,
            purpose="stripe_identity_conflict",
            stage=conflict.source,
            telegram_id=conflict.requested_telegram_id,
            category="invalid_checkout_metadata",
            stripe_retry="нет",
            recovery_reminder="не применимо",
            safe_ref=safe_admin_context_reference(
                "stripe_identity_conflict",
                conflict.conflict_type,
                conflict.safe_stripe_id,
                conflict.existing_telegram_id,
                conflict.requested_telegram_id,
            ),
            note=(
                f"conflict_type: {conflict.conflict_type}\n"
                f"stripe_id: {conflict.safe_stripe_id}\n"
                f"existing_telegram_id: {conflict.existing_telegram_id or 'unknown'}\n"
                f"requested_telegram_id: {conflict.requested_telegram_id or 'unknown'}\n"
                "Платёжная транзакция откатилась. Доступ и Stripe-связи не изменены."
            ),
            severity="CRITICAL",
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


async def finalize_stripe_identity_conflict_response(
    conflict,
    event_id,
    event_type,
    *,
    claim_generation,
    **audit_kwargs,
):
    try:
        persist_stripe_identity_conflict_audit(
            conflict,
            event_id,
            event_type,
            **audit_kwargs,
        )
    except Exception as audit_error:
        await release_event_processing(event_id, claim_generation)
        logging.exception(
            "STRIPE_IDENTITY_CONFLICT_AUDIT_FAILED: event_id=%s, event.type=%s, "
            "source=%s, conflict_type=%s, stripe_id=%s, error=%s",
            safe_log_id(event_id),
            event_type,
            conflict.source,
            conflict.conflict_type,
            conflict.safe_stripe_id,
            audit_error,
        )
        await enqueue_admin_payment_problem_now(
            event_id=event_id,
            purpose="stripe_identity_conflict_audit_failed",
            stage=conflict.source or "webhook",
            telegram_id=conflict.requested_telegram_id,
            category="webhook_processing_failed",
            exception=audit_error,
            stripe_retry="да",
            recovery_reminder="не применимо",
            safe_ref=safe_admin_error_reference("stripe_identity_conflict_audit", audit_error),
            note="Webhook вернул 500, Stripe повторит событие. Identity conflict audit не сохранён.",
        )
        return web.Response(status=500)
    await mark_event_processed(event_id, claim_generation)
    return web.Response(status=200)


async def finalize_stripe_identity_unique_violation_webhook_response(
    error,
    requested_telegram_id,
    customer_id,
    subscription_id,
    source,
    event_id,
    event_type,
    *,
    claim_generation,
    **audit_kwargs,
):
    conflict = stripe_identity_conflict_from_unique_violation(
        error,
        requested_telegram_id,
        customer_id=customer_id,
        subscription_id=subscription_id,
        source=source,
    )
    if not conflict:
        raise error

    populate_known_stripe_identity_unique_violation_owner(conflict)
    if conflict.requested_telegram_id is None or conflict.existing_telegram_id is None:
        logging.warning(
            "STRIPE_IDENTITY_UNIQUE_OWNER_UNKNOWN: event_id=%s, event.type=%s, "
            "source=%s, conflict_type=%s, stripe_id=%s; event claim will be released",
            safe_log_id(event_id),
            event_type,
            source,
            conflict.conflict_type,
            conflict.safe_stripe_id,
        )
        await release_event_processing(event_id, claim_generation)
        return web.Response(status=500)
    if int(conflict.existing_telegram_id) == int(conflict.requested_telegram_id):
        logging.warning(
            "STRIPE_IDENTITY_SAME_USER_UNIQUE_RACE: event_id=%s, event.type=%s, "
            "source=%s, conflict_type=%s, stripe_id=%s, telegram_id=%s; "
            "event claim will be released for Stripe retry",
            safe_log_id(event_id),
            event_type,
            source,
            conflict.conflict_type,
            conflict.safe_stripe_id,
            conflict.requested_telegram_id,
        )
        await release_event_processing(event_id, claim_generation)
        return web.Response(status=500)
    return await finalize_stripe_identity_conflict_response(
        conflict,
        event_id,
        event_type,
        claim_generation=claim_generation,
        **audit_kwargs,
    )


def upsert_stripe_link(
    cur,
    telegram_id,
    stripe_customer_id=None,
    stripe_subscription_id=None,
    customer_email=None,
    status=None,
    current_period_end=None,
    is_active=False,
    source=None,
):
    if not telegram_id or (not stripe_customer_id and not stripe_subscription_id):
        return

    current_period_end_dt = (
        current_period_end
        if isinstance(current_period_end, datetime)
        else stripe_period_to_datetime(current_period_end)
    )
    if stripe_subscription_id:
        cur.execute(
            """
            SELECT telegram_id
            FROM stripe_links
            WHERE stripe_subscription_id = %s
            FOR UPDATE
            """,
            (stripe_subscription_id,),
        )
        existing_row = cur.fetchone()
        if existing_row:
            existing_telegram_id = int(existing_row[0])
            if existing_telegram_id != int(telegram_id):
                raise_stripe_identity_conflict(
                    "stripe_links_subscription_conflict",
                    stripe_subscription_id,
                    existing_telegram_id,
                    telegram_id,
                    source,
                )
            cur.execute(
                """
                UPDATE stripe_links
                SET stripe_customer_id = COALESCE(%s, stripe_customer_id),
                    customer_email = COALESCE(%s, customer_email),
                    status = COALESCE(%s, status),
                    current_period_end = COALESCE(%s, current_period_end),
                    is_active = %s,
                    source = COALESCE(%s, source),
                    updated_at = NOW()
                WHERE stripe_subscription_id = %s
                  AND telegram_id = %s
                """,
                (
                    stripe_customer_id,
                    customer_email,
                    status,
                    current_period_end_dt,
                    bool(is_active),
                    source,
                    stripe_subscription_id,
                    int(telegram_id),
                ),
            )
            return

        cur.execute(
            """
            INSERT INTO stripe_links (
                telegram_id,
                stripe_customer_id,
                stripe_subscription_id,
                customer_email,
                status,
                current_period_end,
                is_active,
                source,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                int(telegram_id),
                stripe_customer_id,
                stripe_subscription_id,
                customer_email,
                status,
                current_period_end_dt,
                bool(is_active),
                source,
            ),
        )
        return

    cur.execute("""
        INSERT INTO stripe_links (
            telegram_id,
            stripe_customer_id,
            stripe_subscription_id,
            customer_email,
            status,
            current_period_end,
            is_active,
            source,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (telegram_id, stripe_customer_id, stripe_subscription_id) DO UPDATE SET
            customer_email = COALESCE(EXCLUDED.customer_email, stripe_links.customer_email),
            status = COALESCE(EXCLUDED.status, stripe_links.status),
            current_period_end = COALESCE(EXCLUDED.current_period_end, stripe_links.current_period_end),
            is_active = EXCLUDED.is_active,
            source = COALESCE(EXCLUDED.source, stripe_links.source),
            updated_at = NOW()
    """, (
        int(telegram_id),
        stripe_customer_id,
        stripe_subscription_id,
        customer_email,
        status,
        current_period_end_dt,
        bool(is_active),
        source,
    ))


def mark_stripe_link_subscription_terminal(cur, stripe_subscription_id, status):
    if not stripe_subscription_id:
        return
    if not is_terminal_subscription_status(status):
        return
    cur.execute("""
        UPDATE stripe_links
        SET status = %s,
            is_active = FALSE,
            updated_at = NOW()
        WHERE stripe_subscription_id = %s
    """, (status, stripe_subscription_id))


def assert_existing_subscription_identity_available(cur, stripe_subscription_id, customer_id=None, source=None):
    if not stripe_subscription_id:
        return None
    cur.execute(
        """
        SELECT telegram_id
        FROM users
        WHERE stripe_subscription_id = %s
        FOR UPDATE
        """,
        (stripe_subscription_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    telegram_id = row[0]
    assert_stripe_identity_available(
        cur,
        telegram_id,
        customer_id=customer_id,
        subscription_id=stripe_subscription_id,
        source=source,
    )
    return telegram_id


def find_telegram_id_for_stripe(cur, metadata_telegram_id=None, stripe_subscription_id=None, stripe_customer_id=None):
    if metadata_telegram_id:
        try:
            metadata_telegram_id = int(metadata_telegram_id)
        except (TypeError, ValueError):
            metadata_telegram_id = None

        if metadata_telegram_id:
            cur.execute("SELECT telegram_id FROM users WHERE telegram_id = %s", (metadata_telegram_id,))
            row = cur.fetchone()
            if row:
                return row[0], "metadata.telegram_id"

    if stripe_subscription_id:
        cur.execute("SELECT telegram_id FROM users WHERE stripe_subscription_id = %s", (stripe_subscription_id,))
        row = cur.fetchone()
        if row:
            return row[0], "users.stripe_subscription_id"

    if stripe_customer_id:
        cur.execute("SELECT telegram_id FROM users WHERE stripe_customer_id = %s", (stripe_customer_id,))
        row = cur.fetchone()
        if row:
            return row[0], "users.stripe_customer_id"

    if stripe_subscription_id:
        cur.execute("""
            SELECT telegram_id
            FROM stripe_links
            WHERE stripe_subscription_id = %s
            ORDER BY is_active DESC, updated_at DESC
            LIMIT 1
        """, (stripe_subscription_id,))
        row = cur.fetchone()
        if row:
            return row[0], "stripe_links.stripe_subscription_id"

    if stripe_customer_id:
        cur.execute("""
            SELECT telegram_id
            FROM stripe_links
            WHERE stripe_customer_id = %s
            ORDER BY is_active DESC, updated_at DESC
            LIMIT 1
        """, (stripe_customer_id,))
        row = cur.fetchone()
        if row:
            return row[0], "stripe_links.stripe_customer_id"

    return None, None


def save_unlinked_stripe_event(
    cur,
    event_id,
    event_type,
    invoice_id=None,
    stripe_customer_id=None,
    stripe_subscription_id=None,
    customer_email=None,
    amount_paid=None,
    currency=None,
    billing_reason=None,
    period_end=None,
    raw_summary=None,
):
    period_end_dt = period_end if isinstance(period_end, datetime) else stripe_period_to_datetime(period_end)
    cur.execute("""
        INSERT INTO unlinked_stripe_events (
            event_id,
            event_type,
            invoice_id,
            stripe_customer_id,
            stripe_subscription_id,
            customer_email,
            amount_paid,
            currency,
            billing_reason,
            period_end,
            raw_summary,
            resolved
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)
        ON CONFLICT (event_id) DO UPDATE SET
            event_type = EXCLUDED.event_type,
            invoice_id = COALESCE(EXCLUDED.invoice_id, unlinked_stripe_events.invoice_id),
            stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, unlinked_stripe_events.stripe_customer_id),
            stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, unlinked_stripe_events.stripe_subscription_id),
            customer_email = COALESCE(EXCLUDED.customer_email, unlinked_stripe_events.customer_email),
            amount_paid = COALESCE(EXCLUDED.amount_paid, unlinked_stripe_events.amount_paid),
            currency = COALESCE(EXCLUDED.currency, unlinked_stripe_events.currency),
            billing_reason = COALESCE(EXCLUDED.billing_reason, unlinked_stripe_events.billing_reason),
            period_end = COALESCE(EXCLUDED.period_end, unlinked_stripe_events.period_end),
            raw_summary = COALESCE(EXCLUDED.raw_summary, unlinked_stripe_events.raw_summary)
    """, (
        event_id,
        event_type,
        invoice_id,
        stripe_customer_id,
        stripe_subscription_id,
        customer_email,
        amount_paid,
        currency,
        billing_reason,
        period_end_dt,
        raw_summary,
    ))


def fetch_unlinked_events_for_manual_link(customer_id, subscription_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                event_id,
                event_type,
                invoice_id,
                stripe_customer_id,
                stripe_subscription_id,
                amount_paid,
                currency,
                billing_reason,
                period_end,
                created_at
            FROM unlinked_stripe_events
            WHERE resolved IS NOT TRUE
              AND event_type = 'invoice.payment_succeeded'
              AND (
                  stripe_customer_id = %s
                  OR stripe_subscription_id = %s
              )
            ORDER BY created_at ASC
        """, (customer_id, subscription_id))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


async def prepare_manual_link_payment_events(customer_id, subscription_id):
    rows = fetch_unlinked_events_for_manual_link(customer_id, subscription_id)
    prepared = []
    for row in rows:
        (
            event_id,
            event_type,
            invoice_id,
            row_customer_id,
            row_subscription_id,
            amount_paid,
            currency,
            billing_reason,
            period_end,
            created_at,
        ) = row
        amount_due = 0
        period_start = None
        tariff_code = "unknown"
        invoice_action = None
        try:
            if invoice_id:
                invoice = await asyncio.to_thread(stripe.Invoice.retrieve, invoice_id)
                amount_paid = get_obj_value(invoice, "amount_paid") if get_obj_value(invoice, "amount_paid") is not None else amount_paid
                amount_due = get_obj_value(invoice, "amount_due") or 0
                currency = get_obj_value(invoice, "currency") or currency
                billing_reason = get_obj_value(invoice, "billing_reason") or billing_reason
                period_start, invoice_period_end = invoice_line_period_datetimes(invoice)
                period_end = invoice_period_end or period_end
                tariff_code = tariff_code_from_invoice(invoice)
                invoice_action = successful_invoice_action(
                    amount_paid,
                    billing_reason,
                    None,
                    None,
                    invoice=invoice,
                    amount_due=amount_due,
                )
        except Exception as e:
            logging.warning(
                "MANUAL_LINK_PAYMENT_EVENT_INVOICE_RETRIEVE_FAILED: event_id=%s, invoice_id=%s, error=%s",
                safe_log_id(event_id),
                safe_log_id(invoice_id),
                e,
            )
        prepared.append({
            "event_id": event_id,
            "event_type": event_type,
            "invoice_id": invoice_id,
            "stripe_customer_id": row_customer_id or customer_id,
            "stripe_subscription_id": row_subscription_id or subscription_id,
            "payment_kind": classify_manual_link_payment_kind(billing_reason, invoice_action),
            "billing_reason": billing_reason,
            "tariff_code": tariff_code,
            "amount_paid": amount_paid,
            "amount_due": amount_due,
            "currency": currency,
            "period_start": period_start,
            "period_end": period_end,
            "created_at": created_at,
            "create_payment_event": should_create_manual_link_payment_event(amount_paid),
        })
    return prepared


def backfill_payment_events_for_manual_link(cur, telegram_id, prepared_events):
    inserted = 0
    for event in prepared_events:
        if not event["create_payment_event"]:
            continue
        insert_payment_event(
            cur,
            event["event_id"],
            event["event_type"],
            "succeeded",
            telegram_id=telegram_id,
            invoice_id=event["invoice_id"],
            stripe_customer_id=event["stripe_customer_id"],
            stripe_subscription_id=event["stripe_subscription_id"],
            payment_kind=event["payment_kind"],
            billing_reason=event["billing_reason"],
            tariff_code=event["tariff_code"],
            amount_paid=event["amount_paid"],
            amount_due=event["amount_due"],
            currency=event["currency"],
            period_start=event["period_start"],
            period_end=event["period_end"],
            created_at=event["created_at"],
        )
        inserted += cur.rowcount
    return inserted


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
async def generate_invite_link():
    try:
        options = invite_link_options("payment_rejoin", "user")
        invite = await bot.create_chat_invite_link(
            chat_id=int(GROUP_ID),
            **options,
        )
        conn = get_db_conn()
        cur = conn.cursor()
        try:
            save_bot_invite_link(cur, invite.invite_link, "payment_rejoin", None, options.get("expire_date"))
            conn.commit()
        finally:
            cur.close()
            conn.close()
        return invite.invite_link
    except Exception as e:
        logging.error(f"Ошибка создания ссылки: {e}")
        return None

def get_tariffs_keyboard(show_trial=True):
    rows = []
    if show_trial:
        rows.append([InlineKeyboardButton(text="🌟 Пробная неделя", callback_data="sub_trial")])
    rows.extend([
        [InlineKeyboardButton(text="💳 1 месяц (50€)", callback_data="sub_1")],
        [InlineKeyboardButton(text="💳 6 месяцев (240€)", callback_data="sub_6")],
        [InlineKeyboardButton(text="💳 12 месяцев (410€)", callback_data="sub_12")],
    ])
    return inline_keyboard(rows)

def get_cancel_subscription_keyboard():
    return inline_keyboard([[
        InlineKeyboardButton(text="❌ Отменить подписку", callback_data="cancel_subscription")
    ]])


def stripe_delivery_key(event_id, purpose):
    return f"stripe:{event_id}:{purpose}"


def stripe_delivery_payload(text, keyboard_kind=None, parse_mode=None, **extra):
    payload = {"text": text}
    if keyboard_kind:
        payload["keyboard_kind"] = keyboard_kind
    if parse_mode:
        payload["parse_mode"] = parse_mode
    payload.update({key: value for key, value in extra.items() if value is not None})
    return payload


def gift_duration_days(tariff_code):
    tariff = GIFT_TARIFFS.get(tariff_code)
    return tariff["duration_days"] if tariff else None


def gift_tariff_label(tariff_code):
    tariff = GIFT_TARIFFS.get(tariff_code)
    return tariff["label"] if tariff else "неизвестно"


def gift_price_id(tariff_code):
    tariff = GIFT_TARIFFS.get(tariff_code)
    return os.getenv(tariff["price_env"]) if tariff else None


def gift_required_price_envs():
    return [config["price_env"] for config in GIFT_TARIFFS.values()]


def gift_access_unavailable_text():
    return "Подарочные сертификаты временно недоступны"


def sanitize_gift_text(value, limit):
    value = (value or "").replace("\x00", "").strip()
    value = "".join(ch for ch in value if ch == "\n" or ch == "\t" or ord(ch) >= 32)
    if len(value) > limit:
        value = value[:limit].rstrip()
    return value


def gift_sender_default_name(telegram_user):
    return sanitize_gift_text(getattr(telegram_user, "full_name", None) or "Ваш друг", GIFT_NAME_LIMIT)


def gift_token_secret():
    secret = (os.getenv(GIFT_TOKEN_SECRET_ENV) or "").strip()
    if not secret:
        raise ValueError("gift_token_secret_missing")
    if len(secret) < 32:
        raise ValueError("gift_token_secret_too_short")
    return secret


def gift_token_secret_configured():
    try:
        gift_token_secret()
        return True
    except ValueError:
        return False


def gift_token_hash(token):
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def _gift_b64url_encode(raw):
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _gift_b64url_decode(value):
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def generate_gift_token(public_reference, token_version):
    secret = gift_token_secret()
    reference_text = str(public_reference or "")
    if not reference_text.startswith("GIFT-") or len(reference_text) != 21:
        raise ValueError("gift_public_reference_invalid")
    try:
        reference_bytes = bytes.fromhex(reference_text[5:])
    except ValueError as exc:
        raise ValueError("gift_public_reference_invalid") from exc
    version = int(token_version)
    if version < 1 or version > 0xFFFFFFFF:
        raise ValueError("gift_token_version_invalid")
    payload = reference_bytes + version.to_bytes(4, "big")
    signature = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()[:16]
    return _gift_b64url_encode(payload + signature)


def parse_gift_token(token):
    if not token:
        return None
    try:
        token_text = str(token)
        if not token_text or any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-" for ch in token_text):
            return None
        raw = _gift_b64url_decode(token_text)
        if len(raw) != 28:
            return None
        reference_bytes = raw[:8]
        version_bytes = raw[8:12]
        signature = raw[12:]
        public_reference = f"GIFT-{reference_bytes.hex().upper()}"
        token_version = int.from_bytes(version_bytes, "big")
        if token_version < 1:
            return None
        payload = reference_bytes + version_bytes
        expected = hmac.new(gift_token_secret().encode("utf-8"), payload, hashlib.sha256).digest()[:16]
    except Exception:
        return None
    if not hmac.compare_digest(signature, expected):
        return None
    return public_reference, token_version


def gift_token_hash_for_reference(public_reference, token_version):
    return gift_token_hash(generate_gift_token(public_reference, token_version))


def gift_public_reference():
    return f"GIFT-{secrets.token_hex(8).upper()}"


def gift_deep_link(token):
    username = os.getenv("BOT_USERNAME")
    if username:
        return f"https://t.me/{username.lstrip('@')}?start={GIFT_TOKEN_PREFIX}{token}"
    return f"https://t.me/{os.getenv('BOT_NAME', 'Natalia_SoulFit_bot')}?start={GIFT_TOKEN_PREFIX}{token}"


def gift_safe_user_text(value):
    return html.escape(value or "", quote=False)


def gift_certificate_caption(row):
    recipient = gift_safe_user_text(row.get("recipient_name") or "для вас")
    sender = gift_safe_user_text(row.get("sender_name") or "друга")
    message = gift_safe_user_text(row.get("gift_message"))
    lines = [
        "🎁 Вам подарили доступ в клуб Натальи Ребковец",
        "",
        f"Для: {recipient}",
        f"От: {sender}",
        f"Срок доступа: {gift_tariff_label(row.get('tariff_code'))}",
    ]
    if message:
        lines.extend(["", message])
    lines.extend([
        "",
        "Как получить подарок:",
        "",
        "1. Нажмите «Активировать подарок».",
        "2. Если Telegram попросит запустить бот — нажмите Start / Начать.",
        "3. Бот активирует сертификат и откроет доступ в клуб.",
        "4. После активации вы получите кнопку для входа в закрытый клуб.",
    ])
    return "\n".join(lines)


def gift_certificate_delivery_caption(base_caption, button_url):
    caption = html.unescape(base_caption or "").strip()
    url = button_url or ""
    suffix = f"\n\nЕсли кнопка не открывается, используйте эту ссылку:\n{url}"
    available = GIFT_CERTIFICATE_CAPTION_LIMIT - len(suffix)
    if available < 0:
        return html.escape(suffix.strip()[:GIFT_CERTIFICATE_CAPTION_LIMIT], quote=True)
    if len(caption) > available:
        caption = caption[:available].rstrip()
    final_caption = f"{caption}{suffix}" if caption else suffix.strip()
    return html.escape(final_caption, quote=True)


def gift_delivery_key(public_reference, purpose, recipient_id=None, token_version=None, recipient_kind=None):
    if purpose in (GIFT_CERTIFICATE_BUYER, GIFT_CERTIFICATE_RECIPIENT):
        kind = recipient_kind or ("buyer" if purpose == GIFT_CERTIFICATE_BUYER else "recipient")
        return f"gift:{public_reference}:certificate:{kind}:v{int(token_version)}"
    suffix = f":{int(recipient_id)}" if recipient_id is not None else ""
    return f"gift:{public_reference}:{purpose}{suffix}"


def enqueue_gift_message_delivery(cur, delivery_key, telegram_id, delivery_type, payload, delay_seconds=0):
    cur.execute("""
        INSERT INTO message_delivery_events (
            delivery_key, telegram_id, delivery_type, status, attempt_count, last_error, payload_json, next_attempt_at
        )
        VALUES (%s, %s, %s, 'pending', 0, NULL, %s, NOW() + (%s * INTERVAL '1 second'))
        ON CONFLICT (delivery_key) DO NOTHING
        RETURNING delivery_key
    """, (
        delivery_key,
        int(telegram_id),
        delivery_type,
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        int(delay_seconds),
    ))
    return cur.fetchone() is not None


def enqueue_gift_text_delivery(cur, public_reference, telegram_id, purpose, text, delay_seconds=0, **extra):
    return enqueue_gift_message_delivery(
        cur,
        gift_delivery_key(public_reference, purpose, telegram_id if purpose.startswith("gift_admin_") else None),
        int(telegram_id),
        purpose,
        stripe_delivery_payload(text, **extra),
        delay_seconds=delay_seconds,
    )


def enqueue_gift_admin_delivery(cur, public_reference, purpose, text, severity="INFO"):
    count = 0
    for admin_id in ADMIN_IDS:
        if enqueue_gift_text_delivery(
            cur,
            public_reference,
            int(admin_id),
            purpose,
            text,
            severity=severity,
            safe_ref=public_reference,
        ):
            count += 1
    return count


def enqueue_gift_certificate_failure_notices(public_reference, telegram_id, gift_row):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        enqueue_gift_message_delivery(
            cur,
            gift_delivery_key(public_reference, "gift_certificate_failed_buyer"),
            int(telegram_id),
            "gift_certificate_failed_buyer",
            {
                "public_reference": public_reference,
                "token_version": int(gift_row["token_version"]),
            },
        )
        enqueue_gift_admin_delivery(
            cur,
            public_reference,
            "gift_admin_certificate_problem",
            gift_admin_text(
                "⚠️ Gift certificate delivery problem",
                gift_row,
                extra="Платёж и ссылка активации сохранены; требуется проверить локальный рендеринг или доставку Telegram.",
            ),
            severity="CRITICAL",
        )
        conn.commit()
    except Exception as error:
        conn.rollback()
        logging.error(
            "GIFT_CERTIFICATE_FAILURE_NOTICE_ENQUEUE_FAILED: safe_ref=%s error_class=%s",
            safe_admin_context_reference("gift_certificate", public_reference),
            error.__class__.__name__,
        )
    finally:
        cur.close()
        conn.close()


def gift_admin_recipient_allowed(telegram_id):
    return int(telegram_id) in ADMIN_IDS


def enqueue_gift_certificate_delivery(cur, row, telegram_id, delivery_type):
    token_version = int(row["token_version"])
    token_hash = gift_token_hash_for_reference(row["public_reference"], token_version)
    if not hmac.compare_digest(str(row.get("token_hash") or ""), token_hash):
        raise ValueError("gift_certificate_token_hash_mismatch")
    caption = gift_certificate_caption(row)
    return enqueue_gift_message_delivery(
        cur,
        gift_delivery_key(
            row["public_reference"],
            delivery_type,
            token_version=token_version,
            recipient_kind="buyer" if delivery_type == GIFT_CERTIFICATE_BUYER else "recipient",
        ),
        int(telegram_id),
        delivery_type,
        {
            "public_reference": row["public_reference"],
            "token_version": token_version,
            "recipient_kind": "buyer" if delivery_type == GIFT_CERTIFICATE_BUYER else "recipient",
            "caption": caption,
            "parse_mode": "HTML",
            "button_text": "🎁 Активировать подарок",
        },
        delay_seconds=0,
    )


def gift_row_dict(cur, row):
    if not row:
        return None
    return {desc[0]: value for desc, value in zip(cur.description, row)}


def record_gift_event(cur, gift_row, event_type, telegram_id=None, source=None, notes=None):
    cur.execute("""
        INSERT INTO gift_access_events (
            gift_id, public_reference, telegram_id, event_type, source, notes
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        gift_row["id"],
        gift_row["public_reference"],
        int(telegram_id) if telegram_id is not None else None,
        event_type,
        source,
        notes,
    ))


def fetch_gift_by_public_reference(cur, public_reference, for_update=False):
    lock_sql = " FOR UPDATE" if for_update else ""
    cur.execute(f"""
        SELECT *
        FROM gift_access_grants
        WHERE public_reference = %s
        {lock_sql}
    """, (public_reference,))
    return gift_row_dict(cur, cur.fetchone())


def fetch_gift_by_token_hash(cur, token_hash, for_update=False):
    lock_sql = " FOR UPDATE" if for_update else ""
    cur.execute(f"""
        SELECT *
        FROM gift_access_grants
        WHERE token_hash = %s
        {lock_sql}
    """, (token_hash,))
    return gift_row_dict(cur, cur.fetchone())


def fetch_gift_by_public_reference_version(cur, public_reference, token_version, for_update=False):
    lock_sql = " FOR UPDATE" if for_update else ""
    cur.execute(f"""
        SELECT *
        FROM gift_access_grants
        WHERE public_reference = %s
          AND token_version = %s
        {lock_sql}
    """, (public_reference, int(token_version)))
    return gift_row_dict(cur, cur.fetchone())


def gift_configuration_status(cur=None):
    missing_prices = [name for name in gift_required_price_envs() if not os.getenv(name)]
    missing_secrets = [] if gift_token_secret_configured() else [GIFT_TOKEN_SECRET_ENV]
    assets = certificate_assets_status()
    return {
        "configured": not missing_prices and not missing_secrets,
        "missing_prices": missing_prices,
        "missing_secrets": missing_secrets,
        **assets,
    }


def gift_tariffs_keyboard():
    return inline_keyboard([
        [InlineKeyboardButton(text="🎁 1 месяц", callback_data="gift_tariff:gift_1m")],
        [InlineKeyboardButton(text="🎁 6 месяцев", callback_data="gift_tariff:gift_6m")],
        [InlineKeyboardButton(text="🎁 12 месяцев", callback_data="gift_tariff:gift_12m")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="gift_cancel_flow")],
    ])


def gift_certificate_name_keyboard():
    return inline_keyboard([
        [InlineKeyboardButton(text="Без имени", callback_data="gift_certificate_without_name")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="gift_cancel_flow")],
    ])


def gift_preview_keyboard():
    return inline_keyboard([
        [InlineKeyboardButton(text="💳 Оплатить подарок", callback_data="gift_pay")],
        [InlineKeyboardButton(text="✏️ Изменить", callback_data="gift_edit")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="gift_cancel_flow")],
    ])


def gift_activation_keyboard(public_reference):
    return inline_keyboard([[
        InlineKeyboardButton(text="🎁 Активировать подарок", callback_data=f"gift_activate:{public_reference}")
    ]])


def gift_checkout_keyboard(checkout_url):
    return inline_keyboard([[
        InlineKeyboardButton(text="💳 Оплатить подарок", url=checkout_url)
    ]])


def gift_active_checkout_conflict_keyboard(row):
    buttons = []
    checkout_url = row.get("checkout_url")
    if checkout_url:
        buttons.append([InlineKeyboardButton(text="💳 Вернуться к оплате", url=checkout_url)])
    return inline_keyboard(buttons) if buttons else None


def build_gift_preview_text(data):
    certificate_name = data.get("certificate_name") or "без имени"
    recipient_name = data.get("recipient_name") or "не указано"
    sender_name = data.get("sender_name") or "не указано"
    gift_message = data.get("gift_message") or "без личного сообщения"
    tariff_code = data.get("tariff_code")
    return (
        "🎁 Проверьте подарочный сертификат\n\n"
        f"Срок доступа: {gift_tariff_label(tariff_code)}\n"
        f"Подпись на сертификате: {gift_safe_user_text(certificate_name)}\n"
        f"Получатель: {gift_safe_user_text(recipient_name)}\n"
        f"От кого: {gift_safe_user_text(sender_name)}\n"
        f"Сообщение: {gift_safe_user_text(gift_message)}\n\n"
        "После оплаты я пришлю вам сертификат с одноразовой ссылкой."
    )


def create_gift_checkout_draft(
    cur, purchaser_telegram_id, tariff_code, recipient_name, sender_name, gift_message, certificate_name=None
):
    gift_id = str(uuid.uuid4())
    public_reference = gift_public_reference()
    duration_days = gift_duration_days(tariff_code)
    if not duration_days:
        raise ValueError("invalid_gift_tariff")
    token_hash = gift_token_hash_for_reference(public_reference, 1)
    cur.execute("""
        INSERT INTO gift_access_grants (
            id, public_reference, purchaser_telegram_id, certificate_name, recipient_name, sender_name,
            gift_message, tariff_code, duration_days, status, token_hash, token_version
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'checkout_pending', %s, 1)
        RETURNING *
    """, (
        gift_id,
        public_reference,
        int(purchaser_telegram_id),
        certificate_name,
        recipient_name,
        sender_name,
        gift_message,
        tariff_code,
        duration_days,
        token_hash,
    ))
    row = gift_row_dict(cur, cur.fetchone())
    record_gift_event(cur, row, "checkout_draft_created", purchaser_telegram_id, source="gift_fsm")
    return row


def find_or_create_gift_checkout_draft(
    cur, purchaser_telegram_id, tariff_code, recipient_name, sender_name, gift_message, certificate_name=None
):
    cur.execute("""
        UPDATE gift_access_grants
        SET status = 'cancelled',
            cancelled_at = NOW(),
            last_error = 'checkout_expired_local',
            last_error_category = 'checkout_expired_local',
            updated_at = NOW()
        WHERE purchaser_telegram_id = %s
          AND tariff_code = %s
          AND status = 'checkout_open'
          AND checkout_expires_at IS NOT NULL
          AND checkout_expires_at <= NOW()
    """, (int(purchaser_telegram_id), tariff_code))
    cur.execute("""
        SELECT *
        FROM gift_access_grants
        WHERE purchaser_telegram_id = %s
          AND tariff_code = %s
          AND status IN ('checkout_pending', 'checkout_open', 'payment_pending')
        ORDER BY created_at
        LIMIT 1
        FOR UPDATE
    """, (int(purchaser_telegram_id), tariff_code))
    existing = gift_row_dict(cur, cur.fetchone())
    if existing:
        same_details = (
            (existing.get("certificate_name") or "") == (certificate_name or "")
            and (existing.get("recipient_name") or "") == (recipient_name or "")
            and (existing.get("sender_name") or "") == (sender_name or "")
            and (existing.get("gift_message") or "") == (gift_message or "")
        )
        has_stripe_checkout = bool(existing.get("stripe_session_id") or existing.get("checkout_url"))
        if same_details:
            return existing, "checkout_reused" if has_stripe_checkout else "draft_reused"
        if existing["status"] == "checkout_pending" and not has_stripe_checkout:
            cur.execute("""
                UPDATE gift_access_grants
                SET status = 'cancelled',
                    cancelled_at = NOW(),
                    last_error = 'checkout_draft_replaced',
                    last_error_category = 'checkout_draft_replaced',
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'checkout_pending'
                  AND stripe_session_id IS NULL
            """, (existing["id"],))
            record_gift_event(cur, existing, "checkout_draft_replaced", purchaser_telegram_id, source="gift_fsm")
            return create_gift_checkout_draft(
                cur, purchaser_telegram_id, tariff_code, recipient_name, sender_name, gift_message, certificate_name
            ), False
        if existing["status"] == "payment_pending":
            return existing, "payment_pending_conflict"
        return existing, "active_checkout_conflict"
    return create_gift_checkout_draft(
        cur, purchaser_telegram_id, tariff_code, recipient_name, sender_name, gift_message, certificate_name
    ), False


def mark_gift_checkout_open(cur, gift_id, expected_status, session_id, checkout_url, checkout_expires_at=None):
    cur.execute("""
        UPDATE gift_access_grants
        SET stripe_session_id = %s,
            checkout_url = %s,
            checkout_expires_at = CASE WHEN %s IS NULL THEN checkout_expires_at ELSE to_timestamp(%s) AT TIME ZONE 'UTC' END,
            status = 'checkout_open',
            updated_at = NOW()
        WHERE id = %s
          AND status = %s
        RETURNING *
    """, (session_id, checkout_url, checkout_expires_at, checkout_expires_at, gift_id, expected_status))
    return gift_row_dict(cur, cur.fetchone())


def mark_gift_checkout_failed(cur, gift_id, error_text):
    category = classify_payment_problem(exception=error_text)["category"]
    cur.execute("""
        UPDATE gift_access_grants
        SET status = 'checkout_pending',
            last_error = %s,
            last_error_category = %s,
            updated_at = NOW()
        WHERE id = %s
          AND status IN ('checkout_pending', 'checkout_open', 'payment_pending')
    """, (category, category, gift_id))


def cancel_local_gift_checkout(cur, gift_row, actor_id=None, source="gift_cancel"):
    cur.execute("""
        UPDATE gift_access_grants
        SET status = 'cancelled',
            cancelled_at = NOW(),
            last_error = %s,
            last_error_category = %s,
            updated_at = NOW()
        WHERE id = %s
          AND status = 'checkout_pending'
          AND stripe_session_id IS NULL
        RETURNING *
    """, (source, source, gift_row["id"]))
    updated = gift_row_dict(cur, cur.fetchone())
    if not updated:
        return None
    record_gift_event(cur, updated, "gift_cancelled", actor_id, source=source)
    return updated


async def expire_stripe_checkout_session(session_id):
    return await asyncio.to_thread(stripe.checkout.Session.expire, session_id)


def stripe_checkout_session_is_expired(session):
    return stripe_value(session, "status") == "expired"


async def safely_cancel_gift_checkout(public_reference, actor_id=None, source="gift_cancel"):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        gift_row = fetch_gift_by_public_reference(cur, public_reference)
    finally:
        cur.close()
        conn.close()
    if not gift_row:
        return {"status": "failed", "reason": "gift_not_found", "admin_message": "Подарок не найден."}

    current_status = gift_row["status"]
    if current_status in ("paid_unclaimed", "reserved", "redeemed"):
        return {
            "status": "failed",
            "reason": "gift_paid_requires_refund",
            "public_reference": public_reference,
            "gift_status": current_status,
            "admin_message": (
                "❌ Оплаченный подарок нельзя отменить локально.\n\n"
                "Оформите возврат в Stripe; после этого состояние изменит refund webhook."
            ),
        }
    if current_status == "payment_pending":
        return {
            "status": "failed",
            "reason": "gift_payment_pending",
            "public_reference": public_reference,
            "gift_status": current_status,
            "admin_message": "❌ Оплата подарка уже обрабатывается. Дождитесь результата Stripe или проверьте платёж вручную.",
        }
    if current_status == "checkout_pending" and not gift_row.get("stripe_session_id"):
        local_conn = get_db_conn()
        local_cur = local_conn.cursor()
        try:
            locked = fetch_gift_by_public_reference(local_cur, public_reference, for_update=True)
            updated = cancel_local_gift_checkout(local_cur, locked, actor_id, source=source) if locked else None
            if not updated:
                local_conn.rollback()
                return {"status": "failed", "reason": "gift_cancel_state_changed", "public_reference": public_reference}
            enqueue_gift_admin_delivery(local_cur, public_reference, "gift_admin_problem", gift_admin_text("🎁 Gift checkout cancelled", updated))
            local_conn.commit()
            return {"status": "completed", "public_reference": public_reference, "gift_status": "cancelled"}
        finally:
            local_cur.close()
            local_conn.close()
    if current_status != "checkout_open" or not gift_row.get("stripe_session_id"):
        return {
            "status": "failed",
            "reason": "gift_not_cancellable",
            "public_reference": public_reference,
            "gift_status": current_status,
            "admin_message": "❌ Этот подарок нельзя отменить в текущем статусе.",
        }

    session_id = gift_row["stripe_session_id"]
    expired_session = await expire_stripe_checkout_session(session_id)
    if not stripe_checkout_session_is_expired(expired_session):
        return {
            "status": "failed",
            "reason": "stripe_checkout_not_expired",
            "public_reference": public_reference,
            "gift_status": current_status,
            "admin_message": "❌ Stripe Checkout Session не перешла в expired. Подарок не изменён.",
        }

    update_conn = get_db_conn()
    update_cur = update_conn.cursor()
    try:
        update_cur.execute("""
            SELECT *
            FROM gift_access_grants
            WHERE public_reference = %s
            FOR UPDATE
        """, (public_reference,))
        locked = gift_row_dict(update_cur, update_cur.fetchone())
        if (
            not locked
            or locked["status"] != "checkout_open"
            or locked.get("stripe_session_id") != session_id
        ):
            update_conn.rollback()
            return {"status": "failed", "reason": "gift_cancel_state_changed", "public_reference": public_reference}
        update_cur.execute("""
            UPDATE gift_access_grants
            SET status = 'cancelled',
                cancelled_at = NOW(),
                last_error = %s,
                last_error_category = %s,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'checkout_open'
              AND stripe_session_id = %s
            RETURNING *
        """, (source, source, locked["id"], session_id))
        updated = gift_row_dict(update_cur, update_cur.fetchone())
        if not updated:
            update_conn.rollback()
            return {"status": "failed", "reason": "gift_cancel_state_changed", "public_reference": public_reference}
        record_gift_event(update_cur, updated, "gift_cancelled", actor_id, source=source)
        enqueue_gift_admin_delivery(update_cur, public_reference, "gift_admin_problem", gift_admin_text("🎁 Gift checkout cancelled", updated))
        update_conn.commit()
        return {"status": "completed", "public_reference": public_reference, "gift_status": "cancelled"}
    finally:
        update_cur.close()
        update_conn.close()


def gift_payment_metadata_valid(metadata, gift_row, session):
    return (
        stripe_value(metadata, "payment_kind") == GIFT_PAYMENT_KIND
        and stripe_value(metadata, "gift_id") == str(gift_row["id"])
        and stripe_value(metadata, "purchaser_telegram_id") == str(gift_row["purchaser_telegram_id"])
        and stripe_value(metadata, "tariff_code") == gift_row["tariff_code"]
        and stripe_value(metadata, "duration_days") == str(gift_row["duration_days"])
        and (stripe_value(session, "mode") == "payment")
        and (stripe_value(session, "payment_status") == "paid")
    )


def validate_gift_payment_proof(session, line_item, price, gift_row):
    metadata = stripe_value(session, "metadata") or {}
    if not gift_payment_metadata_valid(metadata, gift_row, session):
        return False
    if stripe_value(session, "id") != gift_row.get("stripe_session_id"):
        return False
    if str(stripe_value(session, "client_reference_id")) != str(gift_row["purchaser_telegram_id"]):
        return False
    if int(stripe_value(line_item, "quantity") or 0) != 1:
        return False
    expected_price_id = gift_price_id(gift_row["tariff_code"])
    if stripe_value(price, "id") != expected_price_id:
        return False
    if stripe_value(price, "type") != "one_time":
        return False
    amount_total = stripe_value(session, "amount_total")
    currency = stripe_value(session, "currency")
    if not isinstance(amount_total, int) or amount_total <= 0:
        return False
    if stripe_value(price, "unit_amount") != amount_total:
        return False
    if stripe_value(price, "currency") != currency:
        return False
    return True


def _stripe_collection_first(value):
    data = stripe_value(value, "data")
    if data and len(data) == 1:
        return data[0]
    if data and len(data) != 1:
        raise ValueError("gift_checkout_line_item_count_mismatch")
    return None


def fetch_gift_checkout_payment_proof(session_id):
    session = stripe.checkout.Session.retrieve(session_id, expand=["line_items"])
    line_item = _stripe_collection_first(stripe_value(session, "line_items"))
    if not line_item:
        listed = stripe.checkout.Session.list_line_items(session_id, limit=2)
        data = stripe_value(listed, "data") or []
        if len(data) != 1:
            raise ValueError("gift_checkout_line_item_count_mismatch")
        line_item = data[0]
    price = stripe_value(line_item, "price")
    price_id = get_stripe_object_id(price)
    if isinstance(price, str) or not stripe_value(price, "type"):
        price = stripe.Price.retrieve(price_id)
    return session, line_item, price


def cancel_gift_certificate_deliveries_for_version(cur, public_reference, token_version, reason):
    for delivery_type, kind in ((GIFT_CERTIFICATE_BUYER, "buyer"), (GIFT_CERTIFICATE_RECIPIENT, "recipient")):
        cancel_message_delivery(
            cur,
            gift_delivery_key(public_reference, delivery_type, token_version=token_version, recipient_kind=kind),
            reason,
        )


def gift_refund_amount_from_event(event_type, event_object):
    if event_type == "charge.refunded":
        payment_intent = get_stripe_object_id(stripe_value(event_object, "payment_intent"))
        amount_refunded = stripe_value(event_object, "amount_refunded") or 0
        charge_amount = stripe_value(event_object, "amount") or 0
        is_full = bool(stripe_value(event_object, "refunded")) and amount_refunded >= charge_amount > 0
        return payment_intent, amount_refunded, is_full, "succeeded"
    payment_intent = get_stripe_object_id(stripe_value(event_object, "payment_intent"))
    amount = stripe_value(event_object, "amount") or 0
    status = stripe_value(event_object, "status")
    return payment_intent, amount, status == "succeeded", status or "unknown"


def apply_gift_refund_event(cur, event_id, event_type, event_object, gift_row):
    payment_intent, refund_amount, event_full_refund, refund_status = gift_refund_amount_from_event(event_type, event_object)
    if not payment_intent or payment_intent != gift_row.get("stripe_payment_intent_id"):
        raise ValueError("gift_refund_payment_intent_mismatch")
    if event_type in ("refund.created", "refund.updated") and refund_status != "succeeded":
        event_name = "gift_refund_pending" if refund_status == "pending" else f"gift_refund_{refund_status}_ignored"
        record_gift_event(cur, gift_row, event_name, gift_row.get("purchaser_telegram_id"), source="stripe_webhook", notes=f"event={safe_log_id(event_id)}")
        return gift_row
    gift_amount = int(gift_row.get("amount_total") or 0)
    full_refund = bool(event_full_refund and gift_amount and int(refund_amount or 0) >= gift_amount)
    partial_refund = bool(refund_amount and (not full_refund))
    if gift_row["status"] in ("refunded", "review_required"):
        record_gift_event(cur, gift_row, f"{event_type}_duplicate_ignored", gift_row.get("purchaser_telegram_id"), source="stripe_webhook")
        return gift_row
    if full_refund and gift_row["status"] in ("paid_unclaimed", "reserved"):
        new_version = int(gift_row["token_version"]) + 1
        new_hash = gift_token_hash_for_reference(gift_row["public_reference"], new_version)
        cancel_gift_certificate_deliveries_for_version(cur, gift_row["public_reference"], gift_row["token_version"], "gift_refunded")
        cur.execute("""
            UPDATE gift_access_grants
            SET status = 'refunded',
                refunded_at = COALESCE(refunded_at, NOW()),
                token_version = %s,
                token_hash = %s,
                updated_at = NOW()
            WHERE id = %s
              AND status IN ('paid_unclaimed', 'reserved')
            RETURNING *
        """, (new_version, new_hash, gift_row["id"]))
        updated = gift_row_dict(cur, cur.fetchone())
        if not updated:
            return gift_row
        record_gift_event(cur, updated, "gift_refunded", updated["purchaser_telegram_id"], source="stripe_webhook", notes=f"event={safe_log_id(event_id)}")
        enqueue_gift_text_delivery(
            cur,
            updated["public_reference"],
            updated["purchaser_telegram_id"],
            "gift_refunded_buyer",
            "🎁 Возврат по подарку подтверждён. Сертификат больше не активен.",
        )
        if updated.get("recipient_telegram_id"):
            enqueue_gift_text_delivery(
                cur,
                updated["public_reference"],
                updated["recipient_telegram_id"],
                "gift_refunded_recipient",
                "🎁 По подарку оформлен возврат, поэтому доступ по нему не будет активирован.",
            )
        enqueue_gift_admin_delivery(
            cur,
            updated["public_reference"],
            "gift_admin_refund",
            gift_admin_text("🎁 Gift refunded before redemption", updated, extra=f"event: {event_type}"),
            severity="WARNING",
        )
        return updated
    cur.execute("""
        UPDATE gift_access_grants
        SET status = 'review_required',
            updated_at = NOW(),
            last_error = %s,
            last_error_category = %s
        WHERE id = %s
          AND status IN ('paid_unclaimed', 'reserved', 'redeemed')
        RETURNING *
    """, ("gift_refund_requires_review", "gift_refund_requires_review", gift_row["id"]))
    updated = gift_row_dict(cur, cur.fetchone()) or gift_row
    event_name = "gift_refund_after_redeemed_review_required" if gift_row["status"] == "redeemed" else "gift_partial_refund_review_required"
    if partial_refund or full_refund:
        record_gift_event(cur, updated, event_name, updated.get("purchaser_telegram_id"), source="stripe_webhook", notes=f"event={safe_log_id(event_id)}")
        enqueue_gift_admin_delivery(
            cur,
            updated["public_reference"],
            "gift_admin_refund",
            gift_admin_text("🚨 Gift refund requires manual review", updated, extra=f"event: {event_type}; auto revoke: no"),
            severity="CRITICAL",
        )
    return updated


def stripe_refund_object_id(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return stripe_value(value, "id")


def normalize_currency(value):
    return str(value or "").strip().lower() or None


def subscription_refund_proof_from_event(event_type, event_object):
    charge = event_object if event_type == "charge.refunded" else None
    refund = event_object if event_type in ("refund.created", "refund.updated") else None
    refund_id = stripe_value(refund, "id") if refund is not None else None
    refunds_data = stripe_value(charge, "refunds", "data") or []
    charge_id = stripe_refund_object_id(stripe_value(refund, "charge")) if refund is not None else stripe_value(charge, "id")
    payment_intent_id = (
        stripe_refund_object_id(stripe_value(refund, "payment_intent"))
        if refund is not None
        else stripe_refund_object_id(stripe_value(charge, "payment_intent"))
    )
    amount_refunded = (
        stripe_value(refund, "amount")
        if refund is not None
        else stripe_value(charge, "amount_refunded")
    )
    refund_status = (
        stripe_value(refund, "status")
        if refund is not None
        else ("succeeded" if stripe_value(charge, "refunded") else "unknown")
    )
    reconciliation_key = f"refund:{refund_id}" if refund_id else f"charge:{charge_id}" if charge_id else None
    pending_refunds = 0
    failed_refunds = 0
    succeeded_refund_total = 0
    if charge is not None and refunds_data:
        for refund_row in refunds_data:
            row_status = stripe_value(refund_row, "status") or "unknown"
            row_amount = int(stripe_value(refund_row, "amount") or 0)
            if row_status == "succeeded":
                succeeded_refund_total += row_amount
            elif row_status in ("pending", "requires_action"):
                pending_refunds += 1
            elif row_status == "failed":
                failed_refunds += 1
        if succeeded_refund_total:
            amount_refunded = succeeded_refund_total
        elif stripe_value(charge, "amount_refunded") is not None:
            amount_refunded = stripe_value(charge, "amount_refunded")
    return {
        "reconciliation_key": reconciliation_key,
        "proof_model": "refund" if refund_id else "charge",
        "refund_id": refund_id,
        "charge_id": charge_id,
        "payment_intent_id": payment_intent_id,
        "invoice_id": stripe_refund_object_id(stripe_value(charge, "invoice")) if charge is not None else None,
        "customer_id": stripe_refund_object_id(stripe_value(charge, "customer")) if charge is not None else None,
        "subscription_id": None,
        "amount_refunded": int(amount_refunded or 0),
        "refund_status": refund_status or "unknown",
        "currency": stripe_value(refund, "currency") if refund is not None else stripe_value(charge, "currency"),
        "pending_refunds": pending_refunds,
        "failed_refunds": failed_refunds,
    }


async def enrich_subscription_refund_proof(proof):
    retrieved_charge = None
    if proof.get("charge_id") and (not proof.get("invoice_id") or not proof.get("customer_id")):
        charge = await asyncio.to_thread(stripe.Charge.retrieve, proof["charge_id"], expand=["invoice"])
        retrieved_charge = charge
        proof["invoice_id"] = proof.get("invoice_id") or stripe_refund_object_id(stripe_value(charge, "invoice"))
        proof["customer_id"] = proof.get("customer_id") or stripe_refund_object_id(stripe_value(charge, "customer"))
        proof["payment_intent_id"] = proof.get("payment_intent_id") or stripe_refund_object_id(stripe_value(charge, "payment_intent"))
        proof["currency"] = proof.get("currency") or stripe_value(charge, "currency")
        if not proof.get("amount_refunded"):
            proof["amount_refunded"] = int(stripe_value(charge, "amount_refunded") or 0)
    expanded_invoice = stripe_value(retrieved_charge, "invoice") if retrieved_charge is not None else None
    if proof.get("invoice_id") and not proof.get("subscription_id") and expanded_invoice is not None and not isinstance(expanded_invoice, str):
        invoice = expanded_invoice
        proof["subscription_id"] = stripe_refund_object_id(stripe_value(invoice, "subscription")) or stripe_refund_object_id(
            stripe_value(invoice, "parent", "subscription_details", "subscription")
        )
        proof["customer_id"] = proof.get("customer_id") or stripe_refund_object_id(stripe_value(invoice, "customer"))
    elif proof.get("invoice_id") and not proof.get("subscription_id") and retrieved_charge is None:
        invoice = await asyncio.to_thread(stripe.Invoice.retrieve, proof["invoice_id"])
        proof["subscription_id"] = stripe_refund_object_id(stripe_value(invoice, "subscription")) or stripe_refund_object_id(
            stripe_value(invoice, "parent", "subscription_details", "subscription")
        )
        proof["customer_id"] = proof.get("customer_id") or stripe_refund_object_id(stripe_value(invoice, "customer"))
    return proof


def subscription_refund_review_text(proof, reason, telegram_id=None):
    return (
        "🚨 Refund requires manual review\n\n"
        f"reason: {reason}\n"
        f"telegram_id: {telegram_id if telegram_id is not None else 'unknown'}\n"
        f"refund_id: {safe_log_id(proof.get('refund_id')) or 'none'}\n"
        f"charge_id: {safe_log_id(proof.get('charge_id')) or 'none'}\n"
        f"payment_intent_id: {safe_log_id(proof.get('payment_intent_id')) or 'none'}\n"
        f"invoice_id: {safe_log_id(proof.get('invoice_id')) or 'none'}\n"
        f"subscription_id: {safe_log_id(proof.get('subscription_id')) or 'none'}\n"
        "access_changed: false"
    )


def insert_subscription_refund_reconciliation(
    cur,
    event_id,
    proof,
    result,
    event_type=None,
    review_reason=None,
    telegram_id=None,
    original_payment_event_id=None,
    original_amount=None,
    access_revoked_at=None,
):
    reconciliation_key = proof.get("reconciliation_key")
    if not reconciliation_key:
        reconciliation_key = (
            f"refund:{proof.get('refund_id')}"
            if proof.get("refund_id")
            else f"charge:{proof.get('charge_id')}"
            if proof.get("charge_id")
            else f"event:{event_id}"
        )
        proof["reconciliation_key"] = reconciliation_key
    cur.execute(
        """
        INSERT INTO subscription_refund_reconciliations (
            reconciliation_key, refund_id, stripe_event_id, charge_id, payment_intent_id, invoice_id,
            customer_id, subscription_id, telegram_id, original_payment_event_id,
            amount_refunded, original_amount, currency, refund_status, is_full_refund,
            reconciliation_result, review_reason, access_revoked_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (reconciliation_key) DO UPDATE SET
            stripe_event_id = EXCLUDED.stripe_event_id,
            charge_id = COALESCE(EXCLUDED.charge_id, subscription_refund_reconciliations.charge_id),
            payment_intent_id = COALESCE(EXCLUDED.payment_intent_id, subscription_refund_reconciliations.payment_intent_id),
            invoice_id = COALESCE(EXCLUDED.invoice_id, subscription_refund_reconciliations.invoice_id),
            customer_id = COALESCE(EXCLUDED.customer_id, subscription_refund_reconciliations.customer_id),
            subscription_id = COALESCE(EXCLUDED.subscription_id, subscription_refund_reconciliations.subscription_id),
            telegram_id = COALESCE(EXCLUDED.telegram_id, subscription_refund_reconciliations.telegram_id),
            original_payment_event_id = COALESCE(EXCLUDED.original_payment_event_id, subscription_refund_reconciliations.original_payment_event_id),
            amount_refunded = EXCLUDED.amount_refunded,
            original_amount = COALESCE(EXCLUDED.original_amount, subscription_refund_reconciliations.original_amount),
            currency = COALESCE(EXCLUDED.currency, subscription_refund_reconciliations.currency),
            refund_status = EXCLUDED.refund_status,
            is_full_refund = EXCLUDED.is_full_refund,
            reconciliation_result = EXCLUDED.reconciliation_result,
            review_reason = EXCLUDED.review_reason,
            access_revoked_at = COALESCE(EXCLUDED.access_revoked_at, subscription_refund_reconciliations.access_revoked_at),
            updated_at = NOW()
        RETURNING id, reconciliation_result
        """,
        (
            reconciliation_key,
            proof.get("refund_id"),
            event_id,
            proof.get("charge_id"),
            proof.get("payment_intent_id"),
            proof.get("invoice_id"),
            proof.get("customer_id"),
            proof.get("subscription_id"),
            int(telegram_id) if telegram_id is not None else None,
            original_payment_event_id,
            proof.get("amount_refunded"),
            original_amount,
            proof.get("currency"),
            proof.get("refund_status"),
            bool(original_amount and proof.get("amount_refunded") == original_amount),
            result,
            review_reason,
            access_revoked_at,
        ),
    )
    row = cur.fetchone()
    reconciliation_id = row[0] if row and len(row) > 1 else None
    reconciliation_result = row[1] if row and len(row) > 1 else result
    if reconciliation_id is not None:
        record_subscription_refund_event(cur, reconciliation_id, event_id, event_type)
    return reconciliation_result


def record_subscription_refund_event(cur, reconciliation_id, event_id, event_type=None):
    cur.execute(
        """
        INSERT INTO subscription_refund_events (reconciliation_id, stripe_event_id, event_type)
        VALUES (%s, %s, %s)
        ON CONFLICT (stripe_event_id) DO NOTHING
        """,
        (reconciliation_id, event_id, event_type),
    )


def find_access_revoked_refund_reconciliation(cur, original_payment_event_id):
    cur.execute(
        """
        SELECT id, reconciliation_key, reconciliation_result
        FROM subscription_refund_reconciliations
        WHERE original_payment_event_id = %s
          AND reconciliation_result = %s
        FOR UPDATE
        """,
        (original_payment_event_id, SUBSCRIPTION_REFUND_ACCESS_REVOKED),
    )
    return cur.fetchone()


def alias_subscription_refund_reconciliation(cur, reconciliation_id, event_id, proof):
    cur.execute(
        """
        UPDATE subscription_refund_reconciliations
        SET stripe_event_id = %s,
            charge_id = COALESCE(charge_id, %s),
            payment_intent_id = COALESCE(payment_intent_id, %s),
            invoice_id = COALESCE(invoice_id, %s),
            customer_id = COALESCE(customer_id, %s),
            subscription_id = COALESCE(subscription_id, %s),
            updated_at = NOW()
        WHERE id = %s
        """,
        (
            event_id,
            proof.get("charge_id"),
            proof.get("payment_intent_id"),
            proof.get("invoice_id"),
            proof.get("customer_id"),
            proof.get("subscription_id"),
            reconciliation_id,
        ),
    )


def enqueue_subscription_refund_review(cur, event_id, proof, reason, telegram_id=None, event_type=None):
    insert_subscription_refund_reconciliation(
        cur,
        event_id,
        proof,
        SUBSCRIPTION_REFUND_REVIEW_REQUIRED,
        event_type=event_type,
        review_reason=reason,
        telegram_id=telegram_id,
    )
    return enqueue_stripe_admin_message(
        cur,
        event_id,
        "subscription_refund_review_required",
        subscription_refund_review_text(proof, reason, telegram_id),
        severity="CRITICAL",
        category="subscription_refund_review_required",
        safe_ref=safe_admin_context_reference("subscription_refund_review", event_id, proof.get("refund_id"), proof.get("charge_id")),
    )


def find_refunded_subscription_payment_event(cur, proof):
    clauses = []
    params = []
    if proof.get("invoice_id"):
        clauses.append("invoice_id = %s")
        params.append(proof["invoice_id"])
    elif proof.get("customer_id") and proof.get("subscription_id"):
        clauses.append("(stripe_customer_id = %s AND stripe_subscription_id = %s)")
        params.extend([proof["customer_id"], proof["subscription_id"]])
    if not clauses:
        return "review_required", "missing_payment_match_ids", None
    cur.execute(
        f"""
        SELECT id, telegram_id, amount_paid, currency, stripe_customer_id,
               stripe_subscription_id, payment_kind, payment_status, period_end, created_at
        FROM payment_events
        WHERE payment_status = 'succeeded'
          AND payment_kind IN ('initial_subscription', 'recurring')
          AND ({' OR '.join(clauses)})
        ORDER BY created_at DESC, id DESC
        FOR UPDATE
        """,
        tuple(params),
    )
    rows = cur.fetchall()
    if len(rows) != 1:
        return "review_required", "ambiguous_payment_match" if rows else "missing_payment_event", None
    return "matched", None, rows[0]


def cancel_access_restoration_deliveries_after_revoke(cur, telegram_id, reason):
    cur.execute(
        """
        UPDATE message_delivery_events
        SET status = 'cancelled',
            last_error = LEFT(%s, 500),
            lease_until = NULL,
            next_attempt_at = NULL
        WHERE telegram_id = %s
          AND delivery_type = ANY(%s)
          AND status IN ('pending', 'failed', 'processing')
    """,
        (reason, int(telegram_id), list(SUBSCRIPTION_REFUND_DANGEROUS_DELIVERY_TYPES)),
    )
    return getattr(cur, "rowcount", 0)


def enqueue_subscription_refund_group_removal(cur, telegram_id, reason="subscription_refund_reconciled", revoke_started_at=None):
    revoke_started_at = revoke_started_at or datetime.utcnow()
    cur.execute(
        """
        INSERT INTO subscription_removal_events (
            telegram_id, status, reason, owner_id, attempt_count, revoke_started_at, created_at, updated_at
        )
        VALUES (%s, 'pending', %s, NULL, 0, %s, NOW(), NOW())
        ON CONFLICT (telegram_id) DO UPDATE SET
            status = 'pending',
            reason = EXCLUDED.reason,
            owner_id = NULL,
            claimed_at = NULL,
            lease_until = NULL,
            telegram_removed_at = NULL,
            db_finalized_at = NULL,
            attempt_count = 0,
            last_error = NULL,
            revoke_started_at = EXCLUDED.revoke_started_at,
            updated_at = NOW()
        """,
        (int(telegram_id), reason, revoke_started_at),
    )


def apply_manual_access_revoke(cur, telegram_id, reason, admin_id=None, action_id=None):
    cur.execute(
        """
        SELECT paid, expiry_date, stripe_subscription_id, stripe_customer_id, auto_renew
        FROM users
        WHERE telegram_id = %s
        FOR UPDATE
        """,
        (int(telegram_id),),
    )
    row = cur.fetchone()
    if not row:
        return {"status": "failed", "reason": "user_not_found"}
    paid, old_expiry, subscription_id, customer_id, auto_renew = row
    now = datetime.utcnow()
    new_expiry = min(old_expiry or now, now)
    cur.execute(
        """
        UPDATE users
        SET paid = FALSE,
            expiry_date = LEAST(COALESCE(expiry_date, NOW()), NOW()),
            auto_renew = FALSE,
            payment_failed = FALSE,
            payment_failed_at = NULL,
            grace_period_end = NULL,
            reminder_sent = TRUE
        WHERE telegram_id = %s
        """,
        (int(telegram_id),),
    )
    record_access_event_cur(
        cur,
        telegram_id,
        MANUAL_ACCESS_REVOKED_EVENT,
        source="admin_action_confirmed",
        old_expiry=old_expiry,
        new_expiry=new_expiry,
        stripe_subscription_id=subscription_id,
        notes=f"reason={str(reason)[:160]}; admin_id={admin_id}; action_id={action_id or 'none'}",
    )
    cancelled_deliveries = cancel_access_restoration_deliveries_after_revoke(cur, telegram_id, "manual_access_revoked")
    enqueue_subscription_refund_group_removal(cur, telegram_id, reason="manual_access_revoked", revoke_started_at=now)
    return {
        "status": "completed",
        "telegram_id": int(telegram_id),
        "old_expiry": old_expiry,
        "new_expiry": new_expiry,
        "subscription_id": subscription_id,
        "customer_id": customer_id,
        "cancelled_deliveries": cancelled_deliveries,
        "was_paid": bool(paid),
        "old_auto_renew": bool(auto_renew),
    }


def apply_subscription_refund_reconciliation(cur, event_id, proof, source="stripe_webhook", event_type=None):
    reconciliation_key = proof.get("reconciliation_key") or (
        f"refund:{proof.get('refund_id')}"
        if proof.get("refund_id")
        else f"charge:{proof.get('charge_id')}"
        if proof.get("charge_id")
        else f"event:{event_id}"
    )
    proof["reconciliation_key"] = reconciliation_key
    cur.execute(
        """
        SELECT reconciliation_result
        FROM subscription_refund_reconciliations
        WHERE reconciliation_key = %s
        FOR UPDATE
        """,
        (reconciliation_key,),
    )
    existing = cur.fetchone()
    if existing and existing[0] in (SUBSCRIPTION_REFUND_ACCESS_REVOKED, SUBSCRIPTION_REFUND_ALREADY_RECONCILED):
        insert_subscription_refund_reconciliation(
            cur,
            event_id,
            proof,
            existing[0],
            event_type=event_type,
            review_reason="final_state_duplicate",
        )
        return {"result": SUBSCRIPTION_REFUND_ALREADY_RECONCILED, "reason": existing[0]}
    if proof.get("pending_refunds"):
        enqueue_subscription_refund_review(cur, event_id, proof, "pending_refunds_present", event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": "pending_refunds_present"}
    if proof.get("failed_refunds"):
        enqueue_subscription_refund_review(cur, event_id, proof, "failed_refunds_present", event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": "failed_refunds_present"}
    if proof.get("refund_status") != "succeeded":
        enqueue_subscription_refund_review(cur, event_id, proof, f"refund_status_{proof.get('refund_status')}", event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": "refund_not_succeeded"}

    match_status, review_reason, payment = find_refunded_subscription_payment_event(cur, proof)
    if match_status != "matched":
        enqueue_subscription_refund_review(cur, event_id, proof, review_reason, event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": review_reason}
    (
        payment_event_id,
        telegram_id,
        original_amount,
        original_currency,
        payment_customer_id,
        payment_subscription_id,
        _payment_kind,
        _payment_status,
        payment_period_end,
        payment_created_at,
    ) = payment

    review_reason = None
    if proof.get("amount_refunded") != original_amount:
        review_reason = "amount_mismatch_or_partial_refund"
    elif normalize_currency(proof.get("currency")) != normalize_currency(original_currency):
        review_reason = "currency_mismatch"
    elif proof.get("customer_id") and proof.get("customer_id") != payment_customer_id:
        review_reason = "customer_mismatch"
    elif proof.get("subscription_id") and proof.get("subscription_id") != payment_subscription_id:
        review_reason = "subscription_mismatch"
    elif not payment_period_end:
        review_reason = "missing_payment_period_end"
    if review_reason:
        enqueue_subscription_refund_review(cur, event_id, proof, review_reason, telegram_id=telegram_id, event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": review_reason, "telegram_id": telegram_id}

    already_revoked = find_access_revoked_refund_reconciliation(cur, payment_event_id)
    if already_revoked:
        reconciliation_id, existing_key, existing_result = already_revoked
        alias_subscription_refund_reconciliation(cur, reconciliation_id, event_id, proof)
        record_subscription_refund_event(cur, reconciliation_id, event_id, event_type)
        return {
            "result": SUBSCRIPTION_REFUND_ALREADY_RECONCILED,
            "reason": existing_result,
            "telegram_id": telegram_id,
            "reconciliation_key": existing_key,
        }

    cur.execute(
        """
        SELECT paid, expiry_date, stripe_subscription_id, stripe_customer_id
        FROM users
        WHERE telegram_id = %s
        FOR UPDATE
        """,
        (int(telegram_id),),
    )
    user_row = cur.fetchone()
    if not user_row:
        enqueue_subscription_refund_review(cur, event_id, proof, "user_not_found", telegram_id=telegram_id, event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": "user_not_found", "telegram_id": telegram_id}
    paid, old_expiry, user_subscription_id, user_customer_id = user_row
    if paid is not True:
        insert_subscription_refund_reconciliation(
            cur,
            event_id,
            proof,
            SUBSCRIPTION_REFUND_ALREADY_INACTIVE,
            event_type=event_type,
            review_reason="user_already_inactive",
            telegram_id=telegram_id,
            original_payment_event_id=payment_event_id,
            original_amount=original_amount,
        )
        return {"result": SUBSCRIPTION_REFUND_ALREADY_INACTIVE, "reason": "user_already_inactive", "telegram_id": telegram_id}
    if user_customer_id and payment_customer_id and user_customer_id != payment_customer_id:
        enqueue_subscription_refund_review(cur, event_id, proof, "current_customer_mismatch", telegram_id=telegram_id, event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": "current_customer_mismatch", "telegram_id": telegram_id}
    if user_subscription_id and payment_subscription_id and user_subscription_id != payment_subscription_id:
        enqueue_subscription_refund_review(cur, event_id, proof, "current_subscription_mismatch", telegram_id=telegram_id, event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": "current_subscription_mismatch", "telegram_id": telegram_id}
    if old_expiry and old_expiry > payment_period_end + timedelta(seconds=SUBSCRIPTION_REFUND_EXPIRY_TOLERANCE_SECONDS):
        enqueue_subscription_refund_review(cur, event_id, proof, "current_expiry_exceeds_refunded_period", telegram_id=telegram_id, event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": "current_expiry_exceeds_refunded_period", "telegram_id": telegram_id}

    cur.execute(
        """
        SELECT 1
        FROM payment_events
        WHERE telegram_id = %s
          AND payment_status = 'succeeded'
          AND payment_kind IN ('initial_subscription', 'recurring')
          AND id <> %s
          AND created_at > %s
        LIMIT 1
        """,
        (int(telegram_id), payment_event_id, payment_created_at),
    )
    if cur.fetchone():
        enqueue_subscription_refund_review(cur, event_id, proof, "newer_succeeded_payment_exists", telegram_id=telegram_id, event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": "newer_succeeded_payment_exists", "telegram_id": telegram_id}
    cur.execute(
        """
        SELECT 1
        FROM access_events
        WHERE telegram_id = %s
          AND created_at > %s
          AND event_type <> %s
          AND (
                new_expiry IS NULL
                OR new_expiry > %s + (%s * INTERVAL '1 second')
                OR event_type IN (
                    'stripe_invoice_paid',
                    'manual_set_expiry',
                    'manual_restore_access',
                    'restore_access_invite_sent',
                    'manual_give_access',
                    'manual_link_stripe_user',
                    'gift_access_applied'
                )
          )
        LIMIT 1
        """,
        (
            int(telegram_id),
            payment_created_at,
            SUBSCRIPTION_REFUND_REVOKE_EVENT,
            payment_period_end,
            SUBSCRIPTION_REFUND_EXPIRY_TOLERANCE_SECONDS,
        ),
    )
    if cur.fetchone():
        enqueue_subscription_refund_review(cur, event_id, proof, "newer_access_change_exists", telegram_id=telegram_id, event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": "newer_access_change_exists", "telegram_id": telegram_id}
    cur.execute(
        """
        SELECT 1
        FROM stripe_links
        WHERE telegram_id = %s
          AND (
                (stripe_customer_id IS NOT NULL AND stripe_customer_id IS DISTINCT FROM %s)
                OR (stripe_subscription_id IS NOT NULL AND stripe_subscription_id IS DISTINCT FROM %s)
          )
          AND (is_active IS TRUE OR status IN ('active', 'trialing'))
        LIMIT 1
        """,
        (int(telegram_id), payment_customer_id, payment_subscription_id),
    )
    if cur.fetchone():
        enqueue_subscription_refund_review(cur, event_id, proof, "new_active_subscription_exists", telegram_id=telegram_id, event_type=event_type)
        return {"result": SUBSCRIPTION_REFUND_REVIEW_REQUIRED, "reason": "new_active_subscription_exists", "telegram_id": telegram_id}

    now = datetime.utcnow()
    new_expiry = min(old_expiry or now, now)
    cur.execute(
        """
        UPDATE users
        SET paid = FALSE,
            expiry_date = LEAST(COALESCE(expiry_date, NOW()), NOW()),
            auto_renew = FALSE,
            payment_failed = FALSE,
            payment_failed_at = NULL,
            grace_period_end = NULL,
            reminder_sent = TRUE,
            stripe_subscription_id = CASE
                WHEN stripe_subscription_id = %s THEN NULL
                ELSE stripe_subscription_id
            END
        WHERE telegram_id = %s
        """,
        (payment_subscription_id, int(telegram_id)),
    )
    record_access_event_cur(
        cur,
        telegram_id,
        SUBSCRIPTION_REFUND_REVOKE_EVENT if source == "stripe_webhook" else MANUAL_ACCESS_REVOKED_EVENT,
        source=source,
        old_expiry=old_expiry,
        new_expiry=new_expiry,
        stripe_event_id=event_id,
        stripe_subscription_id=payment_subscription_id,
        notes=f"reconciliation={safe_delivery_hash(reconciliation_key)}; payment_event_id={payment_event_id}; customer_id={safe_log_id(payment_customer_id)}",
    )
    cancelled_deliveries = cancel_access_restoration_deliveries_after_revoke(cur, telegram_id, "subscription_refund_reconciled")
    enqueue_subscription_refund_group_removal(cur, telegram_id, revoke_started_at=now)
    insert_subscription_refund_reconciliation(
        cur,
        event_id,
        proof,
        SUBSCRIPTION_REFUND_ACCESS_REVOKED,
        event_type=event_type,
        telegram_id=telegram_id,
        original_payment_event_id=payment_event_id,
        original_amount=original_amount,
        access_revoked_at=now,
    )
    enqueue_stripe_admin_message(
        cur,
        event_id,
        "subscription_refund_access_revoked",
        "✅ Subscription refund reconciled\n\n"
        f"telegram_id: {telegram_id}\n"
        f"refund_id: {safe_log_id(proof.get('refund_id')) or 'none'}\n"
        f"subscription_id: {safe_log_id(payment_subscription_id)}\n"
        f"old_expiry: {old_expiry or 'none'}\n"
        f"new_expiry: {new_expiry}\n"
        f"cancelled_deliveries: {cancelled_deliveries}\n"
        "group_removal: queued",
        severity="CRITICAL",
        category="subscription_refund_access_revoked",
        safe_ref=safe_admin_context_reference("subscription_refund_revoked", event_id, proof.get("refund_id"), proof.get("charge_id"), telegram_id),
    )
    return {"result": SUBSCRIPTION_REFUND_ACCESS_REVOKED, "telegram_id": telegram_id, "old_expiry": old_expiry, "new_expiry": new_expiry}


def build_gift_buyer_paid_text(row):
    return (
        "🎁 Подарок оплачен\n\n"
        "Всё готово. Подарочный сертификат отправлен предыдущим сообщением.\n\n"
        "Перешлите предыдущее сообщение целиком человеку, которому предназначен подарок.\n\n"
        "Получателю нужно будет нажать «Активировать подарок» — после этого бот поможет ему получить доступ в клуб.\n\n"
        "Вам больше ничего делать не нужно."
    )


def build_gift_redeemed_recipient_text(row, expiry_date):
    return (
        "🎉 Подарок активирован\n\n"
        f"Ваш доступ в клуб продлён до {expiry_date.strftime('%d.%m.%Y')}.\n\n"
        "Вы уже состоите в клубе, поэтому повторно вступать не нужно."
    )


def build_gift_redeemed_invite_text(expiry_date):
    return (
        "🎉 Подарок активирован\n\n"
        f"Вам открыт доступ в закрытый клуб Натальи Ребковец до {expiry_date.strftime('%d.%m.%Y')}.\n\n"
        "Нажмите кнопку ниже, чтобы присоединиться к клубу.\n"
        "Ссылка персональная и действует 24 часа."
    )


def build_gift_redeemed_buyer_text(row, expiry_date):
    return (
        "🎁 Ваш подарок активирован получателем.\n\n"
        f"Срок доступа: до {expiry_date.strftime('%d.%m.%Y')}."
    )


def build_gift_reserved_recipient_text(row):
    return (
        "🎁 Подарок пока нельзя активировать автоматически\n\n"
        "Сейчас у вас активна автопродлеваемая подписка. "
        "Чтобы безопасно применить подарок, пожалуйста, напишите администратору."
    )


async def gift_recipient_subscription_state(recipient_telegram_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT paid, expiry_date, auto_renew, stripe_subscription_id
            FROM users
            WHERE telegram_id = %s
        """, (int(recipient_telegram_id),))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row:
        return {"action": "apply", "subscription_id": None, "status": None, "cancel_at_period_end": None}
    paid, expiry_date, auto_renew, stripe_subscription_id = row
    if not stripe_subscription_id:
        return {"action": "apply", "subscription_id": stripe_subscription_id, "status": None, "cancel_at_period_end": None}
    try:
        subscription = await asyncio.to_thread(stripe.Subscription.retrieve, stripe_subscription_id)
    except Exception as e:
        logging.warning(
            "GIFT_RECIPIENT_SUBSCRIPTION_RETRIEVE_FAILED: telegram_id=%s subscription=%s error_ref=%s",
            safe_log_id(recipient_telegram_id),
            safe_log_id(stripe_subscription_id),
            safe_admin_error_reference("gift_recipient_subscription_retrieve", e),
            exc_info=True,
        )
        return {"action": "fail", "subscription_id": stripe_subscription_id, "reason": "stripe_unavailable"}
    status = stripe_value(subscription, "status") or "unknown"
    cancel_at_period_end = bool(stripe_value(subscription, "cancel_at_period_end"))
    if status in ("active", "trialing") and not cancel_at_period_end:
        action = "block_active_auto_renew"
    elif status in ("active", "trialing") and cancel_at_period_end:
        action = "apply_after_current_expiry"
    elif status in ("canceled", "incomplete_expired", "unpaid"):
        action = "apply"
    else:
        action = "fail"
    return {
        "action": action,
        "subscription_id": stripe_subscription_id,
        "status": status,
        "cancel_at_period_end": cancel_at_period_end,
        "reason": None if action != "fail" else "stripe_status_unknown",
    }


def gift_admin_text(title, row, extra=None):
    status_labels = {
        "paid_unclaimed": "paid, awaiting activation",
        "reserved": "reserved for safe application",
        "redeemed": "redeemed",
        "cancelled": "cancelled",
        "refunded": "refunded",
        "review_required": "manual review required",
    }
    text = (
        f"{title}\n\n"
        f"Gift ref: {safe_log_id(row['public_reference'])}\n"
        f"Purchaser: {safe_log_id(row['purchaser_telegram_id'])}\n"
        f"Recipient: {safe_log_id(row.get('recipient_telegram_id')) if row.get('recipient_telegram_id') else 'not yet claimed'}\n"
        f"Tariff: {gift_tariff_label(row['tariff_code'])}\n"
        f"Status: {status_labels.get(row['status'], 'unknown')}"
    )
    if extra:
        text += f"\n{extra}"
    return text


def mark_gift_paid_and_enqueue(cur, event_id, event_type, session, line_item, price, gift_row):
    metadata = stripe_value(session, "metadata") or {}
    if not validate_gift_payment_proof(session, line_item, price, gift_row):
        raise ValueError("gift_checkout_payment_proof_mismatch")
    amount_total = stripe_value(session, "amount_total")
    currency = stripe_value(session, "currency")
    payment_intent = get_stripe_object_id(stripe_value(session, "payment_intent"))
    token_hash = gift_token_hash_for_reference(gift_row["public_reference"], gift_row["token_version"])
    cur.execute("""
        UPDATE gift_access_grants
        SET status = 'paid_unclaimed',
            paid_at = COALESCE(paid_at, NOW()),
            stripe_payment_intent_id = COALESCE(stripe_payment_intent_id, %s),
            amount_total = COALESCE(amount_total, %s),
            currency = COALESCE(currency, %s),
            token_hash = %s,
            updated_at = NOW(),
            last_error = NULL
        WHERE id = %s
          AND status IN ('checkout_pending', 'checkout_open', 'payment_pending')
        RETURNING *
    """, (payment_intent, amount_total, currency, token_hash, gift_row["id"]))
    updated = gift_row_dict(cur, cur.fetchone())
    if not updated:
        if gift_row.get("status") in ("cancelled", "refunded", "review_required"):
            insert_payment_event(
                cur,
                event_id,
                event_type,
                "succeeded",
                telegram_id=gift_row["purchaser_telegram_id"],
                checkout_session_id=stripe_value(session, "id"),
                stripe_customer_id=get_stripe_object_id(stripe_value(session, "customer")),
                stripe_subscription_id=None,
                payment_kind=GIFT_PAYMENT_KIND,
                tariff_code=gift_row["tariff_code"],
                amount_paid=amount_total,
                amount_due=amount_total,
                currency=currency,
                period_start=datetime.utcnow(),
                period_end=datetime.utcnow() + timedelta(days=gift_row["duration_days"]),
            )
            cur.execute("""
                UPDATE gift_access_grants
                SET status = 'review_required',
                    paid_at = COALESCE(paid_at, NOW()),
                    stripe_payment_intent_id = COALESCE(stripe_payment_intent_id, %s),
                    amount_total = COALESCE(amount_total, %s),
                    currency = COALESCE(currency, %s),
                    last_error = 'gift_payment_after_terminal_status',
                    last_error_category = 'manual_review_required',
                    updated_at = NOW()
                WHERE id = %s
                  AND status IN ('cancelled', 'refunded', 'review_required')
                RETURNING *
            """, (payment_intent, amount_total, currency, gift_row["id"]))
            review_row = gift_row_dict(cur, cur.fetchone()) or gift_row
            record_gift_event(
                cur,
                review_row,
                "gift_payment_after_terminal_status",
                review_row["purchaser_telegram_id"],
                source="stripe_webhook",
                notes=f"event={safe_log_id(event_id)}",
            )
            enqueue_gift_admin_delivery(
                cur,
                review_row["public_reference"],
                "gift_admin_problem",
                gift_admin_text(
                    "🚨 CRITICAL gift payment needs manual review",
                    review_row,
                    extra="Paid webhook arrived after terminal gift status. Payment was recorded but gift activation was not treated as ordinary success.",
                ),
                severity="CRITICAL",
            )
            return review_row
        return gift_row
    insert_payment_event(
        cur,
        event_id,
        event_type,
        "succeeded",
        telegram_id=updated["purchaser_telegram_id"],
        checkout_session_id=stripe_value(session, "id"),
        stripe_customer_id=get_stripe_object_id(stripe_value(session, "customer")),
        stripe_subscription_id=None,
        payment_kind=GIFT_PAYMENT_KIND,
        tariff_code=updated["tariff_code"],
        amount_paid=amount_total,
        amount_due=amount_total,
        currency=currency,
        period_start=datetime.utcnow(),
        period_end=datetime.utcnow() + timedelta(days=updated["duration_days"]),
    )
    record_gift_event(cur, updated, "gift_paid", updated["purchaser_telegram_id"], source="stripe_webhook", notes=f"event={safe_log_id(event_id)}")
    enqueue_gift_text_delivery(
        cur,
        updated["public_reference"],
        updated["purchaser_telegram_id"],
        "gift_paid_buyer",
        build_gift_buyer_paid_text(updated),
        delay_seconds=1,
        gift_reference=updated["public_reference"],
        token_version=int(updated["token_version"]),
    )
    enqueue_gift_certificate_delivery(cur, updated, updated["purchaser_telegram_id"], GIFT_CERTIFICATE_BUYER)
    enqueue_gift_admin_delivery(
        cur,
        updated["public_reference"],
        "gift_admin_success",
        gift_admin_text("🎁 Gift payment succeeded", updated),
        severity="INFO",
    )
    return updated


def apply_gift_access_in_transaction(cur, gift_row, recipient_telegram_id, subscription_state=None):
    now = datetime.utcnow()
    cur.execute("""
        SELECT paid, expiry_date, auto_renew, stripe_subscription_id
        FROM users
        WHERE telegram_id = %s
        FOR UPDATE
    """, (int(recipient_telegram_id),))
    user_row = cur.fetchone()
    paid = bool(user_row[0]) if user_row else False
    old_expiry = user_row[1] if user_row else None
    auto_renew = bool(user_row[2]) if user_row else False
    stripe_subscription_id = user_row[3] if user_row else None

    if subscription_state and subscription_state.get("subscription_id") is not None:
        if subscription_state.get("subscription_id") != stripe_subscription_id:
            raise ValueError("gift_recipient_subscription_identity_changed")
        if subscription_state.get("action") == "fail":
            raise ValueError(subscription_state.get("reason") or "gift_recipient_subscription_check_failed")

    if subscription_state and subscription_state.get("action") == "block_active_auto_renew":
        return gift_row, "blocked_active_auto_renew", old_expiry

    base_expiry = old_expiry if paid and old_expiry and old_expiry > now else now
    new_expiry = base_expiry + timedelta(days=gift_row["duration_days"])
    cur.execute("""
        INSERT INTO users (
            telegram_id, paid, expiry_date, payment_failed, payment_failed_at,
            grace_period_end, reminder_sent, blocked_bot, auto_renew
        )
        VALUES (%s, TRUE, %s, FALSE, NULL, NULL, FALSE, FALSE, FALSE)
        ON CONFLICT (telegram_id) DO UPDATE SET
            paid = TRUE,
            expiry_date = EXCLUDED.expiry_date,
            payment_failed = FALSE,
            payment_failed_at = NULL,
            grace_period_end = NULL,
            reminder_sent = FALSE,
            blocked_bot = FALSE,
            auto_renew = CASE
                WHEN users.stripe_subscription_id IS NULL THEN FALSE
                ELSE users.auto_renew
            END
    """, (int(recipient_telegram_id), new_expiry))
    cur.execute("""
        UPDATE gift_access_grants
        SET status = 'redeemed',
            recipient_telegram_id = COALESCE(recipient_telegram_id, %s),
            redeemed_at = COALESCE(redeemed_at, NOW()),
            applied_at = COALESCE(applied_at, NOW()),
            applied_expiry = %s,
            updated_at = NOW()
        WHERE id = %s
          AND status = 'paid_unclaimed'
        RETURNING *
    """, (int(recipient_telegram_id), new_expiry, gift_row["id"]))
    redeemed = gift_row_dict(cur, cur.fetchone())
    if not redeemed:
        raise ValueError("gift_not_redeemable")
    record_access_event_cur(
        cur,
        recipient_telegram_id,
        "gift_access_redeemed",
        source="gift_access",
        old_expiry=old_expiry,
        new_expiry=new_expiry,
        notes=f"gift={redeemed['public_reference']}",
    )
    record_gift_event(cur, redeemed, "gift_redeemed", recipient_telegram_id, source="recipient_activation")
    enqueue_automatic_membership_repair(
        cur,
        recipient_telegram_id,
        new_expiry,
        "gift_access",
        reason="gift_access_redeemed",
    )
    enqueue_gift_text_delivery(
        cur,
        redeemed["public_reference"],
        redeemed["purchaser_telegram_id"],
        "gift_redeemed_buyer",
        build_gift_redeemed_buyer_text(redeemed, new_expiry),
    )
    enqueue_gift_admin_delivery(
        cur,
        redeemed["public_reference"],
        "gift_admin_redeemed",
        gift_admin_text("🎁 Gift redeemed", redeemed, extra=f"new_expiry: {new_expiry}"),
    )
    return redeemed, "redeemed", new_expiry


def format_user_access_date(expiry_date):
    if not expiry_date:
        return None
    return expiry_date.strftime("%d.%m.%Y")


def build_user_payment_success_message(action, expiry_date):
    expiry_text = format_user_access_date(expiry_date)
    if not expiry_text:
        return (
            "Оплата прошла успешно 🤍\n\n"
            "Доступ к клубу открыт.\n\n"
            "Мы дополнительно проверяем дату окончания подписки."
        )
    if action == "renewal_success":
        return (
            "Подписка успешно продлена 🤍\n\n"
            f"Доступ к клубу сохранён до {expiry_text}.\n\n"
            "Спасибо, что остаётесь с нами."
        )
    if action == "payment_recovered":
        return (
            "Оплата прошла успешно 🤍\n\n"
            f"Подписка снова активна, а доступ к клубу продлён до {expiry_text}.\n\n"
            "Спасибо, что всё получилось. Все материалы уже доступны в меню."
        )
    if action == "payment_success":
        return (
            "Оплата прошла успешно 🤍\n\n"
            f"Доступ к клубу открыт до {expiry_text}.\n\n"
            "Спасибо, что присоединились. Все материалы уже доступны в меню."
        )
    raise ValueError(f"unknown payment success action: {action}")


def payment_success_purpose(payment_kind, was_payment_failed=False):
    if was_payment_failed:
        return "payment_recovered"
    if payment_kind == "recurring":
        return "renewal_success"
    return "payment_success"


PAYMENT_PROBLEM_LABELS = {
    "checkout_creation_failed": "не удалось создать ссылку оплаты",
    "checkout_expired": "ссылка оплаты истекла",
    "checkout_async_payment_failed": "асинхронная оплата не прошла",
    "card_declined": "банк отклонил карту",
    "insufficient_funds": "недостаточно средств",
    "authentication_required": "требуется подтверждение оплаты",
    "invoice_payment_failed": "Stripe сообщил об ошибке оплаты invoice",
    "stale_historical_invoice": "старый invoice безопасно проигнорирован",
    "customer_subscription_conflict": "конфликт Stripe customer/subscription",
    "missing_subscription_identity": "не найдена Stripe subscription identity",
    "invalid_checkout_metadata": "некорректные данные Checkout",
    "missing_subscription_period": "не найден срок периода подписки",
    "stripe_api_unavailable": "Stripe API временно недоступен",
    "webhook_processing_failed": "ошибка обработки Stripe webhook",
    "unknown_payment_error": "неизвестная ошибка оплаты",
}

PAYMENT_STAGE_LABELS = {
    "checkout_creation": "создание Checkout",
    "checkout_completed": "завершение Checkout",
    "checkout_expired": "Checkout истёк",
    "checkout_async_payment_failed": "асинхронная оплата Checkout",
    "invoice_payment_succeeded": "успешный invoice",
    "invoice_payment_failed": "ошибка invoice",
    "webhook": "Stripe webhook",
}


def classify_payment_problem(category=None, stripe_code=None, exception=None, event_type=None):
    if stripe_code == "card_declined":
        category = "card_declined"
    elif stripe_code == "insufficient_funds":
        category = "insufficient_funds"
    elif stripe_code in ("authentication_required", "payment_intent_authentication_failure"):
        category = "authentication_required"
    elif exception is not None:
        exc_name = type(exception).__name__.lower()
        if "api" in exc_name or "connection" in exc_name or "timeout" in exc_name:
            category = "stripe_api_unavailable"
    elif event_type == "checkout.session.expired":
        category = "checkout_expired"
    elif event_type == "checkout.session.async_payment_failed":
        category = "checkout_async_payment_failed"
    elif event_type == "invoice.payment_failed":
        category = "invoice_payment_failed"
    category = category or "unknown_payment_error"
    return {
        "category": category,
        "label": PAYMENT_PROBLEM_LABELS.get(category, PAYMENT_PROBLEM_LABELS["unknown_payment_error"]),
    }


def format_admin_amount(amount, currency):
    if amount is None:
        return "не определена"
    try:
        value = int(amount)
    except (TypeError, ValueError):
        return "не определена"
    currency_text = (currency or "").upper() or "не определена"
    return f"{value / 100:.2f} {currency_text}"


def admin_access_date(expiry_date):
    return expiry_date.strftime("%d.%m.%Y") if expiry_date else "не определён"


def admin_payment_kind_label(purpose):
    if purpose == "payment_recovered":
        return "восстановление после ошибки"
    if purpose == "renewal_success":
        return "продление"
    return "первая оплата"


def tariff_code_from_checkout_days(days):
    return {
        7: "sub_trial",
        30: "sub_1",
        180: "sub_6",
        365: "sub_12",
    }.get(days, "не определён")


def stripe_failure_code_from_invoice(invoice):
    candidates = (
        stripe_value(invoice, 'last_payment_error', 'decline_code'),
        stripe_value(invoice, 'last_payment_error', 'code'),
        stripe_value(invoice, 'payment_intent', 'last_payment_error', 'decline_code'),
        stripe_value(invoice, 'payment_intent', 'last_payment_error', 'code'),
    )
    return next((code for code in candidates if code), None)


def build_admin_payment_success_text(purpose, telegram_id, tariff, amount, currency, effective_expiry, payment_ref):
    return (
        "✅ Оплата подтверждена\n\n"
        f"Тип: {admin_payment_kind_label(purpose)}\n"
        f"Пользователь: {telegram_id if telegram_id is not None else 'не определён'}\n"
        f"Тариф: {tariff or 'не определён'}\n"
        f"Сумма: {format_admin_amount(amount, currency)}\n"
        f"Доступ до: {admin_access_date(effective_expiry)}\n"
        f"Payment ref: {payment_ref}"
    )


def build_admin_payment_problem_text(
    stage,
    telegram_id,
    problem,
    stripe_retry="неизвестно",
    recovery_reminder="неизвестно",
    safe_ref=None,
    note=None,
):
    text = (
        "⚠️ Оплата не завершена\n\n"
        f"Этап: {PAYMENT_STAGE_LABELS.get(stage, stage)}\n"
        f"Пользователь: {telegram_id if telegram_id is not None else 'не определён'}\n"
        f"Причина: {problem['label']}\n"
        f"Автоматический повтор Stripe: {stripe_retry}\n"
        f"Напоминание через 24 часа: {recovery_reminder}\n"
        f"Error ref: {safe_ref or 'не определён'}"
    )
    if note:
        text += f"\n\n{note}"
    return text


def stripe_admin_event_key(event_id, context_ref):
    return str(event_id) if event_id else f"ctx:{safe_delivery_hash(context_ref)}"


def enqueue_stripe_admin_message(cur, event_id, purpose, text, severity="WARNING", category=None, safe_ref=None):
    count = 0
    event_key = stripe_admin_event_key(event_id, safe_ref or purpose)
    for admin_id in ADMIN_IDS:
        if enqueue_message_delivery(
            cur,
            f"stripe-admin:{event_key}:{purpose}:{admin_id}",
            int(admin_id),
            "stripe_admin_message",
            stripe_delivery_payload(
                text,
                severity=severity,
                category=category,
                safe_ref=safe_ref,
            ),
        ):
            count += 1
    return count


def enqueue_admin_payment_success(cur, event_id, purpose, telegram_id, tariff, amount, currency, effective_expiry, payment_ref):
    logging.info(
        "ADMIN_NOTIFICATION_SUPPRESSED: category=%s, result=payment_recorded, "
        "telegram_id=%s, tariff=%s, access_until=%s, payment_ref=%s",
        purpose,
        telegram_id,
        tariff or "unknown",
        effective_expiry.isoformat() if effective_expiry else "unknown",
        safe_log_id(payment_ref) or "none",
    )
    return 0


def enqueue_admin_payment_problem(
    cur,
    event_id,
    purpose,
    stage,
    telegram_id=None,
    category=None,
    stripe_code=None,
    exception=None,
    stripe_retry="неизвестно",
    recovery_reminder="неизвестно",
    safe_ref=None,
    note=None,
    severity="WARNING",
):
    problem = classify_payment_problem(category=category, stripe_code=stripe_code, exception=exception)
    safe_ref = safe_ref or safe_admin_context_reference(purpose, event_id, telegram_id, category, stripe_code, type(exception).__name__ if exception else None)
    return enqueue_stripe_admin_message(
        cur,
        event_id,
        purpose,
        build_admin_payment_problem_text(
            stage,
            telegram_id,
            problem,
            stripe_retry=stripe_retry,
            recovery_reminder=recovery_reminder,
            safe_ref=safe_ref,
            note=note,
        ),
        severity=severity,
        category=problem["category"],
        safe_ref=safe_ref,
    )


def admin_recovery_reminder_status(
    immediate_retry_enqueued=False,
    durable_24h_enqueued=False,
    scheduler_will_check=False,
):
    if durable_24h_enqueued:
        return "запланировано"
    if scheduler_will_check:
        return "будет проверено через 24 часа"
    if immediate_retry_enqueued:
        return "не применимо"
    return "неизвестно"


def log_admin_payment_enqueue_failure(purpose=None, category=None, safe_ref=None):
    logging.warning(
        "ADMIN_PAYMENT_NOTIFICATION_ENQUEUE_FAILED: purpose=%s, category=%s, safe_ref=%s",
        purpose or "unknown",
        category or "unknown",
        safe_ref or "none",
    )


def enqueue_admin_payment_notification_savepoint(cur, enqueue_func, purpose=None, category=None, safe_ref=None):
    cur.execute("SAVEPOINT admin_payment_notification")
    try:
        result = enqueue_func()
        cur.execute("RELEASE SAVEPOINT admin_payment_notification")
        return result
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT admin_payment_notification")
        cur.execute("RELEASE SAVEPOINT admin_payment_notification")
        log_admin_payment_enqueue_failure(purpose=purpose, category=category, safe_ref=safe_ref)
        return 0


def enqueue_admin_payment_success_safely(
    cur,
    event_id,
    purpose,
    telegram_id,
    tariff,
    amount,
    currency,
    effective_expiry,
    payment_ref,
):
    return enqueue_admin_payment_notification_savepoint(
        cur,
        lambda: enqueue_admin_payment_success(
            cur,
            event_id,
            purpose,
            telegram_id,
            tariff,
            amount,
            currency,
            effective_expiry,
            payment_ref,
        ),
        purpose=purpose,
        category=purpose,
        safe_ref=payment_ref,
    )


def enqueue_admin_payment_problem_safely(cur, **kwargs):
    return enqueue_admin_payment_notification_savepoint(
        cur,
        lambda: enqueue_admin_payment_problem(cur, **kwargs),
        purpose=kwargs.get("purpose"),
        category=kwargs.get("category"),
        safe_ref=kwargs.get("safe_ref"),
    )


async def try_enqueue_admin_payment_problem_now(**kwargs):
    conn = None
    cur = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        enqueue_admin_payment_problem(cur, **kwargs)
        conn.commit()
        return True
    except Exception:
        if conn:
            conn.rollback()
        log_admin_payment_enqueue_failure(
            purpose=kwargs.get("purpose"),
            category=kwargs.get("category"),
            safe_ref=kwargs.get("safe_ref"),
        )
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


async def enqueue_admin_payment_problem_now(**kwargs):
    return await try_enqueue_admin_payment_problem_now(**kwargs)


def enqueue_user_payment_success_message(cur, event_id, telegram_id, purpose, expiry_date, keyboard_kind=None):
    return enqueue_stripe_user_message(
        cur,
        event_id,
        telegram_id,
        purpose,
        build_user_payment_success_message(purpose, expiry_date),
        keyboard_kind=keyboard_kind,
        new_expiry=expiry_date.isoformat() if expiry_date else None,
    )


def enqueue_stripe_user_message(cur, event_id, telegram_id, purpose, text, keyboard_kind=None, parse_mode=None, delivery_type=None, **extra):
    return enqueue_message_delivery(
        cur,
        stripe_delivery_key(event_id, purpose),
        int(telegram_id),
        delivery_type or "stripe_user_message",
        stripe_delivery_payload(text, keyboard_kind=keyboard_kind, parse_mode=parse_mode, **extra),
    )


def failed_renewal_message_text():
    return (
        "⚠️ Не удалось продлить подписку\n\n"
        "Платёж не прошёл. Мы сохраним доступ в клуб ещё на 48 часов.\n\n"
        "Пожалуйста, пополните карту или обновите способ оплаты. "
        "Если оплата пройдёт в течение этого времени, доступ продолжится автоматически."
    )


def failed_renewal_delivery_key(telegram_id, stripe_subscription_id, payment_failed_at):
    failure_point = payment_failed_at.isoformat() if payment_failed_at else "unknown"
    episode_hash = safe_delivery_hash(
        f"{int(telegram_id)}:{stripe_subscription_id or 'none'}:{failure_point}"
    )
    return f"failed-renewal:{episode_hash}"


def enqueue_failed_renewal_user_message(
    cur,
    telegram_id,
    stripe_subscription_id,
    payment_failed_at,
):
    return enqueue_message_delivery(
        cur,
        failed_renewal_delivery_key(
            telegram_id,
            stripe_subscription_id,
            payment_failed_at,
        ),
        int(telegram_id),
        "stripe_user_message",
        stripe_delivery_payload(
            failed_renewal_message_text(),
            keyboard_kind="billing_portal",
        ),
    )


def cancel_failed_renewal_user_messages(cur, telegram_id, reason="payment_recovered"):
    cur.execute(
        """
        UPDATE message_delivery_events
        SET status = 'cancelled',
            last_error = LEFT(%s, 500),
            lease_until = NULL,
            next_attempt_at = NULL
        WHERE telegram_id = %s
          AND delivery_key LIKE 'failed-renewal:%%'
          AND status IN ('pending', 'failed', 'processing')
        """,
        (reason, int(telegram_id)),
    )


def subscription_expired_delivery_key(telegram_id, expiry_date):
    expiry_point = expiry_date.isoformat() if expiry_date else "unknown"
    return f"subscription-expired:{safe_delivery_hash(f'{int(telegram_id)}:{expiry_point}')}"


def enqueue_subscription_expired_user_message(cur, telegram_id, expiry_date, text=None):
    return enqueue_message_delivery(
        cur,
        subscription_expired_delivery_key(telegram_id, expiry_date),
        int(telegram_id),
        "subscription_expired_user",
        stripe_delivery_payload(
            text or (
                "⚠️ Ваша подписка истекла. Доступ закрыт.\n"
                "Вы можете оформить новую подписку в любое время."
            ),
            keyboard_kind="tariffs",
            show_trial=False,
        ),
    )


def first_purchase_recovery_delivery_key(telegram_id):
    return f"first_purchase_recovery:{safe_delivery_hash(int(telegram_id))}"


def first_purchase_recovery_reminder_text():
    return (
        "Похоже, вчера оформление доступа не завершилось.\n\n"
        "Мы всё проверили — сейчас можно попробовать ещё раз 🤍\n\n"
        "Если снова что-то не получится, просто напишите нам, и мы поможем."
    )


FIRST_PURCHASE_RECOVERY_REASON_LABELS = {
    "checkout_async_payment_failed": "асинхронная оплата не прошла",
    "checkout_expired": "ссылка оплаты истекла",
    "card_declined": "банк отклонил карту",
    "insufficient_funds": "недостаточно средств",
    "authentication_required": "требуется подтверждение оплаты",
    "invoice_payment_failed": "Stripe сообщил об ошибке оплаты",
    "checkout_creation_failed": "не удалось создать ссылку оплаты",
    "checkout_not_completed": "успешная оплата не была подтверждена; точная причина не определена",
    "payment_confirmation_pending": "Checkout завершён, но успешная оплата не подтверждена",
    "checkout_retry_unresolved": "предыдущая попытка оплаты осталась незавершённой",
    "stripe_api_unavailable": "Stripe API временно недоступен",
    "unknown_payment_error": "оплата не завершилась",
}

FIRST_PURCHASE_RECOVERY_STAGE_LABELS = {
    "checkout_creation": "создание Checkout",
    "checkout": "Checkout",
    "payment_confirmation": "подтверждение оплаты",
    "checkout_async_payment_failed": "асинхронная оплата Checkout",
    "checkout_expired": "Checkout истёк",
    "invoice_payment_failed": "ошибка invoice",
}


FIRST_PURCHASE_RECOVERY_ALLOWED_ERROR_CONTEXTS = {
    "checkout.session.async_payment_failed",
    "checkout.session.expired",
    "checkout_creation_failed",
    "invoice_payment_failed",
    "invoice_payment_failed:card_declined",
    "invoice_payment_failed:insufficient_funds",
    "invoice_payment_failed:authentication_required",
    "stripe_api_unavailable",
}


def normalize_first_purchase_recovery_error_context(value):
    if not value:
        return None
    token = str(value).strip().lower()
    if token in FIRST_PURCHASE_RECOVERY_ALLOWED_ERROR_CONTEXTS:
        return token
    return None


def invoice_payment_failed_recovery_context_token(failure_code=None):
    problem = classify_payment_problem(event_type="invoice.payment_failed", stripe_code=failure_code)
    category = problem["category"]
    if category in ("card_declined", "insufficient_funds", "authentication_required"):
        return f"invoice_payment_failed:{category}"
    return "invoice_payment_failed"


def checkout_creation_recovery_error_token(exception):
    problem = classify_payment_problem(exception=exception)
    if problem["category"] == "stripe_api_unavailable":
        return "stripe_api_unavailable"
    return "checkout_creation_failed"


def persist_first_purchase_recovery_invoice_failure_context(cur, telegram_id, stripe_subscription_id, failure_code=None):
    token = invoice_payment_failed_recovery_context_token(failure_code)
    cur.execute("""
        UPDATE checkout_sessions
        SET last_error = %s
        WHERE id = (
            SELECT id
            FROM checkout_sessions
            WHERE telegram_id = %s
              AND mode = 'subscription'
              AND (
                    stripe_subscription_id = %s
                    OR stripe_subscription_id IS NULL
                  )
              AND status = ANY(%s::text[])
            ORDER BY
                CASE WHEN stripe_subscription_id = %s THEN 0 ELSE 1 END,
                COALESCE(updated_at, created_at) DESC
            LIMIT 1
        )
        RETURNING id
    """, (
        token,
        int(telegram_id),
        stripe_subscription_id,
        list(FIRST_PURCHASE_RECOVERY_ATTEMPT_STATUSES),
        stripe_subscription_id,
    ))
    row = cur.fetchone()
    return token, row[0] if row else None


def classify_first_purchase_recovery_context(attempt_status=None, attempt_source=None, attempt_error_context=None):
    status = (attempt_status or "").lower()
    source = (attempt_source or "").lower()
    error_context = normalize_first_purchase_recovery_error_context(attempt_error_context)
    if error_context == "checkout.session.async_payment_failed":
        return "checkout_async_payment_failed", "checkout_async_payment_failed"
    if error_context == "checkout.session.expired":
        return "checkout_expired", "checkout_expired"
    if error_context == "invoice_payment_failed:card_declined":
        return "card_declined", "invoice_payment_failed"
    if error_context == "invoice_payment_failed:insufficient_funds":
        return "insufficient_funds", "invoice_payment_failed"
    if error_context == "invoice_payment_failed:authentication_required":
        return "authentication_required", "invoice_payment_failed"
    if error_context == "invoice_payment_failed":
        return "invoice_payment_failed", "invoice_payment_failed"
    if error_context == "stripe_api_unavailable":
        return "stripe_api_unavailable", "checkout_creation"
    if error_context == "checkout_creation_failed":
        return "checkout_creation_failed", "checkout_creation"
    if source == "checkout_retry_event":
        return "checkout_retry_unresolved", "checkout_creation"
    if status == "expired":
        return "checkout_expired", "checkout"
    if status == "creation_unknown":
        return "checkout_creation_failed", "checkout_creation"
    if status == "failed":
        return "unknown_payment_error", "checkout"
    if status == "completed":
        return "payment_confirmation_pending", "payment_confirmation"
    if status in ("creating", "open"):
        return "checkout_not_completed", "checkout"
    return "unknown_payment_error", "checkout"


def first_purchase_recovery_context(
    telegram_id,
    latest_attempt_at,
    tariff_code=None,
    attempt_status=None,
    attempt_source=None,
    attempt_error_context=None,
):
    reason_category, stage = classify_first_purchase_recovery_context(
        attempt_status=attempt_status,
        attempt_source=attempt_source,
        attempt_error_context=attempt_error_context,
    )
    attempted_at = latest_attempt_at.isoformat() if latest_attempt_at else None
    safe_ref = safe_admin_context_reference(
        "first_purchase_recovery",
        telegram_id,
        attempted_at,
        tariff_code,
        attempt_status,
        attempt_source,
        normalize_first_purchase_recovery_error_context(attempt_error_context),
        reason_category,
    )
    return {
        "reason_category": reason_category,
        "reason_label": FIRST_PURCHASE_RECOVERY_REASON_LABELS.get(
            reason_category,
            FIRST_PURCHASE_RECOVERY_REASON_LABELS["unknown_payment_error"],
        ),
        "stage": stage,
        "stage_label": FIRST_PURCHASE_RECOVERY_STAGE_LABELS.get(stage, stage),
        "tariff_code": tariff_code or "unknown",
        "attempt_status": attempt_status or "unknown",
        "attempt_source": attempt_source or "unknown",
        "attempt_error_context": normalize_first_purchase_recovery_error_context(attempt_error_context) or "unknown",
        "attempted_at": attempted_at,
        "safe_ref": safe_ref,
    }


def first_purchase_recovery_row_context(row):
    if not row:
        return {}
    telegram_id = row[0]
    latest_attempt_at = row[1] if len(row) > 1 else None
    tariff_code = row[2] if len(row) > 2 else None
    attempt_status = row[3] if len(row) > 3 else None
    attempt_source = row[4] if len(row) > 4 else None
    attempt_error_context = row[5] if len(row) > 5 else None
    return first_purchase_recovery_context(
        telegram_id,
        latest_attempt_at,
        tariff_code=tariff_code,
        attempt_status=attempt_status,
        attempt_source=attempt_source,
        attempt_error_context=attempt_error_context,
    )


def first_purchase_recovery_admin_sent_delivery_key(recovery_delivery_key, admin_id):
    return f"first_purchase_recovery_admin_sent:{safe_delivery_hash(recovery_delivery_key)}:{int(admin_id)}"


def parse_first_purchase_recovery_attempted_at(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    parsed = normalize_utc_naive(parsed)
    return parsed


def format_first_purchase_recovery_attempted_at(value):
    parsed = parse_first_purchase_recovery_attempted_at(value)
    if not parsed:
        return "не определена"
    return f"{parsed.strftime('%d.%m.%Y %H:%M')} UTC"


def first_purchase_recovery_tariff_label(value):
    if value is None:
        return "не определён"
    text = str(value).strip()
    if not text or text.lower() in ("none", "unknown"):
        return "не определён"
    return text


def build_first_purchase_recovery_admin_sent_text(telegram_id, payload):
    safe_ref = payload.get("safe_ref") or safe_admin_context_reference(
        "first_purchase_recovery_sent",
        telegram_id,
        payload.get("attempted_at") or payload.get("latest_attempt_at"),
    )
    return (
        "🔁 Повторная попытка оплаты предложена\n\n"
        f"Пользователь: {telegram_id}\n"
        f"Последняя попытка: {format_first_purchase_recovery_attempted_at(payload.get('attempted_at'))}\n"
        f"Этап: {payload.get('stage_label') or 'не определён'}\n"
        f"Причина: {payload.get('reason_label') or 'не определена'}\n"
        f"Тариф: {first_purchase_recovery_tariff_label(payload.get('tariff_code'))}\n"
        "Напоминание: отправлено\n"
        f"Reference: {safe_ref}"
    )


def enqueue_first_purchase_recovery_admin_sent_notices(cur, recovery_delivery_key, telegram_id, payload):
    safe_ref = payload.get("safe_ref") or safe_admin_context_reference(
        "first_purchase_recovery_sent",
        telegram_id,
        payload.get("attempted_at") or payload.get("latest_attempt_at"),
    )
    logging.info(
        "ADMIN_NOTIFICATION_SUPPRESSED: category=first_purchase_recovery_sent, "
        "result=user_message_sent, telegram_id=%s, delivery_ref=%s, safe_ref=%s",
        telegram_id,
        safe_delivery_hash(recovery_delivery_key),
        safe_ref,
    )
    return 0


def enqueue_first_purchase_recovery_admin_sent_notices_safely(cur, recovery_delivery_key, telegram_id, payload):
    safe_ref = payload.get("safe_ref") if isinstance(payload, dict) else None
    return enqueue_admin_payment_notification_savepoint(
        cur,
        lambda: enqueue_first_purchase_recovery_admin_sent_notices(cur, recovery_delivery_key, telegram_id, payload),
        purpose="first_purchase_recovery_sent",
        category="first_purchase_recovery_sent",
        safe_ref=safe_ref,
    )


def first_purchase_recovery_eligibility_sql(single_user=False, current_delivery_key=False, count_only=False):
    delivery_clause = """
              AND (%s IS NULL OR md.delivery_key <> %s)
    """ if current_delivery_key else ""
    select_clause = "COUNT(*)" if count_only else (
        "u.telegram_id, la.latest_attempt_at, la.tariff_code, la.attempt_status, la.attempt_source, la.attempt_error_context"
    )
    user_clause = "AND u.telegram_id = %s" if single_user else ""
    limit_clause = "" if single_user or count_only else "ORDER BY la.latest_attempt_at ASC LIMIT %s"
    return f"""
        WITH attempts AS (
            SELECT
                telegram_id,
                COALESCE(updated_at, created_at) AS attempt_at,
                tariff_code,
                status AS attempt_status,
                'checkout_session' AS attempt_source,
                last_error AS attempt_error_context
            FROM checkout_sessions
            WHERE telegram_id IS NOT NULL
              AND status = ANY(%s::text[])
            UNION ALL
            SELECT
                telegram_id,
                attempt_at,
                tariff_code,
                'checkout_retry_unresolved' AS attempt_status,
                'checkout_retry_event' AS attempt_source,
                NULL AS attempt_error_context
            FROM checkout_retry_events
            WHERE telegram_id IS NOT NULL
              AND resolved_at IS NULL
        ),
        latest_attempt AS (
            SELECT DISTINCT ON (telegram_id)
                telegram_id,
                attempt_at AS latest_attempt_at,
                tariff_code,
                attempt_status,
                attempt_source,
                attempt_error_context
            FROM attempts
            WHERE attempt_at IS NOT NULL
              AND COALESCE(tariff_code, '') NOT ILIKE 'test%%'
            ORDER BY telegram_id, attempt_at DESC
        )
        SELECT {select_clause}
        FROM latest_attempt la
        JOIN users u ON u.telegram_id = la.telegram_id
        WHERE la.latest_attempt_at <= (NOW() AT TIME ZONE 'UTC') - (%s * INTERVAL '1 hour')
          AND NOT (u.telegram_id = ANY(%s::bigint[]))
          {user_clause}
          AND u.paid IS NOT TRUE
          AND (u.expiry_date IS NULL OR u.expiry_date <= (NOW() AT TIME ZONE 'UTC'))
          AND u.first_payment_done IS NOT TRUE
          AND u.blocked_bot IS NOT TRUE
          AND NOT EXISTS (
              SELECT 1
              FROM payment_events pe
              WHERE pe.telegram_id = u.telegram_id
                AND pe.payment_status = 'succeeded'
          )
          AND NOT EXISTS (
              SELECT 1
              FROM subscription_refund_reconciliations srr
              WHERE srr.telegram_id = u.telegram_id
                AND srr.reconciliation_result IN ('access_revoked', 'review_required')
                AND srr.created_at >= la.latest_attempt_at
          )
          AND NOT EXISTS (
              SELECT 1
              FROM access_events ae
              WHERE ae.telegram_id = u.telegram_id
                AND ae.created_at >= la.latest_attempt_at
                AND ae.new_expiry IS NOT NULL
                AND ae.new_expiry > (NOW() AT TIME ZONE 'UTC')
          )
          AND NOT EXISTS (
              SELECT 1
              FROM stripe_links sl
              WHERE sl.telegram_id = u.telegram_id
                AND (
                    sl.is_active IS TRUE
                    OR sl.status IN ('active', 'trialing')
                )
          )
          AND NOT EXISTS (
              SELECT 1
              FROM access_events manual_ae
              WHERE manual_ae.telegram_id = u.telegram_id
                AND manual_ae.new_expiry IS NOT NULL
                AND manual_ae.new_expiry > (NOW() AT TIME ZONE 'UTC')
                AND COALESCE(manual_ae.source, '') IN ('manual', 'admin', 'out_of_band', 'manual_link')
          )
          AND NOT EXISTS (
              SELECT 1
              FROM message_delivery_events md
              WHERE md.telegram_id = u.telegram_id
                AND md.delivery_type = 'first_purchase_recovery_reminder'
                AND md.status IN ('pending', 'processing', 'sent')
                {delivery_clause}
          )
        {limit_clause}
    """


def fetch_due_first_purchase_recovery_users(cur, limit=100):
    params = [
        list(FIRST_PURCHASE_RECOVERY_ATTEMPT_STATUSES),
        FIRST_PURCHASE_RECOVERY_REMINDER_DELAY_HOURS,
        list(ADMIN_IDS),
        int(limit),
    ]
    cur.execute(first_purchase_recovery_eligibility_sql(), params)
    return cur.fetchall()


def count_due_first_purchase_recovery_users(cur):
    cur.execute(
        first_purchase_recovery_eligibility_sql(count_only=True),
        (
            list(FIRST_PURCHASE_RECOVERY_ATTEMPT_STATUSES),
            FIRST_PURCHASE_RECOVERY_REMINDER_DELAY_HOURS,
            list(ADMIN_IDS),
        ),
    )
    row = cur.fetchone()
    return row[0] if row else 0


def fetch_first_purchase_recovery_user_if_due(cur, telegram_id, current_delivery_key=None):
    params = [
        list(FIRST_PURCHASE_RECOVERY_ATTEMPT_STATUSES),
        FIRST_PURCHASE_RECOVERY_REMINDER_DELAY_HOURS,
        list(ADMIN_IDS),
        int(telegram_id),
    ]
    if current_delivery_key is not None:
        params.extend([current_delivery_key, current_delivery_key])
    cur.execute(
        first_purchase_recovery_eligibility_sql(
            single_user=True,
            current_delivery_key=current_delivery_key is not None,
        ),
        params,
    )
    return cur.fetchone()


def enqueue_first_purchase_recovery_reminder(cur, telegram_id, latest_attempt_at, context=None):
    context = context or first_purchase_recovery_context(telegram_id, latest_attempt_at)
    payload_json = json.dumps(
        stripe_delivery_payload(
            first_purchase_recovery_reminder_text(),
            keyboard_kind="retry_payment",
            latest_attempt_at=latest_attempt_at.isoformat() if latest_attempt_at else None,
            **context,
        ),
        ensure_ascii=False,
        sort_keys=True,
    )
    cur.execute("""
        INSERT INTO message_delivery_events (
            delivery_key, telegram_id, delivery_type, status, attempt_count, last_error, payload_json, next_attempt_at
        )
        VALUES (%s, %s, 'first_purchase_recovery_reminder', 'pending', 0, NULL, %s, NOW())
        ON CONFLICT (delivery_key) DO UPDATE SET
            status = 'pending',
            last_error = NULL,
            payload_json = EXCLUDED.payload_json,
            claimed_at = NULL,
            lease_until = NULL,
            sent_at = NULL,
            next_attempt_at = NOW()
        WHERE message_delivery_events.status = 'cancelled'
        RETURNING delivery_key
    """, (first_purchase_recovery_delivery_key(telegram_id), int(telegram_id), payload_json))
    return cur.fetchone() is not None


def first_purchase_recovery_reminder_still_due(cur, telegram_id, current_delivery_key=None):
    return fetch_first_purchase_recovery_user_if_due(cur, telegram_id, current_delivery_key=current_delivery_key) is not None


def cancel_first_purchase_recovery_delivery(cur, delivery_key, reason):
    cancel_message_delivery(cur, delivery_key, reason=reason)


def cancel_first_purchase_recovery_deliveries(cur, telegram_id, reason="first_payment_succeeded"):
    cur.execute("""
        UPDATE message_delivery_events
        SET status = 'cancelled',
            last_error = LEFT(%s, 500),
            lease_until = NULL,
            next_attempt_at = NULL
        WHERE delivery_key = %s
          AND telegram_id = %s
          AND delivery_type = 'first_purchase_recovery_reminder'
          AND status IN ('pending', 'failed', 'processing')
    """, (str(reason), first_purchase_recovery_delivery_key(telegram_id), int(telegram_id)))


def enqueue_rejoin_invite_after_payment(cur, telegram_id, expiry_date, source, stripe_event_id, stripe_subscription_id=None):
    expiry_text = expiry_date.strftime("%d.%m.%Y") if expiry_date else "активен"
    text = (
        f"Ссылка для входа в клуб активна до {expiry_text}.\n\n"
        "{invite_link}"
    )
    return enqueue_stripe_user_message(
        cur,
        stripe_event_id,
        telegram_id,
        "rejoin_invite",
        text,
        delivery_type="stripe_rejoin_check",
        source=source,
        new_expiry=expiry_date.isoformat() if expiry_date else None,
        stripe_event_id=stripe_event_id,
        stripe_subscription_id=stripe_subscription_id,
    )


def access_restore_delivery_key(admin_action_id, telegram_id):
    return f"access-restore:{admin_action_id}:{int(telegram_id)}"


def access_restore_auto_delivery_key(source, telegram_id, expiry_date):
    if expiry_date:
        expiry_stamp = int(expiry_date.replace(tzinfo=timezone.utc).timestamp())
    else:
        expiry_stamp = "unknown"
    return f"access-restore:{source}:{int(telegram_id)}:{expiry_stamp}"


def access_restore_invite_text(expiry_date):
    expiry_text = expiry_date.strftime("%d.%m.%Y") if expiry_date else "активна"
    return (
        f"Привет! Проверила: ваша подписка активна до {expiry_text}.\n\n"
        "Доступ к закрытому клубу восстановлен 🤍\n\n"
        "Нажмите кнопку ниже, чтобы снова присоединиться. "
        "Ссылка одноразовая и действует 24 часа."
    )


def access_restore_invite_keyboard(invite_link):
    return inline_keyboard([[
        InlineKeyboardButton(text="Вступить в клуб", url=invite_link)
    ]])


def access_restore_payload(telegram_id, effective_expiry, source, requested_by_admin_id=None, admin_action_id=None, reason=None):
    payload = {
        "telegram_id": int(telegram_id),
        "effective_expiry": effective_expiry.isoformat() if effective_expiry else None,
        "source": source,
        "reason": reason or "active_access_restoration",
    }
    if requested_by_admin_id is not None:
        payload["requested_by_admin_id"] = int(requested_by_admin_id)
    if admin_action_id:
        payload["admin_action_id"] = str(admin_action_id)
    return payload


def enqueue_access_restore_invite(
    cur,
    telegram_id,
    effective_expiry,
    source,
    requested_by_admin_id=None,
    admin_action_id=None,
    reason=None,
    delivery_key=None,
):
    delivery_key = delivery_key or access_restore_delivery_key(admin_action_id, telegram_id)
    return enqueue_message_delivery(
        cur,
        delivery_key,
        int(telegram_id),
        ACCESS_RESTORE_DELIVERY_TYPE,
        access_restore_payload(
            telegram_id,
            effective_expiry,
            source,
            requested_by_admin_id=requested_by_admin_id,
            admin_action_id=admin_action_id,
            reason=reason,
        ),
    )


def enqueue_automatic_membership_repair(
    cur,
    telegram_id,
    effective_expiry,
    source,
    requested_by_admin_id=None,
    admin_action_id=None,
    reason=None,
):
    if not has_restorable_group_access(True, effective_expiry):
        return False
    if source == ACCESS_RESTORE_SOURCE_AUTO_SYNC:
        delivery_key = access_restore_auto_delivery_key("auto-sync", telegram_id, effective_expiry)
    elif admin_action_id:
        delivery_key = access_restore_delivery_key(admin_action_id, telegram_id)
    else:
        delivery_key = access_restore_auto_delivery_key(source, telegram_id, effective_expiry)
    return enqueue_access_restore_invite(
        cur,
        telegram_id,
        effective_expiry,
        source,
        requested_by_admin_id=requested_by_admin_id,
        admin_action_id=admin_action_id,
        reason=reason or "automatic_membership_repair",
        delivery_key=delivery_key,
    )


def record_access_event_cur(
    cur,
    telegram_id,
    event_type,
    source=None,
    old_expiry=None,
    new_expiry=None,
    stripe_event_id=None,
    stripe_subscription_id=None,
    notes=None,
):
    cur.execute("""
        INSERT INTO access_events (
            telegram_id, event_type, source, old_expiry, new_expiry,
            stripe_event_id, stripe_subscription_id, notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        int(telegram_id),
        event_type,
        source,
        old_expiry,
        new_expiry,
        stripe_event_id,
        stripe_subscription_id,
        notes,
    ))


def member_has_group_access(member_status, restricted_has_access=True):
    return member_status in ("member", "administrator", "creator") or (
        member_status == "restricted" and restricted_has_access
    )


def restore_access_membership_decision(status, is_member=True):
    if status in ("member", "administrator", "creator"):
        return "already_member"
    if status == "restricted":
        return "already_member" if is_member else "needs_invite"
    if status == "left":
        return "needs_invite"
    if status == "kicked":
        return "needs_unban_and_invite"
    return "fail_closed"


def has_restorable_group_access(paid, expiry_date, now=None):
    now = now or datetime.utcnow()
    return bool(paid) and bool(expiry_date) and expiry_date > now


def restore_access_admin_message(kind, telegram_id, expiry_date=None, safe_ref=None):
    expiry_text = expiry_date.strftime("%d.%m.%Y") if expiry_date else "нет"
    ref_text = f"\nref: {safe_ref}" if safe_ref else ""
    if kind == "already_member":
        return (
            "✅ Доступ подтверждён\n\n"
            "Пользователь уже находится в закрытом клубе.\n"
            f"telegram_id: {telegram_id}\n"
            f"Доступ до: {expiry_text}"
        )
    if kind == "queued":
        return (
            "✅ Восстановление доступа поставлено в очередь\n\n"
            f"telegram_id: {telegram_id}\n"
            f"Доступ до: {expiry_text}\n"
            "Пользователь получит одноразовую ссылку после повторной проверки."
        )
    if kind == "telegram_membership_check_failed":
        return (
            "⚠️ Оплаченный период подтверждён, но Telegram-проверка не завершилась\n\n"
            f"telegram_id: {telegram_id}\n"
            f"Доступ до: {expiry_text}\n"
            "Бот не смог проверить, находится ли пользователь в закрытом клубе. "
            "Проверьте права бота в группе и повторите /restore_access."
            f"{ref_text}"
        )
    if kind == "telegram_unban_failed":
        return (
            "⚠️ Оплаченный период подтверждён, но бан не снят\n\n"
            f"telegram_id: {telegram_id}\n"
            f"Доступ до: {expiry_text}\n"
            "Бот не смог снять бан в закрытом клубе. Проверьте права бота и повторите /restore_access."
            f"{ref_text}"
        )
    if kind == "stripe_synced_telegram_check_failed":
        return (
            "⚠️ Stripe-данные синхронизированы, но Telegram-проверка не завершилась\n\n"
            f"telegram_id: {telegram_id}\n"
            f"Доступ до: {expiry_text}\n"
            "paid/expiry_date уже обновлены по активной подписке Stripe. "
            "Бот не смог проверить членство в группе; проверьте права бота и повторите /restore_access."
            f"{ref_text}"
        )
    if kind == "stripe_synced_telegram_unban_failed":
        return (
            "⚠️ Stripe-данные синхронизированы, но бан не снят\n\n"
            f"telegram_id: {telegram_id}\n"
            f"Доступ до: {expiry_text}\n"
            "paid/expiry_date уже обновлены по активной подписке Stripe. "
            "Бот не смог снять бан; проверьте права бота и повторите /restore_access."
            f"{ref_text}"
        )
    if kind == "stripe_unavailable":
        return (
            "⚠️ Не удалось проверить активный период в Stripe\n\n"
            f"telegram_id: {telegram_id}\n"
            "Данные пользователя не изменены. Повторите /restore_access позже."
            f"{ref_text}"
        )
    if kind == "stripe_period_missing":
        return (
            "⚠️ Подписка Stripe активна, но оплаченный период не найден\n\n"
            f"telegram_id: {telegram_id}\n"
            "current_period_end отсутствует и paid invoice fallback не дал будущую дату. "
            "Доступ не восстановлен, delivery не создан."
        )
    if kind == "stripe_period_not_future":
        status_text = safe_ref or "unknown"
        return (
            "⚠️ Будущий оплаченный период Stripe не подтверждён\n\n"
            f"telegram_id: {telegram_id}\n"
            f"status: {status_text}\n"
            "Найденный период уже завершился.\n"
            "Доступ не восстановлен, delivery не создан."
        )
    if kind == "stripe_identity_changed":
        return (
            "⚠️ Stripe subscription пользователя изменилась во время проверки\n\n"
            f"telegram_id: {telegram_id}\n"
            "Данные пользователя не изменены. Повторите команду, чтобы проверить текущую подписку."
        )
    if kind == "stripe_not_active":
        status_text = safe_ref or "unknown"
        return (
            "⚠️ Подписка Stripe не активна\n\n"
            f"telegram_id: {telegram_id}\n"
            f"status: {status_text}\n"
            "Доступ не восстановлен, delivery не создан."
        )
    return (
        "❌ Восстановление не выполнено\n\n"
        "Активный оплаченный период не подтверждён.\n"
        "Данные пользователя не изменены."
    )


def stripe_delivery_reply_markup(payload):
    keyboard_kind = payload.get("keyboard_kind")
    if keyboard_kind == "retry_payment":
        return inline_keyboard([[
            InlineKeyboardButton(text="🔁 Выбрать тариф заново", callback_data="retry_payment")
        ]])
    if keyboard_kind == "cancel_subscription":
        return get_cancel_subscription_keyboard()
    if keyboard_kind == "tariffs":
        return get_tariffs_keyboard(show_trial=bool(payload.get("show_trial", False)))
    return None


async def stripe_delivery_reply_markup_for_user(payload, telegram_id):
    if payload.get("keyboard_kind") != "billing_portal":
        return stripe_delivery_reply_markup(payload)

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT stripe_customer_id FROM users WHERE telegram_id = %s",
            (int(telegram_id),),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    stripe_customer_id = row[0] if row else None
    portal_url = await create_billing_portal_url(stripe_customer_id)
    if not portal_url:
        raise RuntimeError("billing_portal_unavailable")
    return inline_keyboard([[
        InlineKeyboardButton(text="Обновить способ оплаты", url=portal_url)
    ]])


def get_reusable_checkout_session(cache_key):
    now_timestamp = datetime.utcnow().timestamp()
    expired_cache_keys = []

    for existing_key, existing_session in checkout_session_cache.items():
        cache_age = now_timestamp - existing_session["cached_at"]
        stripe_expires_at = existing_session.get("expires_at")
        if cache_age >= CHECKOUT_SESSION_COOLDOWN_SECONDS or (
            stripe_expires_at and stripe_expires_at <= now_timestamp
        ):
            expired_cache_keys.append(existing_key)

    for expired_key in expired_cache_keys:
        checkout_session_cache.pop(expired_key, None)

    cached_session = checkout_session_cache.get(cache_key)
    if not cached_session:
        return None

    cache_age = now_timestamp - cached_session["cached_at"]
    stripe_expires_at = cached_session.get("expires_at")

    if cache_age >= CHECKOUT_SESSION_COOLDOWN_SECONDS:
        checkout_session_cache.pop(cache_key, None)
        return None

    if stripe_expires_at and stripe_expires_at <= now_timestamp:
        checkout_session_cache.pop(cache_key, None)
        return None

    return cached_session


def clear_cached_checkout_sessions_for_user(user_id):
    user_id = int(user_id)
    cache_keys = [key for key in checkout_session_cache if key[0] == user_id]
    for cache_key in cache_keys:
        checkout_session_cache.pop(cache_key, None)

    if cache_keys:
        logging.info(f"Checkout Session cache cleared: user_id={user_id}, entries={len(cache_keys)}")


def register_checkout_attempt(telegram_user, sub_type):
    user_id = int(telegram_user.id)
    now = datetime.utcnow()
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO checkout_retry_events (
                telegram_id, tariff_code, username, first_name, last_name, attempt_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                user_id,
                sub_type,
                getattr(telegram_user, "username", None),
                getattr(telegram_user, "first_name", None),
                getattr(telegram_user, "last_name", None),
                now,
            ),
        )
        cur.execute(
            """
            SELECT COUNT(*)
            FROM checkout_retry_events
            WHERE telegram_id = %s
              AND attempt_at >= %s
            """,
            (user_id, now - timedelta(seconds=CHECKOUT_RETRY_WINDOW_SECONDS)),
        )
        attempt_count = cur.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    checkout_retry_state[user_id] = {
        "username": getattr(telegram_user, "username", None),
        "first_name": getattr(telegram_user, "first_name", None),
        "last_name": getattr(telegram_user, "last_name", None),
    }
    if attempt_count >= 2:
        logging.warning(
            f"Checkout retry detected: user_id={user_id}, sub_type={sub_type}, "
            f"attempts_in_window={attempt_count}, window_seconds={CHECKOUT_RETRY_WINDOW_SECONDS}"
        )

    return attempt_count, now.timestamp()


class CheckoutPreparationError(Exception):
    def __init__(self, category, safe_ref, message=None):
        self.category = str(category or "checkout_preparation_failed")
        self.safe_ref = safe_ref or safe_admin_context_reference("checkout_preparation_failed", self.category)
        super().__init__(message or self.category)


def checkout_preparation_error_category(error):
    if isinstance(error, CheckoutPreparationError):
        return error.category
    if isinstance(error, (ConnectionError, TimeoutError)):
        return "stripe_api_unavailable"
    return "checkout_preparation_failed"


def checkout_preparation_error_ref(error, user_id, sub_type):
    if isinstance(error, CheckoutPreparationError):
        return error.safe_ref
    return safe_admin_error_reference("checkout_preparation", error)


def build_checkout_reuse_info_text(user_id, sub_type, attempt_count):
    return (
        "ℹ️ Повторный запрос ссылки на оплату\n\n"
        f"Пользователь: {int(user_id)}\n"
        f"Тариф: {sub_type}\n"
        f"Попыток за последние 5 минут: {attempt_count}\n\n"
        "Активная Stripe Checkout ссылка была отправлена пользователю повторно.\n"
        "Успешной оплаты пока нет.\n\n"
        "Возможная причина: пользователь вернулся из встроенного браузера Telegram\n"
        "или повторно нажал кнопку тарифа."
    )


def build_checkout_preparation_failed_text(user_id, category, safe_ref):
    return (
        "⚠️ Не удалось подготовить оплату\n\n"
        "Этап: Checkout\n"
        f"Пользователь: {int(user_id)}\n"
        f"Причина: {category}\n"
        f"Error ref: {safe_ref}"
    )


async def try_enqueue_checkout_preparation_failed_alert(user_id, sub_type, category, safe_ref):
    conn = None
    cur = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        enqueue_stripe_admin_message(
            cur,
            None,
            "checkout_preparation_failed",
            build_checkout_preparation_failed_text(user_id, category, safe_ref),
            severity="WARNING",
            category=category,
            safe_ref=safe_ref,
        )
        conn.commit()
        return True
    except Exception:
        if conn:
            conn.rollback()
        log_admin_payment_enqueue_failure(
            purpose="checkout_preparation_failed",
            category=category,
            safe_ref=safe_ref,
        )
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


async def notify_admins_about_checkout_reuse(user_id, sub_type, attempt_count, session_id, attempt_timestamp):
    user_id = int(user_id)
    alert_enqueued = False
    if attempt_count < CHECKOUT_REUSE_ADMIN_ALERT_THRESHOLD:
        logging.info(
            "CHECKOUT_REUSE_INFO: user_id=%s, sub_type=%s, attempts=%s, session_id=%s, alert_enqueued=%s",
            user_id,
            sub_type,
            attempt_count,
            safe_log_id(session_id),
            alert_enqueued,
        )
        return

    if not ADMIN_IDS:
        logging.info(
            "CHECKOUT_REUSE_INFO: user_id=%s, sub_type=%s, attempts=%s, session_id=%s, alert_enqueued=%s",
            user_id,
            sub_type,
            attempt_count,
            safe_log_id(session_id),
            alert_enqueued,
        )
        return

    attempt_dt = datetime.utcfromtimestamp(attempt_timestamp)
    cooldown_before = attempt_dt - timedelta(seconds=CHECKOUT_REUSE_ADMIN_ALERT_COOLDOWN_SECONDS)
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"checkout-reuse-alert:{user_id}:{sub_type}",),
        )
        cur.execute(
            """
            SELECT MAX(last_admin_alert_at)
            FROM checkout_retry_events
            WHERE telegram_id = %s
              AND tariff_code = %s
            """,
            (user_id, sub_type),
        )
        row = cur.fetchone()
        last_alert_at = row[0] if row else None
        if last_alert_at and last_alert_at >= cooldown_before:
            conn.commit()
            logging.info(
                "CHECKOUT_REUSE_INFO: user_id=%s, sub_type=%s, attempts=%s, session_id=%s, alert_enqueued=%s",
                user_id,
                sub_type,
                attempt_count,
                safe_log_id(session_id),
                alert_enqueued,
            )
            return
        cur.execute(
            """
            WITH latest AS (
                SELECT id
                FROM checkout_retry_events
                WHERE telegram_id = %s
                  AND tariff_code = %s
                ORDER BY attempt_at DESC
                LIMIT 1
                FOR UPDATE
            )
            UPDATE checkout_retry_events
            SET last_admin_alert_at = %s
            WHERE id IN (SELECT id FROM latest)
            RETURNING id
            """,
            (user_id, sub_type, attempt_dt),
        )
        row = cur.fetchone()
        if row:
            safe_ref = safe_admin_context_reference("checkout_open_reused", user_id, sub_type, row[0])
            enqueue_stripe_admin_message(
                cur,
                None,
                "checkout_open_reused",
                build_checkout_reuse_info_text(user_id, sub_type, attempt_count),
                severity="INFO",
                category="checkout_open_reused",
                safe_ref=safe_ref,
            )
            alert_enqueued = True
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    logging.info(
        "CHECKOUT_REUSE_INFO: user_id=%s, sub_type=%s, attempts=%s, session_id=%s, alert_enqueued=%s",
        user_id,
        sub_type,
        attempt_count,
        safe_log_id(session_id),
        alert_enqueued,
    )


async def notify_admins_about_checkout_retry(user_id, sub_type, attempt_count, session_id, attempt_timestamp):
    user_id = int(user_id)
    if attempt_count < 2:
        return

    if not ADMIN_IDS:
        logging.warning(
            f"Checkout retry admin alert skipped: ADMIN_IDS не настроен, user_id={user_id}, "
            f"sub_type={sub_type}, attempts={attempt_count}"
        )
        return

    attempt_dt = datetime.utcfromtimestamp(attempt_timestamp)
    cooldown_before = attempt_dt - timedelta(seconds=CHECKOUT_ADMIN_ALERT_COOLDOWN_SECONDS)
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH latest AS (
                SELECT id
                FROM checkout_retry_events
                WHERE telegram_id = %s
                ORDER BY attempt_at DESC
                LIMIT 1
                FOR UPDATE
            )
            UPDATE checkout_retry_events
            SET last_admin_alert_at = %s
            WHERE id IN (SELECT id FROM latest)
              AND (
                    last_admin_alert_at IS NULL
                    OR last_admin_alert_at < %s
                  )
            RETURNING username, first_name, last_name
            """,
            (user_id, attempt_dt, cooldown_before),
        )
        row = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    if not row:
        return

    username = row[0] or checkout_retry_state.get(user_id, {}).get("username")
    username_text = f"@{username}" if username else "нет"
    name_parts = [
        row[1] or checkout_retry_state.get(user_id, {}).get("first_name"),
        row[2] or checkout_retry_state.get(user_id, {}).get("last_name"),
    ]
    name_text = " ".join(part for part in name_parts if part) or "нет"
    attempt_time_text = attempt_dt.strftime("%d.%m.%Y %H:%M:%S UTC")

    await enqueue_admin_payment_problem_now(
        event_id=None,
        purpose="checkout_retry_issue",
        stage="checkout_creation",
        telegram_id=user_id,
        category="checkout_creation_failed",
        stripe_retry="неизвестно",
        recovery_reminder="неизвестно",
        safe_ref=safe_admin_context_reference("checkout_retry_issue", user_id, sub_type, session_id),
        note=(
            "Пользователь несколько раз открыл оплату, но успешной оплаты пока нет.\n"
            f"Тариф: {sub_type}\n"
            f"Попыток за последние 5 минут: {attempt_count}\n"
            f"Время последней попытки: {attempt_time_text}\n"
            "Возможная причина: Stripe Checkout сбрасывается во встроенном браузере Telegram."
        ),
    )
    logging.info(
        f"Admin checkout issue alert sent: user_id={user_id}, sub_type={sub_type}, "
        f"attempts={attempt_count}, session_id={session_id}"
    )


def reset_checkout_retry_state_after_success(user_id, source):
    user_id = int(user_id)
    clear_cached_checkout_sessions_for_user(user_id)
    checkout_retry_state.pop(user_id, None)
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE checkout_retry_events
            SET resolved_at = COALESCE(resolved_at, NOW()),
                resolved_source = COALESCE(resolved_source, %s)
            WHERE telegram_id = %s
              AND resolved_at IS NULL
            """,
            (source, user_id),
        )
        cur.execute(
            """
            DELETE FROM checkout_retry_events
            WHERE resolved_at IS NOT NULL
              AND resolved_at < NOW() - INTERVAL '30 days'
            """
        )
        cur.execute(
            """
            DELETE FROM checkout_sessions
            WHERE telegram_id = %s
              AND status IN ('completed', 'expired', 'failed')
              AND updated_at < NOW() - INTERVAL '30 days'
            """,
            (user_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    logging.info(
        f"Checkout retry state reset after successful payment: user_id={user_id}, source={source}"
    )


async def send_checkout_open_instruction(callback, checkout_url, user_id, session_id, sub_type, mode, reused=False):
    payment_keyboard = inline_keyboard([
        [InlineKeyboardButton(text="💳 Перейти к оплате", url=checkout_url)],
        [InlineKeyboardButton(text="🔙 Назад к тарифам", callback_data="back_to_tariffs")],
    ])
    instruction_text = (
        f"{CHECKOUT_OPEN_INSTRUCTION}\n\n"
        f"Ссылка для оплаты:\n{checkout_url}"
    )
    await callback.message.answer(instruction_text, reply_markup=payment_keyboard)
    logging.info(
        f"Payment button sent: user_id={user_id}, session_id={safe_log_id(session_id)}, "
        f"sub_type={sub_type}, mode={mode}, checkout_url_present={bool(checkout_url)}, reused={reused}"
    )
    logging.info(
        f"Checkout opened instruction sent: user_id={user_id}, session_id={safe_log_id(session_id)}, "
        f"sub_type={sub_type}, reused={reused}"
    )
    logging.info(
        f"Checkout external browser instruction sent: user_id={user_id}, session_id={safe_log_id(session_id)}, "
        f"sub_type={sub_type}, reused={reused}"
    )

def split_telegram_text(text, limit=4096):
    text = str(text)
    return [text[i:i + limit] for i in range(0, len(text), limit)] or [""]


async def notify_admins(text: str, alert_key=None, severity="WARNING", dedupe_forever=False):
    dedupe_key = alert_key or (f"critical:{critical_alert_fingerprint(text)}" if severity == "CRITICAL" else None)
    claim_id = None
    if dedupe_key:
        conn = None
        cur = None
        try:
            conn = get_db_conn()
            cur = conn.cursor()
            dedupe_status_sql = (
                """AND (
                            status IN ('delivered', 'partial')
                            OR (
                                status IN ('claimed', 'failed')
                                AND created_at > NOW() - INTERVAL '15 minutes'
                            )
                        )"""
                if dedupe_forever
                else """AND created_at > NOW() - INTERVAL '15 minutes'
                        AND status IN ('claimed', 'delivered', 'partial', 'failed')"""
            )
            cur.execute(
                f"""
                INSERT INTO admin_alerts (alert_key, severity, text, status, delivered_admin_ids)
                SELECT %s, %s, %s, 'claimed', ''
                WHERE pg_try_advisory_xact_lock(hashtext(%s))
                  AND NOT EXISTS (
                      SELECT 1 FROM admin_alerts
                      WHERE alert_key = %s
                        {dedupe_status_sql}
                  )
                RETURNING id
                """,
                (dedupe_key, severity, str(text)[:4000], dedupe_key, dedupe_key),
            )
            row = cur.fetchone()
            conn.commit()
            if not row:
                logging.info("ADMIN_ALERT_DEDUPED: alert_key=%s, severity=%s", safe_log_id(dedupe_key), severity)
                return {"delivered": [], "failed": [], "deduped": True}
            claim_id = row[0]
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error("ADMIN_ALERT_CLAIM_FAILED: alert_key=%s, severity=%s, error=%s", safe_log_id(dedupe_key), severity, str(e), exc_info=True)
            if severity != "CRITICAL":
                return {"delivered": [], "failed": [], "claim_failed": True}
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    delivered = []
    failed = []
    for admin_id in ADMIN_IDS:
        try:
            for chunk in split_telegram_text(f"⚠️ {text}"):
                await bot.send_message(admin_id, chunk)
                await asyncio.sleep(0.05)
            delivered.append(admin_id)
        except Exception as e:
            failed.append(admin_id)
            logging.error("NOTIFY_ADMINS_FAILED: admin_id=%s, error=%s", admin_id, str(e), exc_info=True)

    if claim_id:
        conn = None
        cur = None
        try:
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE admin_alerts
                SET status = %s,
                    delivered_admin_ids = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (
                    "delivered" if delivered and not failed else "partial" if delivered else "failed",
                    ",".join(str(x) for x in delivered),
                    claim_id,
                ),
            )
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error("ADMIN_ALERT_RESULT_SAVE_FAILED: alert_id=%s, error=%s", claim_id, str(e), exc_info=True)
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    return {"delivered": delivered, "failed": failed}


async def notify_critical_delivery_failed(telegram_id, event_type, action, error, db_state_note=""):
    text = (
        "Не удалось отправить критическое сообщение пользователю.\n\n"
        f"telegram_id: {telegram_id}\n"
        f"событие: {event_type}\n"
        f"действие: {action}\n"
        f"ошибка: {error}"
    )

    if db_state_note:
        text += f"\n{db_state_note}"

    await notify_admins(text)


async def get_group_member_status_for_payment(telegram_id, source, stripe_event_id=None):
    try:
        member = await bot.get_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
        status = getattr(member, "status", None)
        has_access = getattr(member, "is_member", True)
        logging.info(
            "ACCESS_REJOIN_MEMBERSHIP_CHECKED: telegram_id=%s, source=%s, "
            "stripe_event_id=%s, status=%s, is_member=%s",
            telegram_id,
            source,
            safe_log_id(stripe_event_id),
            status,
            has_access,
        )
        return status, has_access
    except Exception as e:
        logging.warning(
            "ACCESS_REJOIN_MEMBERSHIP_CHECK_FAILED: telegram_id=%s, source=%s, "
            "stripe_event_id=%s, error=%s",
            telegram_id,
            source,
            safe_log_id(stripe_event_id),
            str(e),
            exc_info=True,
        )
        return None, True


def _fetch_single_count(cur, query, params=()):
    cur.execute(query, params)
    return cur.fetchone()[0]


def _fetch_revenue_by_currency(cur, period_start_utc, period_end_utc):
    cur.execute("""
        SELECT UPPER(COALESCE(currency, '')), COALESCE(SUM(amount_paid), 0)
        FROM payment_events
        WHERE payment_status = 'succeeded'
          AND created_at >= %s
          AND created_at < %s
        GROUP BY UPPER(COALESCE(currency, ''))
    """, (period_start_utc, period_end_utc))
    return {currency: int(amount or 0) for currency, amount in cur.fetchall() if currency}


def _fetch_tariff_counts(cur, period_start_utc, period_end_utc):
    cur.execute("""
        SELECT COALESCE(tariff_code, 'unknown'), COUNT(*)
        FROM payment_events
        WHERE payment_status = 'succeeded'
          AND created_at >= %s
          AND created_at < %s
        GROUP BY COALESCE(tariff_code, 'unknown')
    """, (period_start_utc, period_end_utc))
    return {tariff_code: int(count) for tariff_code, count in cur.fetchall()}


def _fetch_payment_buyers(cur, period_start_utc, period_end_utc):
    cur.execute("""
        SELECT
            pe.created_at,
            pe.telegram_id,
            u.username,
            u.first_name,
            u.last_name,
            pe.tariff_code,
            pe.payment_kind,
            pe.amount_paid,
            pe.currency,
            pe.billing_reason,
            pe.recovered_after_failure
        FROM payment_events pe
        LEFT JOIN users u ON u.telegram_id = pe.telegram_id
        WHERE pe.payment_status = 'succeeded'
          AND pe.telegram_id IS NOT NULL
          AND pe.created_at >= %s
          AND pe.created_at < %s
        ORDER BY pe.created_at ASC, pe.id ASC
    """, (period_start_utc, period_end_utc))
    return [
        {
            "paid_at": row[0],
            "telegram_id": row[1],
            "username": row[2],
            "first_name": row[3],
            "last_name": row[4],
            "tariff_code": row[5] or "unknown",
            "payment_kind": row[6] or "unknown",
            "amount_paid": row[7] or 0,
            "currency": row[8],
            "billing_reason": row[9],
            "recovered_after_failure": row[10],
        }
        for row in cur.fetchall()
    ]


def _fetch_weekly_gifts(cur, period_start_utc, period_end_utc):
    cur.execute("""
        SELECT
            pe.created_at,
            pe.telegram_id,
            purchaser.username,
            purchaser.first_name,
            purchaser.last_name,
            gift.tariff_code,
            gift.amount_total,
            gift.currency,
            gift.status,
            gift.recipient_telegram_id,
            recipient.username,
            recipient.first_name,
            recipient.last_name,
            gift.applied_expiry
        FROM payment_events pe
        JOIN gift_access_grants gift
          ON gift.stripe_session_id = pe.checkout_session_id
        LEFT JOIN users purchaser ON purchaser.telegram_id = pe.telegram_id
        LEFT JOIN users recipient ON recipient.telegram_id = gift.recipient_telegram_id
        WHERE pe.payment_status = 'succeeded'
          AND pe.payment_kind = %s
          AND pe.created_at >= %s
          AND pe.created_at < %s
        ORDER BY pe.created_at ASC, pe.id ASC
    """, (GIFT_PAYMENT_KIND, period_start_utc, period_end_utc))
    return [
        {
            "paid_at": row[0],
            "telegram_id": row[1],
            "username": row[2],
            "first_name": row[3],
            "last_name": row[4],
            "tariff_code": row[5],
            "amount_total": row[6] or 0,
            "currency": row[7],
            "status": row[8],
            "recipient_telegram_id": row[9],
            "recipient_username": row[10],
            "recipient_first_name": row[11],
            "recipient_last_name": row[12],
            "applied_expiry": row[13],
        }
        for row in cur.fetchall()
    ]


def _fetch_weekly_metrics(cur, period_start_utc, period_end_utc):
    metrics = {
        "new_registrations": _fetch_single_count(
            cur,
            "SELECT COUNT(*) FROM users WHERE registered_at >= %s AND registered_at < %s",
            (period_start_utc, period_end_utc),
        ),
        "free_lessons": _fetch_single_count(
            cur,
            "SELECT COUNT(*) FROM users WHERE video_sent_at >= %s AND video_sent_at < %s",
            (period_start_utc, period_end_utc),
        ),
        "group_joins": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM access_events
            WHERE event_type = 'group_member_joined'
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "group_leaves": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM access_events
            WHERE event_type = 'group_member_left'
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "active_paid_now": _fetch_single_count(
            cur,
            "SELECT COUNT(*) FROM users WHERE paid = TRUE AND expiry_date IS NOT NULL AND expiry_date > NOW()",
        ),
        "total_users_now": _fetch_single_count(cur, "SELECT COUNT(*) FROM users"),
        "blocked_bot_now": _fetch_single_count(cur, "SELECT COUNT(*) FROM users WHERE blocked_bot = TRUE"),
        "initial_purchases": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM payment_events
            WHERE payment_status = 'succeeded'
              AND payment_kind = 'initial_subscription'
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "recurring_payments": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM payment_events
            WHERE payment_status = 'succeeded'
              AND payment_kind = 'recurring'
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "trial_payments": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM payment_events
            WHERE payment_status = 'succeeded'
              AND payment_kind = 'trial'
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "adjustment_payments": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM payment_events
            WHERE payment_status = 'succeeded'
              AND payment_kind IN ('adjustment', 'out_of_band', 'unknown')
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "successful_payments": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM payment_events
            WHERE payment_status = 'succeeded'
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "unique_payers": _fetch_single_count(
            cur,
            """
            SELECT COUNT(DISTINCT telegram_id) FROM payment_events
            WHERE payment_status = 'succeeded'
              AND telegram_id IS NOT NULL
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "failed_payments": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM payment_events
            WHERE payment_status = 'failed'
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "recovered_after_failure": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM payment_events
            WHERE payment_status = 'succeeded'
              AND recovered_after_failure = TRUE
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "auto_renew_disabled": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM access_events
            WHERE event_type = 'subscription_auto_renew_disabled'
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "access_closed": _fetch_single_count(
            cur,
            """
            SELECT COUNT(*) FROM access_events
            WHERE event_type = 'auto_access_closed_expired'
              AND created_at >= %s
              AND created_at < %s
            """,
            (period_start_utc, period_end_utc),
        ),
        "grace_period_now": _fetch_single_count(
            cur,
            "SELECT COUNT(*) FROM users WHERE grace_period_end IS NOT NULL AND grace_period_end > NOW()",
        ),
        "payment_failed_now": _fetch_single_count(
            cur,
            "SELECT COUNT(*) FROM users WHERE payment_failed = TRUE",
        ),
        "unlinked_stripe_events": _fetch_single_count(
            cur,
            "SELECT COUNT(*) FROM unlinked_stripe_events WHERE resolved = FALSE",
        ),
        "expired_paid_now": _fetch_single_count(
            cur,
            "SELECT COUNT(*) FROM users WHERE paid = TRUE AND expiry_date IS NOT NULL AND expiry_date < NOW()",
        ),
    }
    metrics["revenue_by_currency"] = _fetch_revenue_by_currency(cur, period_start_utc, period_end_utc)
    metrics["tariff_counts"] = _fetch_tariff_counts(cur, period_start_utc, period_end_utc)
    return metrics


def _fetch_payment_history_started_at(cur):
    cur.execute("SELECT value_text FROM system_settings WHERE key = 'payment_history_started_at'")
    row = cur.fetchone()
    if not row or not row[0]:
        return None
    try:
        return datetime.fromisoformat(str(row[0]).split("+")[0])
    except ValueError:
        return None


def _weekly_report_keyboard(key):
    return inline_keyboard([
        [InlineKeyboardButton(text="📄 Скачать CSV покупок", callback_data=f"weekly_csv:{key}")],
        [InlineKeyboardButton(text="🔄 Обновить отчёт", callback_data=f"weekly_refresh:{key}")],
    ])


async def hydrate_missing_buyer_profiles(payments, concurrency=3):
    missing_ids = [
        int(payment["telegram_id"])
        for payment in payments
        if payment.get("telegram_id")
        and not payment.get("username")
        and not payment.get("first_name")
        and not payment.get("last_name")
    ]
    seen = set()
    missing_ids = [telegram_id for telegram_id in missing_ids if not (telegram_id in seen or seen.add(telegram_id))]
    if not missing_ids:
        return payments

    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_profile(telegram_id):
        async with semaphore:
            try:
                chat = await bot.get_chat(telegram_id)
                save_telegram_user_profile(chat)
                return telegram_id, chat
            except Exception as e:
                logging.warning("WEEKLY_REPORT_PROFILE_FETCH_FAILED: telegram_id=%s, error=%s", telegram_id, e)
                return telegram_id, None

    results = await asyncio.gather(*(fetch_profile(telegram_id) for telegram_id in missing_ids))
    profiles = {telegram_id: profile for telegram_id, profile in results if profile}

    for payment in payments:
        profile = profiles.get(payment.get("telegram_id"))
        if profile:
            payment["username"] = getattr(profile, "username", None)
            payment["first_name"] = getattr(profile, "first_name", None)
            payment["last_name"] = getattr(profile, "last_name", None)
    return payments


async def build_weekly_admin_report(period_start, period_end):
    period_start_utc = to_utc_naive(period_start)
    period_end_utc = to_utc_naive(period_end)
    comparison_start = period_start - timedelta(days=7)
    comparison_end = period_start
    comparison_start_utc = to_utc_naive(comparison_start)
    comparison_end_utc = to_utc_naive(comparison_end)

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        metrics = _fetch_weekly_metrics(cur, period_start_utc, period_end_utc)
        comparison = _fetch_weekly_metrics(cur, comparison_start_utc, comparison_end_utc)
        buyers = _fetch_payment_buyers(cur, period_start_utc, period_end_utc)
        gifts = _fetch_weekly_gifts(cur, period_start_utc, period_end_utc)
        history_started_at = _fetch_payment_history_started_at(cur)
    finally:
        cur.close()
        conn.close()

    buyers = await hydrate_missing_buyer_profiles(buyers)
    history_note = None
    if history_started_at and history_started_at > period_start_utc:
        history_started_moscow = history_started_at.replace(tzinfo=timezone.utc).astimezone(MOSCOW_TZ)
        history_note = (
            "История платежей собирается с "
            f"{history_started_moscow.strftime('%d.%m.%Y')}. "
            "Оплаты до этой даты в выручку не включены."
        )
    text = build_weekly_report_text(
        period_start,
        period_end,
        metrics,
        comparison=comparison,
        buyers=buyers,
        gifts=gifts,
        history_note=history_note,
    )
    return text, buyers


def claim_weekly_report_run(cur, key, period_start, period_end):
    return claim_weekly_report_run_record(
        cur,
        key,
        to_utc_naive(period_start),
        to_utc_naive(period_end),
        datetime.utcnow(),
        lease_minutes=30,
    )


def weekly_report_error_state(permanent_admin_ids, errors):
    return json.dumps({
        "version": 1,
        "permanent_admin_ids": sorted({int(admin_id) for admin_id in permanent_admin_ids}),
        "errors": [str(error)[:120] for error in errors[-5:]],
    }, ensure_ascii=True, sort_keys=True)


def complete_weekly_report_run(cur, key, sent_admin_ids, permanent_admin_ids=None, errors=None):
    permanent_admin_ids = permanent_admin_ids or []
    errors = errors or []
    cur.execute("""
        UPDATE weekly_report_runs
        SET status = 'completed',
            sent_admin_ids = %s,
            updated_at = NOW(),
            completed_at = NOW(),
            error_text = %s
        WHERE report_key = %s
    """, (
        ",".join(str(admin_id) for admin_id in sent_admin_ids),
        weekly_report_error_state(permanent_admin_ids, errors) if permanent_admin_ids else None,
        key,
    ))


def fail_weekly_report_run(cur, key, sent_admin_ids, permanent_admin_ids, errors):
    cur.execute("""
        UPDATE weekly_report_runs
        SET status = 'failed',
            sent_admin_ids = %s,
            updated_at = NOW(),
            completed_at = NOW(),
            error_text = %s
        WHERE report_key = %s
    """, (
        ",".join(str(admin_id) for admin_id in sent_admin_ids),
        weekly_report_error_state(permanent_admin_ids, errors),
        key,
    ))


def save_weekly_report_recipient_progress(cur, key, sent_admin_ids, permanent_admin_ids, errors):
    cur.execute("""
        UPDATE weekly_report_runs
        SET sent_admin_ids = %s,
            error_text = %s,
            updated_at = NOW()
        WHERE report_key = %s
    """, (
        ",".join(str(admin_id) for admin_id in sent_admin_ids),
        weekly_report_error_state(permanent_admin_ids, errors),
        key,
    ))


def weekly_report_recipient_error_is_permanent(error):
    return isinstance(error, TelegramForbiddenError) or is_undeliverable_user_error(error)


async def send_weekly_admin_report():
    period_start, period_end = get_last_completed_week_bounds()
    key = weekly_report_key(period_start)
    if not ADMIN_IDS:
        logging.warning("WEEKLY_ADMIN_REPORT_SKIPPED: ADMIN_IDS не настроен")
        return {"status": "failed", "report_key": None, "sent_admin_ids": [], "errors": ["ADMIN_IDS not configured"]}

    conn = get_db_conn()
    cur = conn.cursor()
    claim_result = {"status": "already_processing", "sent_admin_ids": []}
    try:
        claim_result = claim_weekly_report_run(cur, key, period_start, period_end)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    if claim_result["status"] != "claimed":
        logging.info(
            "WEEKLY_ADMIN_REPORT_DUPLICATE_SKIPPED: report_key=%s, status=%s",
            key,
            claim_result["status"],
        )
        return {
            "status": claim_result["status"],
            "report_key": key,
            "sent_admin_ids": claim_result.get("sent_admin_ids", []),
            "permanent_admin_ids": claim_result.get("permanent_admin_ids", []),
            "errors": [],
        }

    sent_admin_ids = list(claim_result.get("sent_admin_ids", []))
    permanent_admin_ids = list(claim_result.get("permanent_admin_ids", []))
    errors = []
    try:
        text, _ = await build_weekly_admin_report(period_start, period_end)
        keyboard = _weekly_report_keyboard(key)
        for admin_id in ADMIN_IDS:
            if admin_id in sent_admin_ids or admin_id in permanent_admin_ids:
                continue
            try:
                await bot.send_message(admin_id, text, reply_markup=keyboard)
                sent_admin_ids.append(admin_id)
                conn = get_db_conn()
                cur = conn.cursor()
                try:
                    save_weekly_report_recipient_progress(
                        cur,
                        key,
                        sent_admin_ids,
                        permanent_admin_ids,
                        errors,
                    )
                    conn.commit()
                finally:
                    cur.close()
                    conn.close()
            except Exception as e:
                error_ref = safe_admin_error_reference("weekly_report_send", e)
                permanent = weekly_report_recipient_error_is_permanent(e)
                error_summary = f"{admin_id}:{type(e).__name__}:{error_ref}"
                errors.append(error_summary)
                logging.error(
                    "WEEKLY_ADMIN_REPORT_SEND_FAILED: admin_id=%s, report_key=%s, "
                    "error_class=%s, permanent=%s, error_ref=%s",
                    admin_id,
                    key,
                    type(e).__name__,
                    permanent,
                    error_ref,
                )
                if permanent:
                    permanent_admin_ids.append(admin_id)
                    conn = get_db_conn()
                    cur = conn.cursor()
                    try:
                        save_weekly_report_recipient_progress(
                            cur,
                            key,
                            sent_admin_ids,
                            permanent_admin_ids,
                            errors,
                        )
                        conn.commit()
                    finally:
                        cur.close()
                        conn.close()
    except Exception as e:
        error_ref = safe_admin_error_reference("weekly_report_build", e)
        logging.exception(
            "WEEKLY_ADMIN_REPORT_BUILD_FAILED: report_key=%s, error_class=%s, error_ref=%s",
            key,
            type(e).__name__,
            error_ref,
        )
        errors.append(f"build:{type(e).__name__}:{error_ref}")

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        resolved_admin_ids = set(sent_admin_ids) | set(permanent_admin_ids)
        if set(ADMIN_IDS).issubset(resolved_admin_ids):
            complete_weekly_report_run(
                cur,
                key,
                sent_admin_ids,
                permanent_admin_ids,
                errors,
            )
        else:
            fail_weekly_report_run(
                cur,
                key,
                sent_admin_ids,
                permanent_admin_ids,
                errors or ["unknown_error"],
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    status = "failed"
    if set(ADMIN_IDS).issubset(set(sent_admin_ids) | set(permanent_admin_ids)):
        status = "completed"
    elif sent_admin_ids and errors:
        status = "partial"
    return {
        "status": status,
        "report_key": key,
        "sent_admin_ids": sent_admin_ids,
        "permanent_admin_ids": permanent_admin_ids,
        "errors": errors,
    }


async def send_weekly_report_to_admin(message, period_start, period_end, with_actions=True):
    text, _ = await build_weekly_admin_report(period_start, period_end)
    key = weekly_report_key(period_start)
    keyboard = _weekly_report_keyboard(key) if with_actions else None
    await message.answer(text, reply_markup=keyboard)


async def send_weekly_csv(callback, period_start, period_end):
    _, buyers = await build_weekly_admin_report(period_start, period_end)
    csv_bytes = build_payments_csv(buyers)
    start_label = period_start.date().isoformat()
    csv_end = period_end - timedelta(days=1) if period_end.time() == datetime.min.time() and period_end > period_start else period_end
    end_label = csv_end.date().isoformat()
    file_obj = io.BytesIO(csv_bytes)
    file_obj.name = f"weekly_payments_{start_label}_{end_label}.csv"
    await bot.send_document(
        callback.from_user.id,
        types.InputFile(file_obj, filename=file_obj.name),
        caption=f"CSV покупок за {start_label} — {end_label}",
    )


async def payment_needs_rejoin_invite(telegram_id, old_expiry, source, stripe_event_id=None):
    now = datetime.utcnow()
    status, restricted_has_access = await get_group_member_status_for_payment(
        telegram_id,
        source,
        stripe_event_id=stripe_event_id,
    )
    return should_send_rejoin_invite(
        old_expiry,
        now,
        telegram_member_status=status,
        restricted_has_access=restricted_has_access,
    )


def is_benign_rejoin_unban_error(error):
    if not is_telegram_bad_request_error(error):
        return False
    error_text = str(error).lower()
    benign_markers = (
        "user is an administrator",
        "already unbanned",
        "already-unbanned",
        "not banned",
        "not restricted",
        "user is not banned",
    )
    return any(marker in error_text for marker in benign_markers)


REJOIN_GROUP_PERMISSION_FAILURE_LIMIT = 3


def rejoin_delivery_should_skip_invite(member_status, restricted_has_access=True):
    if member_status in ("member", "administrator", "creator"):
        return True
    if member_status == "restricted" and restricted_has_access:
        return True
    return False


def rejoin_delivery_should_send_invite(member_status):
    return member_status in ("left", "kicked")


async def send_rejoin_invite_after_payment(telegram_id, expiry_date, source, stripe_event_id=None, stripe_subscription_id=None):
    raise RuntimeError("Stripe rejoin invites must be enqueued with enqueue_rejoin_invite_after_payment")


def is_undeliverable_user_error(error):
    if is_telegram_forbidden_error(error):
        return True
    if is_telegram_temporary_error(error):
        return False

    error_text = str(error).lower()
    undeliverable_markers = (
        "chat not found",
        "chatnotfound",
        "bot was blocked",
        "user is deactivated",
        "bot can't initiate conversation",
        "forbidden",
    )
    return any(marker in error_text for marker in undeliverable_markers)


OUTBOX_UNKNOWN_FAILURE_LIMIT = 10
OUTBOX_MISSING_FREE_LESSON_VIDEO_LIMIT = 3
OUTBOX_BAD_REQUEST_RETRY_LIMIT = 5
OUTBOX_RETRY_WARNING_AGE = timedelta(hours=1)
OUTBOX_RETRY_ESCALATED_AGE = timedelta(hours=6)
OUTBOX_CRITICAL_RETRY_ATTEMPT_THRESHOLD = 3

OUTBOX_CRITICAL_DELIVERY_TYPES = frozenset({
    ACCESS_RESTORE_DELIVERY_TYPE,
    "stripe_user_message",
    "subscription_expired_user",
    "stripe_rejoin_invite",
    "stripe_rejoin_check",
    GIFT_CERTIFICATE_BUYER,
    GIFT_CERTIFICATE_RECIPIENT,
    "gift_redeemed_recipient",
    "gift_refunded_buyer",
    "gift_refunded_recipient",
})


class MissingFreeLessonVideoError(RuntimeError):
    pass


def clean_error_reason(error, limit=180):
    text = " ".join(str(error).replace("\n", " ").replace("\r", " ").split())
    return text[:limit] if text else type(error).__name__


def classify_delivery_error(error, attempt_count=1, sending_user_message=True):
    if isinstance(error, MissingFreeLessonVideoError):
        permanently_failed = int(attempt_count) >= OUTBOX_MISSING_FREE_LESSON_VIDEO_LIMIT
        return {
            "blocked": False,
            "retryable": not permanently_failed,
            "permanently_failed": permanently_failed,
            "retry_delay_minutes": telegram_retry_delay_minutes(error, attempt_count),
            "reason": "free_lesson_video_id_missing",
        }

    if isinstance(error, TelegramForbiddenError):
        if sending_user_message:
            return {
                "blocked": True,
                "retryable": False,
                "permanently_failed": True,
                "retry_delay_minutes": None,
                "reason": "telegram_forbidden_user_delivery",
            }
        permanently_failed = int(attempt_count) >= REJOIN_GROUP_PERMISSION_FAILURE_LIMIT
        return {
            "blocked": False,
            "retryable": not permanently_failed,
            "permanently_failed": permanently_failed,
            "retry_delay_minutes": None if permanently_failed else telegram_retry_delay_minutes(error, attempt_count),
            "reason": "telegram_forbidden_group_stage",
        }

    if isinstance(error, TelegramRetryAfter):
        return {
            "blocked": False,
            "retryable": True,
            "permanently_failed": False,
            "retry_delay_minutes": telegram_retry_delay_minutes(error, attempt_count),
            "reason": "telegram_retry_after",
        }

    if isinstance(error, TelegramNetworkError):
        return {
            "blocked": False,
            "retryable": True,
            "permanently_failed": False,
            "retry_delay_minutes": telegram_retry_delay_minutes(error, attempt_count),
            "reason": "telegram_network_error",
        }

    if isinstance(error, TelegramBadRequest):
        undeliverable = sending_user_message and is_undeliverable_user_error(error)
        permanently_failed = undeliverable or int(attempt_count) >= OUTBOX_BAD_REQUEST_RETRY_LIMIT
        return {
            "blocked": bool(undeliverable),
            "retryable": not permanently_failed,
            "permanently_failed": permanently_failed,
            "retry_delay_minutes": None if permanently_failed else telegram_retry_delay_minutes(error, attempt_count),
            "reason": "telegram_bad_request_terminal" if permanently_failed else "telegram_bad_request_retryable",
        }

    permanently_failed = int(attempt_count) >= OUTBOX_UNKNOWN_FAILURE_LIMIT
    return {
        "blocked": False,
        "retryable": not permanently_failed,
        "permanently_failed": permanently_failed,
        "retry_delay_minutes": None if permanently_failed else telegram_retry_delay_minutes(error, attempt_count),
        "reason": "unknown_error_limit_reached" if permanently_failed else "unknown_error_retryable",
    }


def log_outbox_delivery_failure(delivery_key, delivery_type, attempt_count, error, decision):
    delay = decision.get("retry_delay_minutes")
    logging.warning(
        "OUTBOX_DELIVERY_FAILED: delivery_type=%s, attempt_count=%s, delivery_key_hash=%s, "
        "error_class=%s, reason=%s, retryable=%s, permanent=%s, next_retry_delay=%s",
        delivery_type,
        attempt_count,
        safe_delivery_hash(delivery_key),
        type(error).__name__,
        clean_error_reason(decision.get("reason") or error),
        bool(decision.get("retryable")),
        bool(decision.get("permanently_failed")),
        f"{delay}m" if delay is not None else "none",
    )


def claim_outbox_retry_escalation(cur, delivery_key, delivery_type, attempt_count, now=None):
    now = now or datetime.utcnow()
    delivery_hash = safe_delivery_hash(delivery_key)
    first_seen_key = f"outbox-retry-observed:{delivery_hash}"
    cur.execute(
        """
        INSERT INTO admin_alerts (alert_key, severity, text, status, delivered_admin_ids, created_at, updated_at)
        SELECT %s, 'INFO', %s, 'observed', '', %s, %s
        WHERE pg_try_advisory_xact_lock(hashtext(%s))
          AND NOT EXISTS (
              SELECT 1 FROM admin_alerts WHERE alert_key = %s
          )
        """,
        (
            first_seen_key,
            f"retryable outbox failure: {delivery_type}",
            now,
            now,
            first_seen_key,
            first_seen_key,
        ),
    )
    cur.execute(
        "SELECT MIN(created_at) FROM admin_alerts WHERE alert_key = %s",
        (first_seen_key,),
    )
    row = cur.fetchone()
    first_seen_at = row[0] if row and row[0] else now
    retry_age = max(timedelta(0), now - first_seen_at)
    critical = delivery_type in OUTBOX_CRITICAL_DELIVERY_TYPES
    if retry_age >= OUTBOX_RETRY_ESCALATED_AGE:
        stage = "age_6h"
    elif retry_age >= OUTBOX_RETRY_WARNING_AGE:
        stage = "age_1h"
    elif critical and int(attempt_count) >= OUTBOX_CRITICAL_RETRY_ATTEMPT_THRESHOLD:
        stage = "critical_attempt_3"
    else:
        stage = None
    return {
        "stage": stage,
        "retry_age": retry_age,
        "critical": critical,
        "delivery_hash": delivery_hash,
    }


async def notify_retryable_outbox_failure(
    delivery_key,
    delivery_type,
    attempt_count,
    error,
    decision,
    escalation=None,
    telegram_id=None,
    payload=None,
):
    conn = None
    cur = None
    if escalation is None:
        try:
            conn = get_db_conn()
            cur = conn.cursor()
            escalation = claim_outbox_retry_escalation(
                cur,
                delivery_key,
                delivery_type,
                attempt_count,
            )
            conn.commit()
        except Exception:
            if conn:
                conn.rollback()
            logging.error(
                "OUTBOX_RETRY_ESCALATION_STATE_FAILED: delivery_type=%s, delivery_key_hash=%s, "
                "attempt_count=%s, error_class=%s",
                delivery_type,
                safe_delivery_hash(delivery_key),
                attempt_count,
                type(error).__name__,
            )
            return
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    stage = escalation["stage"]
    age_minutes = int(escalation["retry_age"].total_seconds() // 60)
    logging.warning(
        "OUTBOX_RETRY_ALERT_POLICY: delivery_type=%s, delivery_key_hash=%s, attempt_count=%s, "
        "retry_age_minutes=%s, error_class=%s, escalation_stage=%s",
        delivery_type,
        escalation["delivery_hash"],
        attempt_count,
        age_minutes,
        type(error).__name__,
        stage or "log_only",
    )
    if not stage:
        return
    await notify_admins(
        render_critical_delivery_alert(
            delivery_type=delivery_type,
            delivery_key=delivery_key,
            telegram_id=telegram_id,
            reason=decision.get("reason"),
            blocked=False,
            retryable=True,
            safe_user_ref=safe_log_id,
            payload=payload,
        ),
        alert_key=f"outbox-retry:{escalation['delivery_hash']}:{stage}",
        severity="WARNING",
        dedupe_forever=True,
    )


async def notify_permanent_outbox_failure(
    delivery_key, delivery_type, attempt_count, error, blocked=False,
    telegram_id=None, payload=None, reason=None,
):
    critical = delivery_type in OUTBOX_CRITICAL_DELIVERY_TYPES
    if not critical:
        logging.warning(
            "OUTBOX_PERMANENT_ALERT_LOG_ONLY: delivery_type=%s, delivery_key_hash=%s, "
            "attempt_count=%s, blocked=%s, error_class=%s",
            delivery_type,
            safe_delivery_hash(delivery_key),
            attempt_count,
            bool(blocked),
            type(error).__name__,
        )
        return
    delivery_hash = safe_delivery_hash(delivery_key)
    await notify_admins(
        render_critical_delivery_alert(
            delivery_type=delivery_type,
            delivery_key=delivery_key,
            telegram_id=telegram_id,
            reason=reason,
            blocked=blocked,
            retryable=False,
            safe_user_ref=safe_log_id,
            payload=payload,
        ),
        alert_key=f"outbox-permanent:{delivery_hash}",
        severity="CRITICAL",
        dedupe_forever=True,
    )


async def notify_terminal_free_lesson_delivery_error(delivery_key, delivery_type, attempt_count, error, decision):
    if not isinstance(error, MissingFreeLessonVideoError):
        return
    if not decision.get("permanently_failed"):
        return
    await notify_admins(
        "FREE_LESSON_VIDEO_ID отсутствует. Доставка бесплатного урока стала terminal после лимита попыток.\n\n"
        f"delivery_type: {delivery_type}\n"
        f"delivery_hash: {safe_delivery_hash(delivery_key)}\n"
        f"attempt_count: {attempt_count}",
        alert_key=f"free_lesson_video_missing:{safe_delivery_hash(delivery_key)}",
        severity="CRITICAL",
    )


# --- АВТОМАТИЧЕСКАЯ ПРОВЕРКА ПОДПИСОК (КРОН) ---
def has_valid_stripe_subscription_id(stripe_subscription_id):
    if not stripe_subscription_id:
        return False

    subscription_id = str(stripe_subscription_id).strip()
    if not subscription_id:
        return False

    if subscription_id.lower() in ("none", "null", "нет"):
        return False

    return subscription_id.startswith("sub_")


async def get_open_invoice_url_for_subscription(stripe_subscription_id):
    try:
        invoices = await asyncio.to_thread(
            stripe.Invoice.list,
            subscription=stripe_subscription_id,
            status="open",
            limit=5
        )
        invoice_data = getattr(invoices, "data", None) or []
        for invoice in invoice_data:
            hosted_invoice_url = getattr(invoice, "hosted_invoice_url", None)
            if hosted_invoice_url:
                return hosted_invoice_url, getattr(invoice, "id", None)
    except Exception as e:
        error_ref = safe_admin_error_reference("open_invoice_lookup", e)
        logging.error(
            "OPEN_INVOICE_LOOKUP_FAILED: stripe_subscription_id=%s, error_type=%s, error_ref=%s",
            safe_log_id(stripe_subscription_id),
            type(e).__name__,
            error_ref,
        )

    return None, None


async def create_billing_portal_url(stripe_customer_id):
    if not stripe_customer_id:
        return None

    try:
        portal = await asyncio.to_thread(
            stripe.billing_portal.Session.create,
            customer=stripe_customer_id,
            return_url="https://t.me/Natalia_SoulFit_bot"
        )
        return getattr(portal, "url", None)
    except Exception as e:
        error_ref = safe_admin_error_reference("billing_portal_create", e)
        logging.error(
            "BILLING_PORTAL_CREATE_FAILED: stripe_customer_id=%s, error_type=%s, error_ref=%s",
            safe_log_id(stripe_customer_id),
            type(e).__name__,
            error_ref,
        )
        return None


async def send_existing_subscription_action(callback, user_id, stripe_subscription_id, stripe_customer_id, status, current_period_end=None):
    invoice_url, invoice_id = await get_open_invoice_url_for_subscription(stripe_subscription_id)
    if invoice_url:
        kb = inline_keyboard([[
            InlineKeyboardButton(text="💳 Оплатить открытый счёт", url=invoice_url)
        ]])
        await callback.message.answer(
            "У вас уже есть подписка Stripe, поэтому новую подписку я не создаю.\n\n"
            "Stripe ждёт оплату открытого счёта. Нажмите кнопку ниже, чтобы оплатить его.",
            reply_markup=kb
        )
        logging.warning(
            "EXISTING_STRIPE_SUBSCRIPTION_FOUND_CHECKOUT_BLOCKED: telegram_id=%s, "
            "stripe_subscription_id=%s, stripe_customer_id=%s, status=%s, action=%s, invoice_id=%s",
            user_id,
            safe_log_id(stripe_subscription_id),
            safe_log_id(stripe_customer_id),
            status,
            "open_invoice_url_sent",
            safe_log_id(invoice_id),
        )
        return True

    portal_url = await create_billing_portal_url(stripe_customer_id)
    if portal_url:
        kb = inline_keyboard([[
            InlineKeyboardButton(text="💳 Управлять оплатой", url=portal_url)
        ]])
        expiry_text = (
            datetime.utcfromtimestamp(current_period_end).strftime("%d.%m.%Y %H:%M")
            if current_period_end else "не определён"
        )
        await callback.message.answer(
            "У вас уже есть подписка Stripe, поэтому новую подписку я не создаю.\n\n"
            f"Статус подписки: {status or 'неизвестен'}.\n"
            f"Текущий период до: {expiry_text}.\n\n"
            "Через кнопку ниже можно оплатить счёт, сменить карту или управлять подпиской.",
            reply_markup=kb
        )
        logging.warning(
            "EXISTING_STRIPE_SUBSCRIPTION_FOUND_CHECKOUT_BLOCKED: telegram_id=%s, "
            "stripe_subscription_id=%s, stripe_customer_id=%s, status=%s, action=%s",
            user_id,
            safe_log_id(stripe_subscription_id),
            safe_log_id(stripe_customer_id),
            status,
            "billing_portal_sent",
        )
        return True

    await callback.message.answer(
        "У вас уже есть подписка Stripe, поэтому новую подписку я не создаю.\n\n"
        "Но мне не удалось сформировать ссылку для оплаты или смены карты. "
        "Пожалуйста, напишите администратору."
    )
    await notify_admins(
        "Checkout заблокирован, потому что у пользователя уже есть Stripe subscription, "
        "но не удалось создать invoice/billing portal ссылку.\n\n"
        f"telegram_id: {user_id}\n"
        f"stripe_customer_id: {safe_log_id(stripe_customer_id) or 'нет'}\n"
        f"stripe_subscription_id: {safe_log_id(stripe_subscription_id) or 'нет'}\n"
        f"status: {status or 'нет'}"
    )
    logging.warning(
        "EXISTING_STRIPE_SUBSCRIPTION_FOUND_CHECKOUT_BLOCKED: telegram_id=%s, "
        "stripe_subscription_id=%s, stripe_customer_id=%s, status=%s, action=%s",
        user_id,
        safe_log_id(stripe_subscription_id),
        safe_log_id(stripe_customer_id),
        status,
        "manual_admin_review_required",
    )
    return True


def claim_subscription_removal(cur, telegram_id, reason, owner_id=None, now=None, lease_minutes=30):
    owner_id = owner_id or OWNER_ID
    now = now or datetime.utcnow()
    lease_until = now + timedelta(minutes=lease_minutes)
    cur.execute(
        """
        SELECT status, lease_until, db_finalized_at, reason, access_expiry,
               stripe_subscription_id, attempt_count, created_at, updated_at
        FROM subscription_removal_events
        WHERE telegram_id = %s
        FOR UPDATE
        """,
        (int(telegram_id),),
    )
    row = cur.fetchone()
    if row:
        (
            current_status,
            current_lease_until,
            db_finalized_at,
            current_reason,
            current_access_expiry,
            current_subscription_id,
            current_attempt_count,
            current_created_at,
            current_updated_at,
        ) = row
        can_start_new_cycle = False
        can_rearm_superseded_cycle = False
        if current_status == "db_finalized" and db_finalized_at:
            cur.execute(
                """
                SELECT 1
                FROM users
                WHERE telegram_id = %s
                  AND (
                        last_payment_succeeded_at > %s
                        OR expiry_date > %s
                  )
                LIMIT 1
                """,
                (int(telegram_id), db_finalized_at, db_finalized_at),
            )
            can_start_new_cycle = cur.fetchone() is not None
        if current_status == "superseded" and current_access_expiry:
            cur.execute(
                """
                SELECT expiry_date
                FROM users
                WHERE telegram_id = %s
                  AND paid = TRUE
                  AND expiry_date IS NOT NULL
                  AND expiry_date < %s
                  AND expiry_date > %s
                FOR UPDATE
                """,
                (int(telegram_id), now, current_access_expiry),
            )
            rearm_user = cur.fetchone()
            if rearm_user:
                current_user_expiry = rearm_user[0]
                record_access_event_cur(
                    cur,
                    telegram_id,
                    "subscription_removal_cycle_superseded",
                    source="subscription_removal_rearm",
                    old_expiry=current_access_expiry,
                    new_expiry=current_user_expiry,
                    stripe_subscription_id=current_subscription_id,
                    notes=(
                        f"status=superseded; reason={current_reason or 'none'}; "
                        f"attempt_count={int(current_attempt_count or 0)}; "
                        f"created_at={current_created_at}; updated_at={current_updated_at}"
                    ),
                )
                can_rearm_superseded_cycle = True
                can_start_new_cycle = True
        if current_status in ("pending", "stripe_canceled", "telegram_failed") or (
            current_status == "processing"
            and current_lease_until
            and current_lease_until < now
        ) or current_status == "telegram_removed" or can_start_new_cycle:
            cur.execute(
                """
                UPDATE subscription_removal_events
                SET status = 'processing',
                    reason = CASE
                        WHEN %s THEN %s
                        WHEN subscription_removal_events.reason IN ('subscription_refund_reconciled', 'manual_access_revoked')
                            THEN subscription_removal_events.reason
                        ELSE %s
                    END,
                    owner_id = %s,
                    claimed_at = %s,
                    lease_until = %s,
                    last_error = NULL,
                    stripe_subscription_id = CASE WHEN %s
                        THEN (SELECT stripe_subscription_id FROM users WHERE telegram_id = %s)
                        ELSE COALESCE(
                            subscription_removal_events.stripe_subscription_id,
                            (SELECT stripe_subscription_id FROM users WHERE telegram_id = %s)
                        )
                    END,
                    access_expiry = CASE WHEN %s
                        THEN (SELECT expiry_date FROM users WHERE telegram_id = %s)
                        ELSE COALESCE(
                            subscription_removal_events.access_expiry,
                            (SELECT expiry_date FROM users WHERE telegram_id = %s)
                        )
                    END,
                    stripe_canceled_at = CASE WHEN %s THEN NULL ELSE stripe_canceled_at END,
                    telegram_banned_at = CASE WHEN %s THEN NULL ELSE telegram_banned_at END,
                    telegram_removed_at = CASE WHEN %s THEN NULL ELSE telegram_removed_at END,
                    db_finalized_at = CASE WHEN %s THEN NULL ELSE db_finalized_at END,
                    admin_notified_at = CASE WHEN %s THEN NULL ELSE admin_notified_at END,
                    revoke_started_at = CASE WHEN %s THEN NULL ELSE revoke_started_at END,
                    attempt_count = CASE WHEN %s THEN 1 ELSE attempt_count + 1 END,
                    created_at = CASE WHEN %s THEN %s ELSE created_at END,
                    updated_at = %s
                WHERE telegram_id = %s
                """,
                (
                    can_rearm_superseded_cycle,
                    reason,
                    reason,
                    owner_id,
                    now,
                    lease_until,
                    can_start_new_cycle,
                    int(telegram_id),
                    int(telegram_id),
                    can_start_new_cycle,
                    int(telegram_id),
                    int(telegram_id),
                    can_start_new_cycle,
                    can_start_new_cycle,
                    can_start_new_cycle,
                    can_start_new_cycle,
                    can_start_new_cycle,
                    can_rearm_superseded_cycle,
                    can_rearm_superseded_cycle,
                    can_rearm_superseded_cycle,
                    now,
                    now,
                    int(telegram_id),
                ),
            )
            if current_status == "telegram_removed" and not can_start_new_cycle:
                return "claimed_after_telegram_removed"
            return "claimed"
        if current_status == "db_finalized":
            return "already_finalized"
        if current_status == "processing":
            return "already_processing"
        return current_status

    cur.execute(
        """
        INSERT INTO subscription_removal_events (
            telegram_id, status, reason, owner_id, claimed_at, lease_until,
            attempt_count, stripe_subscription_id, access_expiry, created_at, updated_at
        )
        SELECT %s, 'processing', %s, %s, %s, %s, 1,
               users.stripe_subscription_id, users.expiry_date, %s, %s
        FROM users
        WHERE users.telegram_id = %s
        ON CONFLICT (telegram_id) DO NOTHING
        RETURNING telegram_id
        """,
        (int(telegram_id), reason, owner_id, now, lease_until, now, now, int(telegram_id)),
    )
    if cur.fetchone():
        return "claimed"
    return "not_claimed"


def mark_subscription_removal_status(cur, telegram_id, status, error_text=None):
    fields = {
        "telegram_removed": "telegram_removed_at = COALESCE(telegram_removed_at, NOW()),",
        "db_finalized": "db_finalized_at = COALESCE(db_finalized_at, NOW()),",
    }.get(status, "")
    lease_fields = (
        "owner_id = NULL, claimed_at = NULL, lease_until = NULL,"
        if status in ("cancelled", "not_due", "superseded")
        else ""
    )
    cur.execute(
        f"""
        UPDATE subscription_removal_events
        SET status = %s,
            {fields}
            {lease_fields}
            last_error = LEFT(%s, 1000),
            updated_at = NOW()
        WHERE telegram_id = %s
        """,
        (status, str(error_text) if error_text else None, int(telegram_id)),
    )


def fetch_subscription_removal_user(telegram_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                paid,
                expiry_date,
                stripe_subscription_id,
                payment_failed,
                payment_failed_at,
                grace_period_end,
                auto_renew,
                stripe_customer_id
            FROM users
            WHERE telegram_id = %s
        """, (int(telegram_id),))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def fetch_subscription_removal_context(telegram_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT stripe_subscription_id, access_expiry, stripe_canceled_at,
                   telegram_banned_at, telegram_removed_at, status
            FROM subscription_removal_events
            WHERE telegram_id = %s
            """,
            (int(telegram_id),),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def mark_subscription_stripe_canceled(telegram_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE subscription_removal_events
            SET stripe_canceled_at = COALESCE(stripe_canceled_at, NOW()),
                status = CASE
                    WHEN status IN ('telegram_removed', 'db_finalized') THEN status
                    ELSE 'stripe_canceled'
                END,
                last_error = NULL,
                updated_at = NOW()
            WHERE telegram_id = %s
            """,
            (int(telegram_id),),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def mark_subscription_telegram_banned(telegram_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE subscription_removal_events
            SET telegram_banned_at = COALESCE(telegram_banned_at, NOW()),
                updated_at = NOW()
            WHERE telegram_id = %s
            """,
            (int(telegram_id),),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def fetch_user_expiry_customer(telegram_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT expiry_date, stripe_customer_id FROM users WHERE telegram_id = %s",
            (int(telegram_id),)
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def set_subscription_reminder_sent(telegram_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET reminder_sent = TRUE WHERE telegram_id = %s", (int(telegram_id),))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


async def enqueue_due_first_purchase_recovery_reminders(limit=100):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        due_users = fetch_due_first_purchase_recovery_users(cur, limit=limit)
        enqueued = 0
        for row in due_users:
            telegram_id, latest_attempt_at = row[0], row[1]
            if enqueue_first_purchase_recovery_reminder(
                cur,
                telegram_id,
                latest_attempt_at,
                first_purchase_recovery_row_context(row),
            ):
                enqueued += 1
        conn.commit()
        logging.info(
            "FIRST_PURCHASE_RECOVERY_REMINDERS_ENQUEUED: due=%s, enqueued=%s",
            len(due_users),
            enqueued,
        )
        return {"due": len(due_users), "enqueued": enqueued}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def update_user_blocked_bot(telegram_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (int(telegram_id),))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def finalize_subscription_removal_in_db(
    telegram_id,
    expiry_date=None,
    subscription_cancelled_after_grace=False,
):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE users
            SET paid = FALSE,
                auto_renew = CASE WHEN %s THEN FALSE ELSE auto_renew END,
                payment_failed = FALSE,
                payment_failed_at = NULL,
                grace_period_end = NULL,
                reminder_sent = FALSE
            WHERE telegram_id = %s
        """, (bool(subscription_cancelled_after_grace), int(telegram_id)))
        final_text = None
        if subscription_cancelled_after_grace:
            final_text = (
                "Подписку не удалось продлить в течение 48 часов, поэтому она была отменена, "
                "а доступ в клуб завершён.\n\n"
                "Вы всегда сможете вернуться и оформить новую подписку снова."
            )
        enqueue_subscription_expired_user_message(cur, telegram_id, expiry_date, text=final_text)
        mark_subscription_removal_status(cur, telegram_id, "db_finalized")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def mark_subscription_removal_short(telegram_id, status, error_text=None):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        mark_subscription_removal_status(cur, telegram_id, status, error_text)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def subscription_refund_group_removal_still_due(telegram_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT reason
            FROM subscription_removal_events
            WHERE telegram_id = %s
            """,
            (int(telegram_id),),
        )
        reason_row = cur.fetchone()
        reason = reason_row[0] if reason_row else None
        if reason not in ("subscription_refund_reconciled", "manual_access_revoked"):
            return True
        if reason == "subscription_refund_reconciled":
            cur.execute(
                """
                SELECT access_revoked_at
                FROM subscription_refund_reconciliations
                WHERE telegram_id = %s
                  AND reconciliation_result = 'access_revoked'
                ORDER BY access_revoked_at DESC NULLS LAST, updated_at DESC
                LIMIT 1
                """,
                (int(telegram_id),),
            )
            reconciliation_row = cur.fetchone()
            if not reconciliation_row or not reconciliation_row[0]:
                return False
            revoked_at = reconciliation_row[0]
        else:
            cur.execute(
                """
                SELECT created_at
                FROM access_events
                WHERE telegram_id = %s
                  AND event_type = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (int(telegram_id), MANUAL_ACCESS_REVOKED_EVENT),
            )
            revoke_event_row = cur.fetchone()
            if not revoke_event_row or not revoke_event_row[0]:
                return False
            revoked_at = revoke_event_row[0]
        cur.execute(
            """
            SELECT paid, expiry_date
            FROM users
            WHERE telegram_id = %s
            """,
            (int(telegram_id),),
        )
        user_row = cur.fetchone()
        if not user_row:
            return True
        paid, expiry_date = user_row
        if paid is True and expiry_date and expiry_date > datetime.utcnow():
            return False
        cur.execute(
            """
            SELECT 1
            FROM payment_events
            WHERE telegram_id = %s
              AND payment_status = 'succeeded'
              AND created_at > %s
            LIMIT 1
            """,
            (int(telegram_id), revoked_at),
        )
        if cur.fetchone():
            return False
        cur.execute(
            """
            SELECT 1
            FROM access_events
            WHERE telegram_id = %s
              AND created_at > %s
              AND event_type NOT IN (%s, %s)
              AND (new_expiry IS NULL OR new_expiry > NOW())
            LIMIT 1
            """,
            (int(telegram_id), revoked_at, SUBSCRIPTION_REFUND_REVOKE_EVENT, MANUAL_ACCESS_REVOKED_EVENT),
        )
        return cur.fetchone() is None
    finally:
        cur.close()
        conn.close()


def ensure_failed_renewal_grace_from_recheck(telegram_id, stripe_subscription_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE users
            SET paid = TRUE,
                payment_failed = TRUE,
                payment_failed_at = COALESCE(payment_failed_at, NOW()),
                grace_period_end = COALESCE(
                    grace_period_end,
                    COALESCE(payment_failed_at, NOW()) + (%s * INTERVAL '1 hour')
                )
            WHERE telegram_id = %s
              AND stripe_subscription_id = %s
            RETURNING payment_failed_at, grace_period_end
            """,
            (PAYMENT_RETRY_GRACE_HOURS, int(telegram_id), stripe_subscription_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        payment_failed_at, grace_period_end = row
        if grace_period_end and datetime.utcnow() < grace_period_end:
            enqueue_failed_renewal_user_message(
                cur,
                telegram_id,
                stripe_subscription_id,
                payment_failed_at,
            )
        conn.commit()
        logging.warning(
            "FAILED_RENEWAL_GRACE_CREATED_FROM_STRIPE_RECHECK: telegram_id=%s, "
            "subscription_id=%s, payment_failed_at=%s, grace_until=%s",
            telegram_id,
            safe_log_id(stripe_subscription_id),
            payment_failed_at,
            grace_period_end,
        )
        return payment_failed_at, grace_period_end
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


async def refresh_active_stripe_subscription(telegram_id, stripe_subscription_id, cur=None):
    if not has_valid_stripe_subscription_id(stripe_subscription_id):
        logging.info(
            f"NO_STRIPE_SUBSCRIPTION_ID — proceed to removal. telegram_id={telegram_id}, "
            f"stripe_subscription_id={safe_log_id(stripe_subscription_id)}"
        )
        return False

    try:
        subscription = await asyncio.to_thread(stripe.Subscription.retrieve, stripe_subscription_id)
        status = getattr(subscription, 'status', None)
        current_period_end = getattr(subscription, 'current_period_end', None)

        if status in ('past_due', 'unpaid'):
            grace_row = ensure_failed_renewal_grace_from_recheck(
                telegram_id,
                stripe_subscription_id,
            )
            if not grace_row:
                logging.warning(
                    "FAILED_RENEWAL_GRACE_RECHECK_IDENTITY_CHANGED: telegram_id=%s, subscription_id=%s",
                    telegram_id,
                    safe_log_id(stripe_subscription_id),
                )
                return "STRIPE_CHECK_FAILED"
            _, grace_until = grace_row
            if grace_until and datetime.utcnow() < grace_until:
                return "STRIPE_GRACE_ACTIVE"
            logging.warning(
                "FAILED_RENEWAL_GRACE_EXPIRED_AFTER_STRIPE_RECHECK: telegram_id=%s, "
                "subscription_id=%s, grace_until=%s",
                telegram_id,
                safe_log_id(stripe_subscription_id),
                grace_until,
            )
            return False

        if status in ('active', 'trialing') and not current_period_end:
            invoices = await asyncio.to_thread(
                stripe.Invoice.list,
                subscription=stripe_subscription_id,
                limit=5
            )
            invoice_data = getattr(invoices, 'data', None) or []

            for invoice in invoice_data:
                invoice_status = getattr(invoice, 'status', None)
                if invoice_status != 'paid':
                    continue

                lines = getattr(invoice, 'lines', None)
                lines_data = getattr(lines, 'data', None) or []
                first_line = lines_data[0] if lines_data else None
                period = getattr(first_line, 'period', None)
                period_end = getattr(period, 'end', None)

                if period_end:
                    current_period_end = period_end
                    break

        if status in ('active', 'trialing') and not current_period_end:
            logging.warning(
                f"Stripe subscription active/trialing, но period_end не найден. "
                f"telegram_id={telegram_id}, stripe_subscription_id={safe_log_id(stripe_subscription_id)}"
            )
            if cur is not None:
                cur.execute("""
                    UPDATE users
                    SET payment_failed = FALSE,
                        payment_failed_at = NULL,
                        last_payment_succeeded_at = NOW(),
                        grace_period_end = NULL,
                        reminder_sent = FALSE,
                        auto_renew = TRUE,
                        blocked_bot = FALSE
                    WHERE telegram_id = %s
                """, (int(telegram_id),))
                cancel_failed_renewal_user_messages(cur, telegram_id, "stripe_active_recheck")
            else:
                conn = get_db_conn()
                active_cur = conn.cursor()
                try:
                    active_cur.execute("""
                        UPDATE users
                        SET payment_failed = FALSE,
                            payment_failed_at = NULL,
                            last_payment_succeeded_at = NOW(),
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            auto_renew = TRUE,
                            blocked_bot = FALSE
                        WHERE telegram_id = %s
                    """, (int(telegram_id),))
                    cancel_failed_renewal_user_messages(active_cur, telegram_id, "stripe_active_recheck")
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    active_cur.close()
                    conn.close()
            return "STRIPE_ACTIVE"

        if status in ('active', 'trialing') and current_period_end:
            new_expiry = datetime.utcfromtimestamp(current_period_end)

            if new_expiry > datetime.utcnow():
                if cur is not None:
                    cur.execute("""
                        UPDATE users
                        SET paid = TRUE,
                            expiry_date = %s,
                            payment_failed = FALSE,
                            payment_failed_at = NULL,
                            last_payment_succeeded_at = NOW(),
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            auto_renew = TRUE,
                            blocked_bot = FALSE
                        WHERE telegram_id = %s
                    """, (new_expiry, int(telegram_id)))
                    cancel_failed_renewal_user_messages(cur, telegram_id, "stripe_active_recheck")
                else:
                    conn = get_db_conn()
                    active_cur = conn.cursor()
                    try:
                        active_cur.execute("""
                            UPDATE users
                            SET paid = TRUE,
                                expiry_date = %s,
                                payment_failed = FALSE,
                                payment_failed_at = NULL,
                                last_payment_succeeded_at = NOW(),
                                grace_period_end = NULL,
                                reminder_sent = FALSE,
                                auto_renew = TRUE,
                                blocked_bot = FALSE
                            WHERE telegram_id = %s
                        """, (new_expiry, int(telegram_id)))
                        cancel_failed_renewal_user_messages(active_cur, telegram_id, "stripe_active_recheck")
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
                    finally:
                        active_cur.close()
                        conn.close()

                logging.info(
                    f"Пользователь {telegram_id} не удален: Stripe подписка активна до {new_expiry} UTC."
                )
                return "STRIPE_ACTIVE"

    except Exception as e:
        logging.error(
            f"Не удалось перепроверить Stripe-подписку {safe_log_id(stripe_subscription_id)} "
            f"для {telegram_id}: {e}"
        )
        error_ref = safe_admin_error_reference("stripe_recheck_before_removal", e)
        await notify_admins(
            f"Не смогла перепроверить Stripe перед удалением пользователя {telegram_id}.\n"
            f"subscription_id: {safe_log_id(stripe_subscription_id) or 'нет'}\n"
            f"Ошибка: временный сбой проверки. ref: {error_ref}\n\n"
            "Пользователь пока НЕ удален автоматически. Проверьте вручную."
        )
        return "STRIPE_CHECK_FAILED"

    return False


async def cancel_failed_renewal_subscription_after_grace(telegram_id, stripe_subscription_id):
    safe_subscription_id = safe_log_id(stripe_subscription_id) or "none"
    logging.warning(
        "FAILED_RENEWAL_SUBSCRIPTION_CANCEL_ATTEMPTED: telegram_id=%s, subscription_id=%s",
        telegram_id,
        safe_subscription_id,
    )
    try:
        subscription = await asyncio.to_thread(
            stripe.Subscription.retrieve,
            stripe_subscription_id,
        )
        status = getattr(subscription, "status", None)
        if status in ("active", "trialing"):
            recovery_status = await refresh_active_stripe_subscription(
                telegram_id,
                stripe_subscription_id,
            )
            if recovery_status != "STRIPE_ACTIVE":
                logging.warning(
                    "FAILED_RENEWAL_SUBSCRIPTION_RECOVERY_SYNC_DEFERRED: telegram_id=%s, "
                    "subscription_id=%s, recovery_status=%s",
                    telegram_id,
                    safe_subscription_id,
                    recovery_status or "unknown",
                )
                return "recovery_sync_deferred"
            logging.info(
                "FAILED_RENEWAL_SUBSCRIPTION_CANCEL_SKIPPED_RECOVERED: telegram_id=%s, "
                "subscription_id=%s, status=%s",
                telegram_id,
                safe_subscription_id,
                status,
            )
            return "recovered"
        if status == "canceled":
            logging.info(
                "FAILED_RENEWAL_SUBSCRIPTION_CANCEL_ALREADY_COMPLETED: telegram_id=%s, "
                "subscription_id=%s",
                telegram_id,
                safe_subscription_id,
            )
            return "already_canceled"
        if status not in ("past_due", "unpaid"):
            logging.warning(
                "FAILED_RENEWAL_SUBSCRIPTION_CANCEL_SKIPPED_STATUS: telegram_id=%s, "
                "subscription_id=%s, status=%s",
                telegram_id,
                safe_subscription_id,
                status or "unknown",
            )
            return "status_not_cancellable"

        latest_invoice = getattr(subscription, "latest_invoice", None)
        if latest_invoice:
            if isinstance(latest_invoice, str):
                latest_invoice = await asyncio.to_thread(
                    stripe.Invoice.retrieve,
                    latest_invoice,
                )
            invoice_status = getattr(latest_invoice, "status", None)
            invoice_paid = bool(getattr(latest_invoice, "paid", False))
            if invoice_status == "paid" or invoice_paid:
                recovery_status = await refresh_active_stripe_subscription(
                    telegram_id,
                    stripe_subscription_id,
                )
                logging.warning(
                    "FAILED_RENEWAL_SUBSCRIPTION_CANCEL_SKIPPED_PAID_INVOICE: telegram_id=%s, "
                    "subscription_id=%s, recovery_status=%s",
                    telegram_id,
                    safe_subscription_id,
                    recovery_status or "unknown",
                )
                return (
                    "recovered"
                    if recovery_status == "STRIPE_ACTIVE"
                    else "recovery_sync_deferred"
                )

        # Stripe has no conditional delete primitive. The live subscription and
        # latest-invoice reads above are the final recovery guards; later Stripe
        # webhooks remain authoritative for reconciliation of an unavoidable race.
        await asyncio.to_thread(stripe.Subscription.delete, stripe_subscription_id)
        logging.info(
            "FAILED_RENEWAL_SUBSCRIPTION_CANCEL_SUCCEEDED: telegram_id=%s, subscription_id=%s",
            telegram_id,
            safe_subscription_id,
        )
        return "canceled"
    except Exception as error:
        error_ref = safe_admin_error_reference("failed_renewal_subscription_cancel", error)
        logging.error(
            "FAILED_RENEWAL_SUBSCRIPTION_CANCEL_FAILED: telegram_id=%s, subscription_id=%s, "
            "error_class=%s, error_ref=%s",
            telegram_id,
            safe_subscription_id,
            type(error).__name__,
            error_ref,
        )
        await notify_admins(
            "Не удалось отменить Stripe subscription после истечения grace period.\n\n"
            f"telegram_id: {telegram_id}\n"
            f"subscription_id: {safe_subscription_id}\n"
            f"Ошибка: временный сбой отмены. ref: {error_ref}\n\n"
            "Доступ и участие в группе пока не изменены; scheduler повторит попытку."
        )
        return "cancel_failed"

async def ban_user_logic(telegram_id, cur=None):
    claim_conn = get_db_conn()
    claim_cur = claim_conn.cursor()
    try:
        claim = claim_subscription_removal(claim_cur, telegram_id, "subscription_expired")
        claim_conn.commit()
    except Exception:
        claim_conn.rollback()
        raise
    finally:
        claim_cur.close()
        claim_conn.close()

    if claim not in ("claimed", "claimed_after_telegram_removed"):
        logging.info(
            "USER_REMOVE_CLAIM_SKIPPED: telegram_id=%s, claim=%s",
            telegram_id,
            claim,
        )
        return claim

    user = fetch_subscription_removal_user(telegram_id)
    removal_context = fetch_subscription_removal_context(telegram_id)

    if not user:
        logging.warning(
            "USER_REMOVE_SKIPPED_SAFETY_CHECK: telegram_id=%s, reason=%s, paid=%s, "
            "expiry_date=%s, grace=%s, auto_renew=%s, stripe_subscription_id=%s",
            telegram_id, "user_not_found", None, None, None, None, None
        )
        mark_subscription_removal_short(telegram_id, "db_finalized", "user_not_found")
        return "not_found"

    (
        paid,
        expiry_date,
        stripe_subscription_id,
        payment_failed,
        payment_failed_at,
        grace_period_end,
        auto_renew,
        stripe_customer_id,
    ) = user
    (
        removal_subscription_id,
        removal_access_expiry,
        stripe_canceled_at,
        telegram_banned_at,
        telegram_removed_at,
        removal_status,
    ) = removal_context or (None, None, None, None, None, None)
    effective_subscription_id = removal_subscription_id or stripe_subscription_id
    effective_expiry = removal_access_expiry or expiry_date
    now = datetime.utcnow()
    reason = "subscription_expired"
    grace = grace_period_end
    failed_renewal_grace_expired = bool(
        stripe_canceled_at
        or (
            payment_failed
            and grace_period_end
            and grace_period_end <= now
            and has_valid_stripe_subscription_id(effective_subscription_id)
        )
    )

    if paid and expiry_date and expiry_date > now:
        if telegram_banned_at:
            try:
                await bot.unban_chat_member(
                    chat_id=int(GROUP_ID),
                    user_id=int(telegram_id),
                    only_if_banned=True,
                )
            except Exception as error:
                mark_subscription_removal_short(telegram_id, "telegram_failed", error)
                return "unban_failed"
        logging.warning(
            "USER_REMOVE_SKIPPED_SAFETY_CHECK: telegram_id=%s, reason=%s, paid=%s, "
            "expiry_date=%s, grace=%s, auto_renew=%s, stripe_subscription_id=%s",
            telegram_id, "active_access_in_db", paid, expiry_date, grace, auto_renew, stripe_subscription_id
        )
        mark_subscription_removal_short(telegram_id, "superseded", "active_access_in_db")
        return "active_in_db"

    if payment_failed and payment_failed_at:
        retry_until = payment_failed_at + timedelta(hours=PAYMENT_RETRY_GRACE_HOURS)
        if now < retry_until:
            logging.warning(
                "USER_REMOVE_SKIPPED_RECENT_PAYMENT_FAILURE: telegram_id=%s, email=%s, "
                "payment_failed_at=%s, grace_until=%s, expiry_date=%s, stripe_subscription_id=%s",
                telegram_id, None, payment_failed_at, retry_until, expiry_date, stripe_subscription_id
            )
            mark_subscription_removal_short(telegram_id, "pending", "recent_payment_failure")
            return "recent_payment_failure"

    if grace_period_end and now < grace_period_end:
        logging.warning(
            "USER_REMOVE_SKIPPED_SAFETY_CHECK: telegram_id=%s, reason=%s, paid=%s, "
            "expiry_date=%s, grace=%s, auto_renew=%s, stripe_subscription_id=%s",
            telegram_id, "grace_period_active", paid, expiry_date, grace_period_end, auto_renew, stripe_subscription_id
        )
        mark_subscription_removal_short(telegram_id, "pending", "grace_period_active")
        return "grace_active"

    if auto_renew and not has_valid_stripe_subscription_id(stripe_subscription_id):
        logging.warning(
            "UNLINKED_STRIPE_NEEDS_MANUAL_REVIEW: telegram_id=%s, paid=%s, expiry_date=%s, "
            "grace=%s, auto_renew=%s, stripe_customer_id=%s, stripe_subscription_id=%s, reason=%s",
            telegram_id,
            paid,
            expiry_date,
            grace_period_end,
            auto_renew,
            stripe_customer_id,
            stripe_subscription_id,
            "auto_renew_without_valid_subscription_id",
        )
        await notify_admins(
            "Пользователь НЕ удален: включен auto_renew, но Stripe-связка неполная.\n\n"
            f"telegram_id: {telegram_id}\n"
            f"paid: {paid}\n"
            f"expiry_date: {expiry_date}\n"
            f"grace_period_end: {grace_period_end or 'нет'}\n"
            f"stripe_customer_id: {safe_log_id(stripe_customer_id) or 'нет'}\n"
            f"stripe_subscription_id: {safe_log_id(stripe_subscription_id) or 'нет'}\n\n"
            "Нужно вручную проверить Stripe и связать пользователя командой "
            "/link_stripe_user <telegram_id> <customer_id> <subscription_id>."
        )
        mark_subscription_removal_short(telegram_id, "pending", "auto_renew_without_valid_subscription_id")
        return "STRIPE_UNLINKED_REVIEW"

    if auto_renew and has_valid_stripe_subscription_id(stripe_subscription_id):
        logging.warning(
            "USER_REMOVE_STRIPE_RECHECK_REQUIRED: telegram_id=%s, reason=%s, paid=%s, "
            "expiry_date=%s, grace=%s, auto_renew=%s, stripe_subscription_id=%s",
            telegram_id, "auto_renew_with_stripe_subscription_needs_recheck", paid, expiry_date,
            grace_period_end, auto_renew, stripe_subscription_id
        )

    if not stripe_canceled_at:
        stripe_guard_status = await refresh_active_stripe_subscription(
            telegram_id,
            effective_subscription_id,
        )
        if stripe_guard_status:
            mark_subscription_removal_short(telegram_id, "pending", stripe_guard_status)
            return stripe_guard_status

    if failed_renewal_grace_expired and not stripe_canceled_at:
        cancellation_status = await cancel_failed_renewal_subscription_after_grace(
            telegram_id,
            effective_subscription_id,
        )
        if cancellation_status == "recovered":
            mark_subscription_removal_short(telegram_id, "pending", "STRIPE_ACTIVE")
            return "STRIPE_ACTIVE"
        if cancellation_status not in ("canceled", "already_canceled"):
            mark_subscription_removal_short(
                telegram_id,
                "pending",
                f"stripe_cancellation_{cancellation_status}",
            )
            return "stripe_cancel_failed"
        mark_subscription_stripe_canceled(telegram_id)
        stripe_canceled_at = datetime.utcnow()

    if not subscription_refund_group_removal_still_due(telegram_id):
        mark_subscription_removal_short(telegram_id, "superseded", "access_revoke_no_longer_current")
        return "access_revoke_no_longer_current"

    if not telegram_banned_at:
        try:
            chat_member = await bot.get_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
            telegram_status = getattr(chat_member, "status", None)
            if telegram_status in ("administrator", "creator"):
                logging.warning(
                    "USER_REMOVE_SKIPPED_TELEGRAM_ADMIN: telegram_id=%s, chat_id=%s, telegram_status=%s",
                    telegram_id,
                    GROUP_ID,
                    telegram_status,
                )
                mark_subscription_removal_short(telegram_id, "pending", "telegram_admin")
                return "telegram_admin"
            if telegram_status in ("kicked", "banned"):
                mark_subscription_telegram_banned(telegram_id)
                telegram_banned_at = datetime.utcnow()
        except Exception as e:
            logging.critical(
                "USER_REMOVE_SKIPPED_TELEGRAM_STATUS_ERROR: telegram_id=%s, chat_id=%s, error=%s",
                telegram_id,
                GROUP_ID,
                str(e),
                exc_info=True,
            )
            error_ref = safe_admin_error_reference("telegram_status_before_removal", e)
            await notify_admins(
                "Критично: не удалось проверить Telegram-статус перед удалением пользователя.\n\n"
                f"telegram_id: {telegram_id}\n"
                f"Ошибка: проверка не выполнена. ref: {error_ref}\n\n"
                "Пользователь НЕ удалён автоматически."
            )
            mark_subscription_removal_short(telegram_id, "pending", f"telegram_status_error: {e}")
            return "telegram_status_error"

    if claim == "claimed_after_telegram_removed":
        finalize_subscription_removal_in_db(
            telegram_id,
            effective_expiry,
            subscription_cancelled_after_grace=failed_renewal_grace_expired,
        )
        return "db_finalized"

    # 1. Пытаемся удалить пользователя из группы
    status = "removed"
    logging.warning(
        "USER_REMOVE_ATTEMPT: telegram_id=%s, username=%s, chat_id=%s, reason=%s, "
        "paid=%s, expiry_date=%s, grace=%s, auto_renew=%s, stripe_subscription_id=%s",
        telegram_id,
        None,
        GROUP_ID,
        reason,
        paid,
        expiry_date,
        grace,
        auto_renew,
        stripe_subscription_id,
    )
    try:
        if not telegram_banned_at:
            await bot.ban_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
            mark_subscription_telegram_banned(telegram_id)
        try:
            await bot.unban_chat_member(
                chat_id=int(GROUP_ID),
                user_id=int(telegram_id),
                only_if_banned=True,
            )
            unban_result = "unban_success"
        except Exception as e:
            error_ref = safe_admin_error_reference("unban_after_remove", e)
            logging.error(
                "USER_UNBAN_AFTER_REMOVE_FAILED: telegram_id=%s, username=%s, chat_id=%s, reason=%s, error=%s",
                telegram_id, None, GROUP_ID, reason, str(e), exc_info=True
            )
            await notify_admins(
                f"Пользователь {telegram_id} удален из группы, но не удалось снять бан.\n"
                f"Ошибка: действие не выполнено. ref: {error_ref}"
            )
            mark_subscription_removal_short(telegram_id, "telegram_failed", e)
            return "unban_failed"
        logging.info(
            "USER_REMOVED_FROM_GROUP: telegram_id=%s, username=%s, chat_id=%s, reason=%s, result=%s",
            telegram_id, None, GROUP_ID, reason, unban_result
        )
        mark_subscription_removal_short(telegram_id, "telegram_removed")
        logging.info(
            "ADMIN_NOTIFICATION_SUPPRESSED: category=expected_access_removal, "
            "result=removed, telegram_id=%s, reason=%s, subscription_ref=%s",
            telegram_id,
            reason,
            safe_log_id(stripe_subscription_id) or "none",
        )
    except Exception as e:
        logging.error(
            "USER_REMOVE_FAILED: telegram_id=%s, username=%s, chat_id=%s, reason=%s, error=%s",
            telegram_id, None, GROUP_ID, reason, str(e), exc_info=True
        )
        error_ref = safe_admin_error_reference("remove_user_from_group", e)
        await notify_admins(
            f"Не удалось удалить пользователя {telegram_id} из группы.\n"
            f"Ошибка: действие не выполнено. ref: {error_ref}\n\n"
            "Пользователь мог остаться в группе. Проверьте вручную."
        )
        mark_subscription_removal_short(telegram_id, "telegram_failed", e)
        status = "kick_failed"

    if status == "kick_failed":
        return status

    # 2. Закрываем доступ в базе только после успешного Telegram side effect.
    finalize_subscription_removal_in_db(
        telegram_id,
        effective_expiry,
        subscription_cancelled_after_grace=failed_renewal_grace_expired,
    )
    return status

def subscription_check_requires_admin_notification(removed_total, reminder_errors, telegram_errors):
    return reminder_errors > 0 or telegram_errors > 0


async def notify_subscription_check_admins_if_needed(report_text, removed_total, reminder_errors, telegram_errors):
    if not subscription_check_requires_admin_notification(removed_total, reminder_errors, telegram_errors):
        return False
    try:
        await notify_admins(report_text)
        return True
    except Exception as e:
        logging.error(
            "SUBSCRIPTION_CHECK_ADMIN_NOTIFICATION_FAILED: error_ref=%s",
            safe_admin_error_reference("subscription_check_admin_notification", e),
        )
        return False


async def check_subscriptions_and_reminders():
    logging.info("--- Запуск ежедневной проверки подписок ---")
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT users.telegram_id, users.expiry_date, users.payment_failed, users.payment_failed_at,
               users.grace_period_end, users.auto_renew, users.reminder_sent, users.trial_used,
               users.stripe_subscription_id, users.stripe_customer_id,
               (
                   users.stripe_subscription_id IS NULL
                   AND EXISTS (
                       SELECT 1
                       FROM gift_access_grants gift
                       WHERE gift.recipient_telegram_id = users.telegram_id
                         AND gift.status = 'redeemed'
                         AND gift.applied_expiry = users.expiry_date
                   )
               ) AS current_access_is_gift
        FROM users
        WHERE paid = TRUE
          AND expiry_date IS NOT NULL
          AND (blocked_bot IS NOT TRUE)
          AND (
              (expiry_date > NOW() AND expiry_date < NOW() + INTERVAL '2 days')
              OR (
                  expiry_date < NOW()
                  AND payment_failed = TRUE
                  AND grace_period_end IS NOT NULL
                  AND grace_period_end > NOW()
              )
          )
    """)
    reminder_users = cur.fetchall()

    cur.execute("""
        SELECT telegram_id, expiry_date, payment_failed, payment_failed_at, grace_period_end, auto_renew, reminder_sent, trial_used, stripe_subscription_id, stripe_customer_id
        FROM users
        WHERE paid = TRUE
          AND expiry_date IS NOT NULL
          AND expiry_date < NOW()
          AND NOT (
              payment_failed = TRUE
              AND grace_period_end IS NOT NULL
              AND grace_period_end > NOW()
          )
    """)
    removal_users = cur.fetchall()
    cur.execute("""
        SELECT users.telegram_id, users.expiry_date, users.payment_failed,
               users.payment_failed_at, users.grace_period_end, users.auto_renew,
               users.reminder_sent, users.trial_used, users.stripe_subscription_id,
               users.stripe_customer_id
        FROM subscription_removal_events removal
        JOIN users ON users.telegram_id = removal.telegram_id
        WHERE removal.status IN ('pending', 'stripe_canceled', 'telegram_failed', 'telegram_removed')
           OR (
                removal.status = 'processing'
                AND (removal.lease_until IS NULL OR removal.lease_until < NOW())
           )
        ORDER BY removal.updated_at ASC, removal.telegram_id ASC
    """)
    retry_removal_users = cur.fetchall()
    durable_retry_ids = {int(row[0]) for row in retry_removal_users}
    existing_removal_ids = {int(row[0]) for row in removal_users}
    removal_users.extend(
        row for row in retry_removal_users if int(row[0]) not in existing_removal_ids
    )
    conn.commit()
    cur.close()
    conn.close()
    now = datetime.utcnow()
    checked_total = len(reminder_users) + len(removal_users)
    logging.info(
        f"Проверка подписок: найдено для reminder={len(reminder_users)}, "
        f"найдено для удаления={len(removal_users)}"
    )
    logging.info(
        "SUBSCRIPTION_REMOVAL_CANDIDATES: count=%s, users=%s",
        len(removal_users),
        [
            {
                "telegram_id": telegram_id,
                "expiry_date": str(expiry),
                "paid": True,
                "grace": str(grace_end) if grace_end else None,
                "auto_renew": auto_renew,
                "stripe_subscription_id": stripe_subscription_id,
                    "reason": "expired_after_grace_or_expiry",
            }
            for (
                telegram_id,
                expiry,
                payment_failed,
                payment_failed_at,
                grace_end,
                auto_renew,
                reminder_sent,
                _,
                stripe_subscription_id,
                stripe_customer_id,
            ) in removal_users
        ],
    )
    expired_total = 0
    grace_total = 0
    reminders_sent = 0
    reminder_errors = 0
    stripe_protected = 0
    removed_total = 0
    active_in_db_skipped = 0
    not_found_total = 0
    telegram_errors = 0
    stripe_cancellation_errors = 0
    removal_failed_total = 0
    pending_access_events = []
    protected_user_details = []
    grace_user_details = []
    expired_user_details = []
    deleted_user_details = []

    def fmt_report_dt(value):
        return value.strftime("%d.%m.%Y %H:%M") if value else "нет"

    def build_report_user(telegram_id, expiry, stripe_subscription_id=None, stripe_customer_id=None, reason=None):
        return {
            "telegram_id": telegram_id,
            "username": None,
            "first_name": None,
            "last_name": None,
            "subscription_end": expiry,
            "stripe_customer_id": stripe_customer_id,
            "stripe_subscription_id": stripe_subscription_id,
            "reason": reason,
        }

    def report_username(user_info):
        username = user_info.get("username")
        return f"@{username}" if username else "нет"

    def report_name(user_info):
        parts = [user_info.get("first_name"), user_info.get("last_name")]
        name = " ".join(str(part) for part in parts if part)
        return name or "нет"

    def log_report_user(prefix, user_info):
        logging.info(
            f"{prefix}: telegram_id={user_info['telegram_id']}, "
            f"username={report_username(user_info)}, "
            f"subscription_end={fmt_report_dt(user_info.get('subscription_end'))}, "
            f"reason={user_info.get('reason') or 'нет'}"
        )

    def format_report_section(title, users):
        if not users:
            return ""

        lines = [f"\n\n{title}:"]
        for index, user_info in enumerate(users[:10], 1):
            lines.extend([
                f"{index}) telegram_id: {user_info['telegram_id']}",
                f"   username: {report_username(user_info)}",
                f"   имя: {report_name(user_info)}",
                f"   подписка до: {fmt_report_dt(user_info.get('subscription_end'))}",
                f"   stripe_customer_id: {safe_log_id(user_info.get('stripe_customer_id')) or 'нет'}",
                f"   stripe_subscription_id: {safe_log_id(user_info.get('stripe_subscription_id')) or 'нет'}",
            ])
            if user_info.get("reason"):
                lines.append(f"   причина: {user_info['reason']}")

        if len(users) > 10:
            lines.append(f"...и еще {len(users) - 10} пользователей")

        return "\n".join(lines)

    for reminder_row in reminder_users:
        (
            telegram_id, expiry, payment_failed, payment_failed_at, grace_end,
            auto_renew, reminder_sent, _, stripe_subscription_id, stripe_customer_id,
        ) = reminder_row[:10]
        current_access_is_gift = bool(reminder_row[10]) if len(reminder_row) > 10 else False
        time_left = expiry - now

        # ----- Reminder после истечения, пока пользователь в льготном периоде -----
        if time_left.total_seconds() < 0:
            expired_total += 1
            expired_user = build_report_user(
                telegram_id,
                expiry,
                stripe_subscription_id,
                stripe_customer_id,
                "expiry_date уже истекла"
            )
            expired_user_details.append(expired_user)

            if payment_failed and grace_end and now < grace_end:
                grace_total += 1
                grace_user = build_report_user(
                    telegram_id,
                    expiry,
                    stripe_subscription_id,
                    stripe_customer_id,
                    "payment_failed grace_period_end еще активен"
                )
                grace_user_details.append(grace_user)
                log_report_user("GRACE_USER", grace_user)

                if auto_renew and has_valid_stripe_subscription_id(stripe_subscription_id):
                    stripe_guard_status = await refresh_active_stripe_subscription(telegram_id, stripe_subscription_id)
                    if stripe_guard_status:
                        protected_user = build_report_user(
                            telegram_id,
                            expiry,
                            stripe_subscription_id,
                            stripe_customer_id,
                            f"{stripe_guard_status} during grace period"
                        )
                        protected_user_details.append(protected_user)
                        log_report_user("PROTECTED_USER", protected_user)
                        stripe_protected += 1
                        continue

                if not reminder_sent:
                    try:
                        await bot.send_message(telegram_id,
                            "⏳ Ваша подписка истекла, но у вас ещё активен льготный период после ошибки оплаты.\n"
                            "Пожалуйста, продлите подписку как можно скорее.",
                            reply_markup=get_tariffs_keyboard(show_trial=False))
                        set_subscription_reminder_sent(telegram_id)
                        reminders_sent += 1
                    except Exception as e:
                        reminder_errors += 1
                        telegram_errors += 1
                        logging.warning(f"Не удалось отправить сообщение пользователю {telegram_id}: {e}")
                        if is_undeliverable_user_error(e):
                            update_user_blocked_bot(telegram_id)

        # ----- Напоминание за 48 часов -----
        elif timedelta(0) < time_left < timedelta(days=2):
            if auto_renew and stripe_subscription_id:
                logging.info(
                    f"Пользователь {telegram_id}: напоминание за 48 часов пропущено, потому что включено auto_renew."
                )
            elif not reminder_sent and not auto_renew:
                text = (
                    "🎁 Ваш подарочный доступ в клуб заканчивается через 2 дня — "
                    f"{expiry.strftime('%d.%m.%Y')}.\n\n"
                    "Если хотите остаться в клубе после окончания подарочного периода, "
                    "можно оформить доступ самостоятельно.\n\n"
                    "Выберите подходящий вариант:"
                    if current_access_is_gift
                    else "⏳ Ваша подписка заканчивается через 48 часов. Продлите доступ, чтобы не потерять связь с клубом."
                )
                try:
                    await bot.send_message(telegram_id, text, reply_markup=get_tariffs_keyboard(show_trial=False))
                    set_subscription_reminder_sent(telegram_id)
                    reminders_sent += 1
                except Exception as e:
                    reminder_errors += 1
                    telegram_errors += 1
                    logging.warning(f"Не удалось отправить напоминание пользователю {telegram_id}: {e}")
                    if is_undeliverable_user_error(e):
                        update_user_blocked_bot(telegram_id)

    for (telegram_id, expiry, payment_failed, payment_failed_at, grace_end, auto_renew, reminder_sent, _, stripe_subscription_id, stripe_customer_id) in removal_users:
        expired_total += 1
        expired_user = build_report_user(
            telegram_id,
            expiry,
            stripe_subscription_id,
            stripe_customer_id,
            "expiry_date уже истекла, пользователь найден для удаления"
        )
        expired_user_details.append(expired_user)

        if payment_failed and grace_end and now < grace_end:
            grace_total += 1
            grace_user = build_report_user(
                telegram_id,
                expiry,
                stripe_subscription_id,
                stripe_customer_id,
                "payment_failed grace_period_end еще активен"
            )
            grace_user_details.append(grace_user)
            log_report_user("GRACE_USER", grace_user)
            logging.info(
                f"GRACE_USER: telegram_id={telegram_id} пропущен из-за активного grace_period_end={fmt_report_dt(grace_end)}"
            )
            continue

        if payment_failed and payment_failed_at:
            retry_until = payment_failed_at + timedelta(hours=PAYMENT_RETRY_GRACE_HOURS)
            if now < retry_until:
                grace_total += 1
                logging.warning(
                    "USER_REMOVE_SKIPPED_RECENT_PAYMENT_FAILURE: telegram_id=%s, email=%s, "
                    "payment_failed_at=%s, grace_until=%s, expiry_date=%s, stripe_subscription_id=%s",
                    telegram_id,
                    None,
                    payment_failed_at,
                    retry_until,
                    expiry,
                    stripe_subscription_id,
                )
                continue

        removal_reason = "NO_STRIPE_SUBSCRIPTION_ID — proceed to removal"

        if (
            has_valid_stripe_subscription_id(stripe_subscription_id)
            and int(telegram_id) not in durable_retry_ids
        ):
            removal_reason = "STRIPE_INACTIVE_OR_EXPIRED — proceed to removal"
            stripe_guard_status = await refresh_active_stripe_subscription(telegram_id, stripe_subscription_id)
            if stripe_guard_status:
                if stripe_guard_status == "STRIPE_GRACE_ACTIVE":
                    grace_total += 1
                    grace_user = build_report_user(
                        telegram_id,
                        expiry,
                        stripe_subscription_id,
                        stripe_customer_id,
                        "live Stripe past_due/unpaid; bounded grace active",
                    )
                    grace_user_details.append(grace_user)
                    log_report_user("GRACE_USER", grace_user)
                    continue
                row = fetch_user_expiry_customer(telegram_id)
                refreshed_expiry = row[0] if row else None
                refreshed_customer_id = row[1] if row else stripe_customer_id
                protected_user = build_report_user(
                    telegram_id,
                    refreshed_expiry or expiry,
                    stripe_subscription_id,
                    refreshed_customer_id,
                    stripe_guard_status
                )
                protected_user_details.append(protected_user)
                log_report_user("PROTECTED_USER", protected_user)
                pending_access_events.append({
                    "telegram_id": telegram_id,
                    "event_type": "auto_stripe_protected_before_removal",
                    "source": "auto_check",
                    "old_expiry": expiry,
                    "new_expiry": refreshed_expiry,
                    "stripe_subscription_id": stripe_subscription_id,
                    "notes": stripe_guard_status
                })
                stripe_protected += 1
                continue
        elif not has_valid_stripe_subscription_id(stripe_subscription_id):
            logging.info(
                f"NO_STRIPE_SUBSCRIPTION_ID — proceed to removal. telegram_id={telegram_id}, "
                f"stripe_subscription_id={stripe_subscription_id or 'нет'}"
            )
        else:
            removal_reason = "DURABLE_REMOVAL_RETRY — proceed from subscription_removal_events"

        ban_status = await ban_user_logic(telegram_id)

        if ban_status == "active_in_db":
            active_in_db_skipped += 1
        elif ban_status in ("STRIPE_ACTIVE", "STRIPE_CHECK_FAILED", "STRIPE_UNLINKED_REVIEW"):
            row = fetch_user_expiry_customer(telegram_id)
            refreshed_expiry = row[0] if row else None
            refreshed_customer_id = row[1] if row else stripe_customer_id
            protected_user = build_report_user(
                telegram_id,
                refreshed_expiry or expiry,
                stripe_subscription_id,
                refreshed_customer_id,
                f"{ban_status} inside ban_user_logic"
            )
            protected_user_details.append(protected_user)
            log_report_user("PROTECTED_USER", protected_user)
            pending_access_events.append({
                "telegram_id": telegram_id,
                "event_type": "auto_stripe_protected_before_removal",
                "source": "auto_check",
                "old_expiry": expiry,
                "new_expiry": refreshed_expiry,
                "stripe_subscription_id": stripe_subscription_id,
                "notes": f"{ban_status} inside ban_user_logic"
            })
            stripe_protected += 1
        elif ban_status in ("recent_payment_failure", "grace_active", "STRIPE_GRACE_ACTIVE"):
            grace_total += 1
            grace_user = build_report_user(
                telegram_id,
                expiry,
                stripe_subscription_id,
                stripe_customer_id,
                f"{ban_status} inside ban_user_logic"
            )
            grace_user_details.append(grace_user)
            log_report_user("GRACE_USER", grace_user)
        elif ban_status == "not_found":
            not_found_total += 1
        elif ban_status in ("removed", "db_finalized"):
            deleted_user = build_report_user(
                telegram_id,
                expiry,
                stripe_subscription_id,
                stripe_customer_id,
                f"{removal_reason}; ban_status={ban_status}"
            )
            deleted_user_details.append(deleted_user)
            log_report_user("DELETED_USER", deleted_user)
            pending_access_events.append({
                "telegram_id": telegram_id,
                "event_type": "auto_access_closed_expired",
                "source": "auto_check",
                "old_expiry": expiry,
                "new_expiry": None,
                "stripe_subscription_id": stripe_subscription_id,
                "notes": f"{removal_reason}; ban_status={ban_status}"
            })
            removed_total += 1
        elif ban_status == "stripe_cancel_failed":
            stripe_cancellation_errors += 1
            removal_failed_total += 1
            logging.error(
                "USER_REMOVAL_NOT_COMPLETED: telegram_id=%s, status=%s",
                telegram_id,
                ban_status,
            )
        elif ban_status in ("kick_failed", "unban_failed"):
            telegram_errors += 1
            removal_failed_total += 1
            logging.error(
                "USER_REMOVAL_NOT_COMPLETED: telegram_id=%s, status=%s",
                telegram_id,
                ban_status,
            )

    for access_event in pending_access_events:
        await log_access_event(**access_event)

    if (
        expired_total == 0
        and grace_total == 0
        and reminders_sent == 0
        and reminder_errors == 0
        and stripe_protected == 0
        and removed_total == 0
        and active_in_db_skipped == 0
        and not_found_total == 0
        and telegram_errors == 0
        and stripe_cancellation_errors == 0
        and removal_failed_total == 0
    ):
        report_text = f"✅ Проверка подписок завершена. Проверено: {checked_total}, удалено: 0, ошибок: 0."
    else:
        report_text = (
            "📊 Проверка подписок завершена\n\n"
            f"Проверено пользователей: {checked_total}\n"
            f"Просроченных найдено: {expired_total}\n"
            f"В льготном периоде: {grace_total}\n"
            f"Напоминаний отправлено: {reminders_sent}\n"
            f"Ошибок напоминаний: {reminder_errors}\n"
            f"Защищены через Stripe/ошибку Stripe: {stripe_protected}\n"
            f"Удалены/закрыт доступ: {removed_total}\n"
            f"Пропущены, доступ уже активен в БД: {active_in_db_skipped}\n"
            f"Не найдены в БД перед удалением: {not_found_total}\n"
            f"Не завершены удаления: {removal_failed_total}\n"
            f"Ошибки отмены Stripe: {stripe_cancellation_errors}\n"
            f"Ошибки Telegram: {telegram_errors}"
        )

    report_text += format_report_section("🛡 Защищены через Stripe / ошибку Stripe", protected_user_details)
    report_text += format_report_section("⏳ В льготном периоде", grace_user_details)
    report_text += format_report_section("⚠️ Просроченные пользователи", expired_user_details)
    report_text += format_report_section("🚪 Удалены / закрыт доступ", deleted_user_details)

    logging.info(
        "SUBSCRIPTION_CHECK_COMPLETED: checked=%s, removed=%s, reminder_errors=%s, "
        "telegram_errors=%s, stripe_cancellation_errors=%s, reminders_sent=%s, stripe_protected=%s",
        checked_total,
        removed_total,
        reminder_errors,
        telegram_errors,
        stripe_cancellation_errors,
        reminders_sent,
        stripe_protected,
    )
    await notify_subscription_check_admins_if_needed(
        report_text,
        removed_total,
        reminder_errors,
        telegram_errors + stripe_cancellation_errors,
    )

async def check_free_lesson_followups():
    logging.info("--- Проверка follow-up после бесплатного урока ---")

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT telegram_id
            FROM users
            WHERE video_sent = TRUE
              AND video_sent_at IS NOT NULL
              AND feedback_sent = FALSE
              AND feedback_received = FALSE
              AND (blocked_bot IS NOT TRUE)
              AND paid = FALSE
              AND video_sent_at <= NOW() - INTERVAL '24 hours'
            ORDER BY video_sent_at ASC
            LIMIT 50
        """)

        users = cur.fetchall()
        cur.close()
        conn.close()
        cur = None
        conn = None

        sent = 0
        blocked = 0
        failed = 0

        for (user_id,) in users:
            try:
                was_sent = await send_free_lesson_followup(user_id)
                if was_sent:
                    sent += 1
            except TelegramForbiddenError:
                blocked += 1
                user_conn = get_db_conn()
                user_cur = user_conn.cursor()
                try:
                    user_cur.execute(
                        "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                        (int(user_id),)
                    )
                    user_conn.commit()
                finally:
                    user_cur.close()
                    user_conn.close()
            except Exception as e:
                failed += 1
                logging.error(f"Ошибка follow-up после бесплатного урока для {user_id}: {e}")

        logging.info(
            f"Follow-up после бесплатного урока: отправлено={sent}, "
            f"заблокировали={blocked}, ошибки={failed}"
        )

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Ошибка check_free_lesson_followups: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- БЭКАП БАЗЫ ДАННЫХ ---
async def send_db_backup():
    filename = f"backup_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.sql"
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        await notify_admins("❌ Ошибка бэкапа: DATABASE_URL не задан!")
        return

    decision = backup_decision(os.environ)
    if not decision["allowed"]:
        await notify_admins(f"❌ Ошибка бэкапа: {decision['reason']}")
        return

    try:
        pg_dump_argv, pg_dump_env = build_pg_dump_command(db_url, os.environ)
        process = await asyncio.create_subprocess_exec(
            *pg_dump_argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=pg_dump_env,
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_msg = mask_secret_text(stderr.decode('utf-8'))
            logging.error(f"pg_dump failed (code {process.returncode}): {error_msg}")
            await notify_admins(f"❌ Ошибка дампа БД. Код: {process.returncode}. Подробности в логах.")
            return

        # Записываем дамп в файл
        with open(filename, 'wb') as f:
            f.write(stdout)

        logging.info(f"Бэкап создан: {filename} (размер: {len(stdout)} байт)")

        if not decision["telegram_enabled"]:
            await notify_admins(
                "✅ Бэкап БД создан и проверен локально, отправка файла в Telegram отключена "
                "(BACKUP_TELEGRAM_ENABLED=false). Локальный .sql удалён."
            )
            return

        encrypted_filename = filename + ".enc"
        key = os.getenv("BACKUP_ENCRYPTION_KEY")
        encrypt_process = await asyncio.create_subprocess_exec(
            "openssl", "enc", "-aes-256-cbc", "-salt", "-pbkdf2",
            "-pass", "env:BACKUP_ENCRYPTION_KEY",
            "-in", filename,
            "-out", encrypted_filename,
            env={**os.environ, "BACKUP_ENCRYPTION_KEY": key},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, enc_stderr = await encrypt_process.communicate()
        if encrypt_process.returncode != 0:
            logging.error("backup encryption failed: %s", mask_secret_text(enc_stderr.decode("utf-8")))
            await notify_admins("❌ Ошибка шифрования бэкапа. Файл не отправлен.")
            return
        if os.path.exists(filename):
            os.remove(filename)

        for admin_id in ADMIN_IDS:
            try:
                with open(encrypted_filename, 'rb') as f:
                    await bot.send_document(admin_id, f, caption=f"📦 Зашифрованный бэкап БД от {datetime.now().strftime('%d.%m.%Y %H:%M')}")
            except Exception as e:
                logging.error(f"Не удалось отправить бэкап админу {admin_id}: {e}")

    except Exception as e:
        logging.exception(f"Критическая ошибка бэкапа: {e}")
        error_ref = safe_admin_error_reference("db_backup", e)
        await notify_admins(f"❌ Непредвиденная ошибка бэкапа. ref: {error_ref}")
    finally:
        if os.path.exists(filename):
            os.remove(filename)
        encrypted_filename = filename + ".enc"
        if os.path.exists(encrypted_filename):
            os.remove(encrypted_filename)

@router.message(F.content_type == 'video', StateFilter(None))
async def reply_with_video_id(message: types.Message):
    # Только в личных сообщениях (не в группе)
    if message.chat.type != 'private':
        return
    # И только для админов (опционально, можно убрать)
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Эта команда только для администратора.")
        return
    file_id = message.video.file_id
    await message.reply(f"Ваш video file_id:\n`{file_id}`", parse_mode="Markdown")

@router.message(F.content_type == 'photo', StateFilter(None))
async def reply_with_photo_id(message: types.Message):
    if message.chat.type != 'private':
        return
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Эта команда только для администратора.")
        return
    file_id = message.photo[-1].file_id
    await message.reply(f"Ваш photo file_id:\n`{file_id}`", parse_mode="Markdown")

@router.message(Command('promo_trial'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def promo_trial(message: types.Message, state: FSMContext):
    await state.clear()
    logging.info(f"Команда promo_trial от {message.from_user.id}")
    if message.from_user.id not in ADMIN_IDS:
        logging.warning(f"Отказано {message.from_user.id}")
        return
    await state.set_state(PromoStates.waiting_for_media)
    await message.reply("📎 Отправьте фото или видео, которое будет в рассылке.\n\n"
                        "Чтобы отменить, отправьте /cancel")

@router.message(Command('cancel'), StateFilter('*'))
async def cancel_handler(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.reply("Нет активного действия для отмены.")
        return
    await state.clear()
    await message.reply("✅ Действие отменено. Можете начать заново.")

@router.message(F.content_type.in_(['photo', 'video']), StateFilter(PromoStates.waiting_for_media))
async def promo_get_media(message: types.Message, state: FSMContext):
    if message.photo:
        file_id = message.photo[-1].file_id
        media_type = 'photo'
    else:
        file_id = message.video.file_id
        media_type = 'video'
    await state.update_data(media_type=media_type, file_id=file_id)
    await state.set_state(PromoStates.waiting_for_text)
    await message.reply("✏️ Теперь отправьте текст сообщения.\n\n"
                        "Можно использовать HTML-разметку (<b>жирный</b>, <i>курсив</i>).")

@router.message(F.content_type == 'text', StateFilter(PromoStates.waiting_for_text))
async def promo_get_text(message: types.Message, state: FSMContext):
    text = message.html_text

    if len(text) > 1000:
        await message.reply(
            f"⚠️ Текст слишком длинный для промо-рассылки с фото/видео.\n\n"
            f"Сейчас: {len(text)} символов.\n"
            f"Максимум: 1000 символов.\n\n"
            f"Сократите текст и отправьте его еще раз."
        )
        return

    data = await state.get_data()
    media_type = data['media_type']
    file_id = data['file_id']

    kb = inline_keyboard([[
        InlineKeyboardButton(text="✅ Да, отправить", callback_data="confirm_promo"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_promo"),
    ]])

    await state.update_data(text=text)

    try:
        if media_type == 'photo':
            await message.reply_photo(
                file_id,
                caption=text + "\n\n---\n<i>Предпросмотр. Отправляем?</i>",
                reply_markup=kb,
                parse_mode="HTML"
            )
        else:
            await message.reply_video(
                file_id,
                caption=text + "\n\n---\n<i>Предпросмотр. Отправляем?</i>",
                reply_markup=kb,
                parse_mode="HTML"
            )

    except Exception as e:
        logging.error(f"Ошибка предпросмотра промо-рассылки: {e}")
        await message.reply(
            "❌ Не удалось создать предпросмотр.\n\n"
            "Возможно, текст все еще слишком длинный или в нем есть ошибка форматирования. "
            "Сократите текст и попробуйте снова."
        )

@router.callback_query(F.data == "confirm_promo", StateFilter(PromoStates.waiting_for_text))
@admin_private_only(ADMIN_IDS)
async def promo_send(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data['text']
    media_type = data['media_type']
    file_id = data['file_id']

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT telegram_id FROM users WHERE paid = FALSE AND (blocked_bot IS NOT TRUE)")
        users = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    kb = inline_keyboard([[
        InlineKeyboardButton(text="Начать пробную неделю", callback_data="sub_trial")
    ]])

    success = 0
    blocked = 0
    failed = 0

    for (user_id,) in users:
        try:
            if media_type == 'photo':
                await bot.send_photo(user_id, file_id, caption=text, reply_markup=kb, parse_mode="HTML")
            else:
                await bot.send_video(user_id, file_id, caption=text, reply_markup=kb, parse_mode="HTML")
            success += 1
        except TelegramForbiddenError:
            blocked += 1
            mark_user_blocked_bot(user_id)
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка промо-рассылки для {user_id}: {e}")

    await callback.message.answer(
        f"✅ Рассылка завершена.\n"
        f"📨 Успешно: {success}\n"
        f"🚫 Заблокировали бота: {blocked}\n"
        f"⚠️ Другие ошибки: {failed}"
    )
    await state.clear()
    await callback.answer()

@router.callback_query(F.data == "cancel_promo", StateFilter(PromoStates.waiting_for_text))
@admin_private_only(ADMIN_IDS)
async def promo_cancel(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Рассылка отменена.")
    await state.clear()
    await callback.answer()

def get_main_keyboard(telegram_id=None):
    rows = [
        [KeyboardButton(text="🧘 Бесплатный урок")],
        [KeyboardButton(text="👤 Профиль и подписка"), KeyboardButton(text="📅 Расписание")],
        [KeyboardButton(text="💬 Задать вопрос"), KeyboardButton(text="🚨 Правила клуба")],
        [KeyboardButton(text="🎁 Подарить доступ в клуб")],
    ]
    if telegram_id is not None and int(telegram_id) in ADMIN_IDS:
        rows.append([KeyboardButton(text="🛠 Управление подарками")])
        rows.append([KeyboardButton(text="📅 Управление расписанием")])
    return reply_keyboard(rows, resize_keyboard=True)


RUSSIAN_MONTH_NAMES = (
    "январь", "февраль", "март", "апрель", "май", "июнь",
    "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь",
)


def schedule_month_details(now=None):
    current = now or datetime.now(MOSCOW_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=MOSCOW_TZ)
    else:
        current = current.astimezone(MOSCOW_TZ)
    month_key = current.strftime("%Y-%m")
    caption = f"📅 Расписание на {RUSSIAN_MONTH_NAMES[current.month - 1]} {current.year}"
    return month_key, caption


def validated_schedule_month(month_key):
    if not isinstance(month_key, str) or re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", month_key) is None:
        raise ValueError("invalid_schedule_month")
    return month_key


def schedule_month_label(month_key):
    validated_schedule_month(month_key)
    year, month = (int(part) for part in month_key.split("-"))
    return f"{RUSSIAN_MONTH_NAMES[month - 1].capitalize()} {year}"


def shifted_schedule_month(month_key, offset):
    validated_schedule_month(month_key)
    year, month = (int(part) for part in month_key.split("-"))
    absolute_month = year * 12 + month - 1 + int(offset)
    shifted_year, shifted_zero_month = divmod(absolute_month, 12)
    return f"{shifted_year:04d}-{shifted_zero_month + 1:02d}"


def fetch_club_schedule(cur, month_key):
    month_key = validated_schedule_month(month_key)
    cur.execute(
        """
        SELECT schedule_month, telegram_file_id, uploaded_by_telegram_id, created_at, updated_at
        FROM club_schedules
        WHERE schedule_month = %s
        """,
        (month_key,),
    )
    row = cur.fetchone()
    if not row:
        return None
    columns = ("schedule_month", "telegram_file_id", "uploaded_by_telegram_id", "created_at", "updated_at")
    return dict(zip(columns, row))


def load_club_schedule(month_key):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        return fetch_club_schedule(cur, month_key)
    finally:
        cur.close()
        conn.close()


def upsert_club_schedule(cur, month_key, file_id, admin_id):
    month_key = validated_schedule_month(month_key)
    if not file_id:
        raise ValueError("schedule_file_id_missing")
    cur.execute(
        """
        INSERT INTO club_schedules (
            schedule_month, telegram_file_id, uploaded_by_telegram_id, created_at, updated_at
        )
        VALUES (%s, %s, %s, NOW(), NOW())
        ON CONFLICT (schedule_month) DO UPDATE
        SET telegram_file_id = EXCLUDED.telegram_file_id,
            uploaded_by_telegram_id = EXCLUDED.uploaded_by_telegram_id,
            updated_at = NOW()
        RETURNING schedule_month
        """,
        (month_key, str(file_id), int(admin_id)),
    )
    return cur.fetchone()[0]


def current_schedule(now=None):
    month_key, caption = schedule_month_details(now)
    schedule = load_club_schedule(month_key)
    return month_key, schedule["telegram_file_id"] if schedule else None, caption


def admin_schedule_keyboard(month_key, exists):
    previous_month = shifted_schedule_month(month_key, -1)
    next_month = shifted_schedule_month(month_key, 1)
    rows = [
        [InlineKeyboardButton(text="📤 Загрузить расписание", callback_data=f"admin_schedule_upload:{month_key}")],
        [
            InlineKeyboardButton(text=f"← {schedule_month_label(previous_month)}", callback_data=f"admin_schedule_open:{previous_month}"),
            InlineKeyboardButton(text=f"{schedule_month_label(next_month)} →", callback_data=f"admin_schedule_open:{next_month}"),
        ],
    ]
    if exists:
        rows.append([InlineKeyboardButton(text="🗑 Удалить расписание", callback_data=f"admin_schedule_remove:{month_key}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_schedule_close")])
    return inline_keyboard(rows)


def admin_schedule_text(month_key, schedule):
    return (
        "📅 Расписание клуба\n\n"
        f"Выбранный месяц: {schedule_month_label(month_key)}\n"
        f"Статус: {'загружено' if schedule else 'не загружено'}"
    )


def load_admin_schedule_view(month_key):
    schedule = load_club_schedule(month_key)
    return admin_schedule_text(month_key, schedule), admin_schedule_keyboard(month_key, bool(schedule))


GIFT_ADMIN_STATUS_LABELS = {
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


def gift_admin_status_label(status):
    return GIFT_ADMIN_STATUS_LABELS.get(status, "Неизвестный статус")


def fetch_admin_gifts(cur, limit=20):
    cur.execute("""
        SELECT
            gift.public_reference,
            gift.purchaser_telegram_id,
            purchaser.username AS purchaser_username,
            gift.recipient_telegram_id,
            recipient.username AS recipient_username,
            gift.tariff_code,
            gift.status,
            gift.created_at,
            gift.paid_at,
            gift.applied_expiry
        FROM gift_access_grants gift
        LEFT JOIN users purchaser ON purchaser.telegram_id = gift.purchaser_telegram_id
        LEFT JOIN users recipient ON recipient.telegram_id = gift.recipient_telegram_id
        ORDER BY gift.created_at DESC, gift.public_reference DESC
        LIMIT %s
    """, (int(limit),))
    columns = [description[0] for description in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_admin_gift(cur, public_reference):
    cur.execute("""
        SELECT
            gift.public_reference,
            gift.purchaser_telegram_id,
            purchaser.username AS purchaser_username,
            gift.recipient_telegram_id,
            recipient.username AS recipient_username,
            gift.tariff_code,
            gift.status,
            gift.created_at,
            gift.paid_at,
            gift.applied_expiry
        FROM gift_access_grants gift
        LEFT JOIN users purchaser ON purchaser.telegram_id = gift.purchaser_telegram_id
        LEFT JOIN users recipient ON recipient.telegram_id = gift.recipient_telegram_id
        WHERE gift.public_reference = %s
    """, (public_reference,))
    row = cur.fetchone()
    if not row:
        return None
    return dict(zip((description[0] for description in cur.description), row))


def admin_gift_center_keyboard(gifts):
    rows = [
        [InlineKeyboardButton(
            text=f"{gift['public_reference']} · {gift_admin_status_label(gift['status'])}",
            callback_data=f"admin_gift_open:{gift['public_reference']}",
        )]
        for gift in gifts
    ]
    return inline_keyboard(rows) if rows else None


def admin_gift_detail_keyboard(gift):
    rows = []
    if gift["status"] in ("checkout_pending", "checkout_open"):
        rows.append([InlineKeyboardButton(
            text="❌ Отменить подарок",
            callback_data=f"admin_gift_cancel:{gift['public_reference']}",
        )])
    rows.append([InlineKeyboardButton(text="⬅️ К списку подарков", callback_data="admin_gift_center")])
    return inline_keyboard(rows)


def admin_gift_detail_text(gift):
    purchaser_username = f"@{gift['purchaser_username']}" if gift.get("purchaser_username") else "не указан"
    recipient_username = f"@{gift['recipient_username']}" if gift.get("recipient_username") else "не указан"
    recipient_id = gift.get("recipient_telegram_id") or "не указан"
    lines = [
        "🎁 Управление подарком",
        "",
        f"Референс: {gift['public_reference']}",
        f"Покупатель telegram_id: {gift['purchaser_telegram_id']}",
        f"Покупатель username: {purchaser_username}",
        f"Получатель telegram_id: {recipient_id}",
        f"Получатель username: {recipient_username}",
        f"Тариф: {gift_tariff_label(gift['tariff_code'])}",
        f"Статус: {gift_admin_status_label(gift['status'])}",
        f"Создан: {gift.get('created_at') or 'не указано'}",
        f"Оплачен: {gift.get('paid_at') or 'нет'}",
        f"Срок доступа после активации: {gift.get('applied_expiry') or 'не активирован'}",
    ]
    if gift["status"] in ("paid_unclaimed", "reserved"):
        lines.extend((
            "",
            "⚠️ Подарок оплачен. Локальная отмена запрещена: требуется полный refund в Stripe,",
            "после которого статус изменит Stripe webhook.",
        ))
    elif gift["status"] == "redeemed":
        lines.extend((
            "",
            "⚠️ Подарок уже активирован. Refund не отзывает доступ автоматически;",
            "случай требует отдельной проверки администратора.",
        ))
    elif gift["status"] == "payment_pending":
        lines.extend(("", "⚠️ Оплата обрабатывается. Дождитесь результата Stripe или проверьте платёж вручную."))
    elif gift["status"] == "review_required":
        lines.extend(("", "⚠️ Автоматические действия недоступны. Требуется проверка администратора."))
    return "\n".join(lines)


def load_admin_gifts():
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        return fetch_admin_gifts(cur)
    finally:
        cur.close()
        conn.close()


def load_admin_gift(public_reference):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        return fetch_admin_gift(cur, public_reference)
    finally:
        cur.close()
        conn.close()


@router.message(Command('menu'), StateFilter('*'))
async def show_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Главное меню\n\nВыберите нужный раздел:",
        reply_markup=get_main_keyboard(message.from_user.id)
    )


@router.message(F.text == "📅 Расписание", StateFilter('*'))
async def schedule_button_handler(message: types.Message, state: FSMContext):
    await state.clear()
    month_key, file_id, caption = current_schedule()
    if file_id:
        try:
            await message.answer_photo(photo=file_id, caption=caption)
        except Exception as e:
            error_ref = safe_admin_error_reference("club_schedule_send", e)
            logging.error(
                "CLUB_SCHEDULE_SEND_FAILED: month=%s, error_type=%s, error_ref=%s",
                month_key,
                type(e).__name__,
                error_ref,
            )
            await message.answer("📅 Не получилось загрузить расписание. Мы уже проверяем файл.")
            await notify_admins(
                f"⚠️ Не удалось отправить расписание за {month_key}. Запись не удалена. ref: {error_ref}",
                alert_key=f"club-schedule-send:{month_key}:{error_ref}",
            )
        return
    await message.answer("📅 Расписание на этот месяц скоро появится.")


@router.message(F.text == "📅 Управление расписанием", StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_schedule_button_handler(message: types.Message, state: FSMContext):
    await state.clear()
    month_key, _ = schedule_month_details()
    text, keyboard = load_admin_schedule_view(month_key)
    await message.answer(text, reply_markup=keyboard)


@router.callback_query(F.data.startswith("admin_schedule_open:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_schedule_open_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    month_key = validated_schedule_month(callback.data.split(":", 1)[1])
    text, keyboard = load_admin_schedule_view(month_key)
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()


@router.callback_query(F.data.startswith("admin_schedule_upload:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_schedule_upload_callback(callback: types.CallbackQuery, state: FSMContext):
    month_key = validated_schedule_month(callback.data.split(":", 1)[1])
    await state.set_state(ScheduleAdminStates.waiting_for_photo)
    await state.update_data(schedule_month=month_key)
    await callback.message.answer(
        f"Отправьте изображение расписания на {schedule_month_label(month_key)}.",
        reply_markup=inline_keyboard([[
            InlineKeyboardButton(text="❌ Отмена", callback_data="admin_schedule_upload_cancel")
        ]]),
    )
    await callback.answer()


@router.callback_query(F.data == "admin_schedule_upload_cancel", StateFilter(ScheduleAdminStates.waiting_for_photo))
@admin_private_only(ADMIN_IDS)
async def admin_schedule_upload_cancel_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Загрузка расписания отменена.")
    await callback.answer()


@router.message(F.photo, StateFilter(ScheduleAdminStates.waiting_for_photo))
@admin_private_only(ADMIN_IDS)
async def admin_schedule_photo_received(message: types.Message, state: FSMContext):
    data = await state.get_data()
    month_key = validated_schedule_month(data.get("schedule_month"))
    file_id = message.photo[-1].file_id
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        upsert_club_schedule(cur, month_key, file_id, message.from_user.id)
        conn.commit()
    except Exception as e:
        conn.rollback()
        error_ref = safe_admin_error_reference("club_schedule_upload", e)
        logging.error(
            "CLUB_SCHEDULE_UPLOAD_FAILED: month=%s, admin_id=%s, error_type=%s, error_ref=%s",
            month_key,
            message.from_user.id,
            type(e).__name__,
            error_ref,
        )
        await state.clear()
        await message.answer(f"❌ Не удалось сохранить расписание. ref: {error_ref}")
        return
    finally:
        cur.close()
        conn.close()
    await state.clear()
    confirmation = f"✅ Расписание на {schedule_month_label(month_key).lower()} сохранено."
    try:
        await message.answer_photo(photo=file_id, caption=confirmation)
    except Exception as e:
        logging.warning(
            "CLUB_SCHEDULE_PREVIEW_FAILED: month=%s, error_type=%s, error_ref=%s",
            month_key,
            type(e).__name__,
            safe_admin_error_reference("club_schedule_preview", e),
        )
        await message.answer(confirmation)


@router.callback_query(F.data.startswith("admin_schedule_remove:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_schedule_delete_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    month_key = validated_schedule_month(callback.data.split(":", 1)[1])
    await callback.message.edit_text(
        f"Удалить расписание на {schedule_month_label(month_key)}?",
        reply_markup=inline_keyboard([[
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"admin_schedule_delete_confirm:{month_key}"),
            InlineKeyboardButton(text="❌ Нет", callback_data=f"admin_schedule_open:{month_key}"),
        ]]),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_schedule_delete_confirm:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_schedule_delete_confirm_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    month_key = validated_schedule_month(callback.data.split(":", 1)[1])
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM club_schedules WHERE schedule_month = %s RETURNING schedule_month", (month_key,))
        deleted = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    text, keyboard = load_admin_schedule_view(month_key)
    prefix = "✅ Расписание удалено.\n\n" if deleted else "ℹ️ Расписание уже отсутствует.\n\n"
    await callback.message.edit_text(prefix + text, reply_markup=keyboard)
    await callback.answer()


@router.callback_query(F.data == "admin_schedule_close", StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_schedule_close_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Управление расписанием закрыто.")
    await callback.answer()


@router.message(F.text == "🛠 Управление подарками", StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_gift_center_button_handler(message: types.Message, state: FSMContext):
    await state.clear()
    gifts = load_admin_gifts()
    if not gifts:
        await message.answer("🎁 Подарков пока нет.")
        return
    await message.answer(
        "🎁 Управление подарками\n\nПоследние 20 подарков всех пользователей:",
        reply_markup=admin_gift_center_keyboard(gifts),
    )


@router.callback_query(F.data == "admin_gift_center", StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_gift_center_callback(callback: types.CallbackQuery):
    gifts = load_admin_gifts()
    text = "🎁 Управление подарками\n\nПоследние 20 подарков всех пользователей:" if gifts else "🎁 Подарков пока нет."
    await callback.message.edit_text(text, reply_markup=admin_gift_center_keyboard(gifts))
    await callback.answer()


@router.callback_query(F.data.startswith("admin_gift_open:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_gift_open_callback(callback: types.CallbackQuery):
    public_reference = callback.data.split(":", 1)[1]
    gift = load_admin_gift(public_reference)
    if not gift:
        await callback.answer("Подарок не найден.", show_alert=True)
        return
    await callback.message.edit_text(
        admin_gift_detail_text(gift),
        reply_markup=admin_gift_detail_keyboard(gift),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_gift_cancel:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_gift_cancel_callback(callback: types.CallbackQuery):
    public_reference = callback.data.split(":", 1)[1]
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        gift = fetch_gift_by_public_reference(cur, public_reference)
        if not gift:
            await callback.answer("Подарок не найден.", show_alert=True)
            return
        if gift["status"] not in ("checkout_pending", "checkout_open"):
            await callback.answer("Этот подарок нельзя отменить локально.", show_alert=True)
            return
        action_id = make_action_request(
            cur,
            callback.from_user.id,
            "gift_cancel",
            {"public_reference": public_reference, "admin_id": callback.from_user.id},
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    await send_admin_action_confirmation(
        callback.message,
        action_id,
        gift_admin_text("Подтвердите отмену подарка", gift),
    )
    await callback.answer()


@router.message(F.text == "👤 Профиль и подписка", StateFilter('*'))
async def profile_button_handler(message: types.Message, state: FSMContext):
    await state.clear()
    await profile(message)


@router.message(F.text == "🎁 Подарить доступ в клуб", StateFilter('*'))
async def gift_access_button_handler(message: types.Message, state: FSMContext):
    await state.clear()
    save_telegram_user_profile(message.from_user)
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        status = gift_configuration_status(cur)
    finally:
        cur.close()
        conn.close()
    if not status["configured"]:
        await message.answer(gift_access_unavailable_text(), reply_markup=get_main_keyboard(message.from_user.id))
        await enqueue_admin_payment_problem_now(
            event_id=None,
            purpose="gift_access_configuration_missing",
            stage="gift_access",
            telegram_id=message.from_user.id,
            category="configuration_missing",
            stripe_retry="нет",
            recovery_reminder="не применимо",
            safe_ref=safe_admin_context_reference("gift_config", status["missing_prices"], status["template_count"]),
            note=(
                f"missing_prices: {', '.join(status['missing_prices']) or 'нет'}\n"
                f"templates: {status['template_count']}/{status['required_template_count']}"
            ),
        )
        return
    await state.set_state(GiftPurchaseStates.tariff)
    await message.answer(
        "🎁 На какой срок подарить доступ?",
        reply_markup=gift_tariffs_keyboard(),
    )


@router.callback_query(F.data.startswith("gift_tariff:"), StateFilter(GiftPurchaseStates.tariff))
async def gift_tariff_selected(callback: types.CallbackQuery, state: FSMContext):
    tariff_code = callback.data.split(":", 1)[1]
    if tariff_code not in GIFT_TARIFFS:
        await callback.answer("Неизвестный тариф.", show_alert=True)
        return
    await state.update_data(tariff_code=tariff_code)
    await state.set_state(GiftPurchaseStates.certificate_name)
    await callback.message.answer(
        "Как подписать сертификат?\n\n"
        "Можно указать имя или имя и фамилию получателя.\n\n"
        "Например:\nАнастасия\nАнастасия Иванова\n\n"
        "Или выберите «Без имени».",
        reply_markup=gift_certificate_name_keyboard(),
    )
    await callback.answer()


async def continue_gift_after_certificate_name(message, state, certificate_name):
    await state.update_data(certificate_name=certificate_name)
    await state.set_state(GiftPurchaseStates.recipient_name)
    await message.answer(
        "Напишите имя получателя для сообщения об активации.\n\n"
        "Можно отправить «-», если не хотите указывать имя."
    )


@router.callback_query(
    F.data == "gift_certificate_without_name",
    StateFilter(GiftPurchaseStates.certificate_name),
)
async def gift_certificate_without_name_callback(callback: types.CallbackQuery, state: FSMContext):
    await continue_gift_after_certificate_name(callback.message, state, None)
    await callback.answer()


@router.message(StateFilter(GiftPurchaseStates.certificate_name))
async def gift_certificate_name_received(message: types.Message, state: FSMContext):
    try:
        certificate_name = validate_certificate_name_fits(message.text)
    except CertificateNameError as error:
        if error.args and error.args[0] in ("certificate_name_too_long", "certificate_name_does_not_fit"):
            text = CERTIFICATE_NAME_TOO_LONG_TEXT
        else:
            text = "Укажите имя или имя и фамилию буквами; можно использовать пробел и дефис."
        await message.answer(text, reply_markup=gift_certificate_name_keyboard())
        return
    await continue_gift_after_certificate_name(message, state, certificate_name)


@router.message(StateFilter(GiftPurchaseStates.recipient_name))
async def gift_recipient_name_received(message: types.Message, state: FSMContext):
    text = sanitize_gift_text(message.text, GIFT_NAME_LIMIT)
    recipient_name = "" if text in ("-", "—") else text
    await state.update_data(recipient_name=recipient_name)
    await state.set_state(GiftPurchaseStates.sender_name)
    await message.answer(
        "Как подписать отправителя?\n\n"
        f"По умолчанию: {gift_sender_default_name(message.from_user)}\n"
        "Отправьте «-», чтобы оставить значение по умолчанию."
    )


@router.message(StateFilter(GiftPurchaseStates.sender_name))
async def gift_sender_name_received(message: types.Message, state: FSMContext):
    text = sanitize_gift_text(message.text, GIFT_NAME_LIMIT)
    sender_name = gift_sender_default_name(message.from_user) if text in ("", "-", "—") else text
    await state.update_data(sender_name=sender_name)
    await state.set_state(GiftPurchaseStates.message)
    await message.answer(
        "Добавьте короткое пожелание для сертификата.\n\n"
        "Можно отправить «-», если личное сообщение не нужно."
    )


@router.message(StateFilter(GiftPurchaseStates.message))
async def gift_message_received(message: types.Message, state: FSMContext):
    text = sanitize_gift_text(message.text, GIFT_MESSAGE_LIMIT)
    gift_message = "" if text in ("-", "—") else text
    await state.update_data(gift_message=gift_message)
    data = await state.get_data()
    await state.set_state(GiftPurchaseStates.preview)
    await message.answer(
        build_gift_preview_text(data),
        reply_markup=gift_preview_keyboard(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "gift_edit", StateFilter(GiftPurchaseStates.preview))
async def gift_edit_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(GiftPurchaseStates.tariff)
    await callback.message.answer("Хорошо, начнём заново. Выберите срок подарка:", reply_markup=gift_tariffs_keyboard())
    await callback.answer()


@router.callback_query(F.data == "gift_cancel_flow", StateFilter('*'))
async def gift_cancel_flow_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Подарок отменён.", reply_markup=get_main_keyboard(callback.from_user.id))
    await callback.answer()


@router.callback_query(F.data == "gift_pay", StateFilter(GiftPurchaseStates.preview))
async def gift_pay_callback(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("⏳ Создаём оплату...")
    data = await state.get_data()
    tariff_code = data.get("tariff_code")
    price_id = gift_price_id(tariff_code)
    if tariff_code not in GIFT_TARIFFS or not price_id:
        await callback.message.answer(gift_access_unavailable_text(), reply_markup=get_main_keyboard(callback.from_user.id))
        await state.clear()
        return

    gift_row = None
    reused_checkout_url = None
    gift_unavailable = False
    draft_failed = False
    conflict_result = None
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        status = gift_configuration_status(cur)
        if not status["configured"]:
            conn.commit()
            gift_unavailable = True
        else:
            gift_row, draft_result = find_or_create_gift_checkout_draft(
                cur,
                callback.from_user.id,
                tariff_code,
                data.get("recipient_name") or "",
                data.get("sender_name") or gift_sender_default_name(callback.from_user),
                data.get("gift_message") or "",
                data.get("certificate_name"),
            )
            conn.commit()
            if draft_result in ("active_checkout_conflict", "payment_pending_conflict"):
                conflict_result = draft_result
            elif draft_result == "checkout_reused" and gift_row.get("checkout_url") and (
                not gift_row.get("checkout_expires_at") or gift_row["checkout_expires_at"] > datetime.utcnow()
            ):
                reused_checkout_url = gift_row["checkout_url"]
    except Exception as e:
        conn.rollback()
        logging.error(
            "GIFT_CHECKOUT_DRAFT_FAILED: user=%s error_ref=%s",
            callback.from_user.id,
            safe_admin_error_reference("gift_checkout_draft", e),
            exc_info=True,
        )
        draft_failed = True
    finally:
        cur.close()
        conn.close()

    if gift_unavailable:
        await callback.message.answer(gift_access_unavailable_text(), reply_markup=get_main_keyboard(callback.from_user.id))
        await state.clear()
        return

    if draft_failed:
        await callback.message.answer("Не получилось подготовить подарок. Попробуйте позже или напишите администратору.")
        await state.clear()
        return

    if conflict_result == "active_checkout_conflict":
        await callback.message.answer(
            "У вас уже есть незавершённая оплата подарка с другими данными.\n\n"
            "Можно вернуться к прежней оплате или безопасно отменить её перед созданием нового подарка.",
            reply_markup=gift_active_checkout_conflict_keyboard(gift_row),
        )
        await state.clear()
        return

    if conflict_result == "payment_pending_conflict":
        await callback.message.answer(
            "Оплата предыдущего подарка уже обрабатывается.\n\n"
            "Дождитесь результата Stripe или напишите администратору, если статус долго не меняется.",
            reply_markup=get_main_keyboard(callback.from_user.id),
        )
        await state.clear()
        return

    if reused_checkout_url:
        await callback.message.answer(CHECKOUT_OPEN_INSTRUCTION, reply_markup=gift_checkout_keyboard(reused_checkout_url))
        await state.clear()
        return

    try:
        session = await asyncio.to_thread(
            stripe.checkout.Session.create,
            idempotency_key=f"gift:{gift_row['id']}",
            payment_method_types=["card"],
            line_items=[{"price": price_id, "quantity": 1}],
            mode="payment",
            success_url="https://t.me/Natalia_SoulFit_bot",
            cancel_url="https://t.me/Natalia_SoulFit_bot",
            client_reference_id=str(callback.from_user.id),
            metadata={
                "payment_kind": GIFT_PAYMENT_KIND,
                "gift_id": str(gift_row["id"]),
                "purchaser_telegram_id": str(callback.from_user.id),
                "tariff_code": tariff_code,
                "duration_days": str(gift_row["duration_days"]),
            },
        )
        session_id = session.id
        checkout_url = session.url
        checkout_expires_at = stripe_value(session, "expires_at")
        if not checkout_url:
            raise ValueError("gift_checkout_url_missing")
        open_conn = get_db_conn()
        open_cur = open_conn.cursor()
        try:
            opened = mark_gift_checkout_open(open_cur, gift_row["id"], gift_row["status"], session_id, checkout_url, checkout_expires_at)
            if not opened:
                raise ValueError("gift_checkout_identity_changed")
            record_gift_event(open_cur, opened, "checkout_opened", callback.from_user.id, source="stripe_checkout")
            open_conn.commit()
        finally:
            open_cur.close()
            open_conn.close()
        await callback.message.answer(CHECKOUT_OPEN_INSTRUCTION, reply_markup=gift_checkout_keyboard(checkout_url))
        await state.clear()
    except Exception as e:
        fail_conn = get_db_conn()
        fail_cur = fail_conn.cursor()
        try:
            mark_gift_checkout_failed(fail_cur, gift_row["id"], e)
            fail_conn.commit()
        finally:
            fail_cur.close()
            fail_conn.close()
        logging.error(
            "GIFT_CHECKOUT_CREATE_FAILED: gift=%s error_ref=%s",
            safe_log_id(gift_row["public_reference"]),
            safe_admin_error_reference("gift_checkout_create", e),
            exc_info=True,
        )
        await enqueue_admin_payment_problem_now(
            event_id=None,
            purpose="gift_checkout_creation_failed",
            stage="checkout_creation",
            telegram_id=callback.from_user.id,
            category="checkout_creation_failed",
            exception=e,
            stripe_retry="неизвестно",
            recovery_reminder="не применимо",
            safe_ref=safe_admin_context_reference("gift_checkout_create", gift_row["public_reference"]),
            note=f"gift: {gift_row['public_reference']}",
        )
        await callback.message.answer("Техническая ошибка при создании оплаты. Попробуйте позже или напишите администратору.")
        await state.clear()


@router.callback_query(F.data.startswith("gift_activate:"), StateFilter('*'))
async def gift_activate_callback(callback: types.CallbackQuery, state: FSMContext):
    public_reference = callback.data.split(":", 1)[1]
    state_data = await state.get_data()
    token_hash = state_data.get("gift_token_hash")
    token_version = state_data.get("gift_token_version")
    state_public_reference = state_data.get("gift_public_reference")
    if not token_hash or not token_version or state_public_reference != public_reference:
        await callback.answer("Откройте подарочную ссылку ещё раз.", show_alert=True)
        return
    subscription_state = await gift_recipient_subscription_state(callback.from_user.id)
    if subscription_state.get("action") == "fail":
        await callback.message.answer("🎁 Сейчас не удалось безопасно проверить текущую подписку. Попробуйте позже или напишите администратору.")
        await callback.answer()
        return
    activation_response_text = None
    activation_failed = False
    updated = None
    action = None
    effective_expiry = None
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        gift_row = fetch_gift_by_public_reference_version(cur, public_reference, token_version, for_update=True)
        if not gift_row or not hmac.compare_digest(str(gift_row.get("token_hash") or ""), str(token_hash)):
            conn.rollback()
            activation_response_text = "🎁 Эта подарочная ссылка недействительна или была перевыпущена."
        elif gift_row["status"] == "redeemed":
            conn.rollback()
            activation_response_text = "🎁 Этот подарок уже активирован."
        elif gift_row["status"] not in ("paid_unclaimed",):
            conn.rollback()
            activation_response_text = "🎁 Этот подарок сейчас нельзя активировать."
        elif gift_row.get("recipient_telegram_id") and int(gift_row["recipient_telegram_id"]) != int(callback.from_user.id):
            conn.rollback()
            activation_response_text = "🎁 Этот подарок уже закреплён за другим получателем."
        else:
            updated, action, effective_expiry = apply_gift_access_in_transaction(cur, gift_row, callback.from_user.id, subscription_state)
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(
            "GIFT_ACTIVATION_FAILED: gift=%s user=%s error_ref=%s",
            safe_log_id(public_reference),
            callback.from_user.id,
            safe_admin_error_reference("gift_activation", e),
            exc_info=True,
        )
        activation_failed = True
    finally:
        cur.close()
        conn.close()
    if activation_response_text:
        await callback.message.answer(activation_response_text)
        await callback.answer()
        return
    if activation_failed:
        await callback.message.answer("Не получилось активировать подарок. Попробуйте позже или напишите администратору.")
        await callback.answer()
        return
    if action == "blocked_active_auto_renew":
        await callback.message.answer(build_gift_reserved_recipient_text(updated), reply_markup=get_main_keyboard(callback.from_user.id))
        await callback.answer()
        return
    await state.clear()
    await callback.answer("Подарок активирован")


@router.message(F.text == "🚨 Правила клуба", StateFilter('*'))
async def rules_button_handler(message: types.Message, state: FSMContext):
    await state.clear()

    rules_text = """📜 <b>Правила и регламент онлайн-клуба</b>

Чувствуйте себя комфортно и относитесь бережно к себе, к своему телу и друг другу.

<b>Основные правила, по которым мы будем взаимодействовать:</b>

<b>1. Клуб закрытый и включает:</b>
— неограниченный доступ ко всем материалам
— тренировки в записи
— рецепты
— общение и обратную связь

Также остаются живые тренировки по расписанию.

<b>2. На живые тренировки обязательна предварительная запись.</b>

<b>3. Чтобы записаться, нужно отметить себя в голосовании,</b> которое я буду создавать накануне занятия.

<b>4. Если на тренировку записывается менее 3 человек, занятие не проводится.</b>

<b>5. Записей живых тренировок не будет.</b>

<b>6. Заморозка абонемента не предусмотрена,</b> так как у вас всегда есть доступ ко всем тренировкам в записи и вы можете заниматься в удобное время.


<b>Что входит в абонемент клуба:</b>

— большая <b>база тренировок разной направленности, которая будет постоянно пополняться:</b>
<i>антистулость, сила и гибкость, работа с мышцами тазового дна, ягодицы, руки, ноги, кор, балансы</i>

— тренировки, направленные не только на тело, но и на <b>улучшение нейропластичности, координации и общего качества движений</b>

— короткие <b>зарядки 10–15 минут</b> для ежедневной практики

— <b>мини-уроки:</b> дыхание, работа со стопами, расслабление

— <b>медитации и техники восстановления</b>

— <b>живые тренировки со мной</b>
это не просто тренировки, а возможность поработать со мной лично: разобрать технику, задать вопросы, скорректировать движения и глубже понять свое тело

— <b>постоянная обратная связь:</b> вы можете задавать любые вопросы в чате, я всегда на связи"""

    await message.answer(rules_text, parse_mode="HTML", reply_markup=get_main_keyboard(message.from_user.id))


@router.message(F.text == "💬 Задать вопрос", StateFilter('*'))
async def ask_question_button(message: types.Message, state: FSMContext):
    await state.clear()

    kb = reply_keyboard([[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True)

    await message.answer(
        "💬 Напишите ваш вопрос одним сообщением.\n\n"
        "Я передам его администратору, и вам ответят здесь, в этом чате.",
        reply_markup=kb
    )

    await state.set_state(ContactState.waiting_for_message)

@router.message(StateFilter(ContactState.waiting_for_message))
async def forward_question_to_admin(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer(
            "Отправка вопроса отменена.",
            reply_markup=get_main_keyboard(message.from_user.id)
        )
        return

    user = message.from_user
    username = f"@{user.username}" if user.username else "username не указан"

    try:
        for admin_id in ADMIN_IDS:
            await bot.forward_message(
                chat_id=admin_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )

            kb = inline_keyboard([[
                InlineKeyboardButton(text="✍️ Ответить", callback_data=f"reply_to_{user.id}")
            ]])

            await bot.send_message(
                admin_id,
                f"📩 Новый вопрос от пользователя:\n\n"
                f"ID: {user.id}\n"
                f"Username: {username}\n"
                f"Имя: {user.full_name}",
                reply_markup=kb
            )

        conn = get_db_conn()
        cur = conn.cursor()

        try:
            cur.execute(
                "UPDATE users SET feedback_received = TRUE WHERE telegram_id = %s",
                (user.id,)
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        await message.answer(
            "✅ Ваш вопрос отправлен администратору.\n"
            "Ответ придет здесь, в этом чате.",
            reply_markup=get_main_keyboard(message.from_user.id)
        )

    except Exception as e:
        logging.error(f"Ошибка отправки вопроса админу от {user.id}: {e}")
        await message.answer(
            "❌ Не удалось отправить вопрос. Попробуйте позже или напишите @re_tasha.",
            reply_markup=get_main_keyboard(message.from_user.id)
        )

    finally:
        await state.clear()

@router.callback_query(F.data.startswith("reply_to_"), StateFilter('*'))
async def start_admin_reply(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Недоступно.", show_alert=True)
        return

    try:
        target_user_id = int(callback.data.replace("reply_to_", ""))
    except ValueError:
        await callback.answer("Ошибка ID пользователя.", show_alert=True)
        return

    await state.update_data(reply_to_user=target_user_id)
    await state.set_state(ReplyState.waiting_for_reply)

    await callback.message.answer(
        f"✍️ Отправьте ответ для пользователя {target_user_id} одним сообщением.\n\n"
        f"Можно отправить текст, фото, видео, голосовое или документ.\n"
        f"Чтобы отменить, отправьте /cancel."
    )

    await callback.answer()

@router.message(StateFilter(ReplyState.waiting_for_reply))
async def send_admin_reply(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    if message.text in ["/cancel", "❌ Отмена"]:
        await state.clear()
        await message.answer("Ответ отменен.")
        return

    data = await state.get_data()
    target_user_id = data.get("reply_to_user")

    if not target_user_id:
        await state.clear()
        await message.answer("❌ Не найден пользователь для ответа.")
        return

    try:
        await bot.send_message(
            int(target_user_id),
            "💬 Ответ администратора:",
            reply_markup=inline_keyboard([[
                InlineKeyboardButton(
                    text="Ответить администратору",
                    callback_data="support_reply",
                )
            ]]),
        )

        await bot.copy_message(
            chat_id=int(target_user_id),
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )

        await message.answer(f"✅ Ответ отправлен пользователю {target_user_id}.")
        await state.clear()

    except TelegramForbiddenError:
        conn = get_db_conn()
        cur = conn.cursor()

        try:
            cur.execute(
                "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                (int(target_user_id),)
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        await message.answer("⚠️ Пользователь заблокировал бота. Ответ не отправлен.")
        await state.clear()

    except Exception as e:
        logging.error(f"Ошибка отправки ответа пользователю {target_user_id}: {e}")
        error_ref = safe_admin_error_reference("admin_reply_send", e)
        await message.answer(f"❌ Не удалось отправить ответ. ref: {error_ref}")
        await state.clear()


@router.callback_query(F.data == "support_reply", StateFilter('*'))
async def start_support_reply(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(SupportReplyState.waiting_for_message)
    await callback.message.answer(
        "Напишите сообщение или отправьте фото/видео, и я передам его администратору."
    )
    await callback.answer()


def support_reply_kind(message):
    if getattr(message, "photo", None):
        return "photo"
    if getattr(message, "video", None):
        return "video"
    if getattr(message, "text", None):
        return "text"
    return None


def support_reply_is_within_telegram_limits(message, kind):
    if kind == "text":
        return len(message.text) <= 4096
    return len(getattr(message, "caption", None) or "") <= 1024


def build_support_reply_context(user):
    username = f"@{user.username}" if getattr(user, "username", None) else "username не указан"
    display_name = getattr(user, "full_name", None) or "имя не указано"
    return (
        "💬 Ответ пользователя по обращению\n\n"
        f"ID: {user.id}\n"
        f"Username: {username}\n"
        f"Имя: {display_name}"
    )


async def deliver_support_reply_to_admin(admin_id, message, kind, context):
    await bot.send_message(
        admin_id,
        context,
        reply_markup=inline_keyboard([[
            InlineKeyboardButton(
                text="✍️ Ответить",
                callback_data=f"reply_to_{message.from_user.id}",
            )
        ]]),
    )
    if kind == "text":
        await bot.send_message(admin_id, message.text)
    elif kind == "photo":
        await bot.send_photo(admin_id, message.photo[-1].file_id, caption=getattr(message, "caption", None))
    else:
        await bot.send_video(admin_id, message.video.file_id, caption=getattr(message, "caption", None))


@router.message(StateFilter(SupportReplyState.waiting_for_message))
async def send_support_reply(message: types.Message, state: FSMContext):
    kind = support_reply_kind(message)
    if kind is None:
        await message.answer("Можно отправить только текст, фото или видео.")
        return
    if not support_reply_is_within_telegram_limits(message, kind):
        await message.answer("Сообщение слишком длинное. Сократите текст и попробуйте снова.")
        return

    user = message.from_user
    context = build_support_reply_context(user)
    delivered_admin_ids = []
    for admin_id in ADMIN_IDS:
        try:
            await deliver_support_reply_to_admin(admin_id, message, kind, context)
            delivered_admin_ids.append(admin_id)
        except Exception as e:
            logging.error(
                "SUPPORT_REPLY_DELIVERY_FAILED: user_id=%s, admin_id=%s, ref=%s",
                user.id, admin_id, safe_admin_error_reference("support_reply_delivery", e),
            )

    if not delivered_admin_ids:
        await message.answer("❌ Не удалось передать сообщение. Попробуйте ещё раз.")
        return

    await state.clear()
    await message.answer(
        "Спасибо, сообщение передано администратору.",
        reply_markup=inline_keyboard([[
            InlineKeyboardButton(text="Ответить ещё", callback_data="support_reply")
        ]]),
    )

@router.message(Command('ask'), StateFilter('*'))
async def ask_command(message: types.Message, state: FSMContext):
    await ask_question_button(message, state)

@router.callback_query(F.data == "feedback_join", StateFilter('*'))
async def feedback_join(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()

    user_id = callback.from_user.id

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE users
            SET feedback_received = TRUE
            WHERE telegram_id = %s
        """, (user_id,))

        cur.execute("""
            SELECT paid, trial_used
            FROM users
            WHERE telegram_id = %s
        """, (user_id,))

        row = cur.fetchone()
        conn.commit()

        paid = row[0] if row else False
        trial_used = row[1] if row else False
        show_trial = not (paid or trial_used)

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка feedback_join для {user_id}: {e}")
        cur.close()
        conn.close()
        await callback.answer("Не удалось открыть тарифы. Попробуйте /start.", show_alert=True)
        return

    finally:
        if not conn.closed:
            cur.close()
            conn.close()

    await state.set_state(RegistrationStates.choice)

    await callback.message.answer(
        "Отлично. Выберите удобный формат участия:",
        reply_markup=get_tariffs_keyboard(show_trial=show_trial)
    )

    await callback.answer()


@router.callback_query(F.data == "feedback_question", StateFilter('*'))
async def feedback_question(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE users
            SET feedback_received = TRUE
            WHERE telegram_id = %s
        """, (user_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка feedback_question для {user_id}: {e}")
    finally:
        cur.close()
        conn.close()

    await state.clear()

    kb = reply_keyboard([[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True)

    await callback.message.answer(
        "💬 Напишите ваш вопрос одним сообщением.\n\n"
        "Я передам его администратору, и вам ответят здесь, в этом чате.",
        reply_markup=kb
    )

    await state.set_state(ContactState.waiting_for_message)
    await callback.answer()


@router.callback_query(F.data == "feedback_think", StateFilter('*'))
async def feedback_think(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()

    user_id = callback.from_user.id

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE users
            SET feedback_received = TRUE
            WHERE telegram_id = %s
        """, (user_id,))
        conn.commit()

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка feedback_think для {user_id}: {e}")
        cur.close()
        conn.close()
        await callback.answer("Ошибка. Попробуйте позже.", show_alert=True)
        return

    finally:
        if not conn.closed:
            cur.close()
            conn.close()

    await callback.message.answer(
        "Хорошо, возвращайтесь, когда будет удобно.\n\n"
        "В меню ниже можно открыть тарифы, задать вопрос или посмотреть профиль.",
        reply_markup=get_main_keyboard(callback.from_user.id)
    )

    await callback.answer()

@router.message(F.text == "🧘 Бесплатный урок", StateFilter('*'))
async def free_lesson_button(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = int(message.from_user.id)
    show_trial = True
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO users (telegram_id, paid)
            VALUES (%s, FALSE)
            ON CONFLICT (telegram_id) DO NOTHING
        """, (user_id,))
        cur.execute("""
            SELECT video_sent, paid, trial_used
            FROM users
            WHERE telegram_id = %s
        """, (user_id,))
        row = cur.fetchone()
        video_sent = row[0] if row else False
        paid = row[1] if row else False
        trial_used = row[2] if row else False
        show_trial = not (paid or trial_used)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    if video_sent:
        await message.answer(
            "✅ Вы уже получали бесплатный урок.\n\n"
            "Если вам понравился формат, вы можете оформить доступ к клубу и продолжить занятия:",
            reply_markup=get_tariffs_keyboard(show_trial=show_trial)
        )
        return

    result = await process_claimed_delivery(
        get_db_conn,
        f"free_lesson:{user_id}",
        user_id,
        "free_lesson",
        lambda: send_free_lesson_delivery(user_id, {"variant": "manual"}),
        blocked_exc=(TelegramForbiddenError,),
        classify_error_func=classify_delivery_error,
        log_failure_func=log_outbox_delivery_failure,
        terminal_error_callback=lambda error, decision, current_attempt_count: notify_terminal_free_lesson_delivery_error(
            f"free_lesson:{user_id}",
            "free_lesson",
            current_attempt_count,
            error,
            decision,
        ),
    )
    if result in ("already_sent", "already_processing"):
        logging.info("FREE_LESSON_DELIVERY_SKIPPED: user_id=%s, status=%s", safe_log_id(user_id), result)
        return
    if result != "sent":
        await message.answer(
            "❌ Не удалось отправить бесплатный урок. Попробуйте позже или напишите @re_tasha.",
            reply_markup=get_main_keyboard(message.from_user.id)
        )

def get_free_lesson_feedback_keyboard():
    return inline_keyboard([
        [InlineKeyboardButton(text="Хочу в клуб", callback_data="feedback_join")],
        [InlineKeyboardButton(text="Задать вопрос", callback_data="feedback_question")],
        [InlineKeyboardButton(text="Пока думаю", callback_data="feedback_think")],
    ])

def get_manual_free_lesson_caption():
    return """<b>Чтобы почувствовать изменения в теле и самочувствии, не нужно усложнять.</b>

Для того чтобы уменьшить напряжение, скованность и дискомфорт в теле, не нужен зал, сложное оборудование и час свободного времени. Иногда достаточно коврика и 15 минут правильного движения.

Именно поэтому я подготовила эту пробную тренировку на осанку — приятную, понятную и эффективную.

<b>Она подойдет, если вы:</b>
— только начинаете тренироваться;
— устали от жестких нагрузок;
— хотите чувствовать тело лучше без перегрузки.

<b>После тренировки вы можете почувствовать:</b>
— больше легкости и подвижности;
— меньше напряжения в теле;
— ощущение, что тело наконец стало более собранным.

Если вам понравится такой подход, вы сможете попробовать онлайн-клуб и получить доступ к полноценным тренировкам, зарядкам, дыхательным практикам, рецептам и поддержке."""


def get_auto_free_lesson_caption():
    return """<b>Я подготовила для вас бесплатную пробную тренировку.</b>

Иногда, чтобы почувствовать больше легкости, подвижности и контакта с телом, не нужен зал, сложное оборудование и час свободного времени. Достаточно коврика и 15 минут правильного движения.

Эта тренировка поможет мягко включиться в практику и почувствовать формат клуба.

<b>Она подойдет, если вы:</b>
— только начинаете тренироваться;
— устали от жестких нагрузок;
— хотите чувствовать тело лучше без перегрузки.

Если вам понравится такой подход, вы сможете попробовать онлайн-клуб и получить доступ к полноценным тренировкам, зарядкам, дыхательным практикам, рецептам и поддержке."""


def get_free_lesson_join_keyboard():
    return inline_keyboard([[
        InlineKeyboardButton(text="Хочу в клуб", callback_data="sub_trial")
    ]])


def get_free_lesson_followup_text():
    return (
        "Как ощущения после пробной тренировки?\n\n"
        "Удалось почувствовать больше легкости, подвижности или контакта с телом?\n\n"
        "Если вам откликнулся такой формат, вы можете продолжить занятия в клубе: "
        "там собраны тренировки, зарядки, дыхательные практики, рецепты и поддержка.\n\n"
        "Выберите, что вам сейчас ближе:"
    )


async def send_free_lesson_delivery(user_id, payload=None):
    video_id = os.getenv("FREE_LESSON_VIDEO_ID")
    if not video_id:
        raise MissingFreeLessonVideoError("FREE_LESSON_VIDEO_ID missing for free_lesson")
    payload = payload or {}
    caption = get_manual_free_lesson_caption() if payload.get("variant") == "manual" else get_auto_free_lesson_caption()
    await bot.send_video(
        chat_id=int(user_id),
        video=video_id,
        caption=caption,
        reply_markup=get_free_lesson_join_keyboard(),
        parse_mode="HTML",
    )


async def send_free_lesson_followup_delivery(user_id, payload=None):
    await bot.send_message(
        int(user_id),
        get_free_lesson_followup_text(),
        reply_markup=get_free_lesson_feedback_keyboard(),
    )


async def send_auto_free_lesson(user_id, cur=None):
    async def send_video_once():
        await send_free_lesson_delivery(user_id, {})

    result = await process_claimed_delivery(
        get_db_conn,
        f"free_lesson:{int(user_id)}",
        int(user_id),
        "free_lesson",
        send_video_once,
        blocked_exc=(TelegramForbiddenError,),
        classify_error_func=classify_delivery_error,
        log_failure_func=log_outbox_delivery_failure,
        terminal_error_callback=lambda error, decision, current_attempt_count: notify_terminal_free_lesson_delivery_error(
            f"free_lesson:{int(user_id)}",
            "free_lesson",
            current_attempt_count,
            error,
            decision,
        ),
    )
    return result == "sent"

async def check_auto_free_lessons():
    logging.info("--- Проверка автоотправки бесплатного урока ---")

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT telegram_id
            FROM users
            WHERE paid = FALSE
              AND trial_used = FALSE
              AND video_sent = FALSE
              AND registered_at IS NOT NULL
              AND registered_at <= NOW() - INTERVAL '2 days'
              AND (blocked_bot IS NOT TRUE)
            ORDER BY registered_at ASC
            LIMIT 50
        """)

        users = cur.fetchall()
        cur.close()
        conn.close()
        cur = None
        conn = None

        sent = 0
        blocked = 0
        failed = 0

        for (user_id,) in users:
            try:
                was_sent = await send_auto_free_lesson(user_id)
                if was_sent:
                    sent += 1
            except TelegramForbiddenError:
                blocked += 1
                user_conn = get_db_conn()
                user_cur = user_conn.cursor()
                try:
                    user_cur.execute(
                        "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                        (int(user_id),)
                    )
                    user_conn.commit()
                finally:
                    user_cur.close()
                    user_conn.close()
            except Exception as e:
                if is_undeliverable_user_error(e):
                    blocked += 1
                    user_conn = get_db_conn()
                    user_cur = user_conn.cursor()
                    try:
                        user_cur.execute(
                            "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                            (int(user_id),)
                        )
                        user_conn.commit()
                    finally:
                        user_cur.close()
                        user_conn.close()
                    logging.info(f"Пользователь {user_id} помечен blocked_bot после ошибки автоурока: {e}")
                else:
                    failed += 1
                    logging.error(f"Ошибка автоотправки бесплатного урока для {user_id}: {e}")

        logging.info(
            f"Автоурок: отправлено={sent}, заблокировали={blocked}, ошибки={failed}"
        )

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка check_auto_free_lessons: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


async def send_free_lesson_followup(user_id, cur=None):
    async def send_followup_once():
        await send_free_lesson_followup_delivery(user_id, {})

    result = await process_claimed_delivery(
        get_db_conn,
        f"free_lesson_followup:{int(user_id)}",
        int(user_id),
        "free_lesson_followup",
        send_followup_once,
        blocked_exc=(TelegramForbiddenError,),
        classify_error_func=classify_delivery_error,
        log_failure_func=log_outbox_delivery_failure,
        terminal_error_callback=lambda error, decision, current_attempt_count: notify_terminal_free_lesson_delivery_error(
            f"free_lesson_followup:{int(user_id)}",
            "free_lesson_followup",
            current_attempt_count,
            error,
            decision,
        ),
        success_update_sql="""
        UPDATE users
        SET feedback_sent = TRUE,
            feedback_sent_at = NOW()
        WHERE telegram_id = %s
        """,
        success_update_params=(int(user_id),),
    )
    if result in ("already_sent", "already_processing"):
        logging.info("FREE_LESSON_FOLLOWUP_DELIVERY_SKIPPED: user_id=%s, status=%s", safe_log_id(user_id), result)
    return result == "sent"

@router.message(F.content_type.in_([
    types.ContentType.NEW_CHAT_MEMBERS,
    types.ContentType.LEFT_CHAT_MEMBER,
]), StateFilter('*'))
async def delete_join_leave_service_messages(message: types.Message):
    if str(message.chat.id) != str(GROUP_ID):
        return

    chat_id = message.chat.id
    chat_title = getattr(message.chat, "title", None)
    message_id = message.message_id
    event_datetime = getattr(message, "date", None)
    event_type = "unknown_service_message"
    event_user = None
    service_event_users = []

    if getattr(message, "new_chat_members", None):
        event_type = "user_joined"
        service_event_users = list(message.new_chat_members or [])
        event_user = service_event_users[0] if service_event_users else None
    elif getattr(message, "left_chat_member", None):
        event_type = "user_left"
        event_user = message.left_chat_member
        service_event_users = [event_user] if event_user else []

    user_id = getattr(event_user, "id", None)
    username = getattr(event_user, "username", None)
    full_name = getattr(event_user, "full_name", None)
    service_message_deleted = False
    for service_user in service_event_users:
        service_user_id = getattr(service_user, "id", None)
        is_bot_user = bool(getattr(service_user, "is_bot", False))
        if not service_user_id or is_bot_user:
            if is_bot_user:
                await log_access_event(
                    service_user_id,
                    "group_member_joined_bot_preserved" if event_type == "user_joined" else "group_member_left_bot",
                    source="telegram_group",
                )
            continue
        save_telegram_user_profile(service_user)
        if event_type == "user_joined":
            access_active = False
            db_error = False
            try:
                access_conn = get_db_conn()
                access_cur = access_conn.cursor()
                try:
                    access_cur.execute(
                        """
                        SELECT paid, expiry_date, payment_failed, grace_period_end
                        FROM users
                        WHERE telegram_id = %s
                        """,
                        (int(service_user_id),),
                    )
                    access_row = access_cur.fetchone()
                    access_active = bool(
                        access_row and has_active_access(
                            access_row[0],
                            access_row[1],
                            payment_failed=access_row[2],
                            grace_period_end=access_row[3],
                        )
                    )
                finally:
                    access_cur.close()
                    access_conn.close()
            except Exception as e:
                db_error = True
                logging.critical(
                    "GROUP_JOIN_ACCESS_DB_ERROR: user_id=%s, error=%s",
                    service_user_id,
                    str(e),
                    exc_info=True,
                )
                error_ref = safe_admin_error_reference("group_join_access_db", e)
                await notify_admins(
                    "Критично: не удалось проверить доступ нового участника группы.\n\n"
                    f"telegram_id: {service_user_id}\n"
                    f"Ошибка: проверка не выполнена. ref: {error_ref}\n\n"
                    "Пользователь оставлен в группе до ручной проверки."
                )

            decision = group_join_decision(
                service_user_id,
                is_bot_user,
                service_user_id in ADMIN_IDS,
                access_active,
                db_error=db_error,
            )
            if decision == "authorized":
                await log_access_event(service_user_id, "group_member_joined_authorized", source="telegram_group")
            elif decision == "remove_unauthorized":
                try:
                    chat_member = await bot.get_chat_member(chat_id=int(GROUP_ID), user_id=int(service_user_id))
                    telegram_status = getattr(chat_member, "status", None)
                    if telegram_status in ("administrator", "creator"):
                        await log_access_event(
                            service_user_id,
                            f"group_member_joined_{telegram_status}_preserved",
                            source="telegram_group",
                        )
                        continue
                except Exception as e:
                    logging.critical(
                        "GROUP_JOIN_TELEGRAM_STATUS_ERROR: user_id=%s, error=%s",
                        service_user_id,
                        str(e),
                        exc_info=True,
                    )
                    error_ref = safe_admin_error_reference("group_join_telegram_status", e)
                    await notify_admins(
                        "Критично: не удалось проверить Telegram-статус нового участника группы.\n\n"
                        f"telegram_id: {service_user_id}\n"
                        f"Ошибка: проверка не выполнена. ref: {error_ref}\n\n"
                        "Пользователь оставлен в группе до ручной проверки."
                    )
                    continue
                try:
                    await bot.ban_chat_member(chat_id=int(GROUP_ID), user_id=int(service_user_id))
                    await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(service_user_id))
                    await log_access_event(
                        service_user_id,
                        "group_member_joined_unauthorized_removed",
                        source="telegram_group",
                    )
                    await notify_admins(
                        "Удалён пользователь без активного доступа, вошедший в закрытую группу.\n\n"
                        f"telegram_id: {service_user_id}"
                    )
                    try:
                        await bot.send_message(
                            int(service_user_id),
                            "Доступ в закрытую группу открыт только после оплаты. Выберите тариф в боте, пожалуйста."
                        )
                    except Exception:
                        logging.info("GROUP_JOIN_UNAUTHORIZED_DM_FAILED: user_id=%s", service_user_id, exc_info=True)
                except Exception as e:
                    logging.error(
                        "GROUP_JOIN_UNAUTHORIZED_REMOVE_FAILED: user_id=%s, error=%s",
                        service_user_id,
                        str(e),
                        exc_info=True,
                    )
                    error_ref = safe_admin_error_reference("group_join_remove", e)
                    await notify_admins(
                        "Не удалось удалить из группы пользователя без активного доступа.\n\n"
                        f"telegram_id: {service_user_id}\n"
                        f"Ошибка: действие не выполнено. ref: {error_ref}"
                    )
            else:
                await log_access_event(service_user_id, f"group_member_joined_{decision}", source="telegram_group")
            continue
        await log_access_event(
            service_user_id,
            "group_member_joined" if event_type == "user_joined" else "group_member_left",
            source="telegram_group",
        )

    logging.info(
        "GROUP_SERVICE_MESSAGE: chat_id=%s, chat_title=%s, message_id=%s, event_type=%s, "
        "user_id=%s, username=%s, full_name=%s, event_datetime=%s, service_message_deleted=%s",
        chat_id,
        chat_title,
        message_id,
        event_type,
        user_id,
        username,
        full_name,
        event_datetime,
        service_message_deleted,
    )

    try:
        await message.delete()
        service_message_deleted = True
        logging.info(
            "SERVICE_MESSAGE_DELETED_ONLY: chat_id=%s, message_id=%s, event_type=%s, user_id=%s",
            chat_id,
            message_id,
            event_type,
            user_id,
        )
        logging.info(
            "GROUP_SERVICE_MESSAGE_DELETE_RESULT: chat_id=%s, chat_title=%s, message_id=%s, "
            "event_type=%s, user_id=%s, username=%s, full_name=%s, "
            "service_message_deleted=%s, result=%s",
            chat_id,
            chat_title,
            message_id,
            event_type,
            user_id,
            username,
            full_name,
            service_message_deleted,
            "success",
        )
    except Exception as e:
        logging.warning(
            "GROUP_SERVICE_MESSAGE_DELETE_RESULT: chat_id=%s, chat_title=%s, message_id=%s, "
            "event_type=%s, user_id=%s, username=%s, full_name=%s, "
            "service_message_deleted=%s, result=%s, error=%s",
            chat_id,
            chat_title,
            message_id,
            event_type,
            user_id,
            username,
            full_name,
            service_message_deleted,
            "error",
            str(e),
            exc_info=True,
        )

# --- ХЕНДЛЕРЫ КОМАНД И КОЛБЭКОВ ---
async def show_gift_deep_link(message, state, token):
    parsed_token = parse_gift_token(token)
    if not parsed_token:
        await message.answer("🎁 Эта подарочная ссылка недействительна или уже была перевыпущена.")
        return
    public_reference, token_version = parsed_token
    token_hash = gift_token_hash(token)
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        gift_row = fetch_gift_by_public_reference_version(cur, public_reference, token_version, for_update=False)
    finally:
        cur.close()
        conn.close()
    if not gift_row or not hmac.compare_digest(str(gift_row.get("token_hash") or ""), token_hash):
        await message.answer("🎁 Эта подарочная ссылка недействительна или уже была перевыпущена.")
        return
    if gift_row["status"] not in ("paid_unclaimed", "reserved"):
        status_text = {
            "checkout_pending": "Подарок ещё не оплачен.",
            "checkout_open": "Подарок ещё не оплачен.",
            "payment_pending": "Оплата подарка ещё подтверждается.",
            "redeemed": "Этот подарок уже активирован.",
            "cancelled": "Этот подарок отменён.",
            "refunded": "Этот подарок был возвращён.",
            "review_required": "Этот подарок требует ручной проверки.",
        }.get(gift_row["status"], "Этот подарок сейчас недоступен.")
        await message.answer(f"🎁 {status_text}")
        return
    if gift_row.get("recipient_telegram_id") and int(gift_row["recipient_telegram_id"]) != int(message.from_user.id):
        await message.answer("🎁 Этот подарок уже закреплён за другим получателем.")
        return
    await state.update_data(
        gift_token_hash=token_hash,
        gift_token_version=token_version,
        gift_public_reference=gift_row["public_reference"],
    )
    text = (
        "🎁 Подарок найден\n\n"
        f"Срок доступа: {gift_tariff_label(gift_row['tariff_code'])}\n"
        f"От: {gift_safe_user_text(gift_row.get('sender_name') or 'друга')}\n\n"
        "Активировать его на этот Telegram-аккаунт?"
    )
    await message.answer(text, reply_markup=gift_activation_keyboard(gift_row["public_reference"]), parse_mode="HTML")


@router.message(CommandStart(), StateFilter('*'))
async def start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    save_telegram_user_profile(message.from_user)

    # Добавляем пользователя в БД (если его ещё нет)
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO users (telegram_id, paid)
            VALUES (%s, FALSE)
            ON CONFLICT (telegram_id) DO NOTHING
        """, (user_id,))
        conn.commit()
    except Exception as e:
        logging.error(f"Ошибка добавления {user_id}: {e}")
    finally:
        cur.close()
        conn.close()

    start_parts = (message.text or "").split(maxsplit=1)
    if len(start_parts) == 2 and start_parts[1].startswith(GIFT_TOKEN_PREFIX):
        await show_gift_deep_link(message, state, start_parts[1][len(GIFT_TOKEN_PREFIX):])
        return

    # Отправка приветствия
    await state.set_state(RegistrationStates.intro)
    text = """<b>Добро пожаловать в закрытый клуб Натальи Ребковец.</b>

Здесь тренировки построены на современных знаниях о движении, нейрофизиологии и работе тела.

Силовые тренировки, йога, пилатес, кинезиологические упражнения, работа с дыханием, мобильностью и двигательными паттернами — для сильного, здорового и функционального тела без перегрузки.

<b>Готовы начать путь к здоровому и сильному телу? Тогда — поехали!</b>"""
    kb = inline_keyboard([[
        InlineKeyboardButton(text="➡️ Продолжить", callback_data="to_desc")
    ]])
    await bot.send_photo(message.chat.id, PHOTO_URL_INTRO, caption=text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "to_desc", StateFilter(RegistrationStates.intro))
async def show_description(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(RegistrationStates.description)
    text = """<b>Внутри клуба вас ждёт:</b>

🧠 <b>Библиотека тренировок</b> — 50+ уроков с системным подходом: осанка, сила, мобильность, стопы, гибкость и работа с движением. База регулярно пополняется.

🔋 <b>Короткие зарядки</b> — 10–15 минут для энергии, снятия напряжения и уменьшения отёков.

🧘🏽‍♀️ <b>Медитации и дыхательные практики</b> — для расслабления, восстановления и работы с нервной системой.

🩹 <b>Фитнес-аптечка</b> — короткие уроки для быстрой помощи при боли, напряжении и дискомфорте в теле.

🥗 <b>Раздел с рецептами</b> и обратной связью от врача-нутрициолога.

👩🏽‍💻 <b>Живые Zoom-уроки 2–4 раза в месяц</b> — разбор техники, двигательных паттернов, перекосов и индивидуальная коррекция в формате группы.

💬 <b>Закрытый чат поддержки,</b> — где я лично отвечаю на вопросы."""
    kb = inline_keyboard([[
        InlineKeyboardButton(text="➡️ Продолжить", callback_data="to_rules")
    ]])

    # ВСТАВЬТЕ СЮДА ВАШ VIDEO FILE_ID, КОТОРЫЙ ВЫ ПОЛУЧИЛИ
    VIDEO_DESCRIPTION = "BAACAgIAAxkBAAIGMmoS7DVlRexpNBTPxk0wPmGESaPYAAKzrgAC-F-YSKfL_HEbOt--OwQ"

    await bot.send_video(
        chat_id=callback.message.chat.id,
        video=VIDEO_DESCRIPTION,
        caption=text,
        reply_markup=kb,
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(F.data == "to_rules", StateFilter(RegistrationStates.description))
async def show_rules(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(RegistrationStates.rules)
    text = """Часто спрашивают:

❔ <i>«Я новичок, справлюсь?»</i>
— Да. Все упражнения имеют упрощённые варианты.

❔ <i>«У меня болит спина / колено / шея»</i>
— Клуб помогает восстанавливаться. Но если острый период — сначала к врачу.

❔ <i>«Нет времени»</i>
— У нас есть зарядки на 10 минут. И система, которая встраивается в ваш ритм.

❔ <i>«Я далеко, в другом часовом поясе»</i>
— Всё онлайн. Доступ из любой точки мира.

Клуб подходит и мужчинам, и женщинам, любому возрасту и уровню подготовки.
Главное — желание чувствовать себя лучше."""
    kb = inline_keyboard([[
        InlineKeyboardButton(text="➡️ Продолжить", callback_data="to_choice")
    ]])
    await bot.send_message(callback.message.chat.id, text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data == "to_choice", StateFilter(RegistrationStates.rules))
async def show_choice(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(RegistrationStates.choice)
    text = """<b>Выберите свой формат участия:</b>

👀 <i>Пробная неделя</i> — чтобы познакомиться с клубом и попробовать формат
💳 <i>Абонемент на 1, 6 или 12 месяцев</i> — для системной работы с телом

Нажмите на кнопку ниже👇🏽 , чтобы перейти к оплате.

И до встречи на тренировках 🤸🏽‍♀️"""
    # Определяем, показывать ли пробный период (если пользователь уже paid — не показываем)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT paid, trial_used FROM users WHERE telegram_id = %s", (callback.from_user.id,))
    row = cur.fetchone()
    show_trial = not (row and (row[0] or row[1])) if row else True
    cur.execute("UPDATE users SET registered_at = COALESCE(registered_at, NOW()) WHERE telegram_id = %s", (callback.from_user.id,))
    conn.commit()
    cur.close()
    conn.close()
    kb = get_tariffs_keyboard(show_trial=show_trial)
    await bot.send_photo(callback.message.chat.id, PHOTO_URL_RULES, caption=text, reply_markup=kb, parse_mode="HTML")
    await callback.message.answer(
        "Главное меню доступно ниже ",
        reply_markup=get_main_keyboard(callback.from_user.id)
    )
    await callback.answer()

@router.callback_query(F.data.startswith('sub_'), StateFilter('*'))
async def process_payment(callback: types.CallbackQuery, state: FSMContext):
    sub_type = callback.data
    price_map = {
        "sub_trial": "PRICE_TRIAL",
        "sub_1": "PRICE_1M",
        "sub_6": "PRICE_6M",
        "sub_12": "PRICE_12M"
    }
    if sub_type not in price_map:
        await callback.answer(
            "Этот тариф больше недоступен. Пожалуйста, выберите тариф заново.",
            show_alert=True,
        )
        return

    await callback.answer("⏳ Проверяем...")
    user_id = callback.from_user.id
    save_telegram_user_profile(callback.from_user)

    # Получаем данные пользователя из БД
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            trial_used,
            paid,
            expiry_date,
            auto_renew,
            stripe_subscription_id,
            payment_failed,
            stripe_customer_id,
            EXISTS (
                SELECT 1
                FROM trial_redemptions tr
                WHERE tr.telegram_id = users.telegram_id
            ) AS trial_redeemed
        FROM users
        WHERE telegram_id = %s
    """, (user_id,))
    row = cur.fetchone()

    trial_used = row[0] if row else False
    paid = row[1] if row else False
    expiry_date = row[2] if row else None
    auto_renew = row[3] if row else False
    stripe_subscription_id = row[4] if row else None
    payment_failed = row[5] if row else False
    stripe_customer_id = row[6] if row else None
    trial_redeemed = row[7] if row else False

    if paid and expiry_date and expiry_date > datetime.utcnow() and not payment_failed:
        logging.info(
            f"Checkout заблокирован: у пользователя {user_id} уже есть активный доступ/подписка."
        )
        cur.close()
        conn.close()
        await callback.message.answer(
            f"✅ У вас уже есть активный доступ до {expiry_date.strftime('%d.%m.%Y %H:%M')}.\n"
            "Повторная оплата не нужна."
        )
        await state.clear()
        return

    mode = 'payment' if sub_type == "sub_trial" else 'subscription'

    if mode == 'subscription' and stripe_customer_id:
        try:
            subscriptions = await asyncio.to_thread(
                stripe.Subscription.list,
                customer=stripe_customer_id,
                status="all",
                limit=20,
            )
            blocking_subscriptions = active_or_resumable_subscriptions(subscriptions)
            if len(blocking_subscriptions) > 1:
                logging.critical(
                    "DUPLICATE_STRIPE_SUBSCRIPTIONS_CHECKOUT_BLOCKED: telegram_id=%s, customer_id=%s, count=%s",
                    user_id,
                    safe_log_id(stripe_customer_id),
                    len(blocking_subscriptions),
                )
                await enqueue_admin_payment_problem_now(
                    event_id=None,
                    purpose="customer_subscription_conflict",
                    stage="checkout_creation",
                    telegram_id=user_id,
                    category="customer_subscription_conflict",
                    stripe_retry="неизвестно",
                    recovery_reminder="неизвестно",
                    safe_ref=safe_admin_context_reference("customer_subscription_conflict", user_id, stripe_customer_id, len(blocking_subscriptions)),
                    note="Новый Checkout НЕ создан. Проверьте Stripe Dashboard вручную.",
                )
                cur.close()
                conn.close()
                await callback.message.answer(
                    "Мы проверяем вашу подписку вручную. Напишите администратору, пожалуйста."
                )
                await state.clear()
                return
            if blocking_subscriptions:
                existing_sub = blocking_subscriptions[0]
                existing_sub_id = getattr(existing_sub, "id", None)
                existing_status = getattr(existing_sub, "status", None)
                upsert_stripe_link(
                    cur,
                    user_id,
                    stripe_customer_id=stripe_customer_id,
                    stripe_subscription_id=existing_sub_id,
                    status=existing_status,
                    current_period_end=getattr(existing_sub, "current_period_end", None),
                    is_active=existing_status in ("active", "trialing"),
                    source="checkout_customer_subscription_guard",
                )
                if existing_sub_id:
                    cur.execute(
                        """
                        UPDATE users
                        SET stripe_subscription_id = COALESCE(stripe_subscription_id, %s)
                        WHERE telegram_id = %s
                        """,
                        (existing_sub_id, user_id),
                    )
                conn.commit()
                cur.close()
                conn.close()
                await send_existing_subscription_action(
                    callback,
                    user_id,
                    existing_sub_id,
                    stripe_customer_id,
                    existing_status,
                    current_period_end=getattr(existing_sub, "current_period_end", None),
                )
                await state.clear()
                return
        except Exception as e:
            logging.error(
                "CHECKOUT_CUSTOMER_SUBSCRIPTIONS_CHECK_FAILED: telegram_id=%s, customer_id=%s, error=%s",
                user_id,
                safe_log_id(stripe_customer_id),
                str(e),
                exc_info=True,
            )
            cur.close()
            conn.close()
            await enqueue_admin_payment_problem_now(
                event_id=None,
                purpose="checkout_customer_subscriptions_check_failed",
                stage="checkout_creation",
                telegram_id=user_id,
                exception=e,
                stripe_retry="неизвестно",
                recovery_reminder="неизвестно",
                safe_ref=safe_admin_error_reference("checkout_customer_subscriptions_check", e),
                note="Новый subscription Checkout НЕ создан. Пользователю предложено повторить позже.",
            )
            await callback.message.answer(
                "Сейчас не получается безопасно проверить вашу подписку в Stripe.\n"
                "Попробуйте позже или напишите администратору."
            )
            await state.clear()
            return

    if mode == 'subscription' and stripe_subscription_id:
        try:
            subscription = await asyncio.to_thread(stripe.Subscription.retrieve, stripe_subscription_id)
            status = getattr(subscription, 'status', None)
            current_period_end = getattr(subscription, 'current_period_end', None)
            customer = getattr(subscription, 'customer', None)
            customer_id = customer if isinstance(customer, str) else getattr(customer, 'id', None)
            customer_id = customer_id or stripe_customer_id
            period_source = "subscription.current_period_end"

            if status in ('past_due', 'unpaid', 'incomplete'):
                upsert_stripe_link(
                    cur,
                    user_id,
                    stripe_customer_id=customer_id,
                    stripe_subscription_id=stripe_subscription_id,
                    status=status,
                    current_period_end=current_period_end,
                    is_active=False,
                    source="checkout_existing_subscription_guard",
                )
                conn.commit()
                cur.close()
                conn.close()
                await send_existing_subscription_action(
                    callback,
                    user_id,
                    stripe_subscription_id,
                    customer_id,
                    status,
                    current_period_end=current_period_end
                )
                await state.clear()
                return

            if status in ('active', 'trialing') and not current_period_end:
                invoices = await asyncio.to_thread(
                    stripe.Invoice.list,
                    subscription=stripe_subscription_id,
                    limit=5
                )
                invoice_data = getattr(invoices, 'data', None) or []

                for invoice in invoice_data:
                    invoice_status = getattr(invoice, 'status', None)
                    if invoice_status != 'paid':
                        continue

                    lines = getattr(invoice, 'lines', None)
                    lines_data = getattr(lines, 'data', None) or []
                    first_line = lines_data[0] if lines_data else None
                    period = getattr(first_line, 'period', None)
                    period_end = getattr(period, 'end', None)

                    if period_end:
                        current_period_end = period_end
                        period_source = "invoice.lines.data[0].period.end"
                        break

            if status in ('active', 'trialing') and current_period_end:
                new_expiry = datetime.utcfromtimestamp(current_period_end)
                if new_expiry > datetime.utcnow():
                    cur.execute("""
                        UPDATE users
                        SET paid = TRUE,
                            expiry_date = GREATEST(COALESCE(expiry_date, %s), %s),
                            payment_failed = FALSE,
                            payment_failed_at = NULL,
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            stripe_customer_id = COALESCE(%s, stripe_customer_id),
                            blocked_bot = FALSE
                        WHERE telegram_id = %s
                    """, (new_expiry, new_expiry, customer_id, user_id))
                    upsert_stripe_link(
                        cur,
                        user_id,
                        stripe_customer_id=customer_id,
                        stripe_subscription_id=stripe_subscription_id,
                        status=status,
                        current_period_end=current_period_end,
                        is_active=True,
                        source="checkout_existing_subscription_guard",
                    )
                    conn.commit()
                    logging.info(
                        f"Checkout заблокирован: у пользователя {user_id} уже есть активная Stripe-подписка. "
                        f"period_source={period_source}"
                    )
                    logging.warning(
                        "EXISTING_STRIPE_SUBSCRIPTION_FOUND_CHECKOUT_BLOCKED: telegram_id=%s, "
                        "stripe_subscription_id=%s, stripe_customer_id=%s, status=%s, action=%s",
                        user_id,
                        safe_log_id(stripe_subscription_id),
                        safe_log_id(customer_id),
                        status,
                        "active_subscription_no_checkout",
                    )
                    cur.close()
                    conn.close()
                    await callback.message.answer(
                        f"✅ У вас уже есть активная подписка до {new_expiry.strftime('%d.%m.%Y %H:%M')}.\n"
                        "Повторная оплата не нужна."
                    )
                    await state.clear()
                    return

            if status in ('active', 'trialing') and not current_period_end:
                cur.execute("""
                    UPDATE users
                    SET stripe_subscription_id = %s,
                        stripe_customer_id = COALESCE(%s, stripe_customer_id),
                        auto_renew = TRUE,
                        payment_failed = FALSE,
                        payment_failed_at = NULL,
                        grace_period_end = NULL,
                        reminder_sent = FALSE,
                        blocked_bot = FALSE
                    WHERE telegram_id = %s
                """, (stripe_subscription_id, customer_id, user_id))
                upsert_stripe_link(
                    cur,
                    user_id,
                    stripe_customer_id=customer_id,
                    stripe_subscription_id=stripe_subscription_id,
                    status=status,
                    current_period_end=current_period_end,
                    is_active=True,
                    source="checkout_existing_subscription_guard",
                )
                conn.commit()
                logging.warning(
                    f"Checkout заблокирован: Stripe subscription active/trialing, но period_end не найден. "
                    f"user_id={user_id}, stripe_subscription_id={safe_log_id(stripe_subscription_id)}, "
                    f"customer_id={safe_log_id(customer_id)}"
                )
                logging.warning(
                    "EXISTING_STRIPE_SUBSCRIPTION_FOUND_CHECKOUT_BLOCKED: telegram_id=%s, "
                    "stripe_subscription_id=%s, stripe_customer_id=%s, status=%s, action=%s",
                    user_id,
                    safe_log_id(stripe_subscription_id),
                    safe_log_id(customer_id),
                    status,
                    "active_subscription_period_unknown_no_checkout",
                )
                cur.close()
                conn.close()
                await callback.message.answer(
                    "✅ У вас уже есть активная подписка.\n"
                    "Повторная оплата не нужна. Если доступ не обновился, напишите администратору."
                )
                await state.clear()
                return

            if status not in (None, 'canceled', 'incomplete_expired'):
                upsert_stripe_link(
                    cur,
                    user_id,
                    stripe_customer_id=customer_id,
                    stripe_subscription_id=stripe_subscription_id,
                    status=status,
                    current_period_end=current_period_end,
                    is_active=False,
                    source="checkout_existing_subscription_guard",
                )
                conn.commit()
                cur.close()
                conn.close()
                await send_existing_subscription_action(
                    callback,
                    user_id,
                    stripe_subscription_id,
                    customer_id,
                    status,
                    current_period_end=current_period_end
                )
                await state.clear()
                return
        except Exception as e:
            logging.error(
                "CHECKOUT_EXISTING_SUBSCRIPTION_GUARD_FAILED: user_id=%s, error_type=%s, error_ref=%s",
                user_id,
                type(e).__name__,
                safe_admin_error_reference("checkout_existing_subscription_guard", e),
            )
            cur.close()
            conn.close()
            await enqueue_admin_payment_problem_now(
                event_id=None,
                purpose="checkout_existing_subscription_guard_failed",
                stage="checkout_creation",
                telegram_id=user_id,
                exception=e,
                stripe_retry="неизвестно",
                recovery_reminder="неизвестно",
                safe_ref=safe_admin_error_reference("checkout_existing_subscription_guard", e),
                note="Новый subscription Checkout НЕ создан. Пользователю предложено повторить позже.",
            )
            await callback.message.answer(
                "Сейчас не получается безопасно проверить вашу подписку в Stripe.\n"
                "Попробуйте позже или напишите администратору."
            )
            await state.clear()
            return

    cur.close()
    conn.close()

    # Если нажата кнопка пробной недели
    if sub_type == "sub_trial":
        # Если пробный период уже использован ИЛИ у пользователя есть активная подписка
        if trial_used or paid:
            # Показываем клавиатуру с обычными тарифами (без пробного)
            await state.clear()
            kb = get_tariffs_keyboard(show_trial=False)
            text = "Вы уже использовали пробную неделю (или у вас активна подписка). Выберите платный тариф:"
            # Если сообщение имеет caption/текст, отредактируем, иначе отправим новое
            try:
                if callback.message.caption is not None:
                    await callback.message.edit_caption(caption=text, reply_markup=kb, parse_mode="HTML")
                elif callback.message.text:
                    await callback.message.edit_text(text=text, reply_markup=kb, parse_mode="HTML")
                else:
                    await callback.message.reply(text, reply_markup=kb, parse_mode="HTML")
            except Exception:
                await callback.message.reply(text, reply_markup=kb, parse_mode="HTML")
            return  # не создаём Stripe сессию
        if trial_redeemed:
            await state.clear()
            kb = get_tariffs_keyboard(show_trial=False)
            await callback.message.answer(
                "Пробная неделя уже была использована. Выберите платный тариф:",
                reply_markup=kb,
                parse_mode="HTML",
            )
            return

        # Иначе (пробный период не использован) – продолжаем создание оплаты пробной недели
        # (весь код ниже для sub_trial, но он такой же, как для остальных тарифов, поэтому вынесем общую логику)

    # Обработка всех тарифов (включая sub_trial, если прошли проверку)
    days_map = {
        "sub_trial": 7,
        "sub_1": 30,
        "sub_6": 180,
        "sub_12": 365
    }
    price_id = os.getenv(price_map[sub_type])
    days = days_map[sub_type]

    if not price_id:
        await callback.answer("Ошибка конфигурации тарифа.", show_alert=True)
        return

    try:
        session_params = {
            'payment_method_types': ['card'],
            'line_items': [{'price': price_id, 'quantity': 1}],
            'mode': mode,
            'success_url': 'https://t.me/Natalia_SoulFit_bot',
            'cancel_url': 'https://t.me/Natalia_SoulFit_bot',
            'client_reference_id': str(user_id),
            'metadata': {'days': str(days), 'telegram_id': str(user_id)}
        }

        if mode == 'subscription':
            session_params['subscription_data'] = {
                'metadata': {
                    'telegram_id': str(user_id)
                }
        }

        cache_key = (int(user_id), sub_type)
        attempt_count, attempt_timestamp = register_checkout_attempt(callback.from_user, sub_type)
        reused = False

        claim_conn = get_db_conn()
        claim_cur = claim_conn.cursor()
        checkout_record = None
        try:
            claim_result = claim_checkout_session_record(claim_cur, user_id, sub_type, mode)
            checkout_record = claim_result["record"]
            claim_conn.commit()
        finally:
            claim_cur.close()
            claim_conn.close()

        if claim_result["action"] == "creating_in_progress":
            await callback.message.answer("⏳ Ссылка на оплату уже создаётся. Попробуйте ещё раз через минуту.")
            await state.clear()
            return

        if claim_result["action"] == "payment_pending":
            await callback.message.answer(
                "⏳ Оплата уже обрабатывается Stripe.\n\n"
                "Пожалуйста, дождитесь подтверждения. Если статус долго не меняется, напишите администратору."
            )
            await state.clear()
            return

        if claim_result["action"] == "manual_review_required":
            logging.warning(
                "CHECKOUT_MANUAL_REVIEW_REQUIRED: record_id=%s, user_id=%s, sub_type=%s, mode=%s, session_id=%s",
                checkout_record.get("id"),
                user_id,
                sub_type,
                mode,
                safe_log_id(checkout_record.get("stripe_session_id")),
            )
            await enqueue_admin_payment_problem_now(
                event_id=None,
                purpose="checkout_manual_review_required",
                stage="checkout_creation",
                telegram_id=user_id,
                category="checkout_creation_failed",
                stripe_retry="неизвестно",
                recovery_reminder="неизвестно",
                safe_ref=safe_admin_context_reference("checkout_manual_review_required", checkout_record.get("id"), user_id, sub_type),
                note=(
                    f"record_id: {checkout_record.get('id')}\n"
                    f"tariff: {sub_type}\n"
                    f"mode: {mode}\n"
                    "Проверьте Stripe Dashboard и используйте /resolve_checkout <record_id> <failed|expired>."
                ),
            )
            await callback.message.answer(
                "Предыдущая попытка оплаты требует ручной проверки. "
                "Администратор уже получил диагностическое сообщение."
            )
            await state.clear()
            return

        if claim_result["action"] == "reuse_open":
            reused = True
            session_id = checkout_record["stripe_session_id"]
            checkout_url = checkout_record["checkout_url"]
            try:
                live_session = await asyncio.to_thread(stripe.checkout.Session.retrieve, session_id)
                live_status = getattr(live_session, "status", None)
                live_expires_at = getattr(live_session, "expires_at", None)
                live_url = getattr(live_session, "url", None)
                if live_status and live_status != "open":
                    terminal_conn = get_db_conn()
                    terminal_cur = terminal_conn.cursor()
                    try:
                        mark_checkout_terminal(terminal_cur, session_id, "completed" if live_status == "complete" else "expired")
                        terminal_conn.commit()
                    finally:
                        terminal_cur.close()
                        terminal_conn.close()
                    await callback.message.answer("Предыдущая ссылка уже закрыта. Нажмите тариф ещё раз, чтобы создать новую.")
                    await state.clear()
                    return
                if live_expires_at and datetime.utcfromtimestamp(int(live_expires_at)) <= datetime.utcnow():
                    terminal_conn = get_db_conn()
                    terminal_cur = terminal_conn.cursor()
                    try:
                        mark_checkout_terminal(terminal_cur, session_id, "expired")
                        terminal_conn.commit()
                    finally:
                        terminal_cur.close()
                        terminal_conn.close()
                    await callback.message.answer("Предыдущая ссылка истекла. Нажмите тариф ещё раз, чтобы создать новую.")
                    await state.clear()
                    return
                if not live_url:
                    raise CheckoutPreparationError(
                        "checkout_session_missing_url",
                        safe_admin_context_reference("checkout_session_missing_url", user_id, sub_type, session_id),
                    )
                checkout_url = live_url
            except Exception as e:
                if isinstance(e, CheckoutPreparationError):
                    raise
                logging.warning(
                    "CHECKOUT_REUSE_LIVE_RETRIEVE_FAILED: user_id=%s, session_id=%s, error=%s",
                    user_id,
                    safe_log_id(session_id),
                    type(e).__name__,
                )
                raise CheckoutPreparationError(
                    "checkout_session_retrieve_failed",
                    safe_admin_context_reference("checkout_session_retrieve_failed", user_id, sub_type, session_id),
                ) from e
            logging.info(
                "CHECKOUT_OPEN_REUSED: user_id=%s, session_id=%s, sub_type=%s, mode=%s",
                user_id,
                safe_log_id(session_id),
                sub_type,
                mode,
            )
        else:
            try:
                logging.info(
                    "CHECKOUT_CREATE_CLAIMED: user_id=%s, sub_type=%s, mode=%s, paid=%s, "
                    "expiry_date=%s, stripe_subscription_id=%s",
                    user_id,
                    sub_type,
                    mode,
                    paid,
                    expiry_date,
                    safe_log_id(stripe_subscription_id),
                )
                session = await asyncio.to_thread(
                    stripe.checkout.Session.create,
                    idempotency_key=checkout_record["idempotency_key"],
                    **session_params,
                )
                session_id = session.id
                checkout_url = session.url

                if not checkout_url:
                    raise ValueError(f"Stripe Checkout Session {safe_log_id(session_id)} не содержит url")

                expires_at = getattr(session, 'expires_at', None)
                expires_at_dt = datetime.utcfromtimestamp(int(expires_at)) if expires_at else None
                open_conn = get_db_conn()
                open_cur = open_conn.cursor()
                try:
                    mark_checkout_open(open_cur, checkout_record["id"], session_id, checkout_url, expires_at_dt)
                    open_conn.commit()
                finally:
                    open_cur.close()
                    open_conn.close()

                checkout_session_cache[cache_key] = {
                    "session_id": session_id,
                    "checkout_url": checkout_url,
                    "cached_at": datetime.utcnow().timestamp(),
                    "expires_at": expires_at,
                }
                logging.info(
                    "CHECKOUT_SESSION_CREATED: user_id=%s, session_id=%s, sub_type=%s, mode=%s",
                    user_id,
                    safe_log_id(session_id),
                    sub_type,
                    mode,
                )
            except Exception as e:
                failed_conn = get_db_conn()
                failed_cur = failed_conn.cursor()
                try:
                    failed_status = "failed" if e.__class__.__name__ == "InvalidRequestError" else "creation_unknown"
                    recovery_error_token = checkout_creation_recovery_error_token(e)
                    mark_checkout_failed(
                        failed_cur,
                        checkout_record["id"],
                        recovery_error_token,
                        status=failed_status,
                    )
                    failed_conn.commit()
                finally:
                    failed_cur.close()
                    failed_conn.close()
                raise

        await send_checkout_open_instruction(
            callback,
            checkout_url,
            user_id,
            session_id,
            sub_type,
            mode,
            reused=reused
        )
        if reused:
            await notify_admins_about_checkout_reuse(
                user_id,
                sub_type,
                attempt_count,
                session_id,
                attempt_timestamp
            )
        await state.clear()
    except Exception as e:
        error_category = checkout_preparation_error_category(e)
        safe_ref = checkout_preparation_error_ref(e, user_id, sub_type)
        logging.error(
            "CHECKOUT_PREPARATION_FAILED: user_id=%s, sub_type=%s, error_category=%s, safe_ref=%s",
            user_id,
            sub_type,
            error_category,
            safe_ref,
        )
        await try_enqueue_checkout_preparation_failed_alert(
            user_id,
            sub_type,
            error_category,
            safe_ref,
        )
        await callback.answer(
            "Техническая ошибка. Попробуйте позже или напишите @re_tasha",
            show_alert=True
        )

@router.callback_query(F.data == "retry_payment", StateFilter('*'))
async def retry_payment(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(RegistrationStates.choice)

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute(
            "SELECT paid, trial_used FROM users WHERE telegram_id = %s",
            (callback.from_user.id,)
        )
        row = cur.fetchone()

        show_trial = not (row and (row[0] or row[1])) if row else True
        kb = get_tariffs_keyboard(show_trial=show_trial)

        text = "Выберите тариф и попробуйте оплатить еще раз:"

        try:
            await callback.message.edit_text(text, reply_markup=kb)
        except Exception:
            await callback.message.answer(text, reply_markup=kb)

        await callback.answer()

    except Exception as e:
        logging.error(f"Ошибка retry_payment: {e}")
        await callback.answer("Ошибка. Попробуйте нажать /start.", show_alert=True)

    finally:
        cur.close()
        conn.close()

@router.callback_query(F.data == "back_to_tariffs", StateFilter('*'))
async def back_to_tariffs(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(RegistrationStates.choice)
    conn = get_db_conn()
    cur = conn.cursor()
    # Исправлено: получаем и paid, и trial_used
    cur.execute("SELECT paid, trial_used FROM users WHERE telegram_id = %s", (callback.from_user.id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    # Показываем триал только если нет ни paid, ни trial_used
    show_trial = not (row and (row[0] or row[1])) if row else True
    kb = get_tariffs_keyboard(show_trial=show_trial)
    text = "Выберите свой формат участия:"
    try:
        await callback.message.edit_caption(caption=text, reply_markup=kb)
    except Exception:
        await callback.message.edit_text(text=text, reply_markup=kb)
    await callback.answer()

@router.callback_query(F.data == "cancel_subscription", StateFilter('*'))
async def cancel_subscription(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT stripe_subscription_id FROM users WHERE telegram_id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row or not row[0]:
        await callback.answer("Активная подписка не найдена.", show_alert=True)
        return

    sub_id = row[0]
    try:
        await asyncio.to_thread(stripe.Subscription.modify, sub_id, cancel_at_period_end=True)
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("UPDATE users SET auto_renew = FALSE WHERE telegram_id = %s", (user_id,))
        conn.commit()
        cur.close()
        conn.close()
        await callback.message.edit_text("✅ Автопродление отключено. Ваш доступ сохранится до конца оплаченного периода.")
    except Exception as e:
        logging.error(f"Ошибка отмены подписки {safe_log_id(sub_id)}: {e}")
        await callback.answer("Ошибка при отмене. Напишите администратору.", show_alert=True)

@router.message(Command('profile'), StateFilter('*'))
async def profile(message: types.Message):
    user_id = message.from_user.id

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                paid,
                expiry_date,
                stripe_subscription_id,
                payment_failed,
                grace_period_end,
                auto_renew,
                trial_used
            FROM users
            WHERE telegram_id = %s
        """, (user_id,))

        user = cur.fetchone()

        keyboard_rows = []

        if not user:
            kb = inline_keyboard([[
                InlineKeyboardButton(text="💳 Выбрать тариф", callback_data="retry_payment")
            ]])
            await message.answer(
                "👤 Ваш профиль\n\n"
                "❌ Активной подписки нет.\n\n"
                "Вы можете выбрать тариф и оформить доступ.",
                reply_markup=kb
            )
            return

        paid, expiry_date, stripe_subscription_id, payment_failed, grace_period_end, auto_renew, trial_used = user

        now = datetime.utcnow()

        expiry_text = expiry_date.strftime("%d.%m.%Y") if expiry_date else "не установлена"
        auto_renew_text = "включено" if auto_renew and stripe_subscription_id else "отключено"

        if paid and expiry_date and expiry_date > now:
            delta = expiry_date - now
            status_text = "✅ Подписка активна"
            time_text = f"осталось {delta.days} дн."

            if stripe_subscription_id and auto_renew:
                keyboard_rows.append([InlineKeyboardButton(text="❌ Отменить автопродление", callback_data="cancel_subscription")])
            else:
                keyboard_rows.append([InlineKeyboardButton(text="💳 Продлить доступ", callback_data="show_renew_options")])

        elif paid and expiry_date and expiry_date <= now:
            delta = now - expiry_date

            if delta < timedelta(days=2):
                status_text = "⏳ Подписка истекла, идет льготный период"
                time_text = f"истекла {delta.days} дн. назад"
            else:
                status_text = "⚠️ Подписка истекла"
                time_text = f"истекла {delta.days} дн. назад"

            keyboard_rows.append([InlineKeyboardButton(text="💳 Продлить доступ", callback_data="show_renew_options")])

        else:
            status_text = "❌ Активной подписки нет"
            time_text = "нет активного доступа"
            keyboard_rows.append([InlineKeyboardButton(text="💳 Выбрать тариф", callback_data="retry_payment")])

        text = (
            "👤 Ваш профиль\n\n"
            f"{status_text}\n"
            f"📅 Действует до: {expiry_text}\n"
            f"⏳ Срок: {time_text}\n"
            f"🔁 Автопродление: {auto_renew_text}\n\n"
            "Вы можете управлять доступом ниже."
        )

        await message.answer(text, reply_markup=inline_keyboard(keyboard_rows))

    except Exception as e:
        logging.error(f"Ошибка profile: {e}")
        await message.answer("❌ Не удалось загрузить профиль. Попробуйте позже или напишите @re_tasha.")

    finally:
        cur.close()
        conn.close()

@router.callback_query(F.data == "show_renew_options", StateFilter('*'))
async def show_renew_options(callback: types.CallbackQuery):
    kb = get_tariffs_keyboard(show_trial=False)
    await callback.message.edit_text("Выберите тариф для продления доступа:", reply_markup=kb)
    await callback.answer()

@router.message(Command('send_user'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def send_user_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split(maxsplit=1)

    if len(args) < 2:
        await message.reply(
            "⚠️ Использование:\n"
            "/send_user <telegram_id> текст сообщения\n\n"
            "Пример:\n"
            "/send_user 123456789 Добрый день! Ваш доступ закончился, вы можете продлить подписку через /profile."
        )
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return

    text = args[1].strip()

    if not text:
        await message.reply("⚠️ Текст сообщения не может быть пустым.")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        await bot.send_message(
            target_user_id,
            text
        )

        await message.answer(f"✅ Сообщение отправлено пользователю {target_user_id}.")

    except TelegramForbiddenError:
        cur.execute(
            "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
            (target_user_id,)
        )
        conn.commit()

        await message.answer("⚠️ Пользователь заблокировал бота. Сообщение не отправлено.")

    except Exception as e:
        logging.error(f"Ошибка send_user для {target_user_id}: {e}")
        error_ref = safe_admin_error_reference("send_user", e)
        await message.answer(
            f"❌ Не удалось отправить сообщение пользователю {target_user_id}.\n\n"
            f"Ошибка: доставка не выполнена. ref: {error_ref}"
        )

    finally:
        cur.close()
        conn.close()

@router.message(Command('broadcast'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def broadcast(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    text = message.text.replace('/broadcast ', '').strip()

    if not text or text == '/broadcast':
        await message.answer("⚠️ Использование: /broadcast текст рассылки")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute("SELECT telegram_id FROM users WHERE (blocked_bot IS NOT TRUE)")
    users = cur.fetchall()
    telegram_ids = [int(row[0]) for row in users]
    preview = broadcast_preview(len(telegram_ids), text)
    action_id = make_action_request(
        cur,
        message.from_user.id,
        "broadcast",
        {"text": text, "telegram_ids": telegram_ids},
    )
    conn.commit()
    cur.close()
    conn.close()

    callbacks = admin_action_confirmation_keyboard(action_id)
    kb = inline_keyboard([[
        InlineKeyboardButton(text="✅ Confirm", callback_data=callbacks["confirm"]),
        InlineKeyboardButton(text="❌ Cancel", callback_data=callbacks["cancel"]),
    ]])
    await message.answer(
        "Подтвердите рассылку.\n\n"
        f"Получателей: {preview['recipient_count']}\n"
        f"Длина: {preview['length']}\n"
        f"Preview:\n{preview['preview']}",
        reply_markup=kb,
    )

@router.message(Command('give_access'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def give_access_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()
    if len(args) < 1 or len(args) > 2:
        await message.reply("⚠️ Использование: /give_access <telegram_id> [дней]")
        return

    try:
        target_user_id = int(args[0])
        days = int(args[1]) if len(args) == 2 else 30
    except ValueError:
        await message.reply("⚠️ telegram_id и количество дней должны быть числами.")
        return

    if days <= 0 or days > 730:
        await message.reply("⚠️ Количество дней должно быть от 1 до 730.")
        return

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT expiry_date FROM users WHERE telegram_id = %s", (target_user_id,))
        row = cur.fetchone()
        old_expiry = row[0] if row else None
        base_expiry = old_expiry if old_expiry and old_expiry > datetime.utcnow() else datetime.utcnow()
        effective_expiry = base_expiry + timedelta(days=days)
        action_id = make_action_request(
            cur,
            message.from_user.id,
            "give_access",
            {
                "telegram_id": target_user_id,
                "days": days,
                "admin_id": message.from_user.id,
            },
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    await send_admin_action_confirmation(
        message,
        action_id,
        "Подтвердите ручную выдачу доступа.\n\n"
        f"telegram_id: {target_user_id}\n"
        f"days: {days}\n"
        f"old_expiry: {old_expiry or 'нет'}\n"
        f"effective_expiry: {effective_expiry}",
    )

@router.message(Command('set_expiry'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def set_expiry_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()
    if len(args) not in (2, 3):
        await message.reply("⚠️ Использование: /set_expiry <telegram_id> <dd.mm.yyyy> [hh:mm]")
        return

    try:
        target_user_id = int(args[0])
        expiry_moscow, expiry_date = parse_moscow_expiry(args[1], args[2] if len(args) == 3 else "23:59")
    except ValueError:
        await message.reply("⚠️ Неверный формат. Пример: /set_expiry 901812366 06.07.2026 23:59")
        return

    if expiry_date <= datetime.utcnow():
        await message.reply("⚠️ Дата окончания должна быть в будущем.")
        return

    expiry_text = expiry_moscow.strftime("%d.%m.%Y %H:%M MSK")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT expiry_date FROM users WHERE telegram_id = %s", (target_user_id,))
        row = cur.fetchone()
        old_expiry = row[0] if row else None
        action_id = make_action_request(
            cur,
            message.from_user.id,
            "set_expiry",
            {
                "telegram_id": target_user_id,
                "expiry_date": expiry_date.isoformat(),
                "expiry_text": expiry_text,
                "admin_id": message.from_user.id,
            },
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    await send_admin_action_confirmation(
        message,
        action_id,
        "Подтвердите изменение даты доступа.\n\n"
        f"telegram_id: {target_user_id}\n"
        f"old_expiry: {old_expiry or 'нет'}\n"
        f"new_expiry: {expiry_text}",
    )


def fetch_restore_access_user(cur, telegram_id):
    cur.execute("""
        SELECT
            paid,
            expiry_date,
            payment_failed,
            grace_period_end,
            blocked_bot,
            stripe_subscription_id,
            stripe_customer_id,
            auto_renew
        FROM users
        WHERE telegram_id = %s
    """, (int(telegram_id),))
    return cur.fetchone()


def restore_access_user_summary(telegram_id, user):
    (
        paid,
        expiry_date,
        payment_failed,
        grace_period_end,
        blocked_bot,
        stripe_subscription_id,
        stripe_customer_id,
        auto_renew,
    ) = user
    return (
        "Подтвердите восстановление доступа.\n\n"
        f"telegram_id: {telegram_id}\n"
        f"paid: {paid}\n"
        f"expiry_date: {expiry_date or 'нет'}\n"
        f"payment_failed: {payment_failed}\n"
        f"grace_period_end: {grace_period_end or 'нет'}\n"
        f"blocked_bot: {blocked_bot}\n"
        f"stripe_subscription_id: {safe_log_id(stripe_subscription_id) or 'нет'}\n"
        f"stripe_customer_id: {safe_log_id(stripe_customer_id) or 'нет'}\n"
        f"auto_renew: {auto_renew}"
    )


@router.message(Command('restore_access'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def restore_access_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()
    if len(args) != 1:
        await message.reply("⚠️ Использование: /restore_access <telegram_id>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return

    reply_text = None
    confirmation_data = None
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        user = fetch_restore_access_user(cur, target_user_id)
        if not user:
            reply_text = "❌ Пользователь не найден в базе."
        else:
            action_id = make_action_request(
                cur,
                message.from_user.id,
                "restore_access",
                {
                    "telegram_id": target_user_id,
                    "admin_id": message.from_user.id,
                },
            )
            confirmation_data = (action_id, restore_access_user_summary(target_user_id, user))
            conn.commit()
    finally:
        cur.close()
        conn.close()

    if reply_text:
        await message.reply(reply_text)
        return

    action_id, summary = confirmation_data
    await send_admin_action_confirmation(message, action_id, summary)


@router.message(Command('gift_templates'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def gift_templates_command(message: types.Message):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT tariff_code, active, updated_at
            FROM gift_certificate_templates
            ORDER BY tariff_code
        """)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    known = {row[0]: row for row in rows}
    lines = ["🎁 Gift templates"]
    for tariff_code in GIFT_TARIFFS:
        row = known.get(tariff_code)
        lines.append(f"{tariff_code}: {'OK' if row and row[1] else 'MISSING'}")
    await message.answer("\n".join(lines))


@router.message(Command('gift_template_upload'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def gift_template_upload_command(message: types.Message, command: CommandObject):
    args = (command.args or "").split()
    if len(args) != 1 or args[0] not in GIFT_TARIFFS:
        await message.reply("⚠️ Использование: /gift_template_upload <gift_1m|gift_6m|gift_12m> ответом на фото")
        return
    if not message.reply_to_message or not message.reply_to_message.photo:
        await message.reply("Отправьте команду ответом на фото сертификата.")
        return
    file_id = message.reply_to_message.photo[-1].file_id
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO gift_certificate_templates (tariff_code, file_id, uploaded_by, active, updated_at)
            VALUES (%s, %s, %s, TRUE, NOW())
            ON CONFLICT (tariff_code) DO UPDATE SET
                file_id = EXCLUDED.file_id,
                uploaded_by = EXCLUDED.uploaded_by,
                active = TRUE,
                updated_at = NOW()
        """, (args[0], file_id, int(message.from_user.id)))
        conn.commit()
    finally:
        cur.close()
        conn.close()
    await message.answer(f"✅ Шаблон {args[0]} сохранён.")


@router.message(Command('gift_info'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def gift_info_command(message: types.Message, command: CommandObject):
    public_reference = (command.args or "").strip()
    if not public_reference:
        await message.reply("⚠️ Использование: /gift_info <public_reference>")
        return
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        gift_row = fetch_gift_by_public_reference(cur, public_reference)
    finally:
        cur.close()
        conn.close()
    if not gift_row:
        await message.answer("Подарок не найден.")
        return
    await message.answer(gift_admin_text("🎁 Gift info", gift_row, extra=f"paid_at: {gift_row.get('paid_at') or 'нет'}\napplied_expiry: {gift_row.get('applied_expiry') or 'нет'}"))


@router.message(Command('gift_cancel'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def gift_cancel_command(message: types.Message, command: CommandObject):
    public_reference = (command.args or "").strip()
    if not public_reference:
        await message.reply("⚠️ Использование: /gift_cancel <public_reference>")
        return
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        gift_row = fetch_gift_by_public_reference(cur, public_reference)
        if not gift_row:
            await message.answer("Подарок не найден.")
            return
        action_id = make_action_request(
            cur,
            message.from_user.id,
            "gift_cancel",
            {"public_reference": public_reference, "admin_id": message.from_user.id},
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    await send_admin_action_confirmation(message, action_id, gift_admin_text("Подтвердите отмену подарка", gift_row))


@router.message(Command('gift_reissue'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def gift_reissue_command(message: types.Message, command: CommandObject):
    public_reference = (command.args or "").strip()
    if not public_reference:
        await message.reply("⚠️ Использование: /gift_reissue <public_reference>")
        return
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        gift_row = fetch_gift_by_public_reference(cur, public_reference)
        if not gift_row:
            await message.answer("Подарок не найден.")
            return
        action_id = make_action_request(
            cur,
            message.from_user.id,
            "gift_reissue",
            {"public_reference": public_reference, "admin_id": message.from_user.id},
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    await send_admin_action_confirmation(message, action_id, gift_admin_text("Подтвердите перевыпуск сертификата", gift_row))


@router.message(Command('gifts_pending'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def gifts_pending_command(message: types.Message):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT public_reference, purchaser_telegram_id, recipient_telegram_id, tariff_code, status, created_at
            FROM gift_access_grants
            WHERE status IN ('checkout_open', 'payment_pending', 'paid_unclaimed', 'reserved', 'review_required')
            ORDER BY created_at DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    if not rows:
        await message.answer("Активных подарков нет.")
        return
    lines = ["🎁 Pending gifts"]
    for row in rows:
        lines.append(f"{row[0]} | buyer={row[1]} | recipient={row[2] or 'нет'} | {row[3]} | {row[4]}")
    await message.answer("\n".join(lines))


@router.message(Command('gift_status'), StateFilter('*'))
async def gift_status_command(message: types.Message):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT public_reference, tariff_code, status, created_at, applied_expiry
            FROM gift_access_grants
            WHERE purchaser_telegram_id = %s
            ORDER BY created_at DESC
            LIMIT 10
        """, (int(message.from_user.id),))
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    if not rows:
        await message.answer("У вас пока нет подарочных сертификатов.")
        return
    lines = ["🎁 Ваши подарки"]
    for row in rows:
        lines.append(f"{row[0]} | {gift_tariff_label(row[1])} | {row[2]} | до {row[4] or 'не активирован'}")
    await message.answer("\n".join(lines))


@router.message(Command('revoke_access'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def revoke_access_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = (command.args or "").split(maxsplit=1)
    if len(args) != 2:
        await message.reply("⚠️ Использование: /revoke_access <telegram_id> <reason>")
        return
    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return
    reason = args[1].strip()
    if not reason:
        await message.reply("⚠️ Укажите причину отзыва доступа.")
        return
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT paid, expiry_date, stripe_customer_id, stripe_subscription_id, auto_renew
            FROM users
            WHERE telegram_id = %s
            """,
            (target_user_id,),
        )
        user = cur.fetchone()
        if not user:
            conn.rollback()
            await message.reply("❌ Пользователь не найден.")
            return
        paid, expiry_date, customer_id, subscription_id, auto_renew = user
        action_id = make_action_request(
            cur,
            message.from_user.id,
            "revoke_access",
            {
                "telegram_id": target_user_id,
                "reason": reason,
                "admin_id": message.from_user.id,
            },
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    await send_admin_action_confirmation(
        message,
        action_id,
        "Подтвердите отзыв доступа.\n\n"
        f"telegram_id: {target_user_id}\n"
        f"paid: {paid}\n"
        f"expiry: {expiry_date or 'нет'}\n"
        f"stripe_customer_id: {safe_log_id(customer_id) or 'нет'}\n"
        f"stripe_subscription_id: {safe_log_id(subscription_id) or 'нет'}\n"
        f"auto_renew: {auto_renew}\n"
        f"reason: {reason}\n\n"
        "Stripe subscription автоматически не отменяется. После Confirm будет поставлено durable удаление из группы.",
    )


@router.message(Command('refund_info'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def refund_info_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return
    query = (command.args or "").strip()
    if not (query.startswith("re_") or query.startswith("ch_") or query.startswith("pi_")):
        await message.reply("⚠️ Использование: /refund_info <re_...|ch_...|pi_...>")
        return
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT refund_id, stripe_event_id, charge_id, payment_intent_id, invoice_id,
                   customer_id, subscription_id, telegram_id, original_payment_event_id,
                   amount_refunded, original_amount, currency, refund_status,
                   is_full_refund, reconciliation_result, review_reason, access_revoked_at
            FROM subscription_refund_reconciliations
            WHERE refund_id = %s
               OR charge_id = %s
               OR payment_intent_id = %s
            ORDER BY created_at DESC
            LIMIT 5
            """,
            (query, query, query),
        )
        reconciliations = cur.fetchall()
        payment_ids = sorted({int(row[8]) for row in reconciliations if row[8] is not None})
        payments = []
        if payment_ids:
            cur.execute(
                """
                SELECT id, telegram_id, invoice_id, stripe_customer_id, stripe_subscription_id,
                       payment_kind, amount_paid, currency, period_end, created_at
                FROM payment_events
                WHERE id = ANY(%s)
                ORDER BY created_at DESC, id DESC
                """,
                (payment_ids,),
            )
            payments = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    lines = [f"Refund info: {safe_log_id(query)}", ""]
    if reconciliations:
        lines.append("Reconciliations:")
        for row in reconciliations:
            auto_revoke_safe = row[14] == SUBSCRIPTION_REFUND_ACCESS_REVOKED
            review_required = row[14] == SUBSCRIPTION_REFUND_REVIEW_REQUIRED
            lines.append(
                f"- result={row[14]}, review={row[15] or 'нет'}, original_payment_event_id={row[8] or 'none'}, "
                f"telegram_id={row[7] or 'unknown'}, "
                f"refund={safe_log_id(row[0]) or 'none'}, charge={safe_log_id(row[2]) or 'none'}, "
                f"pi={safe_log_id(row[3]) or 'none'}, full={row[13]}, status={row[12]}, "
                f"auto_revoke_safe={auto_revoke_safe}, review_required={review_required}, revoked_at={row[16] or 'нет'}"
            )
    else:
        lines.append("Reconciliations: none")
    if payments:
        lines.append("")
        lines.append("Original payments:")
        for row in payments:
            lines.append(
                f"- payment_event_id={row[0]}, telegram_id={row[1]}, kind={row[5]}, "
                f"amount={row[6]} {row[7] or ''}, customer={safe_log_id(row[3]) or 'none'}, "
                f"subscription={safe_log_id(row[4]) or 'none'}, period_end={row[8] or 'none'}"
            )
    else:
        lines.append("Original payments: payment match unavailable locally")
    lines.append("")
    lines.append("read_only: true")
    await message.answer("\n".join(lines)[:4000])


@router.message(Command('sync_stripe_user'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def sync_stripe_user_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()

    if len(args) != 1:
        await message.reply("⚠️ Использование: /sync_stripe_user <telegram_id>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ Использование: /sync_stripe_user <telegram_id>")
        return

    read_conn = get_db_conn()
    read_cur = read_conn.cursor()
    try:
        read_cur.execute("""
            SELECT
                paid,
                expiry_date,
                stripe_subscription_id,
                stripe_customer_id,
                payment_failed,
                grace_period_end,
                blocked_bot
            FROM users
            WHERE telegram_id = %s
        """, (target_user_id,))
        user = read_cur.fetchone()
    finally:
        read_cur.close()
        read_conn.close()

    if not user:
        await message.reply("❌ Пользователь не найден в базе.")
        return

    (
        paid,
        expiry_date,
        stripe_subscription_id,
        stripe_customer_id,
        payment_failed,
        grace_period_end,
        blocked_bot
    ) = user

    if not stripe_subscription_id:
        await message.reply("⚠️ У пользователя нет stripe_subscription_id. Синхронизация со Stripe невозможна.")
        return

    try:
        subscription = await asyncio.to_thread(stripe.Subscription.retrieve, stripe_subscription_id)
    except Exception as e:
        logging.exception("SYNC_STRIPE_USER_SUBSCRIPTION_RETRIEVE_FAILED: telegram_id=%s", target_user_id)
        error_ref = safe_admin_error_reference("sync_stripe_subscription_retrieve", e)
        await message.reply(f"❌ Не удалось получить подписку из Stripe. ref: {error_ref}")
        return

    status = get_obj_value(subscription, 'status')
    current_period_end = get_obj_value(subscription, 'current_period_end')
    period_source = "subscription.current_period_end"
    customer = get_obj_value(subscription, 'customer')
    cancel_at_period_end = bool(get_obj_value(subscription, 'cancel_at_period_end'))
    customer_id = customer if isinstance(customer, str) else get_obj_value(customer, 'id')
    auto_renew = not cancel_at_period_end
    period_end_text = "нет"

    if not current_period_end:
        try:
            invoices = await asyncio.to_thread(
                stripe.Invoice.list,
                subscription=stripe_subscription_id,
                limit=5
            )
            for invoice in (get_obj_value(invoices, 'data') or []):
                if get_obj_value(invoice, 'status') != 'paid':
                    continue
                lines_data = get_obj_value(invoice, 'lines', 'data') or []
                first_line = lines_data[0] if lines_data else None
                period_end = get_obj_value(first_line, 'period', 'end')
                if period_end:
                    current_period_end = period_end
                    period_source = "invoice.lines.data[0].period.end"
                    break
        except Exception as e:
            logging.error(
                "SYNC_STRIPE_USER_INVOICE_LIST_FAILED: telegram_id=%s, error_type=%s, error_ref=%s",
                target_user_id,
                type(e).__name__,
                safe_admin_error_reference("sync_stripe_invoice_list", e),
            )

    if current_period_end:
        period_end_dt = datetime.utcfromtimestamp(current_period_end)
        period_end_text = period_end_dt.strftime("%d.%m.%Y %H:%M")

    reply_text = None
    write_conn = None
    write_cur = None
    try:
        if status in ('active', 'trialing') and current_period_end:
            new_expiry = datetime.utcfromtimestamp(current_period_end)
            write_conn = get_db_conn()
            write_cur = write_conn.cursor()
            write_cur.execute("""
                SELECT stripe_subscription_id, stripe_customer_id, expiry_date
                FROM users
                WHERE telegram_id = %s
                FOR UPDATE
            """, (target_user_id,))
            current_row = write_cur.fetchone()
            if not current_row:
                write_conn.rollback()
                reply_text = "❌ Пользователь не найден в базе."
            else:
                current_subscription_id, current_customer_id, current_old_expiry = current_row
                if current_subscription_id != stripe_subscription_id:
                    write_conn.rollback()
                    reply_text = (
                        "⚠️ Stripe subscription пользователя изменилась во время проверки\n\n"
                        f"telegram_id: {target_user_id}\n"
                        "Данные пользователя не изменены. Повторите /sync_stripe_user, "
                        "чтобы проверить текущую подписку."
                    )
                else:
                    effective_expiry = non_decreasing_expiry(current_old_expiry, new_expiry)
                    write_cur.execute("""
                        UPDATE users
                        SET paid = TRUE,
                            expiry_date = %s,
                            stripe_customer_id = COALESCE(%s, stripe_customer_id),
                            payment_failed = FALSE,
                            payment_failed_at = NULL,
                            last_payment_succeeded_at = NOW(),
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            auto_renew = %s,
                            blocked_bot = FALSE
                        WHERE telegram_id = %s
                    """, (effective_expiry, customer_id, auto_renew, target_user_id))
                    enqueue_automatic_membership_repair(
                        write_cur,
                        target_user_id,
                        effective_expiry,
                        ACCESS_RESTORE_SOURCE_AUTO_SYNC,
                        requested_by_admin_id=message.from_user.id,
                        reason="sync_stripe_user_active_period",
                    )
                    record_access_event_cur(
                        write_cur,
                        target_user_id,
                        "manual_stripe_sync",
                        source="admin_command",
                        old_expiry=current_old_expiry,
                        new_expiry=effective_expiry,
                        stripe_subscription_id=current_subscription_id,
                        notes=f"status={status}; auto_renew={auto_renew}; period_source={period_source}; admin_id={message.from_user.id}"
                    )
                    write_conn.commit()
                    reply_text = (
                        "✅ Stripe-синхронизация выполнена\n\n"
                        f"telegram_id: {target_user_id}\n"
                        f"status: {status}\n"
                        "paid: TRUE\n"
                        f"expiry_date: {effective_expiry.strftime('%d.%m.%Y %H:%M')}\n"
                        f"auto_renew: {auto_renew}\n"
                        f"period_source: {period_source}\n"
                        f"stripe_subscription_id: {safe_log_id(current_subscription_id) or 'нет'}\n"
                        f"stripe_customer_id: {safe_log_id(customer_id or current_customer_id) or 'нет'}"
                    )
        elif status in ('active', 'trialing') and not current_period_end:
            write_conn = get_db_conn()
            write_cur = write_conn.cursor()
            write_cur.execute("""
                SELECT stripe_subscription_id, stripe_customer_id, expiry_date
                FROM users
                WHERE telegram_id = %s
                FOR UPDATE
            """, (target_user_id,))
            current_row = write_cur.fetchone()
            if not current_row:
                write_conn.rollback()
                reply_text = "❌ Пользователь не найден в базе."
            else:
                current_subscription_id, current_customer_id, current_old_expiry = current_row
                if current_subscription_id != stripe_subscription_id:
                    write_conn.rollback()
                    reply_text = (
                        "⚠️ Stripe subscription пользователя изменилась во время проверки\n\n"
                        f"telegram_id: {target_user_id}\n"
                        "Данные пользователя не изменены. Повторите /sync_stripe_user, "
                        "чтобы проверить текущую подписку."
                    )
                else:
                    write_cur.execute("""
                        UPDATE users
                        SET stripe_subscription_id = %s,
                            stripe_customer_id = COALESCE(%s, stripe_customer_id),
                            auto_renew = %s,
                            payment_failed = FALSE,
                            payment_failed_at = NULL,
                            last_payment_succeeded_at = NOW(),
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            blocked_bot = FALSE
                        WHERE telegram_id = %s
                    """, (current_subscription_id, customer_id, auto_renew, target_user_id))
                    write_conn.commit()
                    reply_text = (
                        "⚠️ Подписка активна, customer_id обновлен, но current_period_end не найден. expiry_date не меняла.\n\n"
                        f"telegram_id: {target_user_id}\n"
                        f"status: {status}\n"
                        f"auto_renew: {auto_renew}\n"
                        f"stripe_subscription_id: {safe_log_id(current_subscription_id) or 'нет'}\n"
                        f"stripe_customer_id: {safe_log_id(customer_id or current_customer_id) or 'нет'}"
                    )
        else:
            reply_text = (
                "⚠️ Подписка в Stripe не активна\n\n"
                f"telegram_id: {target_user_id}\n"
                f"status: {status}\n"
                f"current_period_end: {period_end_text}\n"
                f"cancel_at_period_end: {cancel_at_period_end}\n\n"
                "БД автоматически не обновлена до paid=True."
            )
    except Exception as e:
        if write_conn:
            write_conn.rollback()
        error_ref = safe_admin_error_reference("sync_stripe_user", e)
        logging.error(
            "SYNC_STRIPE_USER_FAILED: telegram_id=%s, error_type=%s, error_ref=%s",
            target_user_id,
            type(e).__name__,
            error_ref,
        )
        reply_text = f"❌ Ошибка синхронизации. ref: {error_ref}"
    finally:
        if write_cur:
            write_cur.close()
        if write_conn:
            write_conn.close()

    await message.reply(reply_text)

@router.message(Command('expired_users'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def expired_users_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                telegram_id,
                expiry_date,
                payment_failed,
                grace_period_end,
                reminder_sent,
                blocked_bot,
                EXTRACT(EPOCH FROM (NOW() - expiry_date)) / 86400 AS days_expired
            FROM users
            WHERE paid = TRUE
              AND expiry_date IS NOT NULL
              AND expiry_date < NOW()
            ORDER BY expiry_date ASC
            LIMIT 30
        """)

        users = cur.fetchall()

        if not users:
            await message.answer("✅ Нет пользователей с истекшей датой и paid=True.")
            return

        lines = ["🧯 Пользователи с истекшей датой, но paid=True:\n"]

        for user in users:
            telegram_id, expiry_date, payment_failed, grace_period_end, reminder_sent, blocked_bot, days_expired = user

            expiry_text = expiry_date.strftime("%d.%m.%Y %H:%M") if expiry_date else "нет даты"
            grace_text = grace_period_end.strftime("%d.%m.%Y %H:%M") if grace_period_end else "нет"

            lines.append(
                f"ID: {telegram_id}\n"
                f"Истекла: {expiry_text}\n"
                f"Дней после окончания: {float(days_expired):.1f}\n"
                f"payment_failed: {payment_failed}\n"
                f"grace_period_end: {grace_text}\n"
                f"reminder_sent: {reminder_sent}\n"
                f"blocked_bot: {blocked_bot}\n"
            )

        text = "\n---\n".join(lines)

        if len(text) > 4000:
            text = text[:3900] + "\n\nСообщение обрезано. Показаны не все пользователи."

        await message.answer(text)

    except Exception as e:
        logging.error(f"Ошибка expired_users: {e}")
        error_ref = safe_admin_error_reference("expired_users", e)
        await message.answer(f"❌ Ошибка получения списка. ref: {error_ref}")

    finally:
        cur.close()
        conn.close()

@router.message(Command('user'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def user_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()

    if len(args) != 1:
        await message.reply("⚠️ Использование: /user <telegram_id>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                telegram_id,
                paid,
                expiry_date,
                stripe_subscription_id,
                stripe_customer_id,
                auto_renew,
                trial_used,
                first_payment_done,
                reminder_sent,
                payment_failed,
                grace_period_end,
                blocked_bot,
                registered_at,
                video_sent,
                video_sent_at,
                feedback_sent,
                feedback_sent_at,
                feedback_received
            FROM users
            WHERE telegram_id = %s
        """, (target_user_id,))

        user = cur.fetchone()

        if not user:
            await message.answer("Пользователь не найден в базе.")
            return

        (
            telegram_id,
            paid,
            expiry_date,
            stripe_subscription_id,
            stripe_customer_id,
            auto_renew,
            trial_used,
            first_payment_done,
            reminder_sent,
            payment_failed,
            grace_period_end,
            blocked_bot,
            registered_at,
            video_sent,
            video_sent_at,
            feedback_sent,
            feedback_sent_at,
            feedback_received
        ) = user

        now = datetime.utcnow()

        if expiry_date:
            delta = expiry_date - now
            if delta.total_seconds() >= 0:
                access_text = f"активен, осталось {delta.days} дн."
            else:
                access_text = f"истек, {abs(delta.days)} дн. назад"
        else:
            access_text = "нет даты"

        def fmt_dt(value):
            return value.strftime("%d.%m.%Y %H:%M") if value else "нет"

        stripe_text = safe_log_id(stripe_subscription_id) or "нет"
        stripe_customer_text = safe_log_id(stripe_customer_id) or "нет"

        text = (
            f"👤 Пользователь {telegram_id}\n\n"
            "Доступ:\n"
            f"paid: {paid}\n"
            f"expiry_date: {fmt_dt(expiry_date)}\n"
            f"статус срока: {access_text}\n"
            f"auto_renew: {auto_renew}\n\n"
            "Stripe:\n"
            f"stripe_subscription_id: {stripe_text}\n"
            f"stripe_customer_id: {stripe_customer_text}\n\n"
            "Состояния:\n"
            f"trial_used: {trial_used}\n"
            f"first_payment_done: {first_payment_done}\n"
            f"reminder_sent: {reminder_sent}\n"
            f"payment_failed: {payment_failed}\n"
            f"grace_period_end: {fmt_dt(grace_period_end)}\n"
            f"blocked_bot: {blocked_bot}\n\n"
            "Воронка:\n"
            f"registered_at: {fmt_dt(registered_at)}\n"
            f"video_sent: {video_sent}\n"
            f"video_sent_at: {fmt_dt(video_sent_at)}\n"
            f"feedback_sent: {feedback_sent}\n"
            f"feedback_sent_at: {fmt_dt(feedback_sent_at)}\n"
            f"feedback_received: {feedback_received}"
        )

        await message.answer(text)

    except Exception as e:
        logging.error(f"Ошибка user_command: {e}")
        error_ref = safe_admin_error_reference("user_command", e)
        await message.answer(f"❌ Ошибка получения пользователя. ref: {error_ref}")

    finally:
        cur.close()
        conn.close()

@router.message(Command('access_history'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def access_history_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()

    if len(args) != 1:
        await message.reply("⚠️ Использование: /access_history <telegram_id>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ Использование: /access_history <telegram_id>")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                created_at,
                event_type,
                source,
                old_expiry,
                new_expiry,
                stripe_event_id,
                stripe_subscription_id,
                notes
            FROM access_events
            WHERE telegram_id = %s
            ORDER BY created_at DESC
            LIMIT 15
        """, (target_user_id,))

        events = cur.fetchall()

        if not events:
            await message.answer(f"История доступа для пользователя {target_user_id} пока пустая.")
            return

        def fmt_dt(value):
            return value.strftime("%d.%m.%Y %H:%M") if value else "нет"

        lines = [f"🧾 История доступа пользователя {target_user_id}\n"]

        for (
            created_at,
            event_type,
            source,
            old_expiry,
            new_expiry,
            stripe_event_id,
            stripe_subscription_id,
            notes
        ) in events:
            lines.extend([
                f"Дата: {fmt_dt(created_at)}",
                f"event_type: {event_type}",
                f"source: {source or 'нет'}",
                f"old_expiry: {fmt_dt(old_expiry)}",
                f"new_expiry: {fmt_dt(new_expiry)}",
                f"stripe_event_id: {safe_log_id(stripe_event_id) or 'нет'}",
                f"stripe_subscription_id: {safe_log_id(stripe_subscription_id) or 'нет'}",
                f"notes: {notes or 'нет'}",
                ""
            ])

        text = "\n".join(lines).strip()

        if len(text) > 4000:
            text = text[:3997] + "..."

        await message.answer(text)

    except Exception as e:
        logging.error(f"Ошибка access_history_command для {args[0]}: {e}")
        error_ref = safe_admin_error_reference("access_history", e)
        await message.answer(f"❌ Ошибка получения истории доступа. ref: {error_ref}")

    finally:
        cur.close()
        conn.close()

@router.message(Command('recent_access_events'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def recent_access_events_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                created_at,
                telegram_id,
                event_type,
                source,
                old_expiry,
                new_expiry,
                stripe_event_id,
                stripe_subscription_id,
                notes
            FROM access_events
            ORDER BY created_at DESC
            LIMIT 20
        """)

        events = cur.fetchall()

        if not events:
            await message.answer("История действий по доступу пока пустая.")
            return

        def fmt_dt(value):
            return value.strftime("%d.%m.%Y %H:%M") if value else "нет"

        lines = ["🧾 Последние события по доступу\n"]

        for (
            created_at,
            telegram_id,
            event_type,
            source,
            old_expiry,
            new_expiry,
            stripe_event_id,
            stripe_subscription_id,
            notes
        ) in events:
            lines.extend([
                f"Дата: {fmt_dt(created_at)}",
                f"telegram_id: {telegram_id}",
                f"event_type: {event_type}",
                f"source: {source or 'нет'}",
                f"old_expiry: {fmt_dt(old_expiry)}",
                f"new_expiry: {fmt_dt(new_expiry)}",
                f"stripe_event_id: {safe_log_id(stripe_event_id) or 'нет'}",
                f"stripe_subscription_id: {safe_log_id(stripe_subscription_id) or 'нет'}",
                f"notes: {notes or 'нет'}",
                ""
            ])

        text = "\n".join(lines).strip()

        if len(text) > 4000:
            text = text[:3997] + "..."

        await message.answer(text)

    except Exception as e:
        logging.error(f"Ошибка recent_access_events_command: {e}")
        error_ref = safe_admin_error_reference("recent_access_events", e)
        await message.answer(f"❌ Ошибка получения последних событий доступа. ref: {error_ref}")

    finally:
        cur.close()
        conn.close()

@router.message(Command('outbox_status'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def outbox_status_command(message: types.Message):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT NOW() AT TIME ZONE 'UTC'")
        db_now = cur.fetchone()[0]
        cur.execute("""
            SELECT status, COUNT(*)
            FROM message_delivery_events
            GROUP BY status
        """)
        status_counts = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("""
            SELECT delivery_type, status, COUNT(*)
            FROM message_delivery_events
            GROUP BY delivery_type, status
            ORDER BY delivery_type, status
        """)
        type_counts = cur.fetchall()
        cur.execute("""
            SELECT status, COUNT(*)
            FROM message_delivery_events
            WHERE delivery_type = 'first_purchase_recovery_reminder'
            GROUP BY status
        """)
        first_purchase_recovery_counts = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("""
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE status = 'sent'
              AND sent_at >= NOW() - INTERVAL '24 hours'
        """)
        sent_24h = cur.fetchone()[0]
        cur.execute("""
            SELECT MIN(
                CASE
                    WHEN status = 'failed' THEN claimed_at
                    ELSE COALESCE(claimed_at, next_attempt_at)
                END
            )
            FROM message_delivery_events
            WHERE status IN ('pending', 'failed')
        """)
        oldest_unresolved_at = cur.fetchone()[0]
        cur.execute("""
            SELECT MIN(next_attempt_at)
            FROM message_delivery_events
            WHERE status IN ('pending', 'failed')
              AND next_attempt_at > NOW()
        """)
        next_retry_at = cur.fetchone()[0]
        cur.execute("""
            SELECT MIN(next_attempt_at)
            FROM message_delivery_events
            WHERE status IN ('pending', 'failed')
              AND next_attempt_at IS NOT NULL
        """)
        next_attempt = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(MAX(attempt_count), 0) FROM message_delivery_events")
        max_attempt_count = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE status = 'processing'
              AND lease_until < NOW()
        """)
        stale_processing = cur.fetchone()[0]
        cur.execute("""
            SELECT last_error
            FROM message_delivery_events
            WHERE last_error IS NOT NULL
            ORDER BY COALESCE(claimed_at, next_attempt_at, sent_at) DESC NULLS LAST
            LIMIT 1
        """)
        last_error_row = cur.fetchone()
        cur.execute("""
            SELECT delivery_key, telegram_id, delivery_type, status, attempt_count, last_error, next_attempt_at
            FROM message_delivery_events
            WHERE status IN ('failed', 'permanently_failed')
            ORDER BY COALESCE(next_attempt_at, claimed_at) ASC NULLS LAST
            LIMIT 10
        """)
        failed_rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    oldest_age_text = format_elapsed_duration(outbox_unresolved_age_seconds(db_now, oldest_unresolved_at))
    next_retry_text = format_future_duration(outbox_next_retry_seconds(db_now, next_retry_at))
    lines = [
        "📦 Outbox status",
        "",
        f"pending: {status_counts.get('pending', 0)}",
        f"processing: {status_counts.get('processing', 0)}",
        f"failed/retry: {status_counts.get('failed', 0)}",
        f"permanently_failed: {status_counts.get('permanently_failed', 0)}",
        f"cancelled: {status_counts.get('cancelled', 0)}",
        f"sent за 24 часа: {sent_24h}",
        f"stale processing: {stale_processing}",
        f"oldest unresolved age: {oldest_age_text}",
        f"next retry in: {next_retry_text}",
        f"nearest next_attempt_at: {fmt_outbox_dt(next_attempt)}",
        f"maximum attempt_count: {max_attempt_count}",
        f"latest error category: {safe_error_category(last_error_row[0] if last_error_row else None)}",
        "",
        "By delivery_type:",
    ]
    lines.extend([f"{delivery_type}/{status}: {count}" for delivery_type, status, count in type_counts])
    lines.extend([
        "",
        "First-purchase recovery:",
        f"pending: {first_purchase_recovery_counts.get('pending', 0)}",
        f"sent: {first_purchase_recovery_counts.get('sent', 0)}",
        f"cancelled: {first_purchase_recovery_counts.get('cancelled', 0)}",
        f"permanently_failed: {first_purchase_recovery_counts.get('permanently_failed', 0)}",
    ])
    if failed_rows:
        lines.extend(["", "Failed deliveries для retry:"])
        for delivery_key, telegram_id, delivery_type, status, attempt_count, last_error, next_attempt_at in failed_rows:
            lines.extend([
                f"hash: {safe_delivery_hash(delivery_key)}",
                f"type: {delivery_type}",
                f"status: {status}",
                f"telegram_id: {safe_log_id(telegram_id)}",
                f"attempt_count: {attempt_count}",
                f"next_attempt_at: {fmt_outbox_dt(next_attempt_at)}",
                f"error_category: {safe_error_category(last_error)}",
                "",
            ])
    text = "\n".join(lines).strip()
    if len(text) > 4000:
        text = text[:3997] + "..."
    await message.answer(text)


@router.message(Command('retry_delivery'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def retry_delivery_command(message: types.Message, command: CommandObject):
    args = (command.args or "").split()
    if len(args) != 1:
        await message.reply("⚠️ Использование: /retry_delivery <delivery_hash>")
        return
    requested_hash = args[0].strip().lower()
    conn = get_db_conn()
    cur = conn.cursor()
    reply_text = None
    confirmation = None
    try:
        cur.execute("""
            SELECT delivery_key, telegram_id, delivery_type, status, attempt_count, last_error, next_attempt_at
            FROM message_delivery_events
            WHERE status IN ('failed', 'permanently_failed')
            ORDER BY COALESCE(next_attempt_at, claimed_at) ASC NULLS LAST
            LIMIT 500
        """)
        candidates = cur.fetchall()
        matched = [row for row in candidates if safe_delivery_hash(row[0]) == requested_hash]
        if not matched:
            reply_text = "❌ Delivery не найден или не подходит для retry."
        elif len(matched) > 1:
            reply_text = "⚠️ Найдено несколько delivery с таким hash. Retry отменен."
        else:
            delivery_key, telegram_id, delivery_type, status, attempt_count, last_error, next_attempt_at = matched[0]
            payload = {
                "delivery_key": delivery_key,
                "delivery_hash": requested_hash,
                "admin_id": message.from_user.id,
            }
            action_id = make_action_request(cur, message.from_user.id, "retry_delivery", payload)
            conn.commit()
            confirmation = (
                action_id,
                "Повторить одну outbox-доставку?\n\n"
                f"delivery_hash: {requested_hash}\n"
                f"delivery_type: {delivery_type}\n"
                f"status: {status}\n"
                f"telegram_id: {safe_log_id(telegram_id)}\n"
                f"attempt_count: {attempt_count}\n"
                f"next_attempt_at: {fmt_outbox_dt(next_attempt_at)}\n"
                f"last_error_category: {safe_error_category(last_error)}\n\n"
                "После Confirm команда только вернет delivery в очередь. Worker отправит сообщение сам."
            )
    finally:
        cur.close()
        conn.close()

    if reply_text:
        await message.reply(reply_text)
        return
    if confirmation:
        action_id, confirmation_text = confirmation
        await send_admin_action_confirmation(message, action_id, confirmation_text)


@router.message(Command('find_by_stripe'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def find_by_stripe_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()

    if len(args) != 1:
        await message.reply("⚠️ Использование: /find_by_stripe <sub_... | cus_... | evt_...>")
        return

    query_id = args[0].strip()

    def fmt_dt(value):
        return value.strftime("%d.%m.%Y %H:%M") if value else "нет"

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                telegram_id,
                paid,
                expiry_date,
                stripe_subscription_id,
                stripe_customer_id,
                auto_renew,
                payment_failed,
                grace_period_end,
                blocked_bot
            FROM users
            WHERE stripe_subscription_id = %s
               OR stripe_customer_id = %s
            LIMIT 10
        """, (query_id, query_id))

        users = cur.fetchall()

        cur.execute("""
            SELECT
                created_at,
                telegram_id,
                event_type,
                source,
                old_expiry,
                new_expiry,
                stripe_event_id,
                stripe_subscription_id,
                notes
            FROM access_events
            WHERE stripe_event_id = %s
               OR stripe_subscription_id = %s
            ORDER BY created_at DESC
            LIMIT 10
        """, (query_id, query_id))

        events = cur.fetchall()

        if not users and not events:
            await message.answer(f"Ничего не найдено по Stripe ID:\n{safe_log_id(query_id)}")
            return

        lines = [f"🔎 Найдено по Stripe ID: {safe_log_id(query_id)}\n"]

        if users:
            lines.append("Users:")
            for (
                telegram_id,
                paid,
                expiry_date,
                stripe_subscription_id,
                stripe_customer_id,
                auto_renew,
                payment_failed,
                grace_period_end,
                blocked_bot
            ) in users:
                lines.extend([
                    f"telegram_id: {telegram_id}",
                    f"paid: {paid}",
                    f"expiry_date: {fmt_dt(expiry_date)}",
                    f"stripe_subscription_id: {safe_log_id(stripe_subscription_id) or 'нет'}",
                    f"stripe_customer_id: {safe_log_id(stripe_customer_id) or 'нет'}",
                    f"auto_renew: {auto_renew}",
                    f"payment_failed: {payment_failed}",
                    f"grace_period_end: {fmt_dt(grace_period_end)}",
                    f"blocked_bot: {blocked_bot}",
                    ""
                ])

        if events:
            lines.append("Access events:")
            for (
                created_at,
                telegram_id,
                event_type,
                source,
                old_expiry,
                new_expiry,
                stripe_event_id,
                stripe_subscription_id,
                notes
            ) in events:
                lines.extend([
                    f"Дата: {fmt_dt(created_at)}",
                    f"telegram_id: {telegram_id}",
                    f"event_type: {event_type}",
                    f"source: {source or 'нет'}",
                    f"old_expiry: {fmt_dt(old_expiry)}",
                    f"new_expiry: {fmt_dt(new_expiry)}",
                    f"stripe_event_id: {safe_log_id(stripe_event_id) or 'нет'}",
                    f"stripe_subscription_id: {safe_log_id(stripe_subscription_id) or 'нет'}",
                    f"notes: {notes or 'нет'}",
                    ""
                ])

        text = "\n".join(lines).strip()

        if len(text) > 4000:
            text = text[:3997] + "..."

        await message.answer(text)

    except Exception as e:
        logging.error(f"Ошибка find_by_stripe_command для {query_id}: {e}")
        error_ref = safe_admin_error_reference("find_by_stripe", e)
        await message.answer(f"❌ Ошибка поиска по Stripe ID. ref: {error_ref}")

    finally:
        cur.close()
        conn.close()

@router.message(Command('bot_health'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def bot_health_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    def fmt_dt(value):
        return value.strftime("%d.%m.%Y %H:%M") if value else "нет"

    env_names = list(REQUIRED_ENV_VARS)
    env_lines = [f"{name}: {'OK' if os.getenv(name) else 'MISSING'}" for name in env_names]

    db_status = "OK"
    telegram_status = "UNKNOWN"
    stripe_status = "UNKNOWN"
    pg_dump_status = "OK" if shutil.which("pg_dump") else "MISSING"
    conflict_count = "нет"
    stale_processing_events = "нет"
    user_stats = {
        "total": "нет",
        "paid": "нет",
        "active": "нет",
        "expired_paid": "нет",
        "payment_failed": "нет",
        "grace": "нет",
        "blocked": "нет",
        "missing_stripe_customer_id": "нет",
        "missing_stripe_customer_ids": [],
        "stale_active_stripe_links": "нет",
    }
    access_stats = {
        "total": "нет",
        "last_24h": "нет",
        "last_event": "нет"
    }
    outbox_stats = {
        "pending": "нет",
        "processing": "нет",
        "retryable_failed": "нет",
        "permanently_failed": "нет",
        "cancelled": "нет",
        "blocked": "нет",
        "stale_processing": "нет",
        "sent_24h": "нет",
    }
    first_purchase_recovery_stats = {
        "eligible_now": "нет",
        "pending": "нет",
        "sent_24h": "нет",
        "cancelled": "нет",
        "permanently_failed": "нет",
    }
    gift_stats = {
        "configured": "нет",
        "missing_prices": "нет",
        "template_count": "нет",
        "statuses": {},
        "created_24h": "нет",
        "failed_deliveries": "нет",
    }
    access_mismatch_stats = {
        "active_local_unpaid": "нет",
        "active_missing_or_stale_expiry": "нет",
        "active_unpaid_with_local_payment_proof": "нет",
    }
    conn = None
    cur = None

    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")

        cur.execute("SELECT COUNT(*) FROM users;")
        user_stats["total"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users WHERE paid = TRUE;")
        user_stats["paid"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users WHERE paid = TRUE AND expiry_date IS NOT NULL AND expiry_date > NOW();")
        user_stats["active"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users WHERE paid = TRUE AND expiry_date IS NOT NULL AND expiry_date < NOW();")
        user_stats["expired_paid"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users WHERE payment_failed = TRUE;")
        user_stats["payment_failed"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users WHERE grace_period_end IS NOT NULL AND grace_period_end > NOW();")
        user_stats["grace"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users WHERE blocked_bot = TRUE;")
        user_stats["blocked"] = cur.fetchone()[0]
        cur.execute("""
            SELECT telegram_id
            FROM users
            WHERE stripe_subscription_id IS NOT NULL
              AND stripe_customer_id IS NULL
            ORDER BY telegram_id
            LIMIT 20
        """)
        user_stats["missing_stripe_customer_ids"] = [str(row[0]) for row in cur.fetchall()]
        cur.execute("""
            SELECT COUNT(*)
            FROM users
            WHERE stripe_subscription_id IS NOT NULL
              AND stripe_customer_id IS NULL
        """)
        user_stats["missing_stripe_customer_id"] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*)
            FROM stripe_links
            WHERE is_active IS TRUE
              AND stripe_subscription_id IS NOT NULL
              AND status IN ('canceled', 'incomplete_expired')
        """)
        user_stats["stale_active_stripe_links"] = cur.fetchone()[0]
        access_mismatch_stats = load_access_mismatch_counts(cur)

        cur.execute("SELECT COUNT(*) FROM access_events;")
        access_stats["total"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM access_events WHERE created_at >= NOW() - INTERVAL '24 hours';")
        access_stats["last_24h"] = cur.fetchone()[0]
        cur.execute("""
            SELECT created_at, event_type, telegram_id
            FROM access_events
            ORDER BY created_at DESC
            LIMIT 1
        """)
        last_event = cur.fetchone()
        if last_event:
            access_stats["last_event"] = (
                f"{fmt_dt(last_event[0])}, {last_event[1]}, telegram_id: {last_event[2]}"
            )
        cur.execute("SELECT COUNT(*) FROM stripe_identity_conflicts WHERE resolved IS NOT TRUE;")
        conflict_count = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*)
            FROM stripe_events
            WHERE processed IS NOT TRUE
              AND processed_at < NOW() - INTERVAL '10 minutes'
        """)
        stale_processing_events = cur.fetchone()[0]
        cur.execute("""
            SELECT status, COUNT(*)
            FROM message_delivery_events
            GROUP BY status
        """)
        delivery_status_counts = {row[0]: row[1] for row in cur.fetchall()}
        outbox_stats["pending"] = delivery_status_counts.get("pending", 0)
        outbox_stats["processing"] = delivery_status_counts.get("processing", 0)
        outbox_stats["retryable_failed"] = delivery_status_counts.get("failed", 0)
        outbox_stats["permanently_failed"] = delivery_status_counts.get("permanently_failed", 0)
        outbox_stats["cancelled"] = delivery_status_counts.get("cancelled", 0)
        first_purchase_recovery_stats["eligible_now"] = count_due_first_purchase_recovery_users(cur)
        cur.execute("""
            SELECT status, COUNT(*)
            FROM message_delivery_events
            WHERE delivery_type = 'first_purchase_recovery_reminder'
            GROUP BY status
        """)
        recovery_status_counts = {row[0]: row[1] for row in cur.fetchall()}
        first_purchase_recovery_stats["pending"] = recovery_status_counts.get("pending", 0)
        first_purchase_recovery_stats["cancelled"] = recovery_status_counts.get("cancelled", 0)
        first_purchase_recovery_stats["permanently_failed"] = recovery_status_counts.get("permanently_failed", 0)
        cur.execute("""
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE delivery_type = 'first_purchase_recovery_reminder'
              AND status = 'sent'
              AND sent_at >= NOW() - INTERVAL '24 hours'
        """)
        first_purchase_recovery_stats["sent_24h"] = cur.fetchone()[0]
        gift_config = gift_configuration_status(cur)
        gift_stats["configured"] = "yes" if gift_config["configured"] else "no"
        gift_stats["missing_prices"] = ", ".join(gift_config["missing_prices"]) if gift_config["missing_prices"] else "нет"
        gift_stats["template_count"] = f"{gift_config['template_count']}/{gift_config['required_template_count']}"
        cur.execute("""
            SELECT status, COUNT(*)
            FROM gift_access_grants
            GROUP BY status
            ORDER BY status
        """)
        gift_stats["statuses"] = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("""
            SELECT COUNT(*)
            FROM gift_access_grants
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """)
        gift_stats["created_24h"] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE delivery_type = ANY(%s)
              AND status IN ('failed', 'permanently_failed')
        """, ([GIFT_CERTIFICATE_BUYER, GIFT_CERTIFICATE_RECIPIENT] + list(GIFT_TEXT_DELIVERY_TYPES),))
        gift_stats["failed_deliveries"] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*)
            FROM message_delivery_events m
            JOIN users u ON u.telegram_id = m.telegram_id
            WHERE m.status = 'permanently_failed'
              AND u.blocked_bot = TRUE
        """)
        outbox_stats["blocked"] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE status = 'processing'
              AND lease_until < NOW()
        """)
        outbox_stats["stale_processing"] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*)
            FROM message_delivery_events
            WHERE status = 'sent'
              AND sent_at >= NOW() - INTERVAL '24 hours'
        """)
        outbox_stats["sent_24h"] = cur.fetchone()[0]
    except Exception as e:
        error_ref = safe_admin_error_reference("bot_health_db", e)
        db_status = f"ERROR ref: {error_ref}"
        logging.error(f"Ошибка bot_health_command: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    try:
        await asyncio.wait_for(bot.get_me(), timeout=5)
        webhook_info = await asyncio.wait_for(bot.get_webhook_info(), timeout=5)
        telegram_status = (
            f"OK, pending={getattr(webhook_info, 'pending_update_count', 'нет')}, "
            f"last_error={getattr(webhook_info, 'last_error_message', None) or 'нет'}"
        )
    except Exception as e:
        error_ref = safe_admin_error_reference("bot_health_telegram", e)
        telegram_status = f"ERROR ref: {error_ref}"

    stripe_status = "CONFIGURED (API not probed)" if os.getenv("STRIPE_API_KEY") else "MISSING"

    text = (
        "🩺 Bot health\n\n"
        f"UTC now: {datetime.utcnow().strftime('%d.%m.%Y %H:%M')}\n\n"
        "ENV:\n"
        f"{chr(10).join(env_lines)}\n\n"
        f"DB: {db_status}\n\n"
        f"Telegram: {telegram_status}\n"
        f"Stripe: {stripe_status}\n"
        f"pg_dump: {pg_dump_status}\n"
        f"Stripe identity conflicts: {conflict_count}\n"
        f"Stale processing Stripe events: {stale_processing_events}\n"
        f"Scheduler jobs: {len(scheduler.get_jobs())}\n"
        f"Owner: {OWNER_ID}\n\n"
        "DB pool:\n"
        f"used: {db_pool_health()['pool_used']}\n"
        f"available: {db_pool_health()['pool_available']}\n"
        f"connection errors: {db_pool_health()['connection_errors']}\n"
        f"statement timeout: {db_pool_health()['statement_timeout_ms']} ms\n\n"
        "Outbox:\n"
        f"pending: {outbox_stats['pending']}\n"
        f"processing: {outbox_stats['processing']}\n"
        f"retryable_failed: {outbox_stats['retryable_failed']}\n"
        f"permanently_failed: {outbox_stats['permanently_failed']}\n"
        f"cancelled: {outbox_stats['cancelled']}\n"
        f"blocked: {outbox_stats['blocked']}\n"
        f"stale processing: {outbox_stats['stale_processing']}\n"
        f"sent за 24 часа: {outbox_stats['sent_24h']}\n\n"
        "First-purchase recovery:\n"
        f"eligible now: {first_purchase_recovery_stats['eligible_now']}\n"
        f"pending: {first_purchase_recovery_stats['pending']}\n"
        f"sent за 24 часа: {first_purchase_recovery_stats['sent_24h']}\n"
        f"cancelled: {first_purchase_recovery_stats['cancelled']}\n"
        f"permanently_failed: {first_purchase_recovery_stats['permanently_failed']}\n\n"
        "Gift access:\n"
        f"configured: {gift_stats['configured']}\n"
        f"missing prices: {gift_stats['missing_prices']}\n"
        f"templates: {gift_stats['template_count']}\n"
        f"created за 24 часа: {gift_stats['created_24h']}\n"
        f"failed deliveries: {gift_stats['failed_deliveries']}\n"
        f"statuses: {gift_stats['statuses']}\n\n"
        "Users:\n"
        f"Всего пользователей: {user_stats['total']}\n"
        f"paid=True: {user_stats['paid']}\n"
        f"Активных по expiry_date: {user_stats['active']}\n"
        f"Истекли, но paid=True: {user_stats['expired_paid']}\n"
        f"payment_failed=True: {user_stats['payment_failed']}\n"
        f"В grace period: {user_stats['grace']}\n"
        f"Заблокировали бота: {user_stats['blocked']}\n"
        f"stripe_subscription_id без stripe_customer_id: {user_stats['missing_stripe_customer_id']}"
        f" ({', '.join(user_stats['missing_stripe_customer_ids']) or 'нет'})\n"
        f"active stripe_links с terminal status: {user_stats['stale_active_stripe_links']}\n\n"
        "Active subscription mismatches:\n"
        f"active link + paid=False: {access_mismatch_stats['active_local_unpaid']}\n"
        f"active link + missing/stale expiry: {access_mismatch_stats['active_missing_or_stale_expiry']}\n"
        "active unpaid with local payment proof: "
        f"{access_mismatch_stats['active_unpaid_with_local_payment_proof']}\n\n"
        "Access events:\n"
        f"Всего: {access_stats['total']}\n"
        f"За 24ч: {access_stats['last_24h']}\n"
        f"Последнее событие: {access_stats['last_event']}"
    )

    if len(text) > 4000:
        text = text[:3997] + "..."

    await message.answer(text)


@router.message(Command('storage_diagnostics'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def storage_diagnostics_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    conn = None
    try:
        conn = get_db_conn()
        diagnostics = await asyncio.to_thread(collect_storage_diagnostics, conn)
        for page in render_storage_diagnostics(diagnostics):
            await message.answer(page)
    except Exception as error:
        if conn:
            conn.rollback()
        error_ref = safe_admin_error_reference("storage_diagnostics", error)
        logging.error(
            "STORAGE_DIAGNOSTICS_FAILED: error_class=%s ref=%s",
            type(error).__name__, error_ref,
        )
        await message.answer(f"❌ Storage diagnostics unavailable. ref: {error_ref}")
    finally:
        if conn:
            conn.close()


@router.message(Command('constraint_audit'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def constraint_audit_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    conn = None
    try:
        conn = get_db_conn()
        audit = await asyncio.to_thread(collect_constraint_audit, conn)
        for page in render_constraint_audit(audit):
            await message.answer(page)
    except Exception as error:
        if conn:
            conn.rollback()
        error_ref = safe_admin_error_reference("constraint_audit", error)
        logging.error(
            "CONSTRAINT_AUDIT_FAILED: error_class=%s ref=%s",
            type(error).__name__, error_ref,
        )
        await message.answer(f"❌ Constraint audit unavailable. ref: {error_ref}")
    finally:
        if conn:
            conn.close()


@router.message(Command('stripe_reconcile_audit'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def stripe_reconcile_audit_command(message: types.Message):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        candidates = load_reconcile_candidates(cur, limit=100)
    finally:
        cur.close()
        conn.close()

    audit = await reconcile_candidates(candidates, stripe.Subscription.retrieve)
    for page in render_reconcile_audit(audit, safe_log_id):
        await message.answer(page)


@router.message(Command('access_mismatches'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def access_mismatches_command(message: types.Message):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        counts = load_access_mismatch_counts(cur)
        samples = load_access_mismatch_samples(cur, limit=20)
    finally:
        cur.close()
        conn.close()

    lines = [
        "⚠️ Active subscription / local access mismatches",
        "",
        f"active_local_unpaid: {counts['active_local_unpaid']}",
        f"active_missing_or_stale_expiry: {counts['active_missing_or_stale_expiry']}",
        "active_unpaid_with_local_payment_proof: "
        f"{counts['active_unpaid_with_local_payment_proof']}",
        "",
        "read_only: true",
    ]
    if samples:
        lines.extend(["", "Samples (max 20):"])
    now = datetime.utcnow()
    for (
        telegram_id,
        subscription_id,
        link_status,
        paid,
        expiry_date,
        payment_event_id,
        payment_period_end,
    ) in samples:
        if expiry_date is None:
            expiry_state = "missing"
        elif expiry_date <= now:
            expiry_state = "stale"
        else:
            expiry_state = "future"
        lines.extend([
            "",
            f"telegram: {safe_log_id(str(telegram_id))}",
            f"subscription: {safe_log_id(subscription_id)}",
            f"link_status: {link_status}",
            f"paid: {bool(paid)}",
            f"expiry_state: {expiry_state}",
            f"local_payment_proof: {'yes' if payment_event_id else 'no'}",
            f"payment_event: {safe_log_id(payment_event_id) if payment_event_id else 'none'}",
            f"proof_period_end: {payment_period_end.strftime('%d.%m.%Y') if payment_period_end else 'none'}",
        ])
    text = "\n".join(lines)
    await message.answer(text[:4000])
ADMIN_MENU_SECTIONS = {
    "stats": {
        "button": "📊 Статистика",
        "title": "📊 Статистика",
        "danger": False,
        "commands": [
            "/stats — статистика клуба",
            "/weekly_report — отчёт за прошлую неделю",
            "/weekly_report_current — отчёт за текущую неделю",
            "/weekly_report_send — тестовая отправка weekly report всем админам",
            "/bot_health — диагностика бота",
            "/expiring_users — подписки, истекающие за 48 часов",
            "/expired_users — просроченные подписки",
        ],
    },
    "users": {
        "button": "👤 Пользователи",
        "title": "👤 Пользователи",
        "danger": False,
        "commands": [
            "/user <telegram_id> — карточка пользователя",
            "/find_by_stripe <stripe_id> — поиск по Stripe ID",
            "/access_history <telegram_id> — история доступа пользователя",
            "/recent_access_events — последние события доступа",
            "/send_user <telegram_id> <text> — личное сообщение пользователю",
        ],
    },
    "access": {
        "button": "🔐 Доступ",
        "title": "🔐 Доступ",
        "danger": True,
        "commands": [
            "/give_access <telegram_id> <days> — выдать или продлить доступ",
            "/set_expiry <telegram_id> <YYYY-MM-DD> — установить точную дату доступа",
            "/sync_stripe_user <telegram_id> — синхронизировать Stripe",
            "/restore_access <telegram_id> — восстановить вход в закрытый клуб",
            "/revoke_access <telegram_id> <reason> — отозвать доступ через Confirm",
            "/refund_info <re_...|ch_...|pi_...> — read-only проверка refund reconciliation",
            "/unlinked_stripe — показать Stripe оплаты без пользователя",
            "/stripe_links <telegram_id> — показать Stripe связи пользователя",
            "/stripe_conflicts — показать unresolved Stripe identity conflicts",
            "/access_mismatches — read-only active subscription/local access mismatches",
            "/stripe_reconcile_audit — read-only local/Stripe reconciliation",
            "/duplicate_subscriptions — показать Stripe customers с несколькими подписками",
            "/link_stripe_user <telegram_id> <customer_id> <subscription_id> — связать Stripe с пользователем",
            "/send_invite_link <telegram_id> — отправить invite link",
            "/revoke_invite_links — создать новую ссылку и отозвать безопасно найденные старые",
            "/unban_user <telegram_id> — снять бан в группе",
            "/unblock_user <telegram_id> — снять blocked_bot",
        ],
    },
    "broadcasts": {
        "button": "📣 Рассылки",
        "title": "📣 Рассылки",
        "danger": True,
        "commands": [
            "/broadcast <text> — массовая текстовая рассылка",
            "/promo_trial — промо-рассылка с trial-кнопкой",
        ],
    },
    "checks": {
        "button": "🧪 Проверки",
        "title": "🧪 Проверки",
        "danger": True,
        "commands": [
            "/test_expiry — ручная проверка подписок",
            "/test_grace <telegram_id> — тест grace period",
            "/test_followup <telegram_id> — тест follow-up",
            "/test_auto_lesson <telegram_id> — тест бесплатного урока",
        ],
    },
    "logs": {
        "button": "🧾 Логи",
        "title": "🧾 Логи",
        "danger": False,
        "commands": [
            "/access_history <telegram_id>",
            "/recent_access_events",
            "/outbox_status — состояние outbox доставок",
            "/retry_delivery <delivery_hash> — повторить одну failed/permanently_failed доставку через worker",
            "/find_by_stripe <stripe_id>",
        ],
    },
    "tech": {
        "button": "🛠 Тех. функции",
        "title": "🛠 Тех. функции",
        "danger": True,
        "commands": [
            "/test_backup — backup",
            "/outbox_status — состояние outbox доставок",
            "/retry_delivery <delivery_hash> — подготовить retry одной outbox-доставки",
            "Отправь боту фото или видео от имени админа — бот ответит file_id",
            "/admin_help — список всех команд",
        ],
    },
}


def get_admin_menu_keyboard():
    return inline_keyboard([
        [InlineKeyboardButton(text=section["button"], callback_data=f"admin_menu:{section_key}")]
        for section_key, section in ADMIN_MENU_SECTIONS.items()
    ])


def get_admin_back_keyboard():
    return inline_keyboard([[
        InlineKeyboardButton(text="⬅️ Назад в админ-меню", callback_data="admin_menu:back")
    ]])


def get_admin_menu_text():
    return (
        "🛠 Админ-меню\n\n"
        "Выбери раздел, чтобы посмотреть доступные команды и формат использования.\n"
        "Кнопки ниже ничего не запускают — только показывают справку."
    )


def get_admin_section_text(section_key):
    section = ADMIN_MENU_SECTIONS.get(section_key)
    if not section:
        return get_admin_menu_text()

    lines = [
        section["title"],
        "",
        *section["commands"],
    ]

    if section["danger"]:
        lines.extend([
            "",
            "⚠️ Команда может изменить доступ, отправить сообщения, синхронизировать Stripe или удалить пользователя. Используй только после проверки данных.",
        ])

    return "\n".join(lines)


def get_admin_help_text():
    lines = [
        "🛠 Админ-команды бота",
        "",
        "Открой удобное меню: /admin",
        "",
    ]

    for section_key in ("stats", "users", "access", "broadcasts", "checks", "logs", "tech"):
        section = ADMIN_MENU_SECTIONS[section_key]
        lines.extend([
            section["title"],
            *section["commands"],
            "",
        ])

    lines.append("⚠️ Команды с доступом, рассылками, Stripe и проверками используй только после проверки данных.")
    return "\n".join(lines)


@router.message(Command('admin'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_menu_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Недостаточно прав.")
        return

    await message.answer(get_admin_menu_text(), reply_markup=get_admin_menu_keyboard())


@router.callback_query(F.data.startswith("admin_menu:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_menu_callback(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔ Недостаточно прав.", show_alert=True)
        return

    section_key = callback.data.split(":", 1)[1]

    if section_key == "back":
        await callback.message.edit_text(get_admin_menu_text(), reply_markup=get_admin_menu_keyboard())
        await callback.answer()
        return

    if section_key not in ADMIN_MENU_SECTIONS:
        await callback.answer("Раздел не найден.", show_alert=True)
        return

    await callback.message.edit_text(get_admin_section_text(section_key), reply_markup=get_admin_back_keyboard())
    await callback.answer()


@router.message(Command('admin_help'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_help_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer(get_admin_help_text())

@router.message(Command('expiring_users'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def expiring_users_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                telegram_id,
                expiry_date,
                auto_renew,
                reminder_sent,
                payment_failed,
                trial_used,
                blocked_bot,
                EXTRACT(EPOCH FROM (expiry_date - NOW())) / 86400 AS days_left
            FROM users
            WHERE paid = TRUE
              AND expiry_date IS NOT NULL
              AND expiry_date > NOW()
              AND expiry_date <= NOW() + INTERVAL '2 days'
            ORDER BY expiry_date ASC
            LIMIT 30
        """)

        users = cur.fetchall()

        if not users:
            await message.answer("✅ Нет пользователей, у которых подписка заканчивается в ближайшие 48 часов.")
            return

        lines = ["📅 Подписка заканчивается в ближайшие 48 часов:\n"]

        for user in users:
            (
                telegram_id,
                expiry_date,
                auto_renew,
                reminder_sent,
                payment_failed,
                trial_used,
                blocked_bot,
                days_left
            ) = user

            expiry_text = expiry_date.strftime("%d.%m.%Y %H:%M") if expiry_date else "нет даты"

            lines.append(
                f"ID: {telegram_id}\n"
                f"Заканчивается: {expiry_text}\n"
                f"Осталось дней: {float(days_left):.1f}\n"
                f"auto_renew: {auto_renew}\n"
                f"reminder_sent: {reminder_sent}\n"
                f"payment_failed: {payment_failed}\n"
                f"trial_used: {trial_used}\n"
                f"blocked_bot: {blocked_bot}\n"
            )

        text = "\n---\n".join(lines)

        if len(text) > 4000:
            text = text[:3900] + "\n\nСообщение обрезано. Показаны не все пользователи."

        await message.answer(text)

    except Exception as e:
        logging.error(f"Ошибка expiring_users: {e}")
        error_ref = safe_admin_error_reference("expiring_users", e)
        await message.answer(f"❌ Ошибка получения списка. ref: {error_ref}")

    finally:
        cur.close()
        conn.close()

@router.message(Command('test_followup'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def test_followup_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()

    if len(args) != 1:
        await message.reply("⚠️ Использование: /test_followup <telegram_id>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return

    conn = get_db_conn()
    cur = conn.cursor()
    db_ok = False
    error_text = None

    try:
        cur.execute("""
            INSERT INTO users (telegram_id, paid)
            VALUES (%s, FALSE)
            ON CONFLICT (telegram_id) DO NOTHING
        """, (target_user_id,))
        conn.commit()
        db_ok = True

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка test_followup для {target_user_id}: {e}")
        error_ref = safe_admin_error_reference("test_followup", e)
        error_text = f"❌ Ошибка отправки тестового follow-up. ref: {error_ref}"

    finally:
        cur.close()
        conn.close()

    if not db_ok:
        await message.answer(error_text)
        return

    try:
        was_sent = await send_free_lesson_followup(target_user_id)
    except Exception as e:
        logging.error(f"Ошибка test_followup delivery для {target_user_id}: {e}")
        error_ref = safe_admin_error_reference("test_followup_delivery", e)
        await message.answer(f"❌ Ошибка отправки тестового follow-up. ref: {error_ref}")
        return

    if was_sent:
        await message.answer(f"✅ Тестовый follow-up отправлен пользователю {target_user_id}.")
    else:
        await message.answer("⚠️ Follow-up не отправлен. Проверьте outbox delivery status.")

@router.message(Command('help'), StateFilter('*'))
async def help_command(message: types.Message):
    await message.answer("По всем вопросам @re_tasha")

@router.message(Command('stats'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def stats_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM users WHERE paid = TRUE")
        paid_users = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM users WHERE paid = FALSE")
        unpaid_users = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM users WHERE trial_used = TRUE")
        trial_used = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM users WHERE blocked_bot = TRUE")
        blocked_users = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM users WHERE payment_failed = TRUE")
        payment_failed = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM users WHERE grace_period_end IS NOT NULL AND grace_period_end > NOW()")
        grace_active = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM users
            WHERE paid = TRUE
              AND expiry_date IS NOT NULL
              AND expiry_date > NOW()
              AND expiry_date <= NOW() + INTERVAL '2 days'
        """)
        expiring_soon = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM users
            WHERE paid = TRUE
              AND expiry_date IS NOT NULL
              AND expiry_date < NOW()
        """)
        expired_but_paid = cur.fetchone()[0]

        text = (
            "📊 Статистика бота\n\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"✅ Активных подписок: {paid_users}\n"
            f"👀 Без активной подписки: {unpaid_users}\n"
            f"🌟 Использовали пробную неделю: {trial_used}\n"
            f"🚫 Заблокировали бота: {blocked_users}\n"
            f"⚠️ Ошибка оплаты: {payment_failed}\n"
            f"⏳ В grace period: {grace_active}\n"
            f"📅 Заканчивается в ближайшие 48 часов: {expiring_soon}\n"
            f"🧯 Истекли, но еще paid=True: {expired_but_paid}"
        )

    except Exception as e:
        logging.error(f"Ошибка stats: {e}")
        error_ref = safe_admin_error_reference("stats", e)
        text = f"❌ Ошибка получения статистики. ref: {error_ref}"

    finally:
        cur.close()
        conn.close()

    await message.answer(text)


@router.message(Command('weekly_report'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def weekly_report_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    period_start, period_end = get_last_completed_week_bounds()
    await send_weekly_report_to_admin(message, period_start, period_end)


@router.message(Command('weekly_report_current'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def weekly_report_current_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    period_start, period_end = get_current_week_bounds()
    await send_weekly_report_to_admin(message, period_start, period_end, with_actions=False)


@router.message(Command('weekly_report_send'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def weekly_report_send_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    period_start, _ = get_last_completed_week_bounds()
    key = weekly_report_key(period_start)
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT status, sent_admin_ids FROM weekly_report_runs WHERE report_key = %s", (key,))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if row and row[0] == "completed":
        await message.answer(
            f"⚠️ Отчёт за {key} уже был автоматически отправлен администраторам: {row[1] or 'нет данных'}."
        )
        return
    await message.answer(f"Запускаю тестовую автоматическую отправку отчёта за {key}.")
    result = await send_weekly_admin_report()
    status_text = {
        "completed": "✅ Отчёт отправлен всем доступным администраторам.",
        "partial": "⚠️ Отчёт отправлен частично.",
        "failed": "❌ Отчёт не удалось отправить ни одному администратору.",
        "duplicate_completed": "⚠️ Отчёт уже был отправлен ранее.",
        "already_processing": "⏳ Отчёт уже формируется другим запуском.",
    }.get(result["status"], result["status"])
    await message.answer(
        f"{status_text}\n"
        f"report_key: {result.get('report_key') or key}\n"
        f"sent_admin_ids: {', '.join(str(admin_id) for admin_id in result.get('sent_admin_ids', [])) or 'нет'}"
    )


def weekly_period_from_key(key):
    period_start = datetime.fromisoformat(key).replace(tzinfo=MOSCOW_TZ)
    return period_start, period_start + timedelta(days=7)


@router.callback_query(F.data.startswith("weekly_csv:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def weekly_csv_callback(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Недоступно.", show_alert=True)
        return
    key = callback.data.split(":", 1)[1]
    try:
        period_start, period_end = weekly_period_from_key(key)
    except Exception:
        await callback.answer("Некорректный период.", show_alert=True)
        return
    await callback.answer("Готовлю CSV...")
    await send_weekly_csv(callback, period_start, period_end)


@router.callback_query(F.data.startswith("weekly_refresh:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def weekly_refresh_callback(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Недоступно.", show_alert=True)
        return
    key = callback.data.split(":", 1)[1]
    try:
        period_start, period_end = weekly_period_from_key(key)
    except Exception:
        await callback.answer("Некорректный период.", show_alert=True)
        return
    text, _ = await build_weekly_admin_report(period_start, period_end)
    try:
        await callback.message.edit_text(text, reply_markup=_weekly_report_keyboard(key))
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            raise
    await callback.answer("Обновлено.")


@router.message(Command('test_expiry'))
@admin_private_only(ADMIN_IDS)
async def test_expiry(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        await message.answer("Запускаю проверку подписок...")
        await check_subscriptions_and_reminders()
        await message.answer("Проверка завершена.")
    else:
        await message.answer("Нет прав.")

@router.message(Command('test_grace'))
@admin_private_only(ADMIN_IDS)
async def test_grace(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = (command.args or "").split()
    if len(args) != 1:
        await message.reply("Использование: /test_grace <user_id>")
        return
    user_id = args[0]
    conn = get_db_conn()
    cur = conn.cursor()
    success = False
    error_text = None
    try:
        cur.execute("""
            UPDATE users
            SET payment_failed = TRUE,
                payment_failed_at = COALESCE(payment_failed_at, NOW()),
                grace_period_end = NOW() + INTERVAL '1 day'
            WHERE telegram_id = %s
        """, (int(user_id),))
        conn.commit()
        success = True
    except Exception as e:
        conn.rollback()
        logging.exception("TEST_GRACE_FAILED")
        error_ref = safe_admin_error_reference("test_grace", e)
        error_text = f"Ошибка выполнения. ref: {error_ref}"
    finally:
        cur.close()
        conn.close()

    if not success:
        await message.reply(error_text)
        return

    await message.reply(f"✅ Установлен grace period для {user_id} на 24 часа.")
    await bot.send_message(int(user_id), "⚠️ Тестовое: не удалось списать оплату. У вас есть 24 часа для исправления.")

async def stripe_webhook(request):
    payload = await request.read()
    sig_header = request.headers.get('Stripe-Signature')
    webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET")
    diagnostics = stripe_webhook_diagnostics(
        request,
        payload,
        sig_header,
        webhook_secret,
        os.environ,
    )
    logging.info("Stripe webhook diagnostics: %s", diagnostics)

    if not webhook_secret:
        logging.error("Stripe webhook rejected: STRIPE_WEBHOOK_SECRET не задан. diagnostics=%s", diagnostics)
        return web.Response(status=500, text="Stripe webhook secret missing")
    if not sig_header:
        logging.warning("Stripe webhook rejected: Stripe-Signature header missing. diagnostics=%s", diagnostics)
        return web.Response(status=400, text="Missing Stripe-Signature")

    try:
        event = construct_verified_stripe_event(payload, sig_header, webhook_secret)
    except stripe_signature_error_class() as e:
        logging.warning(
            "Stripe webhook signature verification failed: error=%s diagnostics=%s",
            str(e),
            diagnostics,
        )
        return web.Response(status=400, text="Invalid Stripe signature")
    except LookupError as e:
        logging.warning("Stripe webhook rejected: %s diagnostics=%s", e, diagnostics)
        return web.Response(status=400, text="Missing Stripe-Signature")
    except ValueError as e:
        logging.error("Stripe webhook rejected: %s diagnostics=%s", e, diagnostics)
        return web.Response(status=500, text="Stripe webhook secret missing")
    except Exception as e:
        logging.exception("Stripe webhook construct_event failed: error=%s diagnostics=%s", e, diagnostics)
        return web.Response(status=400, text="Stripe webhook verification error")

    try:
        normalized_event = require_normalized_stripe_event(normalize_stripe_event(event))
        event_id = normalized_event["event_id"]
        event_type = normalized_event["event_type"]
        event_created_at = normalized_event["event_created_at"]
        event_object = normalized_event["event_object"]
        object_id = normalized_event["object_id"]
    except Exception as e:
        logging.exception(
            "Stripe webhook event normalization failed: error=%s diagnostics=%s",
            e,
            diagnostics,
        )
        return web.Response(status=500, text="Stripe webhook event normalization failed")

    logging.info(f"Stripe webhook event: event_id={safe_log_id(event_id)}, event.type={event_type}")

    try:
        claim_result, claim_generation = await claim_normalized_stripe_event(
            claim_event_processing,
            event_id,
            event_created_at=event_created_at,
            event_type=event_type,
            object_id=object_id,
        )
    except Exception as e:
        logging.exception(
            "Stripe webhook event claim failed: event_id=%s, event.type=%s, error=%s",
            safe_log_id(event_id),
            event_type,
            e,
        )
        return web.Response(status=500, text="Stripe webhook event claim failed")
    if claim_result != "claimed":
        logging.info(
            "Stripe webhook event already claimed: event_id=%s, event.type=%s, claim_result=%s",
            safe_log_id(event_id),
            event_type,
            claim_result,
        )
        return web.Response(status=200)

    try:

        def stripe_object_id(value):
            if value is None:
                return None
            if isinstance(value, str):
                return value
            return stripe_value(value, 'id')

        def safe_stripe_repr(value):
            if value is None:
                return None
            if isinstance(value, (str, int, float, bool)):
                return value
            if isinstance(value, dict):
                return {
                    key: (
                        list(val.keys()) if key == "metadata" and isinstance(val, dict)
                        else safe_log_email(val) if "email" in key
                        else safe_log_url(val) if "url" in key
                        else safe_log_id(val) if key.endswith("_id") or key in ("id", "customer", "subscription", "payment_intent")
                        else safe_stripe_repr(val)
                    )
                    for key, val in value.items()
                    if key not in ('payment_method_details', 'card', 'source')
                }
            return str(value)

        def invoice_subscription_field_states(invoice):
            lines_data = stripe_value(invoice, 'lines', 'data') or []
            first_line = lines_data[0] if lines_data else None
            return {
                'invoice.subscription': stripe_object_id(stripe_value(invoice, 'subscription')),
                'invoice.parent.subscription_details.subscription': stripe_object_id(
                    stripe_value(invoice, 'parent', 'subscription_details', 'subscription')
                ),
                'invoice.lines.data[0].subscription': stripe_object_id(stripe_value(first_line, 'subscription')),
            }

        def empty_subscription_fields_text(invoice):
            fields = invoice_subscription_field_states(invoice)
            empty_fields = [name for name, value in fields.items() if not value]
            return ", ".join(empty_fields) if empty_fields else "нет"

        def log_invoice_debug(invoice, subscription_id=None):
            lines_data = stripe_value(invoice, 'lines', 'data') or []
            first_line = lines_data[0] if lines_data else None
            debug_payload = {
                'event_id': safe_log_id(event_id),
                'invoice_id': safe_log_id(stripe_value(invoice, 'id')),
                'billing_reason': stripe_value(invoice, 'billing_reason'),
                'status': stripe_value(invoice, 'status'),
                'amount_paid': stripe_value(invoice, 'amount_paid'),
                'currency': stripe_value(invoice, 'currency'),
                'customer': safe_log_id(stripe_object_id(stripe_value(invoice, 'customer'))),
                'customer_email': safe_log_email(stripe_value(invoice, 'customer_email')),
                'subscription': safe_log_id(stripe_object_id(stripe_value(invoice, 'subscription'))),
                'parent_subscription': safe_log_id(stripe_object_id(stripe_value(invoice, 'parent', 'subscription_details', 'subscription'))),
                'resolved_subscription_id': safe_log_id(subscription_id),
                'payment_intent': safe_log_id(stripe_object_id(stripe_value(invoice, 'payment_intent'))),
                'hosted_invoice_url': safe_log_url(stripe_value(invoice, 'hosted_invoice_url')),
                'metadata_keys': list((stripe_value(invoice, 'metadata') or {}).keys())
                    if isinstance(stripe_value(invoice, 'metadata') or {}, dict) else [],
                'lines_count': len(lines_data),
                'first_line': {
                    'id': safe_log_id(stripe_value(first_line, 'id')),
                    'price_id': safe_log_id(stripe_object_id(stripe_value(first_line, 'price'))),
                    'subscription': safe_log_id(stripe_object_id(stripe_value(first_line, 'subscription'))),
                    'period_start': stripe_value(first_line, 'period', 'start'),
                    'period_end': stripe_value(first_line, 'period', 'end'),
                } if first_line else None,
            }
            logging.info(f"STRIPE INVOICE DEBUG: {debug_payload}")

        async def notify_unlinked_invoice(invoice, subscription_id=None, period_end_override=None):
            invoice_id = stripe_value(invoice, 'id') or "нет"
            billing_reason = stripe_value(invoice, 'billing_reason') or "нет"
            customer = stripe_value(invoice, 'customer')
            customer_id = stripe_object_id(customer) or "нет"
            customer_email = (
                stripe_value(invoice, 'customer_email')
                or stripe_value(customer, 'email')
                or "нет"
            )
            amount_paid = stripe_value(invoice, 'amount_paid')
            lines_data = stripe_value(invoice, 'lines', 'data') or []
            first_line = lines_data[0] if lines_data else None
            period_end = period_end_override or stripe_value(first_line, 'period', 'end')
            await enqueue_admin_payment_problem_now(
                event_id=event_id,
                purpose="invoice_payment_succeeded_unlinked",
                stage="invoice_payment_succeeded",
                telegram_id=None,
                category="missing_subscription_identity",
                stripe_retry="неизвестно",
                recovery_reminder="не применимо",
                safe_ref=safe_admin_context_reference("invoice_payment_succeeded_unlinked", event_id, invoice_id, subscription_id, customer_id),
                note=(
                    "Доступ автоматически НЕ выдан.\n"
                    f"subscription_id: {safe_log_id(subscription_id)}\n"
                    f"billing_reason: {billing_reason}\n"
                    f"amount_paid: {amount_paid if amount_paid is not None else 'нет'}\n"
                    f"period_end: {period_end or 'нет'}\n"
                    f"Пустые subscription-поля: {empty_subscription_fields_text(invoice)}\n"
                    "Используйте /link_stripe_user <telegram_id> <customer_id> <subscription_id>."
                ),
            )

        async def apply_paid_checkout_access(
            session,
            user_id,
            days_to_add,
            sub_id,
            customer_id,
            customer_email,
            checkout_action,
            source_event_type,
        ):
            is_trial = (days_to_add == 7)
            has_subscription = bool(sub_id)
            session_id = stripe_value(session, 'id')
            conn = get_db_conn()
            cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT telegram_id, tariff_code, mode, status
                    FROM checkout_sessions
                    WHERE stripe_session_id = %s
                    FOR UPDATE
                """, (session_id,))
                checkout_row = cur.fetchone()
                checkout_review_reason = None
                expected_tariff = tariff_code_from_checkout_days(days_to_add)
                if not checkout_row:
                    checkout_review_reason = "checkout_session_row_missing"
                else:
                    row_telegram_id, row_tariff_code, row_mode, row_status = checkout_row
                    if int(row_telegram_id) != int(user_id):
                        checkout_review_reason = "checkout_session_telegram_id_mismatch"
                    elif row_mode != "payment":
                        checkout_review_reason = "checkout_session_mode_mismatch"
                    elif expected_tariff != "unknown" and row_tariff_code != expected_tariff:
                        checkout_review_reason = "checkout_session_tariff_mismatch"
                    elif row_status == "completed":
                        conn.commit()
                        logging.info(
                            "CHECKOUT_PAID_ALREADY_COMPLETED: event_id=%s, event.type=%s, session_id=%s, user_id=%s",
                            safe_log_id(event_id),
                            event_type,
                            safe_log_id(session_id),
                            user_id,
                        )
                        return {"applied": False, "already_completed": True}
                    elif row_status not in ("payment_pending", "open"):
                        checkout_review_reason = "checkout_session_status_not_payable"

                if checkout_review_reason:
                    enqueue_admin_payment_problem_safely(
                        cur,
                        event_id=event_id,
                        purpose="checkout_paid_session_row_review_required",
                        stage=source_event_type,
                        telegram_id=user_id,
                        category="invalid_checkout_metadata",
                        stripe_retry="нет",
                        recovery_reminder="не применимо",
                        safe_ref=safe_admin_context_reference(
                            "checkout_paid_session_row_review",
                            event_id,
                            session_id,
                            user_id,
                            checkout_review_reason,
                        ),
                        note=(
                            "Paid Checkout was not applied because the persisted checkout_sessions row "
                            f"did not match the Stripe session. reason={checkout_review_reason}; access_granted: false"
                        ),
                        severity="CRITICAL",
                    )
                    conn.commit()
                    logging.error(
                        "CHECKOUT_PAID_SESSION_ROW_REVIEW_REQUIRED: event_id=%s, event.type=%s, "
                        "session_id=%s, user_id=%s, reason=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(session_id),
                        user_id,
                        checkout_review_reason,
                    )
                    return {"applied": False, "manual_review": True, "reason": checkout_review_reason}

                assert_stripe_identity_available(
                    cur,
                    user_id,
                    customer_id=customer_id,
                    subscription_id=sub_id,
                    source=source_event_type,
                )

                cur.execute("SELECT paid, expiry_date, first_payment_done, payment_failed FROM users WHERE telegram_id = %s", (int(user_id),))
                row = cur.fetchone()
                now = datetime.utcnow()
                old_expiry = row[1] if row else None
                checkout_was_payment_failed = bool(row[3]) if row and len(row) > 3 else False

                if row and row[0] and row[1] and row[1] > now:
                    new_expiry = row[1] + timedelta(days=days_to_add)
                else:
                    new_expiry = now + timedelta(days=days_to_add)

                if is_trial:
                    trial_claimed = claim_trial_redemption(
                        cur,
                        int(user_id),
                        event_id,
                        session_id,
                    )
                    if not trial_claimed:
                        mark_checkout_completed(
                            cur,
                            session_id,
                            customer_id=customer_id,
                            subscription_id=sub_id,
                        )
                        conn.commit()
                        logging.warning(
                            "TRIAL_REDEMPTION_DUPLICATE_BLOCKED: telegram_id=%s, event_id=%s, session_id=%s",
                            user_id,
                            safe_log_id(event_id),
                            safe_log_id(session_id),
                        )
                        await enqueue_admin_payment_problem_now(
                            event_id=event_id,
                            purpose="duplicate_trial_checkout",
                            stage="checkout_completed",
                            telegram_id=user_id,
                            category="invalid_checkout_metadata",
                            stripe_retry="нет",
                            recovery_reminder="не применимо",
                            safe_ref=safe_admin_context_reference("duplicate_trial_checkout", event_id, session_id, user_id),
                            note="Доступ и payment_event не созданы. Проверьте, нужен ли ручной refund.",
                        )
                        return {"applied": False, "duplicate_trial": True}

                needs_link = (row is None) or (not row[0]) or (row[1] is not None and row[1] < now)
                checkout_access_confirmed = checkout_action == "activate_access" and days_to_add > 0
                cur.execute("""
                INSERT INTO users (telegram_id, paid, expiry_date, stripe_subscription_id, stripe_customer_id, auto_renew, trial_used, payment_failed, payment_failed_at, last_payment_succeeded_at, grace_period_end, first_payment_done, blocked_bot)
                VALUES (%s, TRUE, %s, %s, %s, %s, %s, FALSE, NULL, NOW(), NULL, FALSE, FALSE)
                ON CONFLICT (telegram_id) DO UPDATE SET
                    paid = TRUE,
                    expiry_date = EXCLUDED.expiry_date,
                    stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, users.stripe_subscription_id),
                    stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, users.stripe_customer_id),
                    trial_used = CASE WHEN EXCLUDED.trial_used = TRUE THEN TRUE ELSE users.trial_used END,
                    payment_failed = FALSE,
                    payment_failed_at = NULL,
                    last_payment_succeeded_at = NOW(),
                    grace_period_end = NULL,
                    auto_renew = EXCLUDED.auto_renew,
                    reminder_sent = FALSE,
                    blocked_bot = FALSE,
                    first_payment_done = CASE WHEN %s THEN FALSE ELSE COALESCE(users.first_payment_done, FALSE) END
                """, (int(user_id), new_expiry, sub_id, customer_id, has_subscription, is_trial, needs_link))
                upsert_stripe_link(
                    cur,
                    user_id,
                    stripe_customer_id=customer_id,
                    stripe_subscription_id=sub_id,
                    customer_email=customer_email,
                    status="checkout_completed",
                    current_period_end=new_expiry,
                    is_active=True,
                    source=source_event_type,
                )
                mark_checkout_completed(
                    cur,
                    session_id,
                    customer_id=customer_id,
                    subscription_id=sub_id,
                )
                insert_payment_event(
                    cur,
                    event_id,
                    source_event_type,
                    "succeeded",
                    telegram_id=user_id,
                    checkout_session_id=session_id,
                    stripe_customer_id=customer_id,
                    stripe_subscription_id=sub_id,
                    payment_kind="trial" if is_trial and not has_subscription else "unknown",
                    tariff_code="sub_trial" if is_trial and not has_subscription else "unknown",
                    amount_paid=stripe_value(session, 'amount_total'),
                    amount_due=stripe_value(session, 'amount_total'),
                    currency=stripe_value(session, 'currency'),
                    period_start=now,
                    period_end=new_expiry,
                )
                cancel_first_purchase_recovery_deliveries(
                    cur,
                    user_id,
                    reason="checkout_session_completed",
                )
                cur.execute("""
                    INSERT INTO access_events (
                        telegram_id, event_type, source, old_expiry, new_expiry,
                        stripe_event_id, stripe_subscription_id, notes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    int(user_id),
                    "stripe_checkout_completed",
                    "stripe_webhook",
                    old_expiry,
                    new_expiry,
                    event_id,
                    sub_id,
                    f"days={days_to_add}; customer_id={safe_log_id(customer_id)}",
                ))

                if checkout_access_confirmed:
                    if is_trial and not has_subscription:
                        enqueue_stripe_user_message(
                            cur,
                            event_id,
                            user_id,
                            "trial_success",
                            f"Пробная неделя активирована до {new_expiry.strftime('%d.%m.%Y')}.\n\n"
                            "Все материалы уже доступны в меню.",
                        )
                    else:
                        purpose = payment_success_purpose("initial_subscription", checkout_was_payment_failed)
                        enqueue_user_payment_success_message(
                            cur,
                            event_id,
                            user_id,
                            purpose,
                            new_expiry,
                            keyboard_kind="cancel_subscription" if has_subscription else None,
                        )
                        enqueue_admin_payment_success_safely(
                            cur,
                            event_id,
                            purpose,
                            user_id,
                            tariff_code_from_checkout_days(days_to_add),
                            stripe_value(session, 'amount_total'),
                            stripe_value(session, 'currency'),
                            new_expiry,
                            safe_log_id(event_id or session_id),
                        )
                    enqueue_rejoin_invite_after_payment(
                        cur,
                        user_id,
                        new_expiry,
                        source_event_type,
                        event_id,
                        stripe_subscription_id=sub_id,
                    )
                else:
                    purpose = (
                        "trial_success"
                        if is_trial and not has_subscription
                        else payment_success_purpose("initial_subscription", checkout_was_payment_failed)
                    )
                    if purpose == "trial_success":
                        enqueue_stripe_user_message(
                            cur,
                            event_id,
                            user_id,
                            purpose,
                            f"Пробная неделя активирована до {new_expiry.strftime('%d.%m.%Y')}.\n\n"
                            "Все материалы уже доступны в меню.",
                        )
                    else:
                        enqueue_user_payment_success_message(
                            cur,
                            event_id,
                            user_id,
                            purpose,
                            new_expiry,
                            keyboard_kind="cancel_subscription" if has_subscription else None,
                        )
                        enqueue_admin_payment_success_safely(
                            cur,
                            event_id,
                            purpose,
                            user_id,
                            tariff_code_from_checkout_days(days_to_add),
                            stripe_value(session, 'amount_total'),
                            stripe_value(session, 'currency'),
                            new_expiry,
                            safe_log_id(event_id or session_id),
                        )
                conn.commit()
                logging.info(
                    f"Checkout Session marked completed: user_id={user_id}, "
                    f"session_id={safe_log_id(session_id)}, event_id={safe_log_id(event_id)}"
                )
                reset_checkout_retry_state_after_success(user_id, source_event_type)
                logging.info(
                    "User access activated: source=%s, event_id=%s, event.type=%s, "
                    "user_id=%s, customer_id=%s, customer_email=%s, paid=True, expiry_date=%s, "
                    "stripe_subscription_id=%s, blocked_bot=False",
                    source_event_type,
                    safe_log_id(event_id),
                    event_type,
                    user_id,
                    safe_log_id(customer_id),
                    safe_log_email(customer_email),
                    new_expiry,
                    safe_log_id(sub_id),
                )
                return {"applied": True, "new_expiry": new_expiry}
            except StripeIdentityConflictError as conflict:
                conn.rollback()
                return {
                    "applied": False,
                    "identity_conflict_response": await finalize_stripe_identity_conflict_response(
                    conflict,
                    event_id,
                    event_type,
                    claim_generation=claim_generation,
                    checkout_session_id=session_id,
                    amount_paid=stripe_value(session, 'amount_total'),
                    currency=stripe_value(session, 'currency'),
                    ),
                }
            except psycopg2_errors.UniqueViolation as e:
                conn.rollback()
                conflict = stripe_identity_conflict_from_unique_violation(
                    e,
                    user_id,
                    customer_id=customer_id,
                    subscription_id=sub_id,
                    source=source_event_type,
                )
                if not conflict:
                    raise
                if known_stripe_identity_unique_violation_is_same_user(conflict):
                    logging.warning(
                        "STRIPE_IDENTITY_SAME_USER_UNIQUE_RACE: event_id=%s, event.type=%s, "
                        "source=%s, conflict_type=%s, stripe_id=%s, telegram_id=%s; "
                        "event claim will be released for Stripe retry",
                        safe_log_id(event_id),
                        event_type,
                        source_event_type,
                        conflict.conflict_type,
                        conflict.safe_stripe_id,
                        user_id,
                    )
                    raise
                return {
                    "applied": False,
                    "identity_conflict_response": await finalize_stripe_identity_conflict_response(
                    conflict,
                    event_id,
                    event_type,
                    claim_generation=claim_generation,
                    checkout_session_id=session_id,
                    amount_paid=stripe_value(session, 'amount_total'),
                    currency=stripe_value(session, 'currency'),
                    ),
                }
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
                conn.close()

        # ---------- 1. ОПЛАТА ЧЕРЕЗ CHECKOUT (ПЕРВИЧНАЯ ИЛИ ПРОДЛЕНИЕ) ----------
        if event_type == 'checkout.session.completed':
            session = event_object
            gift_metadata = stripe_value(session, "metadata") or {}
            if stripe_value(gift_metadata, "payment_kind") == GIFT_PAYMENT_KIND:
                gift_id = stripe_value(gift_metadata, "gift_id")
                session_id = stripe_value(session, "id")
                try:
                    proof_session, line_item, price = await asyncio.to_thread(fetch_gift_checkout_payment_proof, session_id)
                except Exception as e:
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="gift_checkout_payment_proof_failed",
                        stage="checkout_completed",
                        telegram_id=stripe_value(gift_metadata, "purchaser_telegram_id"),
                        category="webhook_processing_failed",
                        exception=e,
                        stripe_retry="да",
                        recovery_reminder="не применимо",
                        safe_ref=safe_admin_context_reference("gift_checkout_proof", event_id, gift_id),
                        note="Gift checkout payment proof could not be verified. Stripe may retry.",
                        severity="CRITICAL",
                    )
                    await release_event_processing(event_id, claim_generation)
                    return web.Response(status=500)
                conn = get_db_conn()
                cur = conn.cursor()
                gift_response = web.Response(status=200)
                gift_mark_processed = False
                gift_release_event = False
                gift_admin_problem = None
                try:
                    cur.execute("""
                        SELECT *
                        FROM gift_access_grants
                        WHERE id = %s
                          AND stripe_session_id = %s
                        FOR UPDATE
                    """, (gift_id, session_id))
                    gift_row = gift_row_dict(cur, cur.fetchone())
                    if not gift_row:
                        enqueue_gift_admin_delivery(
                            cur,
                            safe_admin_context_reference("gift_missing", event_id, session_id),
                            "gift_admin_problem",
                            "⚠️ Gift payment webhook received for unknown gift.\n\n"
                            f"safe_ref: {safe_admin_context_reference('gift_missing', event_id, session_id)}",
                            severity="CRITICAL",
                        )
                        conn.commit()
                        gift_release_event = True
                        gift_response = web.Response(status=500, text="Unknown gift")
                    elif stripe_value(proof_session, "payment_status") == "paid":
                        mark_gift_paid_and_enqueue(cur, event_id, event_type, proof_session, line_item, price, gift_row)
                        conn.commit()
                        gift_mark_processed = True
                    elif gift_row:
                        cur.execute("""
                            UPDATE gift_access_grants
                            SET status = 'payment_pending',
                                updated_at = NOW()
                            WHERE id = %s
                              AND status IN ('checkout_pending', 'checkout_open', 'payment_pending')
                        """, (gift_row["id"],))
                        record_gift_event(cur, gift_row, "gift_payment_pending", gift_row["purchaser_telegram_id"], source="stripe_webhook")
                        conn.commit()
                        gift_mark_processed = True
                except Exception as e:
                    conn.rollback()
                    error_ref = safe_admin_error_reference("gift_checkout_completed", e)
                    logging.error(
                        "GIFT_CHECKOUT_COMPLETED_FAILED: event_id=%s gift=%s error_ref=%s",
                        safe_log_id(event_id),
                        safe_log_id(gift_id),
                        error_ref,
                        exc_info=True,
                    )
                    gift_admin_problem = {
                        "event_id": event_id,
                        "purpose": "gift_checkout_completed_failed",
                        "stage": "checkout_completed",
                        "telegram_id": stripe_value(gift_metadata, "purchaser_telegram_id"),
                        "category": "webhook_processing_failed",
                        "exception": None,
                        "stripe_retry": "да",
                        "recovery_reminder": "не применимо",
                        "safe_ref": safe_admin_context_reference("gift_checkout_completed", event_id, gift_id),
                        "note": "Gift payment was not applied. Stripe may retry.",
                        "severity": "CRITICAL",
                    }
                    gift_release_event = True
                    gift_response = web.Response(status=500)
                finally:
                    cur.close()
                    conn.close()
                if gift_mark_processed:
                    await mark_event_processed(event_id, claim_generation)
                if gift_admin_problem:
                    await enqueue_admin_payment_problem_now(**gift_admin_problem)
                if gift_release_event:
                    await release_event_processing(event_id, claim_generation)
                return gift_response
            user_id = resolve_checkout_telegram_id(session)
            metadata_obj = stripe_value(session, 'metadata') or {}
            metadata_keys = list(metadata_obj.keys()) if isinstance(metadata_obj, dict) else []
            logging.info(
                "Stripe checkout.session.completed data: "
                f"event_id={safe_log_id(event_id)}, session_id={safe_log_id(stripe_value(session, 'id'))}, "
                f"user_id={user_id}, client_reference_id_present={bool(stripe_value(session, 'client_reference_id'))}, "
                f"metadata_telegram_id_present={bool(stripe_value(session, 'metadata', 'telegram_id'))}, "
                f"metadata_keys={metadata_keys}, "
                f"mode={stripe_value(session, 'mode')}, payment_status={stripe_value(session, 'payment_status')}, "
                f"customer_id={safe_log_id(stripe_object_id(stripe_value(session, 'customer')))}, "
                f"customer_email={safe_log_email(stripe_value(session, 'customer_details', 'email') or stripe_value(session, 'customer_email'))}"
            )
            if not user_id:
                session_id = stripe_value(session, 'id')
                alert_key = f"checkout_invalid_identity:{safe_delivery_hash(event_id or session_id)}"
                logging.error(
                    "CHECKOUT_INVALID_IDENTITY: event_id=%s, session_id=%s, "
                    "client_reference_id_present=%s, metadata_telegram_id_present=%s, metadata_keys=%s. Access not granted.",
                    safe_log_id(event_id),
                    safe_log_id(session_id),
                    bool(stripe_value(session, 'client_reference_id')),
                    bool(stripe_value(session, 'metadata', 'telegram_id')),
                    metadata_keys,
                )
                try:
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="checkout_invalid_identity",
                        stage="checkout_completed",
                        telegram_id=None,
                        category="missing_subscription_identity",
                        stripe_retry="да",
                        recovery_reminder="неизвестно",
                        safe_ref=alert_key,
                        note="client_reference_id/metadata.telegram_id missing, invalid, or conflicting\naccess_granted: false",
                        severity="CRITICAL",
                    )
                except Exception as alert_error:
                    logging.error(
                        "CHECKOUT_INVALID_IDENTITY_ALERT_FAILED: event_id=%s, session_id=%s, error=%s",
                        safe_log_id(event_id),
                        safe_log_id(session_id),
                        alert_error,
                        exc_info=True,
                    )
                finally:
                    await release_event_processing(event_id, claim_generation)
                return web.Response(status=500, text="Invalid checkout Telegram identity")

            sub_id = stripe_object_id(stripe_value(session, 'subscription'))
            customer_id = stripe_object_id(stripe_value(session, 'customer'))
            customer_email = stripe_value(session, 'customer_details', 'email') or stripe_value(session, 'customer_email')
            checkout_mode = stripe_value(session, 'mode') or getattr(session, 'mode', None)
            checkout_action = checkout_completion_action(checkout_mode, sub_id)
            if checkout_action == "link_only":
                session_id = stripe_value(session, 'id')
                if not sub_id and session_id:
                    try:
                        session = await asyncio.to_thread(
                            stripe.checkout.Session.retrieve,
                            session_id,
                            expand=['subscription', 'customer']
                        )
                        sub_id = stripe_object_id(stripe_value(session, 'subscription'))
                        customer_id = customer_id or stripe_object_id(stripe_value(session, 'customer'))
                        customer_email = (
                            customer_email
                            or stripe_value(session, 'customer_details', 'email')
                            or stripe_value(session, 'customer_email')
                        )
                        logging.info(
                            "CHECKOUT_SUBSCRIPTION_SESSION_RETRIEVED: event_id=%s, event.type=%s, "
                            "session_id=%s, user_id=%s, customer_id=%s, subscription_id=%s",
                            safe_log_id(event_id),
                            event_type,
                            safe_log_id(session_id),
                            user_id,
                            safe_log_id(customer_id),
                            safe_log_id(sub_id),
                        )
                    except Exception as e:
                        logging.exception(
                            "CHECKOUT_SUBSCRIPTION_SESSION_RETRIEVE_FAILED: event_id=%s, event.type=%s, "
                            "session_id=%s, user_id=%s, customer_id=%s, error=%s",
                            safe_log_id(event_id),
                            event_type,
                            safe_log_id(session_id),
                            user_id,
                            safe_log_id(customer_id),
                            e,
                        )

                conn = get_db_conn()
                cur = conn.cursor()
                try:
                    assert_stripe_identity_available(
                        cur,
                        user_id,
                        customer_id=customer_id,
                        subscription_id=sub_id,
                        source="checkout.session.completed",
                    )
                    if not sub_id:
                        cur.execute("""
                            INSERT INTO users (
                                telegram_id,
                                paid,
                                stripe_customer_id,
                                auto_renew,
                                blocked_bot
                            )
                            VALUES (%s, FALSE, %s, TRUE, FALSE)
                            ON CONFLICT (telegram_id) DO UPDATE SET
                                stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, users.stripe_customer_id),
                                auto_renew = TRUE,
                                blocked_bot = FALSE
                        """, (int(user_id), customer_id))
                        upsert_stripe_link(
                            cur,
                            user_id,
                            stripe_customer_id=customer_id,
                            stripe_subscription_id=None,
                            customer_email=customer_email,
                            status="checkout_subscription_missing_subscription_id",
                            current_period_end=None,
                            is_active=False,
                            source="checkout.session.completed",
                        )
                        conn.commit()
                        clear_cached_checkout_sessions_for_user(user_id)
                        logging.error(
                            "CHECKOUT_SUBSCRIPTION_MISSING_SUBSCRIPTION_ID: event_id=%s, event.type=%s, "
                            "session_id=%s, user_id=%s, customer_id=%s",
                            safe_log_id(event_id),
                            event_type,
                            safe_log_id(session_id),
                            user_id,
                            safe_log_id(customer_id),
                        )
                        await enqueue_admin_payment_problem_now(
                            event_id=event_id,
                            purpose="checkout_missing_subscription_id",
                            stage="checkout_completed",
                            telegram_id=user_id,
                            category="missing_subscription_identity",
                            stripe_retry="да",
                            recovery_reminder="неизвестно",
                            safe_ref=safe_admin_context_reference("checkout_missing_subscription_id", event_id, session_id, customer_id),
                            note="Доступ НЕ выдан. Webhook вернул 500, Stripe повторит событие.",
                        )
                        await release_event_processing(event_id, claim_generation)
                        return web.Response(status=500)

                    cur.execute("""
                        INSERT INTO users (
                            telegram_id,
                            paid,
                            stripe_subscription_id,
                            stripe_customer_id,
                            auto_renew,
                            blocked_bot
                        )
                        VALUES (%s, FALSE, %s, %s, TRUE, FALSE)
                        ON CONFLICT (telegram_id) DO UPDATE SET
                            stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, users.stripe_subscription_id),
                            stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, users.stripe_customer_id),
                            auto_renew = TRUE,
                            blocked_bot = FALSE
                    """, (int(user_id), sub_id, customer_id))
                    upsert_stripe_link(
                        cur,
                        user_id,
                        stripe_customer_id=customer_id,
                        stripe_subscription_id=sub_id,
                        customer_email=customer_email,
                        status="checkout_subscription_pending_invoice",
                        current_period_end=None,
                        is_active=False,
                        source="checkout.session.completed",
                    )
                    conn.commit()
                    reset_checkout_retry_state_after_success(user_id, "checkout.session.completed")
                    logging.info(
                        "CHECKOUT_SUBSCRIPTION_LINKED_PENDING_INVOICE: event_id=%s, event.type=%s, "
                        "session_id=%s, user_id=%s, customer_id=%s, subscription_id=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(stripe_value(session, 'id')),
                        user_id,
                        safe_log_id(customer_id),
                        safe_log_id(sub_id),
                    )
                    await mark_event_processed(event_id, claim_generation)
                    return web.Response(status=200)
                except StripeIdentityConflictError as conflict:
                    conn.rollback()
                    return await finalize_stripe_identity_conflict_response(
                        conflict,
                        event_id,
                        event_type,
                        claim_generation=claim_generation,
                        checkout_session_id=stripe_value(session, 'id'),
                    )
                except psycopg2_errors.UniqueViolation as e:
                    conn.rollback()
                    conflict = stripe_identity_conflict_from_unique_violation(
                        e,
                        user_id,
                        customer_id=customer_id,
                        subscription_id=sub_id,
                        source="checkout.session.completed",
                    )
                    if not conflict:
                        raise
                    if known_stripe_identity_unique_violation_is_same_user(conflict):
                        logging.warning(
                            "STRIPE_IDENTITY_SAME_USER_UNIQUE_RACE: event_id=%s, event.type=%s, "
                            "source=checkout.session.completed, conflict_type=%s, stripe_id=%s, "
                            "telegram_id=%s; event claim will be released for Stripe retry",
                            safe_log_id(event_id),
                            event_type,
                            conflict.conflict_type,
                            conflict.safe_stripe_id,
                            user_id,
                        )
                        raise
                    return await finalize_stripe_identity_conflict_response(
                        conflict,
                        event_id,
                        event_type,
                        claim_generation=claim_generation,
                        checkout_session_id=stripe_value(session, 'id'),
                    )
                except Exception as e:
                    conn.rollback()
                    logging.exception(
                        "Ошибка связывания checkout.session.completed subscription: event_id=%s, "
                        "user_id=%s, session_id=%s, subscription_id=%s, error=%s",
                        safe_log_id(event_id),
                        user_id,
                        safe_log_id(stripe_value(session, 'id')),
                        safe_log_id(sub_id),
                        e,
                    )
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="checkout_subscription_link_failed",
                        stage="checkout_completed",
                        telegram_id=user_id,
                        category="webhook_processing_failed",
                        exception=e,
                        stripe_retry="да",
                        recovery_reminder="неизвестно",
                        safe_ref=safe_admin_error_reference("checkout_subscription_link", e),
                        note="Подписочный Checkout не связан. Операция не выполнена.",
                    )
                    await release_event_processing(event_id, claim_generation)
                    return web.Response(status=500)
                finally:
                    cur.close()
                    conn.close()

            metadata_raw = stripe_value(session, 'metadata') or getattr(session, 'metadata', None)
            days_to_add = parse_checkout_days(metadata_raw)
            logging.info(f"WEBHOOK DEBUG: user={user_id}, days={days_to_add}, mode={getattr(session, 'mode', '?')}")
            if days_to_add is None:
                session_id = stripe_value(session, 'id')
                alert_key = f"checkout_invalid_days:{safe_delivery_hash(event_id or session_id)}"
                logging.error(
                    "CHECKOUT_INVALID_DAYS: event_id=%s, session_id=%s, user_id=%s, metadata_keys=%s. Access not granted.",
                    safe_log_id(event_id),
                    safe_log_id(session_id),
                    user_id,
                    metadata_keys,
                )
                try:
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="invalid_checkout_metadata",
                        stage="checkout_completed",
                        telegram_id=user_id,
                        category="invalid_checkout_metadata",
                        stripe_retry="да",
                        recovery_reminder="неизвестно",
                        safe_ref=alert_key,
                        note="metadata.days missing or invalid\naccess_granted: false",
                        severity="CRITICAL",
                    )
                except Exception as alert_error:
                    logging.error(
                        "CHECKOUT_INVALID_DAYS_ALERT_FAILED: event_id=%s, session_id=%s, error=%s",
                        safe_log_id(event_id),
                        safe_log_id(session_id),
                        alert_error,
                        exc_info=True,
                    )
                finally:
                    await release_event_processing(event_id, claim_generation)
                return web.Response(status=500, text="Invalid checkout metadata.days")

            session_id = stripe_value(session, 'id')
            payment_decision = checkout_payment_access_decision(
                checkout_mode,
                stripe_value(session, "payment_status"),
                amount_total=stripe_value(session, "amount_total"),
                has_subscription=bool(sub_id),
                days_to_add=days_to_add,
            )
            if payment_decision["action"] == "retrieve" and session_id:
                try:
                    session = await asyncio.to_thread(
                        stripe.checkout.Session.retrieve,
                        session_id,
                        expand=["subscription", "customer"],
                    )
                    sub_id = stripe_object_id(stripe_value(session, "subscription"))
                    customer_id = customer_id or stripe_object_id(stripe_value(session, "customer"))
                    customer_email = (
                        customer_email
                        or stripe_value(session, "customer_details", "email")
                        or stripe_value(session, "customer_email")
                    )
                    checkout_mode = stripe_value(session, "mode") or checkout_mode
                    payment_decision = checkout_payment_access_decision(
                        checkout_mode,
                        stripe_value(session, "payment_status"),
                        amount_total=stripe_value(session, "amount_total"),
                        has_subscription=bool(sub_id),
                        days_to_add=days_to_add,
                    )
                except Exception as e:
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="checkout_payment_status_retrieve_failed",
                        stage="checkout_completed",
                        telegram_id=user_id,
                        category="webhook_processing_failed",
                        exception=e,
                        stripe_retry="да",
                        recovery_reminder="неизвестно",
                        safe_ref=safe_admin_error_reference("checkout_payment_status_retrieve", e),
                        note="Не удалось повторно получить Checkout Session. Доступ не выдан.",
                    )
                    await release_event_processing(event_id, claim_generation)
                    return web.Response(status=500)

            if payment_decision["action"] in ("payment_pending", "review_required"):
                conn = get_db_conn()
                cur = conn.cursor()
                try:
                    terminal_status = (
                        "payment_pending"
                        if payment_decision["action"] == "payment_pending"
                        else "manual_review_required"
                    )
                    mark_checkout_terminal(cur, session_id, terminal_status, error_text=payment_decision["reason"])
                    if payment_decision["action"] == "review_required":
                        enqueue_admin_payment_problem_safely(
                            cur,
                            event_id=event_id,
                            purpose="checkout_payment_status_review_required",
                            stage="checkout_completed",
                            telegram_id=user_id,
                            category="invalid_checkout_metadata",
                            stripe_retry="нет",
                            recovery_reminder="не применимо",
                            safe_ref=safe_admin_context_reference(
                                "checkout_payment_status_review",
                                event_id,
                                session_id,
                                user_id,
                                payment_decision["reason"],
                            ),
                            note=(
                                f"Доступ не выдан. payment_status={stripe_value(session, 'payment_status') or 'нет'}; "
                                f"reason={payment_decision['reason']}"
                            ),
                            severity="CRITICAL",
                        )
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="checkout_payment_status_gate_failed",
                        stage="checkout_completed",
                        telegram_id=user_id,
                        category="webhook_processing_failed",
                        exception=e,
                        stripe_retry="да",
                        recovery_reminder="неизвестно",
                        safe_ref=safe_admin_error_reference("checkout_payment_status_gate", e),
                        note="Payment status gate failed before access grant. Access not granted.",
                    )
                    await release_event_processing(event_id, claim_generation)
                    return web.Response(status=500)
                finally:
                    cur.close()
                    conn.close()

                await mark_event_processed(event_id, claim_generation)
                return web.Response(status=200)

            if payment_decision["action"] != "grant_access":
                await enqueue_admin_payment_problem_now(
                    event_id=event_id,
                    purpose="checkout_payment_status_unknown_action",
                    stage="checkout_completed",
                    telegram_id=user_id,
                    category="webhook_processing_failed",
                    stripe_retry="нет",
                    recovery_reminder="не применимо",
                    safe_ref=safe_admin_context_reference("checkout_payment_status_unknown", event_id, session_id, user_id),
                    note="Payment status decision did not allow access. Access not granted.",
                    severity="CRITICAL",
                )
                await mark_event_processed(event_id, claim_generation)
                return web.Response(status=200)

            try:
                checkout_apply_result = await apply_paid_checkout_access(
                    session,
                    user_id,
                    days_to_add,
                    sub_id,
                    customer_id,
                    customer_email,
                    checkout_action,
                    "checkout.session.completed",
                )
                if checkout_apply_result.get("identity_conflict_response"):
                    return checkout_apply_result["identity_conflict_response"]
            except Exception as e:
                logging.exception(
                    f"Ошибка обработки checkout.session.completed: event_id={safe_log_id(event_id)}, "
                    f"user_id={user_id}, session_id={safe_log_id(stripe_value(session, 'id'))}: {e}"
                )
                await enqueue_admin_payment_problem_now(
                    event_id=event_id,
                    purpose="checkout_completed_processing_failed",
                    stage="checkout_completed",
                    telegram_id=user_id,
                    category="webhook_processing_failed",
                    exception=e,
                    stripe_retry="да",
                    recovery_reminder="неизвестно",
                    safe_ref=safe_admin_error_reference("checkout_completed_processing", e),
                    note="Операция не выполнена. Webhook вернул 500.",
                )
                await release_event_processing(event_id, claim_generation)
                return web.Response(status=500)

        # ---------- 2. УСПЕШНОЕ АВТОПРОДЛЕНИЕ (invoice.payment_succeeded) ----------
            # ---------- 2. УСПЕШНОЕ АВТОПРОДЛЕНИЕ (invoice.payment_succeeded) ----------
        elif event_type == 'invoice.payment_succeeded':
            invoice = event_object
            logging.info(
                "Stripe invoice.payment_succeeded data: "
                f"event_id={safe_log_id(event_id)}, invoice_id={safe_log_id(stripe_value(invoice, 'id'))}, "
                f"customer_id={safe_log_id(stripe_object_id(stripe_value(invoice, 'customer')))}, "
                f"metadata_telegram_id={stripe_value(invoice, 'metadata', 'telegram_id')}"
            )
            sub_id = stripe_object_id(stripe_value(invoice, 'subscription'))
            sub_id = sub_id or stripe_object_id(stripe_value(invoice, 'parent', 'subscription_details', 'subscription'))
            lines_data = stripe_value(invoice, 'lines', 'data') or []
            first_line = lines_data[0] if lines_data else None
            sub_id = sub_id or stripe_object_id(stripe_value(first_line, 'subscription'))
            customer_id = stripe_object_id(stripe_value(invoice, 'customer'))
            subscription = None

            if not sub_id:
                try:
                    invoice = await asyncio.to_thread(
                        stripe.Invoice.retrieve,
                        stripe_value(invoice, 'id'),
                        expand=['subscription', 'customer', 'parent.subscription_details.subscription']
                    )
                    sub_id = stripe_object_id(stripe_value(invoice, 'subscription'))
                    sub_id = sub_id or stripe_object_id(stripe_value(invoice, 'parent', 'subscription_details', 'subscription'))
                    lines_data = stripe_value(invoice, 'lines', 'data') or []
                    first_line = lines_data[0] if lines_data else None
                    sub_id = sub_id or stripe_object_id(stripe_value(first_line, 'subscription'))
                    customer_id = stripe_object_id(stripe_value(invoice, 'customer'))
                except Exception as e:
                    logging.error(f"Не удалось повторно получить invoice {safe_log_id(stripe_value(invoice, 'id'))}: {e}")

            log_invoice_debug(invoice, subscription_id=sub_id)

            conn = get_db_conn()
            cur = conn.cursor()
            linked_telegram_id = None

            try:
                if not sub_id:
                    logging.error(
                        "invoice.payment_succeeded: не найден subscription_id, event=%s",
                        safe_log_id(event_id),
                    )
                    lines_data = stripe_value(invoice, 'lines', 'data') or []
                    first_line = lines_data[0] if lines_data else None
                    save_unlinked_stripe_event(
                        cur,
                        event_id,
                        event_type,
                        invoice_id=stripe_value(invoice, 'id'),
                        stripe_customer_id=customer_id,
                        customer_email=stripe_value(invoice, 'customer_email') or stripe_value(stripe_value(invoice, 'customer'), 'email'),
                        amount_paid=stripe_value(invoice, 'amount_paid'),
                        currency=stripe_value(invoice, 'currency'),
                        billing_reason=stripe_value(invoice, 'billing_reason'),
                        period_end=stripe_value(first_line, 'period', 'end'),
                        raw_summary="invoice.payment_succeeded without subscription_id",
                    )
                    await notify_unlinked_invoice(invoice)
                    conn.commit()
                    await mark_event_processed(event_id, claim_generation)
                    return web.Response(status=200)

                subscription = await asyncio.to_thread(stripe.Subscription.retrieve, sub_id)
                customer_id = customer_id or stripe_object_id(stripe_value(subscription, 'customer'))
                subscription_status = stripe_value(subscription, 'status')
                trial_end = stripe_value(subscription, 'trial_end')
                invoice_id = stripe_value(invoice, 'id') or "нет"
                billing_reason = stripe_value(invoice, 'billing_reason')
                amount_paid = stripe_value(invoice, 'amount_paid')
                amount_due = stripe_value(invoice, 'amount_due')
                invoice_status = stripe_value(invoice, 'status')

                if amount_paid and not stripe_value(invoice, 'payments', 'data'):
                    try:
                        invoice = await asyncio.to_thread(
                            stripe.Invoice.retrieve,
                            invoice_id,
                            expand=['payments']
                        )
                        customer_id = customer_id or stripe_object_id(stripe_value(invoice, 'customer'))
                        amount_paid = stripe_value(invoice, 'amount_paid')
                        amount_due = stripe_value(invoice, 'amount_due')
                        invoice_status = stripe_value(invoice, 'status')
                        billing_reason = stripe_value(invoice, 'billing_reason')
                        logging.info(
                            "Stripe invoice.payment_succeeded payments expanded: event_id=%s, "
                            "invoice_id=%s, payments_count=%s, payment_intent=%s, paid_out_of_band=%s",
                            safe_log_id(event_id),
                            safe_log_id(invoice_id),
                            len(stripe_value(invoice, 'payments', 'data') or []),
                            safe_log_id(stripe_object_id(stripe_value(invoice, 'payment_intent'))),
                            stripe_value(invoice, 'paid_out_of_band'),
                        )
                    except Exception as e:
                        logging.exception(
                            "Не удалось получить invoice payments для классификации оплаты. "
                            "event_id=%s, invoice_id=%s, subscription_id=%s, error=%s",
                            safe_log_id(event_id),
                            safe_log_id(invoice_id),
                            safe_log_id(sub_id),
                            e,
                        )
                        await enqueue_admin_payment_problem_now(
                            event_id=event_id,
                            purpose="invoice_payment_records_check_failed",
                            stage="invoice_payment_succeeded",
                            telegram_id=None,
                            category="stripe_api_unavailable",
                            exception=e,
                            stripe_retry="да",
                            recovery_reminder="не применимо",
                            safe_ref=safe_admin_error_reference("invoice_payment_records_check", e),
                            note="Webhook вернул 500, Stripe повторит событие. Доступ в БД не менялся.",
                        )
                        conn.rollback()
                        await release_event_processing(event_id, claim_generation)
                        return web.Response(status=500)

                invoice_action = successful_invoice_action(
                    amount_paid,
                    billing_reason,
                    subscription_status,
                    trial_end,
                    invoice=invoice,
                    amount_due=amount_due,
                )
                payment_kind = invoice_payment_kind(billing_reason, invoice_action)

                if invoice_action == "ignore_zero":
                    log_marker = (
                        "ZERO_AMOUNT_INVOICE_IGNORED"
                        if is_zero_subscription_update_invoice(amount_paid, billing_reason)
                        else "STALE_INVOICE_EVENT_IGNORED"
                    )
                    logging.info(
                        "%s: event_id=%s, event.type=%s, invoice_id=%s, subscription_id=%s, "
                        "customer_id=%s, billing_reason=%s, invoice_status=%s, amount_paid=%s, "
                        "amount_due=%s, subscription_status=%s, trial_end=%s",
                        log_marker,
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(invoice_id),
                        safe_log_id(sub_id),
                        safe_log_id(customer_id),
                        billing_reason,
                        invoice_status,
                        amount_paid,
                        amount_due,
                        subscription_status,
                        trial_end,
                    )
                    conn.commit()
                    await mark_event_processed(event_id, claim_generation)
                    return web.Response(status=200)

                if invoice_action == "sync_trial":
                    metadata_telegram_id = (
                        stripe_value(invoice, 'metadata', 'telegram_id')
                        or stripe_value(subscription, 'metadata', 'telegram_id')
                    )
                    try:
                        metadata_telegram_id = int(metadata_telegram_id) if metadata_telegram_id else None
                    except (TypeError, ValueError):
                        logging.error(
                            "invoice.payment_succeeded: некорректный metadata.telegram_id=%s, "
                            "subscription_id=%s, event=%s",
                            metadata_telegram_id,
                            safe_log_id(sub_id),
                            safe_log_id(event_id),
                        )
                        metadata_telegram_id = None

                    linked_telegram_id, link_source = find_telegram_id_for_stripe(
                        cur,
                        metadata_telegram_id=metadata_telegram_id,
                        stripe_subscription_id=sub_id,
                        stripe_customer_id=customer_id,
                    )

                    if not linked_telegram_id:
                        save_unlinked_stripe_event(
                            cur,
                            event_id,
                            event_type,
                            invoice_id=invoice_id,
                            stripe_customer_id=customer_id,
                            stripe_subscription_id=sub_id,
                            customer_email=stripe_value(invoice, 'customer_email') or stripe_value(stripe_value(invoice, 'customer'), 'email'),
                            amount_paid=amount_paid,
                            currency=stripe_value(invoice, 'currency'),
                            billing_reason=billing_reason,
                            period_end=trial_end,
                            raw_summary=(
                                "zero amount invoice with active trial; access not synced because user was not found"
                            ),
                        )
                        conn.commit()
                        logging.warning(
                            "ZERO_AMOUNT_INVOICE_IGNORED: active trial found, but user is not linked. "
                            "event_id=%s, event.type=%s, invoice_id=%s, subscription_id=%s, "
                            "customer_id=%s, trial_end=%s",
                            safe_log_id(event_id),
                            event_type,
                            safe_log_id(invoice_id),
                            safe_log_id(sub_id),
                            safe_log_id(customer_id),
                            trial_end,
                        )
                        await notify_unlinked_invoice(invoice, subscription_id=sub_id, period_end_override=trial_end)
                        await mark_event_processed(event_id, claim_generation)
                        return web.Response(status=200)

                    assert_stripe_identity_available(
                        cur,
                        linked_telegram_id,
                        customer_id=customer_id,
                        subscription_id=sub_id,
                        source="invoice.payment_succeeded",
                    )

                    trial_expiry = datetime.utcfromtimestamp(int(trial_end))
                    cur.execute("""
                        WITH target AS (
                            SELECT telegram_id, expiry_date AS old_expiry
                            FROM users
                            WHERE telegram_id = %s
                        )
                        UPDATE users
                        SET expiry_date = CASE
                                WHEN users.expiry_date IS NOT NULL AND users.expiry_date >= %s THEN users.expiry_date
                                ELSE %s
                            END,
                            paid = TRUE,
                            stripe_subscription_id = %s,
                            stripe_customer_id = COALESCE(%s, users.stripe_customer_id),
                            payment_failed = FALSE,
                            payment_failed_at = NULL,
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            auto_renew = TRUE,
                            blocked_bot = FALSE
                        FROM target
                        WHERE users.telegram_id = target.telegram_id
                        RETURNING users.telegram_id, target.old_expiry, users.expiry_date
                    """, (int(linked_telegram_id), trial_expiry, trial_expiry, sub_id, customer_id))
                    trial_row = cur.fetchone()
                    if trial_row:
                        upsert_stripe_link(
                            cur,
                            trial_row[0],
                            stripe_customer_id=customer_id,
                            stripe_subscription_id=sub_id,
                            customer_email=stripe_value(invoice, 'customer_email') or stripe_value(stripe_value(invoice, 'customer'), 'email'),
                            status=subscription_status,
                            current_period_end=trial_end,
                            is_active=True,
                            source="invoice.payment_succeeded",
                        )
                        conn.commit()
                        reset_checkout_retry_state_after_success(trial_row[0], "invoice.payment_succeeded")
                        logging.info(
                            "ACCESS_SYNCED_FROM_STRIPE_TRIAL: event_id=%s, event.type=%s, invoice_id=%s, "
                            "telegram_id=%s, subscription_id=%s, customer_id=%s, billing_reason=%s, "
                            "amount_paid=%s, amount_due=%s, trial_end=%s, old_expiry=%s, new_expiry=%s, "
                            "link_source=%s",
                            safe_log_id(event_id),
                            event_type,
                            safe_log_id(invoice_id),
                            trial_row[0],
                            safe_log_id(sub_id),
                            safe_log_id(customer_id),
                            billing_reason,
                            amount_paid,
                            amount_due,
                            trial_end,
                            trial_row[1],
                            trial_row[2],
                            link_source,
                        )
                        await mark_event_processed(event_id, claim_generation)
                        return web.Response(status=200)

                    conn.commit()
                    logging.warning(
                        "ZERO_AMOUNT_INVOICE_IGNORED: active trial found, but UPDATE users matched 0 rows. "
                        "event_id=%s, event.type=%s, invoice_id=%s, subscription_id=%s, "
                        "customer_id=%s, linked_telegram_id=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(invoice_id),
                        safe_log_id(sub_id),
                        safe_log_id(customer_id),
                        linked_telegram_id,
                    )
                    await notify_unlinked_invoice(invoice, subscription_id=sub_id, period_end_override=trial_end)
                    await mark_event_processed(event_id, claim_generation)
                    return web.Response(status=200)

                current_period_end = stripe_value(subscription, 'current_period_end')
                period_source = "subscription.current_period_end"

                if not current_period_end:
                    lines_data = stripe_value(invoice, 'lines', 'data') or []
                    first_line = lines_data[0] if lines_data else None
                    current_period_end = stripe_value(first_line, 'period', 'end')
                    if current_period_end:
                        period_source = "invoice.lines.data[0].period.end"

                if not current_period_end:
                    invoice_id = stripe_value(invoice, 'id') or "нет"
                    logging.error(
                        "invoice.payment_succeeded: у subscription нет current_period_end. "
                        "subscription_id=%s, customer_id=%s, invoice_id=%s, event=%s",
                        safe_log_id(sub_id),
                        safe_log_id(customer_id),
                        safe_log_id(invoice_id),
                        safe_log_id(event_id),
                    )
                    enqueue_admin_payment_problem_safely(
                        cur,
                        event_id=event_id,
                        purpose="missing_subscription_period",
                        stage="invoice_payment_succeeded",
                        telegram_id=None,
                        category="missing_subscription_period",
                        stripe_retry="нет",
                        recovery_reminder="не применимо",
                        safe_ref=safe_admin_context_reference("missing_subscription_period", event_id, sub_id, invoice_id),
                        note="Webhook не упал, но доступ автоматически не обновлен. Проверьте подписку вручную.",
                    )
                    conn.commit()
                    await mark_event_processed(event_id, claim_generation)
                    return web.Response(status=200)

                stripe_period_expiry = datetime.utcfromtimestamp(current_period_end)
                old_expiry = None
                effective_expiry = None
                row = None
                was_payment_failed = False
                metadata_telegram_id = (
                    stripe_value(invoice, 'metadata', 'telegram_id')
                    or stripe_value(subscription, 'metadata', 'telegram_id')
                )

                if metadata_telegram_id:
                    try:
                        metadata_telegram_id = int(metadata_telegram_id)
                    except (TypeError, ValueError):
                        logging.error(
                            f"invoice.payment_succeeded: некорректный metadata.telegram_id={metadata_telegram_id}, "
                            f"subscription_id={safe_log_id(sub_id)}, event={safe_log_id(event_id)}"
                        )
                        metadata_telegram_id = None

                linked_telegram_id = metadata_telegram_id
                link_source = "metadata.telegram_id" if metadata_telegram_id else None
                if not linked_telegram_id:
                    linked_telegram_id, link_source = find_telegram_id_for_stripe(
                        cur,
                        stripe_subscription_id=sub_id,
                        stripe_customer_id=customer_id,
                    )

                if linked_telegram_id:
                    assert_stripe_identity_available(
                        cur,
                        linked_telegram_id,
                        customer_id=customer_id,
                        subscription_id=sub_id,
                        source="invoice.payment_succeeded",
                    )
                    cur.execute("""
                        WITH target AS (
                            SELECT telegram_id, expiry_date AS old_expiry, payment_failed AS was_payment_failed
                            FROM users
                            WHERE telegram_id = %s
                        )
                        UPDATE users
                        SET expiry_date = CASE
                                WHEN users.expiry_date IS NOT NULL AND users.expiry_date >= %s THEN users.expiry_date
                                ELSE %s
                            END,
                            paid = TRUE,
                            stripe_subscription_id = %s,
                            stripe_customer_id = COALESCE(%s, users.stripe_customer_id),
                            payment_failed = FALSE,
                            payment_failed_at = NULL,
                            last_payment_succeeded_at = NOW(),
                            last_successful_invoice_created_at = COALESCE(
                                GREATEST(COALESCE(users.last_successful_invoice_created_at, %s), %s),
                                users.last_successful_invoice_created_at
                            ),
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            auto_renew = TRUE,
                            blocked_bot = FALSE,
                            first_payment_done = CASE WHEN %s THEN TRUE ELSE users.first_payment_done END
                        FROM target
                        WHERE users.telegram_id = target.telegram_id
                        RETURNING users.telegram_id, target.old_expiry, target.was_payment_failed, users.expiry_date AS effective_expiry
                    """, (
                        linked_telegram_id,
                        stripe_period_expiry,
                        stripe_period_expiry,
                        sub_id,
                        customer_id,
                        event_created_at,
                        event_created_at,
                        payment_kind == "initial_subscription",
                    ))

                    row = cur.fetchone()
                    if row:
                        old_expiry = row[1]
                        was_payment_failed = row[2]
                        effective_expiry = row[3]
                        if was_payment_failed:
                            cancel_failed_renewal_user_messages(
                                cur,
                                row[0],
                                "invoice_payment_succeeded",
                            )
                        if link_source != "metadata.telegram_id":
                            logging.info(
                                "STRIPE_USER_RESOLVED_VIA_LINK: event_id=%s, event.type=%s, telegram_id=%s, "
                                "source=%s, customer_id=%s, subscription_id=%s",
                                safe_log_id(event_id),
                                event_type,
                                row[0],
                                link_source,
                                safe_log_id(customer_id),
                                safe_log_id(sub_id),
                            )

                if not row:
                    lines_data = stripe_value(invoice, 'lines', 'data') or []
                    first_line = lines_data[0] if lines_data else None
                    period_end = current_period_end or stripe_value(first_line, 'period', 'end')
                    save_unlinked_stripe_event(
                        cur,
                        event_id,
                        event_type,
                        invoice_id=stripe_value(invoice, 'id'),
                        stripe_customer_id=customer_id,
                        stripe_subscription_id=sub_id,
                        customer_email=stripe_value(invoice, 'customer_email') or stripe_value(stripe_value(invoice, 'customer'), 'email'),
                        amount_paid=stripe_value(invoice, 'amount_paid'),
                        currency=stripe_value(invoice, 'currency'),
                        billing_reason=stripe_value(invoice, 'billing_reason'),
                        period_end=period_end,
                        raw_summary=f"subscription_id={sub_id}; customer_id={customer_id}; period_source={period_source}",
                    )
                    conn.commit()
                    logging.error(
                        f"invoice.payment_succeeded: пользователь не найден. "
                        f"subscription_id={safe_log_id(sub_id)}, customer_id={safe_log_id(customer_id)}, "
                        f"event={safe_log_id(event_id)}"
                    )

                    await notify_unlinked_invoice(invoice, subscription_id=sub_id, period_end_override=current_period_end)
                    await mark_event_processed(event_id, claim_generation)
                    return web.Response(status=200)

                telegram_id = row[0]
                customer_email = stripe_value(invoice, 'customer_email') or stripe_value(stripe_value(invoice, 'customer'), 'email')
                period_start, period_end = invoice_line_period_datetimes(invoice)
                upsert_stripe_link(
                    cur,
                    telegram_id,
                    stripe_customer_id=customer_id,
                    stripe_subscription_id=sub_id,
                    customer_email=customer_email,
                    status=get_obj_value(subscription, 'status'),
                    current_period_end=current_period_end,
                    is_active=True,
                    source="invoice.payment_succeeded",
                )
                insert_payment_event(
                    cur,
                    event_id,
                    event_type,
                    "succeeded",
                    telegram_id=telegram_id,
                    invoice_id=invoice_id,
                    stripe_customer_id=customer_id,
                    stripe_subscription_id=sub_id,
                    payment_kind=payment_kind,
                    billing_reason=billing_reason,
                    tariff_code=tariff_code_from_invoice(invoice),
                    amount_paid=amount_paid,
                    amount_due=amount_due,
                    currency=stripe_value(invoice, 'currency'),
                    period_start=period_start,
                    period_end=period_end or stripe_period_expiry,
                    recovered_after_failure=was_payment_failed,
                )
                if payment_kind == "initial_subscription":
                    cancel_first_purchase_recovery_deliveries(
                        cur,
                        telegram_id,
                        reason="initial_subscription_payment_succeeded",
                    )

                if payment_kind == "out_of_band":
                    logging.info(
                        "MANUAL_OUT_OF_BAND_PAYMENT_PROCESSED: event_id=%s, event.type=%s, invoice_id=%s, "
                        "telegram_id=%s, subscription_id=%s, customer_id=%s, billing_reason=%s, "
                        "amount_paid=%s, amount_due=%s, invoice_status=%s, stripe_period_expiry=%s, effective_expiry=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(invoice_id),
                        telegram_id,
                        safe_log_id(sub_id),
                        safe_log_id(customer_id),
                        billing_reason,
                        amount_paid,
                        amount_due,
                        invoice_status,
                        stripe_period_expiry,
                        effective_expiry,
                    )
                elif payment_kind == "initial_subscription":
                    logging.info(
                        "INITIAL_SUBSCRIPTION_PAYMENT_PROCESSED: event_id=%s, event.type=%s, invoice_id=%s, "
                        "telegram_id=%s, subscription_id=%s, customer_id=%s, billing_reason=%s, "
                        "amount_paid=%s, amount_due=%s, invoice_status=%s, stripe_period_expiry=%s, effective_expiry=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(invoice_id),
                        telegram_id,
                        safe_log_id(sub_id),
                        safe_log_id(customer_id),
                        billing_reason,
                        amount_paid,
                        amount_due,
                        invoice_status,
                        stripe_period_expiry,
                        effective_expiry,
                    )
                elif payment_kind == "recurring":
                    logging.info(
                        "REAL_RECURRING_PAYMENT_PROCESSED: event_id=%s, event.type=%s, invoice_id=%s, "
                        "telegram_id=%s, subscription_id=%s, customer_id=%s, billing_reason=%s, "
                        "amount_paid=%s, amount_due=%s, invoice_status=%s, stripe_period_expiry=%s, effective_expiry=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(invoice_id),
                        telegram_id,
                        safe_log_id(sub_id),
                        safe_log_id(customer_id),
                        billing_reason,
                        amount_paid,
                        amount_due,
                        invoice_status,
                        stripe_period_expiry,
                        effective_expiry,
                    )
                else:
                    logging.info(
                        "SUBSCRIPTION_PAYMENT_ADJUSTMENT_PROCESSED: event_id=%s, event.type=%s, invoice_id=%s, "
                        "telegram_id=%s, subscription_id=%s, customer_id=%s, billing_reason=%s, "
                        "amount_paid=%s, amount_due=%s, invoice_status=%s, stripe_period_expiry=%s, effective_expiry=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(invoice_id),
                        telegram_id,
                        safe_log_id(sub_id),
                        safe_log_id(customer_id),
                        billing_reason,
                        amount_paid,
                        amount_due,
                        invoice_status,
                        stripe_period_expiry,
                        effective_expiry,
                    )
                logging.info(
                    "User access activated: source=invoice.payment_succeeded, event_id=%s, "
                    "event.type=%s, invoice_id=%s, user_id=%s, customer_id=%s, customer_email=%s, "
                    "subscription_id=%s, status=%s, billing_reason=%s, paid=True, expiry_date=%s, blocked_bot=False",
                    safe_log_id(event_id),
                    event_type,
                    safe_log_id(invoice_id),
                    telegram_id,
                    safe_log_id(customer_id),
                    safe_log_email(customer_email),
                    safe_log_id(sub_id),
                    invoice_status,
                    billing_reason,
                    effective_expiry,
                )
                if was_payment_failed:
                    logging.info(
                        "PAYMENT_RECOVERED_AFTER_FAILURE: telegram_id=%s, customer_id=%s, email=%s, "
                        "subscription_id=%s, invoice_id=%s, effective_expiry_date=%s",
                        telegram_id,
                        safe_log_id(customer_id),
                        safe_log_email(customer_email),
                        safe_log_id(sub_id),
                        safe_log_id(invoice_id),
                        effective_expiry,
                    )

                invoice_access_confirmed = bool(
                    row is not None
                    and telegram_id is not None
                    and effective_expiry is not None
                )

                if should_skip_invoice_notice_for_current_expiry(payment_kind, old_expiry, effective_expiry):
                    logging.info(
                        f"invoice.payment_succeeded: срок уже актуален, пропускаю повторное уведомление. "
                        f"telegram_id={telegram_id}, old_expiry={old_expiry}, effective_expiry={effective_expiry}, event={safe_log_id(event_id)}"
                    )
                    if invoice_access_confirmed:
                        enqueue_rejoin_invite_after_payment(
                            cur,
                            telegram_id,
                            effective_expiry,
                            "invoice.payment_succeeded",
                            event_id,
                            stripe_subscription_id=sub_id,
                        )
                    conn.commit()
                    reset_checkout_retry_state_after_success(telegram_id, "invoice.payment_succeeded")
                    await mark_event_processed(event_id, claim_generation)
                    return web.Response(status=200)

                if invoice_access_confirmed:
                    enqueue_rejoin_invite_after_payment(
                        cur,
                        telegram_id,
                        effective_expiry,
                        "invoice.payment_succeeded",
                        event_id,
                        stripe_subscription_id=sub_id,
                    )

                if payment_kind == "out_of_band":
                    logging.info(
                        "AUTO_RENEW_NOTICE_SKIPPED_OUT_OF_BAND: telegram_id=%s, invoice_id=%s, "
                        "event_id=%s, effective_expiry=%s",
                        telegram_id,
                        safe_log_id(invoice_id),
                        safe_log_id(event_id),
                        effective_expiry,
                    )
                    conn.commit()
                    reset_checkout_retry_state_after_success(telegram_id, "invoice.payment_succeeded")
                    await mark_event_processed(event_id, claim_generation)
                    return web.Response(status=200)

                cur.execute("""
                    INSERT INTO access_events (
                        telegram_id, event_type, source, old_expiry, new_expiry,
                        stripe_event_id, stripe_subscription_id, notes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    int(telegram_id),
                    "stripe_invoice_paid",
                    "stripe_webhook",
                    old_expiry,
                    effective_expiry,
                    event_id,
                    sub_id,
                    f"customer_id={safe_log_id(customer_id)}; invoice_id={safe_log_id(invoice_id)}; period_source={period_source}",
                ))

                delivery_purpose = payment_success_purpose(payment_kind, was_payment_failed)
                if delivery_purpose == "payment_success":
                    notice_marker = "INITIAL_SUBSCRIPTION_NOTICE_SENT"
                elif delivery_purpose == "renewal_success":
                    notice_marker = "AUTO_RENEW_NOTICE_SENT"
                else:
                    notice_marker = "PAYMENT_RECOVERY_NOTICE_SENT"
                enqueue_user_payment_success_message(
                    cur,
                    event_id,
                    telegram_id,
                    delivery_purpose,
                    effective_expiry,
                    keyboard_kind="cancel_subscription",
                )
                enqueue_admin_payment_success_safely(
                    cur,
                    event_id,
                    delivery_purpose,
                    telegram_id,
                    tariff_code_from_invoice(invoice),
                    amount_paid,
                    stripe_value(invoice, 'currency'),
                    effective_expiry,
                    safe_log_id(invoice_id or event_id),
                )
                conn.commit()
                reset_checkout_retry_state_after_success(telegram_id, "invoice.payment_succeeded")
                logging.info(
                    "%s_ENQUEUED: telegram_id=%s, invoice_id=%s, effective_expiry=%s",
                    notice_marker,
                    telegram_id,
                    safe_log_id(invoice_id),
                    effective_expiry,
                )

            except StripeIdentityConflictError as conflict:
                conn.rollback()
                return await finalize_stripe_identity_conflict_response(
                    conflict,
                    event_id,
                    event_type,
                    claim_generation=claim_generation,
                    invoice_id=invoice_id,
                    amount_paid=amount_paid,
                    currency=stripe_value(invoice, 'currency'),
                    billing_reason=billing_reason,
                    period_end=current_period_end,
                )

            except psycopg2_errors.UniqueViolation as e:
                conn.rollback()
                conflict = stripe_identity_conflict_from_unique_violation(
                    e,
                    linked_telegram_id,
                    customer_id=customer_id,
                    subscription_id=sub_id,
                    source="invoice.payment_succeeded",
                )
                if not conflict:
                    raise
                if known_stripe_identity_unique_violation_is_same_user(conflict):
                    logging.warning(
                        "STRIPE_IDENTITY_SAME_USER_UNIQUE_RACE: event_id=%s, event.type=%s, "
                        "source=invoice.payment_succeeded, conflict_type=%s, stripe_id=%s, "
                        "telegram_id=%s; event claim will be released for Stripe retry",
                        safe_log_id(event_id),
                        event_type,
                        conflict.conflict_type,
                        conflict.safe_stripe_id,
                        linked_telegram_id,
                    )
                    raise
                return await finalize_stripe_identity_conflict_response(
                    conflict,
                    event_id,
                    event_type,
                    claim_generation=claim_generation,
                    invoice_id=invoice_id,
                    amount_paid=amount_paid,
                    currency=stripe_value(invoice, 'currency'),
                    billing_reason=billing_reason,
                    period_end=current_period_end,
                )

            except Exception as e:
                conn.rollback()
                logging.exception(
                    f"Ошибка invoice.payment_succeeded: event_id={safe_log_id(event_id)}, "
                    f"subscription_id={safe_log_id(sub_id)}, customer_id={safe_log_id(customer_id)}: {e}"
                )
                await enqueue_admin_payment_problem_now(
                    event_id=event_id,
                    purpose="invoice_payment_succeeded_processing_failed",
                    stage="invoice_payment_succeeded",
                    telegram_id=None,
                    category="webhook_processing_failed",
                    exception=e,
                    stripe_retry="да",
                    recovery_reminder="не применимо",
                    safe_ref=safe_admin_error_reference("invoice_payment_succeeded", e),
                    note="Webhook вернул 500, Stripe повторит событие. Операция не выполнена.",
                )
                await release_event_processing(event_id, claim_generation)
                return web.Response(status=500)

            finally:
                cur.close()
                conn.close()

        # ---------- 3. ОШИБКА ОПЛАТЫ (invoice.payment_failed) – GRACE PERIOD ----------
        elif event_type == 'invoice.payment_failed':
            invoice = event_object
            sub_id = stripe_object_id(stripe_value(invoice, 'subscription'))
            sub_id = sub_id or stripe_object_id(stripe_value(invoice, 'parent', 'subscription_details', 'subscription'))
            lines_data = stripe_value(invoice, 'lines', 'data') or []
            first_line = lines_data[0] if lines_data else None
            sub_id = sub_id or stripe_object_id(stripe_value(first_line, 'subscription'))
            invoice_id = stripe_value(invoice, 'id') or "нет"
            customer_id = stripe_object_id(stripe_value(invoice, 'customer')) or "нет"
            customer_email = (
                stripe_value(invoice, 'customer_email')
                or stripe_value(stripe_value(invoice, 'customer'), 'email')
                or "нет"
            )
            billing_reason = stripe_value(invoice, 'billing_reason') or "нет"
            invoice_status = stripe_value(invoice, 'status') or "нет"
            next_payment_attempt = stripe_value(invoice, 'next_payment_attempt')
            stripe_retry_text = "да" if next_payment_attempt else "нет"
            failure_code = stripe_failure_code_from_invoice(invoice)

            if not sub_id:
                logging.error(
                    "invoice.payment_failed: не найден subscription_id. "
                    "event_id=%s, event.type=%s, invoice_id=%s, customer_id=%s, "
                    "customer_email=%s, status=%s, billing_reason=%s",
                    safe_log_id(event_id),
                    event_type,
                    safe_log_id(invoice_id),
                    safe_log_id(customer_id),
                    safe_log_email(customer_email),
                    invoice_status,
                    billing_reason,
                )
                await enqueue_admin_payment_problem_now(
                    event_id=event_id,
                    purpose="invoice_payment_failed_missing_subscription",
                    stage="invoice_payment_failed",
                    telegram_id=None,
                    category="missing_subscription_identity",
                    stripe_retry=stripe_retry_text,
                    recovery_reminder="неизвестно",
                    safe_ref=safe_admin_context_reference("invoice_payment_failed_missing_subscription", event_id, invoice_id, customer_id),
                    note="payment_failed в БД не обновлен. Проверьте вручную.",
                )
                await mark_event_processed(event_id, claim_generation)
                return web.Response(status=200)

            if sub_id:
                try:
                    subscription = await asyncio.to_thread(stripe.Subscription.retrieve, sub_id)
                except Exception as e:
                    logging.exception(
                        "invoice.payment_failed: не удалось получить актуальную подписку. "
                        "event_id=%s, event.type=%s, invoice_id=%s, subscription_id=%s, "
                        "customer_id=%s, error=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(invoice_id),
                        safe_log_id(sub_id),
                        safe_log_id(customer_id),
                        e,
                    )
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="invoice_payment_failed_subscription_retrieve",
                        stage="invoice_payment_failed",
                        telegram_id=None,
                        exception=e,
                        stripe_retry="да",
                        recovery_reminder="неизвестно",
                        safe_ref=safe_admin_error_reference('payment_failed_subscription_retrieve', e),
                        note="Webhook вернул 500, Stripe повторит событие. Доступ в БД не менялся.",
                    )
                    await release_event_processing(event_id, claim_generation)
                    return web.Response(status=500)

                subscription_status = stripe_value(subscription, 'status')
                trial_end = stripe_value(subscription, 'trial_end')
                cancel_at_period_end = bool(stripe_value(subscription, 'cancel_at_period_end'))
                subscription_customer_id = stripe_object_id(stripe_value(subscription, 'customer'))
                customer_id_for_db = subscription_customer_id or (None if customer_id == "нет" else customer_id)

                if should_ignore_payment_failed_for_active_trial(subscription_status, trial_end):
                    trial_expiry = datetime.utcfromtimestamp(int(trial_end))
                    conn = get_db_conn()
                    cur = conn.cursor()
                    payment_failed_requested_telegram_id = None
                    try:
                        payment_failed_requested_telegram_id = assert_existing_subscription_identity_available(
                            cur,
                            sub_id,
                            customer_id=customer_id_for_db,
                            source="invoice.payment_failed",
                        )
                        cur.execute("""
                            UPDATE users
                            SET paid = TRUE,
                                expiry_date = CASE
                                    WHEN expiry_date IS NOT NULL AND expiry_date >= %s THEN expiry_date
                                    ELSE %s
                                END,
                                payment_failed = FALSE,
                                payment_failed_at = NULL,
                                grace_period_end = NULL,
                                reminder_sent = FALSE,
                                auto_renew = %s,
                                blocked_bot = FALSE,
                                stripe_customer_id = COALESCE(%s, stripe_customer_id)
                            WHERE stripe_subscription_id = %s
                            RETURNING telegram_id, expiry_date
                        """, (trial_expiry, trial_expiry, not cancel_at_period_end, customer_id_for_db, sub_id))
                        row = cur.fetchone()
                        if not row and customer_id_for_db:
                            cur.execute("""
                                SELECT telegram_id, stripe_subscription_id
                                FROM users
                                WHERE stripe_customer_id = %s
                                ORDER BY telegram_id
                            """, (customer_id_for_db,))
                            customer_matches = cur.fetchall()
                            customer_match = customer_matches[0] if len(customer_matches) == 1 else None
                            if customer_match and should_apply_failed_invoice_to_user(customer_match[1], sub_id):
                                cur.execute("""
                                    UPDATE users
                                    SET paid = TRUE,
                                        expiry_date = CASE
                                            WHEN expiry_date IS NOT NULL AND expiry_date >= %s THEN expiry_date
                                            ELSE %s
                                        END,
                                        payment_failed = FALSE,
                                        payment_failed_at = NULL,
                                        grace_period_end = NULL,
                                        reminder_sent = FALSE,
                                        auto_renew = %s,
                                        blocked_bot = FALSE,
                                        stripe_subscription_id = COALESCE(%s, stripe_subscription_id),
                                        stripe_customer_id = COALESCE(%s, stripe_customer_id)
                                    WHERE stripe_customer_id = %s
                                      AND (stripe_subscription_id IS NULL OR stripe_subscription_id = %s)
                                    RETURNING telegram_id, expiry_date
                                """, (
                                    trial_expiry,
                                    trial_expiry,
                                    not cancel_at_period_end,
                                    sub_id,
                                    customer_id_for_db,
                                    customer_id_for_db,
                                    sub_id,
                                ))
                                row = cur.fetchone()
                            else:
                                logging.warning(
                                    "PAYMENT_FAILED_CUSTOMER_FALLBACK_BLOCKED: event_id=%s, event.type=%s, "
                                    "customer_id=%s, invoice_id=%s, subscription_id=%s, matches=%s, reason=active_trial_sync",
                                    safe_log_id(event_id),
                                    event_type,
                                    safe_log_id(customer_id_for_db),
                                    safe_log_id(invoice_id),
                                    safe_log_id(sub_id),
                                    len(customer_matches),
                                )

                        if row:
                            upsert_stripe_link(
                                cur,
                                row[0],
                                stripe_customer_id=customer_id_for_db,
                                stripe_subscription_id=sub_id,
                                customer_email=customer_email if customer_email != "нет" else None,
                                status=subscription_status,
                                current_period_end=trial_end,
                                is_active=True,
                                source="invoice.payment_failed",
                            )

                        conn.commit()
                    except StripeIdentityConflictError as conflict:
                        conn.rollback()
                        return await finalize_stripe_identity_conflict_response(
                            conflict,
                            event_id,
                            event_type,
                            claim_generation=claim_generation,
                            invoice_id=invoice_id,
                            currency=stripe_value(invoice, 'currency'),
                            billing_reason=billing_reason,
                            period_end=trial_end,
                        )
                    except psycopg2_errors.UniqueViolation as e:
                        conn.rollback()
                        return await finalize_stripe_identity_unique_violation_webhook_response(
                            e,
                            payment_failed_requested_telegram_id,
                            customer_id_for_db,
                            sub_id,
                            "invoice.payment_failed",
                            event_id,
                            event_type,
                            claim_generation=claim_generation,
                            invoice_id=invoice_id,
                            currency=stripe_value(invoice, 'currency'),
                            billing_reason=billing_reason,
                            period_end=trial_end,
                        )
                    except Exception as e:
                        conn.rollback()
                        logging.exception(
                            "PAYMENT_FAILED_IGNORED_ACTIVE_TRIAL: ошибка синхронизации trial в БД. "
                            "event_id=%s, event.type=%s, invoice_id=%s, subscription_id=%s, "
                            "customer_id=%s, error=%s",
                            safe_log_id(event_id),
                            event_type,
                            safe_log_id(invoice_id),
                            safe_log_id(sub_id),
                            safe_log_id(customer_id),
                            e,
                        )
                        await enqueue_admin_payment_problem_now(
                            event_id=event_id,
                            purpose="payment_failed_trial_sync_failed",
                            stage="invoice_payment_failed",
                            telegram_id=None,
                            category="stale_historical_invoice",
                            exception=e,
                            stripe_retry="да",
                            recovery_reminder="не применимо",
                            safe_ref=safe_admin_error_reference('payment_failed_trial_sync', e),
                            note="Webhook вернул 500, Stripe повторит событие. Trial sync не выполнен.",
                        )
                        await release_event_processing(event_id, claim_generation)
                        return web.Response(status=500)
                    finally:
                        cur.close()
                        conn.close()

                    logging.info(
                        "PAYMENT_FAILED_IGNORED_ACTIVE_TRIAL: event_id=%s, event.type=%s, invoice_id=%s, "
                        "subscription_id=%s, customer_id=%s, invoice_status=%s, billing_reason=%s, "
                        "subscription_status=%s, trial_end=%s, telegram_id=%s, synced_expiry=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(invoice_id),
                        safe_log_id(sub_id),
                        safe_log_id(customer_id),
                        invoice_status,
                        billing_reason,
                        subscription_status,
                        trial_end,
                        row[0] if row else None,
                        row[1] if row else None,
                    )
                    logging.info(
                        "STALE_INVOICE_EVENT_IGNORED: event_id=%s, event.type=%s, invoice_id=%s, "
                        "subscription_id=%s, reason=active_future_trial",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(invoice_id),
                        safe_log_id(sub_id),
                    )
                    await mark_event_processed(event_id, claim_generation)
                    return web.Response(status=200)

                stale_payment_failed_alert = None
                stale_payment_failed_alert_key = None
                conn = get_db_conn()
                cur = conn.cursor()
                payment_failed_requested_telegram_id = None
                try:
                    payment_failed_requested_telegram_id = assert_existing_subscription_identity_available(
                        cur,
                        sub_id,
                        customer_id=customer_id_for_db,
                        source="invoice.payment_failed",
                    )
                except StripeIdentityConflictError as conflict:
                    conn.rollback()
                    cur.close()
                    conn.close()
                    return await finalize_stripe_identity_conflict_response(
                        conflict,
                        event_id,
                        event_type,
                        claim_generation=claim_generation,
                        invoice_id=invoice_id,
                        currency=stripe_value(invoice, 'currency'),
                        billing_reason=billing_reason,
                    )
                cur.execute(
                    """
                    SELECT last_successful_invoice_created_at
                    FROM users
                    WHERE stripe_subscription_id = %s
                       OR (%s IS NOT NULL AND stripe_customer_id = %s)
                    ORDER BY last_successful_invoice_created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (sub_id, customer_id_for_db, customer_id_for_db),
                )
                last_success_row = cur.fetchone()
                last_success_created_at = last_success_row[0] if last_success_row else None
                if (
                    not should_apply_negative_event(event_created_at, last_success_created_at)
                    and live_subscription_is_paid(subscription_status, "paid")
                ):
                    cur.execute(
                        """
                        INSERT INTO access_events (
                            telegram_id, event_type, source, stripe_event_id, stripe_subscription_id, notes
                        )
                        SELECT telegram_id, 'ignored_stale_negative_event', 'invoice.payment_failed', %s, %s, %s
                        FROM users
                        WHERE stripe_subscription_id = %s
                           OR (%s IS NOT NULL AND stripe_customer_id = %s)
                        LIMIT 1
                        """,
                        (
                            event_id,
                            sub_id,
                            f"event_created_at={event_created_at}; last_successful_invoice_created_at={last_success_created_at}",
                            sub_id,
                            customer_id_for_db,
                            customer_id_for_db,
                        ),
                    )
                    enqueue_admin_payment_problem_safely(
                        cur,
                        event_id=event_id,
                        purpose="stale_historical_invoice",
                        stage="invoice_payment_failed",
                        telegram_id=None,
                        category="stale_historical_invoice",
                        stripe_retry="нет",
                        recovery_reminder="не применимо",
                        safe_ref=safe_admin_context_reference("stale_historical_invoice", event_id, sub_id, customer_id_for_db),
                        note=(
                            "Текущая активная подписка не изменена.\n"
                            "payment_failed пользователю не установлен.\n"
                            "Старый invoice проигнорирован безопасно."
                        ),
                    )
                    conn.commit()
                    logging.warning(
                        "STALE_NEGATIVE_STRIPE_EVENT_IGNORED: event_id=%s, event.type=%s, subscription_id=%s, "
                        "customer_id=%s, event_created_at=%s, last_successful_invoice_created_at=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(sub_id),
                        safe_log_id(customer_id_for_db),
                        event_created_at,
                        last_success_created_at,
                    )
                    await mark_event_processed(event_id, claim_generation)
                    return web.Response(status=200)
                try:
                    cur.execute("""
                        UPDATE users
                        SET payment_failed = TRUE,
                            payment_failed_at = COALESCE(payment_failed_at, NOW()),
                            grace_period_end = COALESCE(
                                grace_period_end,
                                COALESCE(payment_failed_at, NOW()) + (%s * INTERVAL '1 hour')
                            ),
                            stripe_customer_id = COALESCE(%s, stripe_customer_id)
                        WHERE stripe_subscription_id = %s
                        RETURNING telegram_id, paid, expiry_date, payment_failed_at, grace_period_end
                    """, (PAYMENT_RETRY_GRACE_HOURS, customer_id_for_db, sub_id))
                except psycopg2_errors.UniqueViolation as e:
                    conn.rollback()
                    cur.close()
                    conn.close()
                    return await finalize_stripe_identity_unique_violation_webhook_response(
                        e,
                        payment_failed_requested_telegram_id,
                        customer_id_for_db,
                        sub_id,
                        "invoice.payment_failed",
                        event_id,
                        event_type,
                        claim_generation=claim_generation,
                        invoice_id=invoice_id,
                        currency=stripe_value(invoice, 'currency'),
                        billing_reason=billing_reason,
                    )
                row = cur.fetchone()
                if not row and customer_id_for_db:
                    cur.execute("""
                        SELECT telegram_id, stripe_subscription_id
                        FROM users
                        WHERE stripe_customer_id = %s
                        ORDER BY telegram_id
                    """, (customer_id_for_db,))
                    customer_matches = cur.fetchall()
                    customer_match = customer_matches[0] if len(customer_matches) == 1 else None
                    if customer_match and should_apply_failed_invoice_to_user(customer_match[1], sub_id):
                        try:
                            cur.execute("""
                                UPDATE users
                                SET payment_failed = TRUE,
                                    payment_failed_at = COALESCE(payment_failed_at, NOW()),
                                    grace_period_end = COALESCE(
                                        grace_period_end,
                                        COALESCE(payment_failed_at, NOW()) + (%s * INTERVAL '1 hour')
                                    ),
                                    stripe_subscription_id = COALESCE(%s, stripe_subscription_id),
                                    stripe_customer_id = COALESCE(%s, stripe_customer_id)
                                WHERE stripe_customer_id = %s
                                  AND (stripe_subscription_id IS NULL OR stripe_subscription_id = %s)
                                RETURNING telegram_id, paid, expiry_date, payment_failed_at, grace_period_end
                            """, (
                                PAYMENT_RETRY_GRACE_HOURS,
                                sub_id,
                                customer_id_for_db,
                                customer_id_for_db,
                                sub_id,
                            ))
                        except psycopg2_errors.UniqueViolation as e:
                            conn.rollback()
                            cur.close()
                            conn.close()
                            return await finalize_stripe_identity_unique_violation_webhook_response(
                                e,
                                payment_failed_requested_telegram_id,
                                customer_id_for_db,
                                sub_id,
                                "invoice.payment_failed",
                                event_id,
                                event_type,
                                claim_generation=claim_generation,
                                invoice_id=invoice_id,
                                currency=stripe_value(invoice, 'currency'),
                                billing_reason=billing_reason,
                            )
                        row = cur.fetchone()
                    else:
                        if customer_match:
                            cur.execute(
                                """
                                INSERT INTO access_events (
                                    telegram_id, event_type, source, stripe_event_id, stripe_subscription_id, notes
                                )
                                VALUES (%s, 'ignored_stale_invoice_payment_failed', 'invoice.payment_failed', %s, %s, %s)
                                """,
                                (
                                    customer_match[0],
                                    event_id,
                                    sub_id,
                                    "customer fallback blocked because user has different current subscription",
                                ),
                            )
                        logging.warning(
                            "PAYMENT_FAILED_CUSTOMER_FALLBACK_BLOCKED: event_id=%s, event.type=%s, "
                            "customer_id=%s, subscription_id=%s, invoice_id=%s, matches=%s",
                            safe_log_id(event_id),
                            event_type,
                            safe_log_id(customer_id_for_db),
                            safe_log_id(sub_id),
                            safe_log_id(invoice_id),
                            len(customer_matches),
                        )
                        stale_payment_failed_alert_key = (
                            f"payment_failed_customer_fallback_blocked:{safe_delivery_hash(str(event_id))}"
                        )
                        stale_payment_failed_alert = (
                            "Stripe invoice.payment_failed не применён к пользователю, потому что invoice "
                            "относится к другой или неоднозначной subscription.\n\n"
                            f"event_id: {safe_log_id(event_id)}\n"
                            f"invoice_id: {safe_log_id(invoice_id)}\n"
                            f"customer_id: {safe_log_id(customer_id_for_db)}\n"
                            f"subscription_id: {safe_log_id(sub_id)}\n"
                            f"matches: {len(customer_matches)}\n\n"
                            "payment_failed, grace period и stripe_subscription_id не изменялись."
                        )
                    if row:
                        logging.warning(
                            "PAYMENT_FAILED_USER_MATCHED_BY_CUSTOMER_ID: telegram_id=%s, customer_id=%s, "
                            "email=%s, subscription_id=%s, invoice_id=%s, event.type=%s",
                            row[0],
                            safe_log_id(customer_id),
                            safe_log_email(customer_email),
                            safe_log_id(sub_id),
                            safe_log_id(invoice_id),
                            event_type,
                        )
                if row:
                    failed_period_start, failed_period_end = invoice_line_period_datetimes(invoice)
                    failed_kind = invoice_payment_kind(billing_reason, "process_payment")
                    persist_first_purchase_recovery_invoice_failure_context(
                        cur,
                        row[0],
                        sub_id,
                        failure_code=failure_code,
                    )
                    insert_payment_event(
                        cur,
                        event_id,
                        event_type,
                        "failed",
                        telegram_id=row[0],
                        invoice_id=invoice_id,
                        stripe_customer_id=customer_id_for_db,
                        stripe_subscription_id=sub_id,
                        payment_kind=failed_kind,
                        billing_reason=billing_reason,
                        tariff_code=tariff_code_from_invoice(invoice),
                        amount_paid=stripe_value(invoice, 'amount_paid'),
                        amount_due=stripe_value(invoice, 'amount_due'),
                        currency=stripe_value(invoice, 'currency'),
                        period_start=failed_period_start,
                        period_end=failed_period_end,
                    )
                if row:
                    telegram_id, paid, expiry_date, payment_failed_at, grace_until = row
                    logging.warning(
                        "PAYMENT_FAILED_MARKED: telegram_id=%s, customer_id=%s, email=%s, "
                        "subscription_id=%s, invoice_id=%s, paid=%s, expiry_date=%s, "
                        "payment_failed_at=%s, grace_until=%s, event.type=%s, status=%s, billing_reason=%s",
                        telegram_id,
                        safe_log_id(customer_id),
                        safe_log_email(customer_email),
                        safe_log_id(sub_id),
                        safe_log_id(invoice_id),
                        paid,
                        expiry_date,
                        payment_failed_at,
                        grace_until,
                        event_type,
                        invoice_status,
                        billing_reason,
                    )
                    enqueue_failed_renewal_user_message(
                        cur,
                        telegram_id,
                        sub_id,
                        payment_failed_at,
                    )
                    enqueue_admin_payment_problem_safely(
                        cur,
                        event_id=event_id,
                        purpose="invoice_payment_failed",
                        stage="invoice_payment_failed",
                        telegram_id=telegram_id,
                        category="invoice_payment_failed",
                        stripe_code=failure_code,
                        stripe_retry=stripe_retry_text,
                        recovery_reminder=admin_recovery_reminder_status(immediate_retry_enqueued=True),
                        safe_ref=safe_admin_context_reference("invoice_payment_failed", event_id, invoice_id, sub_id),
                        note=(
                            f"next_payment_attempt: {'известен' if next_payment_attempt else 'отсутствует'}\n"
                            f"grace period: {'создан' if grace_until else 'не создан'}\n"
                            "payment_failed message: поставлено в outbox"
                        ),
                    )
                else:
                    logging.warning(
                        "PAYMENT_FAILED_UNLINKED: event_id=%s, event.type=%s, customer_id=%s, email=%s, "
                        "subscription_id=%s, invoice_id=%s, status=%s, billing_reason=%s",
                        safe_log_id(event_id),
                        event_type,
                        safe_log_id(customer_id),
                        safe_log_email(customer_email),
                        safe_log_id(sub_id),
                        safe_log_id(invoice_id),
                        invoice_status,
                        billing_reason,
                    )
                    enqueue_admin_payment_problem_safely(
                        cur,
                        event_id=event_id,
                        purpose="invoice_payment_failed_unlinked",
                        stage="invoice_payment_failed",
                        telegram_id=None,
                        category="invoice_payment_failed",
                        stripe_code=failure_code,
                        stripe_retry=stripe_retry_text,
                        recovery_reminder=admin_recovery_reminder_status(),
                        safe_ref=safe_admin_context_reference("invoice_payment_failed_unlinked", event_id, invoice_id, sub_id),
                        note="Пользователь не найден; user payment_failed message не поставлен.",
                    )
                conn.commit()
                cur.close()
                conn.close()
                if stale_payment_failed_alert:
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="stale_historical_invoice",
                        stage="invoice_payment_failed",
                        telegram_id=None,
                        category="stale_historical_invoice",
                        stripe_retry="нет",
                        recovery_reminder="не применимо",
                        safe_ref=stale_payment_failed_alert_key,
                        note=(
                            "Текущая активная подписка не изменена.\n"
                            "payment_failed пользователю не установлен.\n"
                            "Старый invoice проигнорирован безопасно."
                        ),
                    )

        # ---------- 4. ПОЛЬЗОВАТЕЛЬ ОТМЕНИЛ ПОДПИСКУ (customer.subscription.deleted) ----------
        elif event_type == 'customer.subscription.deleted':
            sub = event_object
            sub_id = stripe_object_id(stripe_value(sub, 'id'))
            customer_id = stripe_object_id(stripe_value(sub, 'customer'))
            status = stripe_value(sub, 'status')
            if sub_id:
                conn = None
                cur = None
                try:
                    conn = get_db_conn()
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE subscription_removal_events
                        SET stripe_canceled_at = COALESCE(stripe_canceled_at, NOW()),
                            status = CASE
                                WHEN status IN ('telegram_removed', 'db_finalized') THEN status
                                ELSE 'stripe_canceled'
                            END,
                            updated_at = NOW()
                        WHERE stripe_subscription_id = %s
                          AND status IN (
                              'pending', 'processing', 'stripe_canceled',
                              'telegram_failed', 'telegram_removed'
                          )
                        """,
                        (sub_id,),
                    )
                    cur.execute("""
                        UPDATE users
                        SET paid = CASE
                                WHEN expiry_date IS NOT NULL AND expiry_date > NOW() THEN paid
                                ELSE FALSE
                            END,
                            auto_renew = FALSE,
                            stripe_subscription_id = NULL
                        WHERE stripe_subscription_id = %s
                        RETURNING telegram_id, paid, expiry_date
                    """, (sub_id,))
                    row = cur.fetchone()
                    mark_stripe_link_subscription_terminal(cur, sub_id, status or "canceled")
                    conn.commit()
                except Exception:
                    if conn:
                        conn.rollback()
                    raise
                finally:
                    if cur:
                        cur.close()
                    if conn:
                        conn.close()
                logging.warning(
                    "STRIPE_SUBSCRIPTION_DELETED_MARKED: event_id=%s, event.type=%s, "
                    "telegram_id=%s, customer_id=%s, subscription_id=%s, status=%s, paid=%s, expiry_date=%s",
                    safe_log_id(event_id),
                    event_type,
                    row[0] if row else None,
                    safe_log_id(customer_id),
                    safe_log_id(sub_id),
                    status,
                    row[1] if row else None,
                    row[2] if row else None,
                )

        # ---------- 4.1. ОБНОВЛЕНИЕ ПОДПИСКИ (customer.subscription.updated) ----------
        elif event_type == 'customer.subscription.updated':
            sub = event_object
            sub_id = stripe_object_id(stripe_value(sub, 'id'))
            cancel_at_period_end = bool(stripe_value(sub, 'cancel_at_period_end'))
            status = stripe_value(sub, 'status')
            customer_id = stripe_object_id(stripe_value(sub, 'customer'))
            current_period_end = stripe_value(sub, 'current_period_end')
            trial_end = stripe_value(sub, 'trial_end')
            period_value, period_source = subscription_update_period(status, current_period_end, trial_end)
            subscription_expiry = datetime.utcfromtimestamp(period_value) if period_value else None
            if sub_id:
                conn = None
                cur = None
                old_auto_renew = None
                try:
                    conn = get_db_conn()
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT auto_renew,
                               last_successful_invoice_created_at,
                               last_subscription_state_event_created_at,
                               telegram_id
                        FROM users
                        WHERE stripe_subscription_id = %s
                        """,
                        (sub_id,)
                    )
                    old_auto_row = cur.fetchone()
                    conn.commit()
                except Exception:
                    if conn:
                        conn.rollback()
                    raise
                finally:
                    if cur:
                        cur.close()
                    if conn:
                        conn.close()
                    cur = None
                    conn = None
                last_success_created_at = None
                last_subscription_state_event_created_at = None
                if old_auto_row:
                    old_auto_renew = old_auto_row[0]
                    last_success_created_at = old_auto_row[1]
                    last_subscription_state_event_created_at = old_auto_row[2]
                if status in ("past_due", "unpaid"):
                    negative_update_skipped = False
                    if event_created_at and last_success_created_at and event_created_at <= last_success_created_at:
                        try:
                            live_subscription = await asyncio.to_thread(
                                stripe.Subscription.retrieve,
                                sub_id,
                                expand=["latest_invoice"],
                            )
                            live_status = stripe_value(live_subscription, "status")
                            latest_invoice_status = stripe_value(live_subscription, "latest_invoice", "status")
                            if live_subscription_is_paid(live_status, latest_invoice_status) or latest_invoice_status == "paid":
                                logging.warning(
                                    "SUBSCRIPTION_UPDATED_NEGATIVE_STALE_IGNORED: event_id=%s, event.type=%s, "
                                    "customer_id=%s, subscription_id=%s, status=%s, live_status=%s, "
                                    "latest_invoice_status=%s, event_created_at=%s, last_successful_invoice_created_at=%s",
                                    safe_log_id(event_id),
                                    event_type,
                                    safe_log_id(customer_id),
                                    safe_log_id(sub_id),
                                    status,
                                    live_status,
                                    latest_invoice_status,
                                    event_created_at,
                                    last_success_created_at,
                                )
                                negative_update_skipped = True
                        except Exception as e:
                            logging.error(
                                "SUBSCRIPTION_UPDATED_STALE_LIVE_CHECK_FAILED: event_id=%s, subscription_id=%s, error=%s",
                                safe_log_id(event_id),
                                safe_log_id(sub_id),
                                str(e),
                                exc_info=True,
                            )
                    conn = get_db_conn()
                    cur = conn.cursor()
                    subscription_update_requested_telegram_id = None
                    try:
                        subscription_update_requested_telegram_id = assert_existing_subscription_identity_available(
                            cur,
                            sub_id,
                            customer_id=customer_id,
                            source="customer.subscription.updated",
                        )
                    except StripeIdentityConflictError as conflict:
                        conn.rollback()
                        cur.close()
                        conn.close()
                        return await finalize_stripe_identity_conflict_response(
                            conflict,
                            event_id,
                            event_type,
                            claim_generation=claim_generation,
                        )
                    if negative_update_skipped:
                        try:
                            cur.execute("""
                                UPDATE users
                                SET last_subscription_state_event_created_at = GREATEST(
                                        COALESCE(last_subscription_state_event_created_at, %s),
                                        %s
                                    ),
                                    stripe_customer_id = COALESCE(%s, stripe_customer_id)
                                WHERE stripe_subscription_id = %s
                            """, (event_created_at, event_created_at, customer_id, sub_id))
                        except psycopg2_errors.UniqueViolation as e:
                            conn.rollback()
                            cur.close()
                            conn.close()
                            return await finalize_stripe_identity_unique_violation_webhook_response(
                                e,
                                subscription_update_requested_telegram_id,
                                customer_id,
                                sub_id,
                                "customer.subscription.updated",
                                event_id,
                                event_type,
                                claim_generation=claim_generation,
                            )
                        row = None
                    else:
                        try:
                            cur.execute("""
                                UPDATE users
                                SET auto_renew = %s,
                                    payment_failed = TRUE,
                                    payment_failed_at = COALESCE(payment_failed_at, NOW()),
                                    grace_period_end = COALESCE(
                                        grace_period_end,
                                        COALESCE(payment_failed_at, NOW()) + (%s * INTERVAL '1 hour')
                                    ),
                                    stripe_customer_id = COALESCE(%s, stripe_customer_id),
                                    last_subscription_state_event_created_at = GREATEST(
                                        COALESCE(last_subscription_state_event_created_at, %s),
                                        %s
                                    )
                                WHERE stripe_subscription_id = %s
                                  AND (
                                        %s IS NULL
                                        OR last_subscription_state_event_created_at IS NULL
                                        OR %s >= last_subscription_state_event_created_at
                                  )
                                RETURNING telegram_id, paid, expiry_date, payment_failed_at, grace_period_end
                            """, (
                                not cancel_at_period_end,
                                PAYMENT_RETRY_GRACE_HOURS,
                                customer_id,
                                event_created_at,
                                event_created_at,
                                sub_id,
                                event_created_at,
                                event_created_at,
                            ))
                        except psycopg2_errors.UniqueViolation as e:
                            conn.rollback()
                            cur.close()
                            conn.close()
                            return await finalize_stripe_identity_unique_violation_webhook_response(
                                e,
                                subscription_update_requested_telegram_id,
                                customer_id,
                                sub_id,
                                "customer.subscription.updated",
                                event_id,
                                event_type,
                                claim_generation=claim_generation,
                            )
                        row = cur.fetchone()
                    logging.warning(
                        "SUBSCRIPTION_RETRY_STATE_MARKED: event_id=%s, event.type=%s, telegram_id=%s, "
                        "customer_id=%s, subscription_id=%s, status=%s, paid=%s, expiry_date=%s, "
                        "payment_failed_at=%s, grace_until=%s",
                        safe_log_id(event_id),
                        event_type,
                        row[0] if row else None,
                        safe_log_id(customer_id),
                        safe_log_id(sub_id),
                        status,
                        row[1] if row else None,
                        row[2] if row else None,
                        row[3] if row else None,
                        row[4] if row else None,
                    )
                    if row:
                        enqueue_failed_renewal_user_message(
                            cur,
                            row[0],
                            sub_id,
                            row[3],
                        )
                        try:
                            upsert_stripe_link(
                                cur,
                                row[0],
                                stripe_customer_id=customer_id,
                                stripe_subscription_id=sub_id,
                                status=status,
                                current_period_end=current_period_end,
                                is_active=False,
                                source="customer.subscription.updated",
                            )
                        except psycopg2_errors.UniqueViolation as e:
                            conn.rollback()
                            cur.close()
                            conn.close()
                            return await finalize_stripe_identity_unique_violation_webhook_response(
                                e,
                                subscription_update_requested_telegram_id,
                                customer_id,
                                sub_id,
                                "customer.subscription.updated",
                                event_id,
                                event_type,
                                claim_generation=claim_generation,
                            )
                elif status in ("active", "trialing"):
                    conn = get_db_conn()
                    cur = conn.cursor()
                    subscription_update_requested_telegram_id = None
                    try:
                        subscription_update_requested_telegram_id = assert_existing_subscription_identity_available(
                            cur,
                            sub_id,
                            customer_id=customer_id,
                            source="customer.subscription.updated",
                        )
                    except StripeIdentityConflictError as conflict:
                        conn.rollback()
                        cur.close()
                        conn.close()
                        return await finalize_stripe_identity_conflict_response(
                            conflict,
                            event_id,
                            event_type,
                            claim_generation=claim_generation,
                        )
                    if subscription_expiry:
                        try:
                            cur.execute("""
                                UPDATE users
                                SET expiry_date = CASE
                                        WHEN users.expiry_date IS NOT NULL AND users.expiry_date >= %s THEN users.expiry_date
                                        ELSE %s
                                    END,
                                    reminder_sent = FALSE,
                                    auto_renew = %s,
                                    stripe_customer_id = COALESCE(%s, stripe_customer_id),
                                    last_subscription_state_event_created_at = GREATEST(
                                        COALESCE(last_subscription_state_event_created_at, %s),
                                        %s
                                    )
                                WHERE stripe_subscription_id = %s
                                  AND (
                                        %s IS NULL
                                        OR last_subscription_state_event_created_at IS NULL
                                        OR %s >= last_subscription_state_event_created_at
                                  )
                                RETURNING telegram_id, paid, expiry_date
                            """, (
                                subscription_expiry,
                                subscription_expiry,
                                not cancel_at_period_end,
                                customer_id,
                                event_created_at,
                                event_created_at,
                                sub_id,
                                event_created_at,
                                event_created_at,
                            ))
                        except psycopg2_errors.UniqueViolation as e:
                            conn.rollback()
                            cur.close()
                            conn.close()
                            return await finalize_stripe_identity_unique_violation_webhook_response(
                                e,
                                subscription_update_requested_telegram_id,
                                customer_id,
                                sub_id,
                                "customer.subscription.updated",
                                event_id,
                                event_type,
                                claim_generation=claim_generation,
                            )
                    else:
                        try:
                            cur.execute("""
                                UPDATE users
                                SET auto_renew = %s,
                                    stripe_customer_id = COALESCE(%s, stripe_customer_id),
                                    last_subscription_state_event_created_at = GREATEST(
                                        COALESCE(last_subscription_state_event_created_at, %s),
                                        %s
                                    )
                                WHERE stripe_subscription_id = %s
                                  AND (
                                        %s IS NULL
                                        OR last_subscription_state_event_created_at IS NULL
                                        OR %s >= last_subscription_state_event_created_at
                                  )
                                RETURNING telegram_id, paid, expiry_date
                            """, (
                                not cancel_at_period_end,
                                customer_id,
                                event_created_at,
                                event_created_at,
                                sub_id,
                                event_created_at,
                                event_created_at,
                            ))
                        except psycopg2_errors.UniqueViolation as e:
                            conn.rollback()
                            cur.close()
                            conn.close()
                            return await finalize_stripe_identity_unique_violation_webhook_response(
                                e,
                                subscription_update_requested_telegram_id,
                                customer_id,
                                sub_id,
                                "customer.subscription.updated",
                                event_id,
                                event_type,
                                claim_generation=claim_generation,
                            )
                    row = cur.fetchone()
                    if row:
                        try:
                            upsert_stripe_link(
                                cur,
                                row[0],
                                stripe_customer_id=customer_id,
                                stripe_subscription_id=sub_id,
                                status=status,
                                current_period_end=period_value,
                                is_active=True,
                                source="customer.subscription.updated",
                            )
                        except psycopg2_errors.UniqueViolation as e:
                            conn.rollback()
                            cur.close()
                            conn.close()
                            return await finalize_stripe_identity_unique_violation_webhook_response(
                                e,
                                subscription_update_requested_telegram_id,
                                customer_id,
                                sub_id,
                                "customer.subscription.updated",
                                event_id,
                                event_type,
                                claim_generation=claim_generation,
                            )
                        if subscription_expiry:
                            logging.info(
                                "SUBSCRIPTION_PERIOD_SYNCED: event_id=%s, event.type=%s, telegram_id=%s, "
                                "customer_id=%s, subscription_id=%s, status=%s, cancel_at_period_end=%s, "
                                "paid=%s, expiry_date=%s, period_value=%s, period_source=%s",
                                safe_log_id(event_id),
                                event_type,
                                row[0],
                                safe_log_id(customer_id),
                                safe_log_id(sub_id),
                                status,
                                cancel_at_period_end,
                                row[1],
                                row[2],
                                period_value,
                                period_source,
                            )
                        else:
                            logging.warning(
                                "SUBSCRIPTION_ACTIVE_STATE_NO_PERIOD_PRESERVED: event_id=%s, event.type=%s, "
                                "telegram_id=%s, customer_id=%s, subscription_id=%s, status=%s, "
                                "cancel_at_period_end=%s, paid=%s, expiry_date=%s",
                                safe_log_id(event_id),
                                event_type,
                                row[0],
                                safe_log_id(customer_id),
                                safe_log_id(sub_id),
                                status,
                                cancel_at_period_end,
                                row[1],
                                row[2],
                            )
                        if status == "trialing" and trial_end and subscription_expiry:
                            logging.info(
                                "ACCESS_SYNCED_FROM_STRIPE_TRIAL: event_id=%s, event.type=%s, telegram_id=%s, "
                                "customer_id=%s, subscription_id=%s, trial_end=%s, expiry_date=%s",
                                safe_log_id(event_id),
                                event_type,
                                row[0],
                                safe_log_id(customer_id),
                                safe_log_id(sub_id),
                                trial_end,
                                row[2],
                            )
                    else:
                        logging.warning(
                            "SUBSCRIPTION_ACTIVE_STATE_UNLINKED: event_id=%s, event.type=%s, "
                            "customer_id=%s, subscription_id=%s, status=%s, current_period_end=%s",
                            safe_log_id(event_id),
                            event_type,
                            safe_log_id(customer_id),
                            safe_log_id(sub_id),
                            status,
                            current_period_end,
                        )
                        await enqueue_admin_payment_problem_now(
                            event_id=event_id,
                            purpose="subscription_active_state_unlinked",
                            stage="webhook",
                            category="missing_subscription_identity",
                            stripe_retry="нет",
                            recovery_reminder="нет",
                            safe_ref=safe_admin_context_reference(
                                "subscription_active_state_unlinked",
                                event_id,
                                customer_id,
                                sub_id,
                                status,
                            ),
                            note=(
                                "Stripe subscription active/trialing, но пользователь не найден в БД. "
                                "Нужно вручную связать Stripe с Telegram ID через /link_stripe_user."
                            ),
                        )
                else:
                    conn = get_db_conn()
                    cur = conn.cursor()
                    subscription_update_requested_telegram_id = None
                    try:
                        subscription_update_requested_telegram_id = assert_existing_subscription_identity_available(
                            cur,
                            sub_id,
                            customer_id=customer_id,
                            source="customer.subscription.updated",
                        )
                    except StripeIdentityConflictError as conflict:
                        conn.rollback()
                        cur.close()
                        conn.close()
                        return await finalize_stripe_identity_conflict_response(
                            conflict,
                            event_id,
                            event_type,
                            claim_generation=claim_generation,
                        )
                    try:
                        cur.execute("""
                            UPDATE users
                            SET auto_renew = %s,
                                stripe_customer_id = COALESCE(%s, stripe_customer_id)
                            WHERE stripe_subscription_id = %s
                            RETURNING telegram_id, paid, expiry_date
                        """, (not cancel_at_period_end, customer_id, sub_id))
                    except psycopg2_errors.UniqueViolation as e:
                        conn.rollback()
                        cur.close()
                        conn.close()
                        return await finalize_stripe_identity_unique_violation_webhook_response(
                            e,
                            subscription_update_requested_telegram_id,
                            customer_id,
                            sub_id,
                            "customer.subscription.updated",
                            event_id,
                            event_type,
                            claim_generation=claim_generation,
                        )
                    row = cur.fetchone()
                    logging.info(
                        "SUBSCRIPTION_UPDATED: event_id=%s, event.type=%s, telegram_id=%s, "
                        "customer_id=%s, subscription_id=%s, status=%s, cancel_at_period_end=%s, "
                        "paid=%s, expiry_date=%s",
                        safe_log_id(event_id),
                        event_type,
                        row[0] if row else None,
                        safe_log_id(customer_id),
                        safe_log_id(sub_id),
                        status,
                        cancel_at_period_end,
                        row[1] if row else None,
                        row[2] if row else None,
                    )
                    if row:
                        try:
                            upsert_stripe_link(
                                cur,
                                row[0],
                                stripe_customer_id=customer_id,
                                stripe_subscription_id=sub_id,
                                status=status,
                                current_period_end=current_period_end,
                                is_active=stripe_link_active_for_status(status),
                                source="customer.subscription.updated",
                            )
                        except psycopg2_errors.UniqueViolation as e:
                            conn.rollback()
                            cur.close()
                            conn.close()
                            return await finalize_stripe_identity_unique_violation_webhook_response(
                                e,
                                subscription_update_requested_telegram_id,
                                customer_id,
                                sub_id,
                                "customer.subscription.updated",
                                event_id,
                                event_type,
                                claim_generation=claim_generation,
                            )
                    if is_terminal_subscription_status(status):
                        mark_stripe_link_subscription_terminal(cur, sub_id, status)
                if row and old_auto_renew is True and cancel_at_period_end:
                    cur.execute("""
                        INSERT INTO access_events (
                            telegram_id,
                            event_type,
                            source,
                            stripe_event_id,
                            stripe_subscription_id,
                            notes
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        row[0],
                        "subscription_auto_renew_disabled",
                        "customer.subscription.updated",
                        event_id,
                        sub_id,
                        f"status={status}; cancel_at_period_end=True",
                    ))
                try:
                    conn.commit()
                except Exception:
                    if conn:
                        conn.rollback()
                    raise
                finally:
                    if cur:
                        cur.close()
                    if conn:
                        conn.close()

        elif event_type == 'checkout.session.async_payment_succeeded':
            session = event_object
            gift_metadata = stripe_value(session, "metadata") or {}
            if stripe_value(gift_metadata, "payment_kind") == GIFT_PAYMENT_KIND:
                gift_id = stripe_value(gift_metadata, "gift_id")
                session_id = stripe_value(session, "id")
                try:
                    proof_session, line_item, price = await asyncio.to_thread(fetch_gift_checkout_payment_proof, session_id)
                except Exception as e:
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="gift_async_payment_proof_failed",
                        stage="checkout_async_payment_succeeded",
                        telegram_id=stripe_value(gift_metadata, "purchaser_telegram_id"),
                        category="webhook_processing_failed",
                        exception=e,
                        stripe_retry="да",
                        recovery_reminder="не применимо",
                        safe_ref=safe_admin_context_reference("gift_async_success_proof", event_id, gift_id),
                        note="Gift async success payment proof could not be verified. Stripe may retry.",
                        severity="CRITICAL",
                    )
                    await release_event_processing(event_id, claim_generation)
                    return web.Response(status=500)
                conn = get_db_conn()
                cur = conn.cursor()
                gift_response = web.Response(status=200)
                gift_mark_processed = False
                gift_release_event = False
                gift_admin_problem = None
                try:
                    cur.execute("""
                        SELECT *
                        FROM gift_access_grants
                        WHERE id = %s
                          AND stripe_session_id = %s
                        FOR UPDATE
                    """, (gift_id, session_id))
                    gift_row = gift_row_dict(cur, cur.fetchone())
                    if not gift_row:
                        raise ValueError("gift_not_found")
                    mark_gift_paid_and_enqueue(cur, event_id, event_type, proof_session, line_item, price, gift_row)
                    conn.commit()
                    gift_mark_processed = True
                except Exception as e:
                    conn.rollback()
                    gift_admin_problem = {
                        "event_id": event_id,
                        "purpose": "gift_async_payment_succeeded_failed",
                        "stage": "checkout_async_payment_succeeded",
                        "telegram_id": stripe_value(gift_metadata, "purchaser_telegram_id"),
                        "category": "webhook_processing_failed",
                        "exception": None,
                        "stripe_retry": "да",
                        "recovery_reminder": "не применимо",
                        "safe_ref": safe_admin_context_reference("gift_async_success", event_id, gift_id),
                        "note": "Gift async success was not applied. Stripe may retry.",
                        "severity": "CRITICAL",
                    }
                    gift_release_event = True
                    gift_response = web.Response(status=500)
                finally:
                    cur.close()
                    conn.close()
                if gift_mark_processed:
                    await mark_event_processed(event_id, claim_generation)
                if gift_admin_problem:
                    await enqueue_admin_payment_problem_now(**gift_admin_problem)
                if gift_release_event:
                    await release_event_processing(event_id, claim_generation)
                return gift_response

            user_id = resolve_checkout_telegram_id(session)
            session_id = stripe_value(session, 'id')
            metadata_raw = stripe_value(session, 'metadata') or getattr(session, 'metadata', None)
            metadata_keys = list(metadata_raw.keys()) if isinstance(metadata_raw, dict) else []
            if not user_id:
                logging.error(
                    "CHECKOUT_ASYNC_SUCCESS_INVALID_IDENTITY: event_id=%s, session_id=%s, metadata_keys=%s. Access not granted.",
                    safe_log_id(event_id),
                    safe_log_id(session_id),
                    metadata_keys,
                )
                await enqueue_admin_payment_problem_now(
                    event_id=event_id,
                    purpose="checkout_async_success_invalid_identity",
                    stage="checkout_async_payment_succeeded",
                    telegram_id=None,
                    category="missing_subscription_identity",
                    stripe_retry="да",
                    recovery_reminder="неизвестно",
                    safe_ref=safe_admin_context_reference("checkout_async_success_invalid_identity", event_id, session_id),
                    note="client_reference_id/metadata.telegram_id missing, invalid, or conflicting\naccess_granted: false",
                    severity="CRITICAL",
                )
                await release_event_processing(event_id, claim_generation)
                return web.Response(status=500, text="Invalid checkout Telegram identity")

            sub_id = stripe_object_id(stripe_value(session, 'subscription'))
            customer_id = stripe_object_id(stripe_value(session, 'customer'))
            customer_email = stripe_value(session, 'customer_details', 'email') or stripe_value(session, 'customer_email')
            checkout_mode = stripe_value(session, 'mode') or getattr(session, 'mode', None)
            payment_decision = checkout_payment_access_decision(
                checkout_mode,
                stripe_value(session, "payment_status"),
                amount_total=stripe_value(session, "amount_total"),
                has_subscription=bool(sub_id),
            )
            if payment_decision["action"] == "retrieve" and session_id:
                try:
                    session = await asyncio.to_thread(
                        stripe.checkout.Session.retrieve,
                        session_id,
                        expand=["subscription", "customer"],
                    )
                    metadata_raw = stripe_value(session, 'metadata') or getattr(session, 'metadata', None)
                    sub_id = stripe_object_id(stripe_value(session, "subscription"))
                    customer_id = customer_id or stripe_object_id(stripe_value(session, "customer"))
                    customer_email = (
                        customer_email
                        or stripe_value(session, "customer_details", "email")
                        or stripe_value(session, "customer_email")
                    )
                    checkout_mode = stripe_value(session, "mode") or checkout_mode
                    payment_decision = checkout_payment_access_decision(
                        checkout_mode,
                        stripe_value(session, "payment_status"),
                        amount_total=stripe_value(session, "amount_total"),
                        has_subscription=bool(sub_id),
                    )
                except Exception as e:
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="checkout_async_success_retrieve_failed",
                        stage="checkout_async_payment_succeeded",
                        telegram_id=user_id,
                        category="webhook_processing_failed",
                        exception=e,
                        stripe_retry="да",
                        recovery_reminder="неизвестно",
                        safe_ref=safe_admin_error_reference("checkout_async_success_retrieve", e),
                        note="Не удалось повторно получить Checkout Session. Доступ не выдан.",
                    )
                    await release_event_processing(event_id, claim_generation)
                    return web.Response(status=500)

            days_to_add = parse_checkout_days(metadata_raw)
            if days_to_add is None:
                await enqueue_admin_payment_problem_now(
                    event_id=event_id,
                    purpose="checkout_async_success_invalid_metadata",
                    stage="checkout_async_payment_succeeded",
                    telegram_id=user_id,
                    category="invalid_checkout_metadata",
                    stripe_retry="да",
                    recovery_reminder="неизвестно",
                    safe_ref=safe_admin_context_reference("checkout_async_success_invalid_days", event_id, session_id, user_id),
                    note="metadata.days missing or invalid\naccess_granted: false",
                    severity="CRITICAL",
                )
                await release_event_processing(event_id, claim_generation)
                return web.Response(status=500, text="Invalid checkout metadata.days")

            checkout_action = checkout_completion_action(checkout_mode, sub_id)
            if payment_decision["action"] == "link_only" or checkout_action == "link_only":
                logging.info(
                    "CHECKOUT_ASYNC_SUCCESS_SUBSCRIPTION_LINK_ONLY: event_id=%s, session_id=%s, user_id=%s",
                    safe_log_id(event_id),
                    safe_log_id(session_id),
                    user_id,
                )
                await mark_event_processed(event_id, claim_generation)
                return web.Response(status=200)

            if payment_decision["action"] != "grant_access":
                conn = get_db_conn()
                cur = conn.cursor()
                try:
                    mark_checkout_terminal(cur, session_id, "manual_review_required", error_text=payment_decision["reason"])
                    enqueue_admin_payment_problem_safely(
                        cur,
                        event_id=event_id,
                        purpose="checkout_async_success_payment_status_review_required",
                        stage="checkout_async_payment_succeeded",
                        telegram_id=user_id,
                        category="invalid_checkout_metadata",
                        stripe_retry="нет",
                        recovery_reminder="не применимо",
                        safe_ref=safe_admin_context_reference(
                            "checkout_async_success_payment_status_review",
                            event_id,
                            session_id,
                            user_id,
                            payment_decision["reason"],
                        ),
                        note=(
                            f"Доступ не выдан. payment_status={stripe_value(session, 'payment_status') or 'нет'}; "
                            f"reason={payment_decision['reason']}"
                        ),
                        severity="CRITICAL",
                    )
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    await enqueue_admin_payment_problem_now(
                        event_id=event_id,
                        purpose="checkout_async_success_gate_failed",
                        stage="checkout_async_payment_succeeded",
                        telegram_id=user_id,
                        category="webhook_processing_failed",
                        exception=e,
                        stripe_retry="да",
                        recovery_reminder="неизвестно",
                        safe_ref=safe_admin_error_reference("checkout_async_success_gate", e),
                        note="Payment status gate failed before access grant. Access not granted.",
                    )
                    await release_event_processing(event_id, claim_generation)
                    return web.Response(status=500)
                finally:
                    cur.close()
                    conn.close()
                await mark_event_processed(event_id, claim_generation)
                return web.Response(status=200)

            try:
                checkout_apply_result = await apply_paid_checkout_access(
                    session,
                    user_id,
                    days_to_add,
                    sub_id,
                    customer_id,
                    customer_email,
                    checkout_action,
                    "checkout.session.async_payment_succeeded",
                )
                if checkout_apply_result.get("identity_conflict_response"):
                    return checkout_apply_result["identity_conflict_response"]
            except Exception as e:
                logging.exception(
                    "Ошибка обработки checkout.session.async_payment_succeeded: event_id=%s, "
                    "user_id=%s, session_id=%s: %s",
                    safe_log_id(event_id),
                    user_id,
                    safe_log_id(session_id),
                    e,
                )
                await enqueue_admin_payment_problem_now(
                    event_id=event_id,
                    purpose="checkout_async_success_processing_failed",
                    stage="checkout_async_payment_succeeded",
                    telegram_id=user_id,
                    category="webhook_processing_failed",
                    exception=e,
                    stripe_retry="да",
                    recovery_reminder="неизвестно",
                    safe_ref=safe_admin_error_reference("checkout_async_success_processing", e),
                    note="Операция не выполнена. Webhook вернул 500.",
                )
                await release_event_processing(event_id, claim_generation)
                return web.Response(status=500)

            await mark_event_processed(event_id, claim_generation)
            return web.Response(status=200)

        elif event_type in ("charge.refunded", "refund.created", "refund.updated"):
            refund_object = event_object
            payment_intent = gift_refund_amount_from_event(event_type, refund_object)[0]
            if payment_intent:
                conn = get_db_conn()
                cur = conn.cursor()
                gift_response = web.Response(status=200)
                gift_mark_processed = False
                gift_release_event = False
                gift_admin_problem = None
                try:
                    cur.execute("""
                        SELECT *
                        FROM gift_access_grants
                        WHERE stripe_payment_intent_id = %s
                        FOR UPDATE
                    """, (payment_intent,))
                    gift_row = gift_row_dict(cur, cur.fetchone())
                    if gift_row:
                        apply_gift_refund_event(cur, event_id, event_type, refund_object, gift_row)
                        conn.commit()
                        gift_mark_processed = True
                    else:
                        conn.rollback()
                except Exception as e:
                    conn.rollback()
                    gift_admin_problem = {
                        "event_id": event_id,
                        "purpose": "gift_refund_processing_failed",
                        "stage": "refund_webhook",
                        "category": "webhook_processing_failed",
                        "exception": None,
                        "stripe_retry": "да",
                        "recovery_reminder": "не применимо",
                        "safe_ref": safe_admin_context_reference("gift_refund", event_id, payment_intent),
                        "note": "Gift refund was not applied. Stripe may retry.",
                        "severity": "CRITICAL",
                    }
                    gift_release_event = True
                    gift_response = web.Response(status=500)
                finally:
                    cur.close()
                    conn.close()
                if gift_mark_processed:
                    await mark_event_processed(event_id, claim_generation)
                if gift_admin_problem:
                    await enqueue_admin_payment_problem_now(**gift_admin_problem)
                if gift_release_event:
                    await release_event_processing(event_id, claim_generation)
                if gift_mark_processed or gift_release_event:
                    return gift_response
            proof = subscription_refund_proof_from_event(event_type, refund_object)
            try:
                proof = await enrich_subscription_refund_proof(proof)
            except Exception as e:
                await enqueue_admin_payment_problem_now(
                    event_id=event_id,
                    purpose="subscription_refund_proof_retrieve_failed",
                    stage="refund_webhook",
                    category="stripe_api_unavailable",
                    exception=e,
                    stripe_retry="да",
                    recovery_reminder="не применимо",
                    safe_ref=safe_admin_error_reference("subscription_refund_proof", e),
                    note="Refund proof could not be completed. Access unchanged; Stripe may retry.",
                    severity="CRITICAL",
                )
                await release_event_processing(event_id, claim_generation)
                return web.Response(status=500)
            refund_conn = get_db_conn()
            refund_cur = refund_conn.cursor()
            try:
                result = apply_subscription_refund_reconciliation(refund_cur, event_id, proof, event_type=event_type)
                refund_conn.commit()
                logging.info(
                    "SUBSCRIPTION_REFUND_RECONCILED: event_id=%s, event.type=%s, result=%s, reason=%s, telegram_id=%s",
                    safe_log_id(event_id),
                    event_type,
                    result.get("result"),
                    result.get("reason"),
                    result.get("telegram_id"),
                )
            except Exception as e:
                refund_conn.rollback()
                await enqueue_admin_payment_problem_now(
                    event_id=event_id,
                    purpose="subscription_refund_reconciliation_failed",
                    stage="refund_webhook",
                    category="webhook_processing_failed",
                    exception=e,
                    stripe_retry="да",
                    recovery_reminder="не применимо",
                    safe_ref=safe_admin_error_reference("subscription_refund_reconciliation", e),
                    note="Refund reconciliation audit/revoke did not commit. Access may be unchanged; Stripe should retry.",
                    severity="CRITICAL",
                )
                await release_event_processing(event_id, claim_generation)
                return web.Response(status=500)
            finally:
                refund_cur.close()
                refund_conn.close()
            await mark_event_processed(event_id, claim_generation)
            return web.Response(status=200)

        # ---------- 5. СЕССИЯ ОПЛАТЫ ИСТЕКЛА ИЛИ НЕ УДАЛАСЬ ----------
        elif event_type in ('checkout.session.expired', 'checkout.session.async_payment_failed'):
            session = event_object
            gift_metadata = stripe_value(session, "metadata") or {}
            if stripe_value(gift_metadata, "payment_kind") == GIFT_PAYMENT_KIND:
                gift_id = stripe_value(gift_metadata, "gift_id")
                session_id = stripe_value(session, "id")
                conn = get_db_conn()
                cur = conn.cursor()
                gift_mark_processed = False
                gift_release_event = False
                gift_admin_problem = None
                gift_response = web.Response(status=200)
                try:
                    cur.execute("""
                        SELECT *
                        FROM gift_access_grants
                        WHERE id = %s
                          AND stripe_session_id = %s
                        FOR UPDATE
                    """, (gift_id, session_id))
                    gift_row = gift_row_dict(cur, cur.fetchone())
                    if gift_row:
                        terminal_status = "cancelled"
                        cur.execute("""
                            UPDATE gift_access_grants
                            SET status = %s,
                                cancelled_at = CASE WHEN %s = 'cancelled' THEN NOW() ELSE cancelled_at END,
                                last_error = %s,
                                last_error_category = %s,
                                updated_at = NOW()
                            WHERE id = %s
                              AND status IN ('checkout_pending', 'checkout_open', 'payment_pending')
                            RETURNING *
                        """, (terminal_status, terminal_status, event_type, event_type, gift_row["id"]))
                        updated_gift = gift_row_dict(cur, cur.fetchone())
                        if updated_gift:
                            record_gift_event(cur, updated_gift, event_type, updated_gift["purchaser_telegram_id"], source="stripe_webhook")
                            buyer_purpose = "gift_checkout_expired_buyer" if event_type == "checkout.session.expired" else "gift_checkout_failed_buyer"
                            enqueue_gift_text_delivery(
                                cur,
                                updated_gift["public_reference"],
                                updated_gift["purchaser_telegram_id"],
                                buyer_purpose,
                                "Похоже, оформление подарка не завершилось. Вы можете создать подарок заново из меню.",
                            )
                            if event_type == "checkout.session.expired":
                                logging.info(
                                    "ADMIN_NOTIFICATION_SUPPRESSED: category=gift_checkout_expired, "
                                    "result=cancelled, gift_ref=%s, purchaser_telegram_id=%s",
                                    safe_log_id(updated_gift["public_reference"]),
                                    updated_gift["purchaser_telegram_id"],
                                )
                            else:
                                enqueue_gift_admin_delivery(
                                    cur,
                                    updated_gift["public_reference"],
                                    "gift_admin_problem",
                                    gift_admin_text("⚠️ Gift checkout did not complete", updated_gift, extra=f"event: {event_type}"),
                                    severity="WARNING",
                                )
                        else:
                            record_gift_event(cur, gift_row, f"{event_type}_ignored", gift_row["purchaser_telegram_id"], source="stripe_webhook")
                    conn.commit()
                    gift_mark_processed = True
                except Exception as e:
                    conn.rollback()
                    gift_admin_problem = {
                        "event_id": event_id,
                        "purpose": "gift_checkout_terminal_failed",
                        "stage": "checkout_terminal",
                        "telegram_id": stripe_value(gift_metadata, "purchaser_telegram_id"),
                        "category": "webhook_processing_failed",
                        "exception": None,
                        "stripe_retry": "да",
                        "recovery_reminder": "не применимо",
                        "safe_ref": safe_admin_context_reference("gift_checkout_terminal", event_id, gift_id),
                        "note": "Gift checkout terminal event was not applied. Stripe may retry.",
                        "severity": "CRITICAL",
                    }
                    gift_release_event = True
                    gift_response = web.Response(status=500)
                finally:
                    cur.close()
                    conn.close()
                if gift_mark_processed:
                    await mark_event_processed(event_id, claim_generation)
                if gift_admin_problem:
                    await enqueue_admin_payment_problem_now(**gift_admin_problem)
                if gift_release_event:
                    await release_event_processing(event_id, claim_generation)
                return gift_response
            user_id = getattr(session, 'client_reference_id', None)
            session_id = stripe_value(session, 'id')
            conn = get_db_conn()
            cur = conn.cursor()
            try:
                mark_checkout_terminal(
                    cur,
                    session_id,
                    "expired" if event_type == 'checkout.session.expired' else "failed",
                    error_text=event_type,
                )
                if user_id:
                    enqueue_stripe_user_message(
                        cur,
                        event_id,
                        user_id,
                        "checkout_expired" if event_type == 'checkout.session.expired' else "checkout_async_payment_failed",
                        "Похоже, оформление доступа не завершилось.\n\n"
                        "Вы можете выбрать тариф еще раз или написать администратору, если нужна помощь.",
                        keyboard_kind="retry_payment",
                    )
                if event_type == 'checkout.session.expired':
                    logging.info(
                        "ADMIN_NOTIFICATION_SUPPRESSED: category=checkout_expired, "
                        "result=terminal_state_recorded, event_ref=%s, session_ref=%s, "
                        "telegram_id=%s, retry_message_enqueued=%s",
                        safe_log_id(event_id),
                        safe_log_id(session_id),
                        user_id or "unknown",
                        bool(user_id),
                    )
                else:
                    enqueue_admin_payment_problem_safely(
                        cur,
                        event_id=event_id,
                        purpose="checkout_async_payment_failed",
                        stage="checkout_async_payment_failed",
                        telegram_id=user_id,
                        category="checkout_async_payment_failed",
                        stripe_retry="нет",
                        recovery_reminder=admin_recovery_reminder_status(
                            immediate_retry_enqueued=bool(user_id),
                            scheduler_will_check=bool(user_id),
                        ),
                        safe_ref=safe_admin_context_reference(event_type, event_id, session_id, user_id),
                        note="Немедленное сообщение retry_payment поставлено в outbox." if user_id else "telegram_id не определён.",
                    )
                conn.commit()
            finally:
                cur.close()
                conn.close()

            if user_id:
                clear_cached_checkout_sessions_for_user(user_id)

        await mark_event_processed(event_id, claim_generation)
        return web.Response(status=200)
    except Exception as e:
        await release_event_processing(event_id, claim_generation)
        logging.exception(
            "STRIPE_WEBHOOK_UNHANDLED_EXCEPTION: event_id=%s, event.type=%s, error=%s",
            safe_log_id(event_id),
            event_type,
            e,
        )
        await enqueue_admin_payment_problem_now(
            event_id=event_id,
            purpose="webhook_processing_failed",
            stage="webhook",
            telegram_id=None,
            category="webhook_processing_failed",
            exception=e,
            stripe_retry="да",
            recovery_reminder="неизвестно",
            safe_ref=safe_admin_error_reference("stripe_webhook", e),
            note="Webhook вернул 500, Stripe может повторить событие.",
        )
        return web.Response(status=500)

@router.message(Command('test_auto_lesson'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def test_auto_lesson_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()

    if len(args) != 1:
        await message.reply("⚠️ Использование: /test_auto_lesson <telegram_id>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO users (telegram_id, paid)
            VALUES (%s, FALSE)
            ON CONFLICT (telegram_id) DO NOTHING
        """, (target_user_id,))

        was_sent = await send_auto_free_lesson(target_user_id, cur)
        conn.commit()

        if was_sent:
            await message.answer(f"✅ Тестовый бесплатный урок отправлен пользователю {target_user_id}.")
        else:
            await message.answer("⚠️ Урок не отправлен. Проверьте FREE_LESSON_VIDEO_ID.")

    except TelegramForbiddenError:
        cur.execute(
            "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
            (target_user_id,)
        )
        conn.commit()
        await message.answer("⚠️ Пользователь заблокировал бота.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка test_auto_lesson для {target_user_id}: {e}")
        error_ref = safe_admin_error_reference("test_auto_lesson", e)
        await message.answer(f"❌ Ошибка отправки тестового урока. ref: {error_ref}")

    finally:
        cur.close()
        conn.close()

@router.message(Command('test_backup'))
@admin_private_only(ADMIN_IDS)
async def test_backup(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет прав.")
        return
    await message.answer("🔄 Запускаю бэкап...")
    await send_db_backup()
    await message.answer("✅ Бэкап завершён. Проверьте личные сообщения от бота (файл должен прийти админам).")

@router.message(Command('unblock_user'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def unblock_user(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = (command.args or "").split()
    if len(args) != 1:
        await message.reply("⚠️ Использование: /unblock_user <telegram_id>")
        return
    user_id = int(args[0])
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET blocked_bot = FALSE WHERE telegram_id = %s", (user_id,))
    conn.commit()
    cur.close()
    conn.close()
    await message.reply(f"✅ Пользователь {user_id} удалён из чёрного списка бота.")

@router.message(Command('send_invite_link'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def send_invite_link_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()

    if len(args) != 1:
        await message.reply("⚠️ Использование: /send_invite_link <telegram_id>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT paid, expiry_date, blocked_bot
            FROM users
            WHERE telegram_id = %s
        """, (target_user_id,))
        user = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not user:
        await message.reply("❌ Пользователь не найден в базе.")
        return

    paid, expiry_date, blocked_bot = user

    if not paid or not expiry_date or expiry_date <= datetime.utcnow():
        await message.reply("⚠️ У пользователя нет активного доступа.")
        return

    try:
        try:
            await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=target_user_id)
        except Exception as e:
            logging.error(f"Ошибка разбана перед /send_invite_link для {target_user_id}: {e}")

        invite_expires_at = datetime.utcnow() + timedelta(hours=24)
        invite = await bot.create_chat_invite_link(
            chat_id=int(GROUP_ID),
            name=f"manual_invite_{target_user_id}",
            expire_date=invite_expires_at,
            member_limit=1
        )
        invite_link = invite.invite_link
        save_conn = get_db_conn()
        save_cur = save_conn.cursor()
        try:
            save_bot_invite_link(save_cur, invite_link, "manual_invite", target_user_id, invite_expires_at)
            save_conn.commit()
        except Exception:
            save_conn.rollback()
            raise
        finally:
            save_cur.close()
            save_conn.close()
        expiry_text = expiry_date.strftime("%d.%m.%Y %H:%M")
        user_text = (
            "Здравствуйте! Мы восстановили вам доступ в клуб.\n\n"
            f"Ваш доступ активен до {expiry_text}.\n\n"
            "Вот новая ссылка для входа:\n"
            f"{invite_link}\n\n"
            "Ссылка действует 24 часа и только для одного входа."
        )

        try:
            await bot.send_message(target_user_id, user_text)
        except TelegramForbiddenError:
            mark_user_blocked_bot(target_user_id)
            await message.answer(
                "⚠️ Ссылка создана, но пользователь заблокировал бота.\n\n"
                f"telegram_id: {target_user_id}\n"
                f"Ссылка для ручной отправки: {invite_link}"
            )
            return
        except Exception as e:
            logging.error(f"Не удалось отправить invite link пользователю {target_user_id}: {e}")
            error_ref = safe_admin_error_reference("send_invite_link_notify", e)
            await message.answer(
                "⚠️ Ссылка создана, но не удалось отправить ее пользователю.\n\n"
                f"telegram_id: {target_user_id}\n"
                f"Ошибка: доставка не выполнена. ref: {error_ref}\n"
                f"Ссылка для ручной отправки: {invite_link}"
            )
            return

        await log_access_event(
            target_user_id,
            "manual_invite_sent",
            source="admin_command",
            new_expiry=expiry_date,
            notes=f"admin_id={message.from_user.id}"
        )
        await message.answer(f"✅ Ссылка отправлена пользователю {target_user_id}")

    except Exception as e:
        logging.error(f"Ошибка /send_invite_link для {target_user_id}: {e}")
        error_ref = safe_admin_error_reference("send_invite_link", e)
        await message.answer(f"❌ Ошибка отправки ссылки. ref: {error_ref}")


@router.message(Command('unlinked_stripe'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def unlinked_stripe_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                event_id,
                event_type,
                invoice_id,
                stripe_customer_id,
                stripe_subscription_id,
                customer_email,
                amount_paid,
                currency,
                billing_reason,
                period_end,
                created_at
            FROM unlinked_stripe_events
            WHERE resolved IS NOT TRUE
            ORDER BY created_at DESC
            LIMIT 10
        """)
        rows = cur.fetchall()

        if not rows:
            await message.reply("✅ Нерешённых unlinked Stripe events нет.")
            return

        lines = ["⚠️ Нерешённые Stripe оплаты без пользователя:"]
        for index, row in enumerate(rows, 1):
            (
                event_id,
                event_type,
                invoice_id,
                customer_id,
                subscription_id,
                customer_email,
                amount_paid,
                currency,
                billing_reason,
                period_end,
                created_at,
            ) = row
            lines.extend([
                "",
                f"{index}) event_id: {safe_log_id(event_id) or 'нет'}",
                f"event_type: {event_type or 'нет'}",
                f"invoice_id: {safe_log_id(invoice_id) or 'нет'}",
                f"customer: {safe_log_id(customer_id) or 'нет'}",
                f"subscription: {safe_log_id(subscription_id) or 'нет'}",
                f"email: {safe_log_email(customer_email) or 'нет'}",
                f"amount: {amount_paid if amount_paid is not None else 'нет'} {currency or ''}".strip(),
                f"billing_reason: {billing_reason or 'нет'}",
                f"period_end: {period_end or 'нет'}",
                f"created_at: {created_at}",
                "Связать: /link_stripe_user <telegram_id> <customer_id> <subscription_id>",
            ])

        await message.reply("\n".join(lines))
    except Exception as e:
        logging.error("UNLINKED_STRIPE_COMMAND_FAILED: error=%s", str(e), exc_info=True)
        error_ref = safe_admin_error_reference("unlinked_stripe", e)
        await message.reply(f"❌ Ошибка /unlinked_stripe. ref: {error_ref}")
    finally:
        cur.close()
        conn.close()


@router.message(Command('stripe_links'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def stripe_links_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()
    if len(args) != 1:
        await message.reply("⚠️ Использование: /stripe_links <telegram_id>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                stripe_customer_id,
                stripe_subscription_id,
                customer_email,
                status,
                current_period_end,
                is_active,
                source,
                created_at,
                updated_at
            FROM stripe_links
            WHERE telegram_id = %s
            ORDER BY updated_at DESC
            LIMIT 20
        """, (target_user_id,))
        rows = cur.fetchall()

        if not rows:
            await message.reply(f"Связей Stripe для telegram_id={target_user_id} пока нет.")
            return

        lines = [f"Stripe связи для telegram_id={target_user_id}:"]
        for index, row in enumerate(rows, 1):
            (
                customer_id,
                subscription_id,
                customer_email,
                status,
                current_period_end,
                is_active,
                source,
                created_at,
                updated_at,
            ) = row
            lines.extend([
                "",
                f"{index}) customer_id: {safe_log_id(customer_id) or 'нет'}",
                f"subscription_id: {safe_log_id(subscription_id) or 'нет'}",
                f"email: {safe_log_email(customer_email) or 'нет'}",
                f"status: {status or 'нет'}",
                f"current_period_end: {current_period_end or 'нет'}",
                f"is_active: {is_active}",
                f"source: {source or 'нет'}",
                f"created_at: {created_at}",
                f"updated_at: {updated_at}",
            ])

        await message.reply("\n".join(lines))
    except Exception as e:
        logging.error("STRIPE_LINKS_COMMAND_FAILED: telegram_id=%s, error=%s", target_user_id, str(e), exc_info=True)
        error_ref = safe_admin_error_reference("stripe_links", e)
        await message.reply(f"❌ Ошибка /stripe_links. ref: {error_ref}")
    finally:
        cur.close()
        conn.close()


async def execute_confirmed_retry_delivery(payload):
    delivery_key = payload.get("delivery_key")
    if not delivery_key:
        raise ValueError("Missing delivery_key")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE message_delivery_events
            SET status = 'failed',
                next_attempt_at = NOW(),
                lease_until = NULL,
                last_error = LEFT(COALESCE(last_error, '') || '; admin_retry_requested', 500)
            WHERE delivery_key = %s
              AND status IN ('failed', 'permanently_failed')
            RETURNING delivery_type, telegram_id, status
        """, (delivery_key,))
        row = cur.fetchone()
        conn.commit()
    finally:
        cur.close()
        conn.close()
    if not row:
        raise ValueError("Delivery is not retryable or already sent")
    return {
        "status": "completed",
        "delivery_hash": safe_delivery_hash(delivery_key),
        "delivery_type": row[0],
        "telegram_id": safe_log_id(row[1]),
        "queued_status": row[2],
    }


async def sync_restore_access_from_stripe(telegram_id, user, admin_id=None, action_id=None):
    stripe_subscription_id = user[5]
    old_expiry = user[1]
    if not stripe_subscription_id:
        return {"ok": False, "reason": "no_stripe_subscription"}
    try:
        subscription = await asyncio.to_thread(stripe.Subscription.retrieve, stripe_subscription_id)
        status = get_obj_value(subscription, "status")
        current_period_end = get_obj_value(subscription, "current_period_end")
        period_source = "subscription.current_period_end"
        if status in ("active", "trialing") and not current_period_end:
            invoices = await asyncio.to_thread(stripe.Invoice.list, subscription=stripe_subscription_id, limit=5)
            for invoice in (get_obj_value(invoices, "data") or []):
                if get_obj_value(invoice, "status") != "paid":
                    continue
                period_end = get_obj_value(invoice, "lines", "data")
                first_line = period_end[0] if period_end else None
                current_period_end = get_obj_value(first_line, "period", "end")
                if current_period_end:
                    period_source = "invoice.lines.data[0].period.end"
                    break
    except Exception as e:
        logging.error(
            "RESTORE_ACCESS_STRIPE_SYNC_FAILED: telegram_id=%s, subscription_id=%s, error=%s",
            telegram_id,
            safe_log_id(stripe_subscription_id),
            str(e),
            exc_info=True,
        )
        return {
            "ok": False,
            "reason": "stripe_unavailable",
            "safe_ref": safe_admin_error_reference("restore_access_stripe_sync", e),
        }

    if status not in ("active", "trialing"):
        return {
            "ok": False,
            "reason": "stripe_not_active",
            "stripe_status": status or "unknown",
        }
    if not current_period_end:
        return {
            "ok": False,
            "reason": "stripe_period_missing",
            "stripe_status": status or "unknown",
        }

    new_expiry = datetime.utcfromtimestamp(int(current_period_end))
    if new_expiry <= datetime.utcnow():
        return {
            "ok": False,
            "reason": "stripe_period_not_future",
            "stripe_status": status or "unknown",
        }

    customer = get_obj_value(subscription, "customer")
    customer_id = customer if isinstance(customer, str) else get_obj_value(customer, "id")
    cancel_at_period_end = bool(get_obj_value(subscription, "cancel_at_period_end"))
    auto_renew = not cancel_at_period_end

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT stripe_subscription_id, expiry_date
            FROM users
            WHERE telegram_id = %s
            FOR UPDATE
        """, (int(telegram_id),))
        current_row = cur.fetchone()
        if not current_row or current_row[0] != stripe_subscription_id:
            conn.rollback()
            return {
                "ok": False,
                "reason": "stripe_identity_changed",
                "stripe_status": status or "unknown",
            }
        current_subscription_id, current_old_expiry = current_row
        cur.execute("""
            UPDATE users
            SET paid = TRUE,
                expiry_date = %s,
                stripe_customer_id = COALESCE(%s, stripe_customer_id),
                payment_failed = FALSE,
                payment_failed_at = NULL,
                last_payment_succeeded_at = NOW(),
                grace_period_end = NULL,
                reminder_sent = FALSE,
                auto_renew = %s,
                blocked_bot = FALSE
            WHERE telegram_id = %s
        """, (new_expiry, customer_id, auto_renew, int(telegram_id)))
        record_access_event_cur(
            cur,
            telegram_id,
            "restore_access_stripe_sync",
            source=ACCESS_RESTORE_SOURCE_ADMIN,
            old_expiry=current_old_expiry,
            new_expiry=new_expiry,
            stripe_subscription_id=current_subscription_id,
            notes=f"status={status}; auto_renew={auto_renew}; period_source={period_source}; admin_id={admin_id}; action_id={action_id}",
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    return {"ok": True, "expiry_date": new_expiry, "status": status}


async def execute_confirmed_restore_access(payload):
    telegram_id = int(payload["telegram_id"])
    admin_id = payload.get("admin_id")
    action_id = payload.get("action_id")

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        user = fetch_restore_access_user(cur, telegram_id)
    finally:
        cur.close()
        conn.close()

    if not user:
        return {
            "status": "completed",
            "telegram_id": telegram_id,
            "restored": False,
            "admin_message": restore_access_admin_message("no_active_access", telegram_id),
        }

    paid, expiry_date, payment_failed, grace_period_end = user[:4]
    stripe_synced = False
    if has_restorable_group_access(paid, expiry_date):
        effective_expiry = expiry_date
    else:
        stripe_result = await sync_restore_access_from_stripe(
            telegram_id,
            user,
            admin_id=admin_id,
            action_id=action_id,
        )
        if not stripe_result.get("ok"):
            stripe_reason = stripe_result.get("reason")
            if stripe_reason == "stripe_unavailable":
                admin_message = restore_access_admin_message(
                    "stripe_unavailable",
                    telegram_id,
                    safe_ref=stripe_result.get("safe_ref"),
                )
                result_status = "failed"
            elif stripe_reason == "stripe_period_missing":
                admin_message = restore_access_admin_message("stripe_period_missing", telegram_id)
                result_status = "completed"
            elif stripe_reason == "stripe_period_not_future":
                admin_message = restore_access_admin_message(
                    "stripe_period_not_future",
                    telegram_id,
                    safe_ref=stripe_result.get("stripe_status") or "unknown",
                )
                result_status = "completed"
            elif stripe_reason == "stripe_identity_changed":
                admin_message = restore_access_admin_message("stripe_identity_changed", telegram_id)
                result_status = "failed"
            elif stripe_reason == "stripe_not_active":
                admin_message = restore_access_admin_message(
                    "stripe_not_active",
                    telegram_id,
                    safe_ref=stripe_result.get("stripe_status") or "unknown",
                )
                result_status = "completed"
            else:
                admin_message = restore_access_admin_message("no_active_access", telegram_id)
                result_status = "completed"
            return {
                "status": result_status,
                "telegram_id": telegram_id,
                "restored": False,
                "reason": stripe_reason,
                "safe_ref": stripe_result.get("safe_ref"),
                "stripe_status": stripe_result.get("stripe_status"),
                "admin_message": admin_message,
            }
        effective_expiry = stripe_result["expiry_date"]
        stripe_synced = True

    try:
        member = await bot.get_chat_member(chat_id=int(GROUP_ID), user_id=telegram_id)
        member_status = getattr(member, "status", None)
        restricted_has_access = getattr(member, "is_member", True)
    except (TelegramNetworkError, TelegramRetryAfter) as e:
        safe_ref = safe_admin_error_reference("restore_access_membership_check", e)
        kind = "stripe_synced_telegram_check_failed" if stripe_synced else "telegram_membership_check_failed"
        return {
            "status": "failed",
            "telegram_id": telegram_id,
            "restored": False,
            "reason": "telegram_membership_check_retryable",
            "safe_ref": safe_ref,
            "expiry_date": str(effective_expiry),
            "admin_message": restore_access_admin_message(kind, telegram_id, effective_expiry, safe_ref=safe_ref),
        }
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        safe_ref = safe_admin_error_reference("restore_access_membership_check", e)
        kind = "stripe_synced_telegram_check_failed" if stripe_synced else "telegram_membership_check_failed"
        return {
            "status": "failed",
            "telegram_id": telegram_id,
            "restored": False,
            "reason": "telegram_group_permission_failed",
            "safe_ref": safe_ref,
            "expiry_date": str(effective_expiry),
            "admin_message": restore_access_admin_message(kind, telegram_id, effective_expiry, safe_ref=safe_ref),
        }

    membership_decision = restore_access_membership_decision(member_status, restricted_has_access)
    if membership_decision == "already_member":
        await log_access_event(
            telegram_id,
            "restore_access_already_member",
            source=ACCESS_RESTORE_SOURCE_ADMIN,
            new_expiry=effective_expiry,
            notes=f"status={member_status}; admin_id={admin_id}; action_id={action_id}",
        )
        return {
            "status": "completed",
            "telegram_id": telegram_id,
            "restored": True,
            "already_member": True,
            "expiry_date": str(effective_expiry),
            "admin_message": restore_access_admin_message("already_member", telegram_id, effective_expiry),
        }

    if membership_decision == "fail_closed":
        safe_ref = f"restore_access_membership_status:{safe_delivery_hash(str(member_status))}"
        kind = "stripe_synced_telegram_check_failed" if stripe_synced else "telegram_membership_check_failed"
        return {
            "status": "failed",
            "telegram_id": telegram_id,
            "restored": False,
            "reason": "telegram_membership_status_unknown",
            "safe_ref": safe_ref,
            "expiry_date": str(effective_expiry),
            "admin_message": restore_access_admin_message(kind, telegram_id, effective_expiry, safe_ref=safe_ref),
        }

    if membership_decision == "needs_unban_and_invite":
        try:
            await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=telegram_id)
        except (TelegramNetworkError, TelegramRetryAfter) as e:
            safe_ref = safe_admin_error_reference("restore_access_unban", e)
            kind = "stripe_synced_telegram_unban_failed" if stripe_synced else "telegram_unban_failed"
            return {
                "status": "failed",
                "telegram_id": telegram_id,
                "restored": False,
                "reason": "telegram_unban_retryable",
                "safe_ref": safe_ref,
                "expiry_date": str(effective_expiry),
                "admin_message": restore_access_admin_message(kind, telegram_id, effective_expiry, safe_ref=safe_ref),
            }
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            if not is_benign_rejoin_unban_error(e):
                safe_ref = safe_admin_error_reference("restore_access_unban", e)
                kind = "stripe_synced_telegram_unban_failed" if stripe_synced else "telegram_unban_failed"
                return {
                    "status": "failed",
                    "telegram_id": telegram_id,
                    "restored": False,
                    "reason": "telegram_unban_permission_failed",
                    "safe_ref": safe_ref,
                    "expiry_date": str(effective_expiry),
                    "admin_message": restore_access_admin_message(kind, telegram_id, effective_expiry, safe_ref=safe_ref),
                }

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        created = enqueue_access_restore_invite(
            cur,
            telegram_id,
            effective_expiry,
            ACCESS_RESTORE_SOURCE_ADMIN,
            requested_by_admin_id=admin_id,
            admin_action_id=action_id,
            reason="manual_restore_access",
            delivery_key=access_restore_delivery_key(action_id, telegram_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    return {
        "status": "completed",
        "telegram_id": telegram_id,
        "restored": True,
        "delivery_created": bool(created),
        "expiry_date": str(effective_expiry),
        "admin_message": restore_access_admin_message("queued", telegram_id, effective_expiry),
    }


async def execute_confirmed_admin_action(action_type, payload):
    if action_type == "broadcast":
        return await execute_confirmed_broadcast(payload)
    if action_type == "give_access":
        return await execute_confirmed_give_access(payload)
    if action_type == "set_expiry":
        return await execute_confirmed_set_expiry(payload)
    if action_type == "link_stripe_user":
        return await execute_confirmed_link_stripe_user(payload)
    if action_type == "resolve_checkout":
        return await execute_confirmed_resolve_checkout(payload)
    if action_type == "revoke_invite_links":
        return await execute_confirmed_revoke_invite_links(payload)
    if action_type == "retry_delivery":
        return await execute_confirmed_retry_delivery(payload)
    if action_type == "restore_access":
        return await execute_confirmed_restore_access(payload)
    if action_type == "revoke_access":
        return await execute_confirmed_revoke_access(payload)
    if action_type == "gift_cancel":
        return await execute_confirmed_gift_cancel(payload)
    if action_type == "gift_reissue":
        return await execute_confirmed_gift_reissue(payload)
    raise ValueError(f"Unsupported admin action: {action_type}")


async def execute_confirmed_broadcast(payload):
    text = payload.get("text", "")
    user_ids = payload.get("telegram_ids", [])
    sent = 0
    failed = 0
    for telegram_id in user_ids:
        try:
            await bot.send_message(int(telegram_id), text)
            sent += 1
        except Exception as e:
            failed += 1
            logging.error(
                "CONFIRMED_BROADCAST_SEND_FAILED: telegram_id=%s, error=%s",
                telegram_id,
                str(e),
                exc_info=True,
            )
    return {"sent": sent, "failed": failed}


async def send_admin_action_confirmation(message, action_id, text):
    callbacks = admin_action_confirmation_keyboard(action_id)
    kb = inline_keyboard([[
        InlineKeyboardButton(text="✅ Confirm", callback_data=callbacks["confirm"]),
        InlineKeyboardButton(text="❌ Cancel", callback_data=callbacks["cancel"]),
    ]])
    await message.reply(text, reply_markup=kb)


def admin_action_status(warnings):
    return "completed_with_warning" if warnings else "completed"


def mark_user_blocked_bot(telegram_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (int(telegram_id),))
        conn.commit()
    finally:
        cur.close()
        conn.close()


async def user_is_current_group_member(telegram_id):
    member = await bot.get_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
    status = getattr(member, "status", None)
    return status in ("member", "administrator", "creator") or (
        status == "restricted" and getattr(member, "is_member", False)
    )


async def perform_give_access(payload):
    telegram_id = int(payload["telegram_id"])
    days = int(payload.get("days", 30))
    warnings = []
    repair_enqueued = False
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT expiry_date, auto_renew, stripe_subscription_id FROM users WHERE telegram_id = %s", (telegram_id,))
        row = cur.fetchone()
        old_expiry = row[0] if row else None
        base_expiry = old_expiry if old_expiry and old_expiry > datetime.utcnow() else datetime.utcnow()
        new_expiry = base_expiry + timedelta(days=days)
        cur.execute(
            """
            INSERT INTO users (telegram_id, paid, expiry_date, auto_renew, blocked_bot)
            VALUES (%s, TRUE, %s, FALSE, FALSE)
            ON CONFLICT (telegram_id) DO UPDATE SET
                paid = TRUE,
                expiry_date = EXCLUDED.expiry_date,
                payment_failed = FALSE,
                payment_failed_at = NULL,
                grace_period_end = NULL,
                reminder_sent = FALSE,
                blocked_bot = FALSE,
                auto_renew = CASE
                    WHEN users.stripe_subscription_id IS NULL THEN FALSE
                    ELSE users.auto_renew
                END
            """,
            (telegram_id, new_expiry),
        )
        repair_enqueued = enqueue_automatic_membership_repair(
            cur,
            telegram_id,
            new_expiry,
            ACCESS_RESTORE_SOURCE_ADMIN,
            requested_by_admin_id=payload.get("admin_id"),
            admin_action_id=payload.get("action_id"),
            reason="manual_give_access_confirmed",
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    try:
        await log_access_event(
            telegram_id,
            "manual_give_access",
            source="admin_action_confirmed",
            old_expiry=old_expiry,
            new_expiry=new_expiry,
            notes=f"days={days}; admin_id={payload.get('admin_id')}"
        )
    except Exception as e:
        warnings.append("access_event_failed")
        logging.error("MANUAL_GIVE_ACCESS_EVENT_FAILED: telegram_id=%s, error=%s", telegram_id, str(e), exc_info=True)
    return {
        "status": admin_action_status(warnings),
        "telegram_id": telegram_id,
        "expiry_date": str(new_expiry),
        "repair_enqueued": repair_enqueued,
        "warnings": warnings,
    }


async def perform_set_expiry(payload):
    telegram_id = int(payload["telegram_id"])
    expiry_date = datetime.fromisoformat(payload["expiry_date"])
    warnings = []
    repair_enqueued = False
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT expiry_date FROM users WHERE telegram_id = %s", (telegram_id,))
        row = cur.fetchone()
        old_expiry = row[0] if row else None
        cur.execute(
            """
            INSERT INTO users (
                telegram_id, paid, expiry_date, payment_failed, payment_failed_at,
                grace_period_end, reminder_sent, blocked_bot, auto_renew, manual_sync_at
            )
            VALUES (%s, TRUE, %s, FALSE, NULL, NULL, FALSE, FALSE, FALSE, NOW())
            ON CONFLICT (telegram_id) DO UPDATE SET
                paid = TRUE,
                expiry_date = EXCLUDED.expiry_date,
                payment_failed = FALSE,
                payment_failed_at = NULL,
                grace_period_end = NULL,
                reminder_sent = FALSE,
                blocked_bot = FALSE,
                manual_sync_at = NOW(),
                auto_renew = CASE
                    WHEN users.stripe_subscription_id IS NULL THEN FALSE
                    ELSE users.auto_renew
                END
            """,
            (telegram_id, expiry_date),
        )
        repair_enqueued = enqueue_automatic_membership_repair(
            cur,
            telegram_id,
            expiry_date,
            ACCESS_RESTORE_SOURCE_ADMIN,
            requested_by_admin_id=payload.get("admin_id"),
            admin_action_id=payload.get("action_id"),
            reason="manual_set_expiry_confirmed",
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    try:
        await log_access_event(
            telegram_id,
            "manual_set_expiry",
            source="admin_action_confirmed",
            old_expiry=old_expiry,
            new_expiry=expiry_date,
            notes=f"admin_id={payload.get('admin_id')}"
        )
    except Exception as e:
        warnings.append("access_event_failed")
        logging.error("MANUAL_SET_EXPIRY_EVENT_FAILED: telegram_id=%s, error=%s", telegram_id, str(e), exc_info=True)
    return {
        "status": admin_action_status(warnings),
        "telegram_id": telegram_id,
        "expiry_date": str(expiry_date),
        "repair_enqueued": repair_enqueued,
        "warnings": warnings,
    }


async def perform_link_stripe_user(payload):
    telegram_id = int(payload["telegram_id"])
    customer_id = payload.get("stripe_customer_id")
    subscription_id = payload.get("stripe_subscription_id")
    admin_id = payload.get("admin_id")
    warnings = []
    invite_sent = False
    subscription = await asyncio.to_thread(stripe.Subscription.retrieve, subscription_id)
    status = getattr(subscription, "status", None)
    current_period_end = getattr(subscription, "current_period_end", None)
    cancel_at_period_end = bool(getattr(subscription, "cancel_at_period_end", False))
    stripe_customer_id = getattr(subscription, "customer", None)
    stripe_customer_id = stripe_customer_id if isinstance(stripe_customer_id, str) else customer_id
    if stripe_customer_id != customer_id:
        raise ValueError("Stripe subscription customer mismatch")
    prepared_payment_events = await prepare_manual_link_payment_events(customer_id, subscription_id)
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        assert_stripe_identity_available(
            cur,
            telegram_id,
            customer_id=customer_id,
            subscription_id=subscription_id,
            source="manual_link_stripe_user_confirmed",
        )
        cur.execute("SELECT expiry_date FROM users WHERE telegram_id = %s", (telegram_id,))
        old_row = cur.fetchone()
        old_expiry = old_row[0] if old_row else None
        access_decision = manual_link_access_decision(status, current_period_end, cancel_at_period_end, old_expiry)
        grant_paid_access = access_decision["grant_paid_access"]
        effective_expiry = access_decision["effective_expiry"]
        safe_auto_renew = access_decision["auto_renew"]
        cur.execute(
            """
            INSERT INTO users (
                telegram_id, stripe_customer_id, stripe_subscription_id, paid, expiry_date,
                auto_renew, payment_failed, payment_failed_at, grace_period_end,
                reminder_sent, manual_sync_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, FALSE, NULL, NULL, FALSE, NOW())
            ON CONFLICT (telegram_id) DO UPDATE SET
                stripe_customer_id = EXCLUDED.stripe_customer_id,
                stripe_subscription_id = EXCLUDED.stripe_subscription_id,
                paid = CASE WHEN %s THEN TRUE ELSE users.paid END,
                expiry_date = CASE
                    WHEN %s THEN COALESCE(GREATEST(users.expiry_date, EXCLUDED.expiry_date), users.expiry_date, EXCLUDED.expiry_date)
                    ELSE users.expiry_date
                END,
                auto_renew = EXCLUDED.auto_renew,
                payment_failed = CASE WHEN %s THEN FALSE ELSE users.payment_failed END,
                payment_failed_at = CASE WHEN %s THEN NULL ELSE users.payment_failed_at END,
                grace_period_end = CASE WHEN %s THEN NULL ELSE users.grace_period_end END,
                reminder_sent = CASE WHEN %s THEN FALSE ELSE users.reminder_sent END,
                manual_sync_at = NOW()
            """,
            (
                telegram_id,
                customer_id,
                subscription_id,
                grant_paid_access,
                effective_expiry if grant_paid_access else None,
                safe_auto_renew,
                grant_paid_access,
                grant_paid_access,
                grant_paid_access,
                grant_paid_access,
                grant_paid_access,
                grant_paid_access,
            ),
        )
        upsert_stripe_link(
            cur,
            telegram_id,
            stripe_customer_id=customer_id,
            stripe_subscription_id=subscription_id,
            status=status,
            current_period_end=current_period_end,
            is_active=status in ("active", "trialing"),
            source="manual_link_stripe_user_confirmed",
        )
        inserted_events = backfill_payment_events_for_manual_link(cur, telegram_id, prepared_payment_events)
        cur.execute(
            """
            UPDATE unlinked_stripe_events
            SET resolved = TRUE,
                resolved_by = %s,
                resolved_at = NOW(),
                resolved_telegram_id = %s
            WHERE resolved IS NOT TRUE
              AND (
                    stripe_customer_id = %s
                    OR stripe_subscription_id = %s
                  )
            """,
            (admin_id, telegram_id, customer_id, subscription_id),
        )
        resolved_unlinked_events = cur.rowcount
        conn.commit()
    except StripeIdentityConflictError as conflict:
        conn.rollback()
        persist_stripe_identity_conflict_audit(
            conflict,
            f"manual_link_stripe_user:{safe_delivery_hash(customer_id + ':' + subscription_id)}",
            "manual_link_stripe_user",
        )
        return {
            "status": "failed",
            "telegram_id": telegram_id,
            "reason": "stripe_identity_conflict",
            "admin_message": (
                "❌ Stripe-связка не выполнена: найден конфликт identity.\n"
                f"type: {conflict.conflict_type}\n"
                f"stripe_id: {conflict.safe_stripe_id}\n"
                f"requested_telegram_id: {conflict.requested_telegram_id or 'unknown'}\n"
                "Проверьте /stripe_conflicts и владельца в Stripe Dashboard."
            ),
        }
    except psycopg2_errors.UniqueViolation as e:
        conn.rollback()
        conflict = stripe_identity_conflict_from_unique_violation(
            e,
            telegram_id,
            customer_id=customer_id,
            subscription_id=subscription_id,
            source="manual_link_stripe_user_confirmed",
        )
        if not conflict:
            raise
        populate_known_stripe_identity_unique_violation_owner(conflict)
        if conflict.existing_telegram_id is None:
            return {
                "status": "failed",
                "telegram_id": telegram_id,
                "reason": "retry_required",
                "admin_message": (
                    "⚠️ Stripe-связка не выполнена: конкурентная запись identity не разобрана.\n"
                    "Повторите команду после проверки текущей связи пользователя."
                ),
            }
        if int(conflict.existing_telegram_id) == int(telegram_id):
            if stripe_identity_already_linked_for_user(telegram_id, customer_id, subscription_id):
                return {
                    "status": "completed",
                    "telegram_id": telegram_id,
                    "reason": "already_linked",
                    "warnings": ["same_user_identity_race"],
                    "admin_message": (
                        "✅ Stripe-связка уже установлена для этого пользователя конкурентной операцией.\n"
                        "Новый conflict audit не создан."
                    ),
                }
            return {
                "status": "failed",
                "telegram_id": telegram_id,
                "reason": "retry_required",
                "admin_message": (
                    "⚠️ Stripe-связка не выполнена из-за конкурентной записи same-user identity.\n"
                    "Повторите команду после повторной проверки текущей связи пользователя."
                ),
            }
        persist_stripe_identity_conflict_audit(
            conflict,
            f"manual_link_stripe_user:{safe_delivery_hash(customer_id + ':' + subscription_id)}",
            "manual_link_stripe_user",
        )
        return {
            "status": "failed",
            "telegram_id": telegram_id,
            "reason": "stripe_identity_conflict",
            "admin_message": (
                "❌ Stripe-связка не выполнена: найден конфликт identity.\n"
                f"type: {conflict.conflict_type}\n"
                f"stripe_id: {conflict.safe_stripe_id}\n"
                "Проверьте /stripe_conflicts и владельца в Stripe Dashboard."
            ),
        }
    finally:
        cur.close()
        conn.close()
    try:
        await log_access_event(
            telegram_id,
            "manual_link_stripe_user",
            source="admin_action_confirmed",
            old_expiry=old_expiry,
            new_expiry=effective_expiry,
            stripe_subscription_id=subscription_id,
            notes=(
                f"status={status}; paid_access_granted={grant_paid_access}; "
                f"inserted_payment_events={inserted_events}; resolved_unlinked_events={resolved_unlinked_events}; "
                f"admin_id={admin_id}"
            )
        )
    except Exception as e:
        warnings.append("access_event_failed")
        logging.error(
            "MANUAL_LINK_STRIPE_EVENT_FAILED: telegram_id=%s, subscription_id=%s, error=%s",
            telegram_id,
            safe_log_id(subscription_id),
            str(e),
            exc_info=True,
        )
    if grant_paid_access:
        try:
            is_member = await user_is_current_group_member(telegram_id)
        except Exception as e:
            warnings.append("membership_check_failed")
            is_member = True
            logging.warning(
                "MANUAL_LINK_STRIPE_MEMBERSHIP_CHECK_FAILED: telegram_id=%s, subscription_id=%s, error=%s",
                telegram_id,
                safe_log_id(subscription_id),
                str(e),
                exc_info=True,
            )
            await notify_admins(f"Не удалось проверить членство после /link_stripe_user для telegram_id={telegram_id}; ссылка не отправлена вслепую.")
        if not is_member:
            invite_link = await generate_invite_link()
            if invite_link:
                invite_sent = True
                try:
                    await bot.send_message(telegram_id, f"✅ Подписка привязана. Ссылка для входа: {invite_link}")
                except TelegramForbiddenError:
                    warnings.append("bot_blocked")
                    mark_user_blocked_bot(telegram_id)
                except Exception as e:
                    warnings.append("notification_failed")
                    logging.error("MANUAL_LINK_STRIPE_INVITE_NOTIFY_FAILED: telegram_id=%s, error=%s", telegram_id, str(e), exc_info=True)
                    error_ref = safe_admin_error_reference("manual_link_stripe_invite_notify", e)
                    await notify_admins(f"Не удалось отправить invite после /link_stripe_user для telegram_id={telegram_id}. ref: {error_ref}")
            else:
                warnings.append("invite_failed")
                await notify_admins(f"Не удалось создать invite link после /link_stripe_user для telegram_id={telegram_id}.")
    return {
        "status": admin_action_status(warnings),
        "telegram_id": telegram_id,
        "stripe_subscription_id": safe_log_id(subscription_id),
        "stripe_status": status,
        "paid_access_granted": bool(grant_paid_access),
        "invite_sent": invite_sent,
        "inserted_payment_events": inserted_events,
        "resolved_unlinked_events": resolved_unlinked_events,
        "warnings": warnings,
    }


async def execute_confirmed_give_access(payload):
    return await perform_give_access(payload)


async def execute_confirmed_set_expiry(payload):
    return await perform_set_expiry(payload)


async def execute_confirmed_link_stripe_user(payload):
    return await perform_link_stripe_user(payload)


async def execute_confirmed_resolve_checkout(payload):
    record_id = int(payload["record_id"])
    terminal_status = payload["terminal_status"]
    if terminal_status not in ("failed", "expired"):
        raise ValueError("Unsupported checkout terminal status")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE checkout_sessions
            SET status = %s,
                updated_at = NOW(),
                last_error = 'manually resolved by admin after Stripe Dashboard check'
            WHERE id = %s
              AND status IN ('creating', 'creation_unknown', 'open')
            RETURNING id, telegram_id, tariff_code, status
            """,
            (terminal_status, record_id),
        )
        row = cur.fetchone()
        conn.commit()
    finally:
        cur.close()
        conn.close()
    if not row:
        raise ValueError("Checkout record was not eligible for manual resolution")
    return {
        "status": "completed",
        "record_id": row[0],
        "telegram_id": row[1],
        "tariff_code": row[2],
        "terminal_status": row[3],
    }


async def execute_confirmed_revoke_invite_links(payload):
    revoked = 0
    failed = 0
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        invite_links = load_active_bot_invite_links(cur, limit=int(payload.get("limit", 100)))
    finally:
        cur.close()
        conn.close()
    for invite_link in invite_links:
        try:
            await bot.revoke_chat_invite_link(chat_id=int(GROUP_ID), invite_link=invite_link)
            revoke_conn = get_db_conn()
            revoke_cur = revoke_conn.cursor()
            try:
                mark_bot_invite_link_revoked(revoke_cur, invite_link)
                revoke_conn.commit()
            finally:
                revoke_cur.close()
                revoke_conn.close()
            revoked += 1
        except Exception as e:
            failed += 1
            logging.error("BOT_INVITE_LINK_REVOKE_FAILED: invite_link=%s, error=%s", safe_log_url(invite_link), str(e), exc_info=True)
    return {"revoked": revoked, "failed": failed}


async def execute_confirmed_gift_cancel(payload):
    public_reference = payload["public_reference"]
    return await safely_cancel_gift_checkout(public_reference, payload.get("admin_id"), source="admin_action")


async def execute_confirmed_gift_reissue(payload):
    public_reference = payload["public_reference"]
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        gift_row = fetch_gift_by_public_reference(cur, public_reference, for_update=True)
        if not gift_row:
            raise ValueError("gift_not_found")
        if gift_row["status"] not in ("paid_unclaimed", "reserved"):
            raise ValueError("gift_not_reissuable")
        new_token_version = int(gift_row["token_version"]) + 1
        new_token_hash = gift_token_hash_for_reference(public_reference, new_token_version)
        for delivery_type, kind in ((GIFT_CERTIFICATE_BUYER, "buyer"), (GIFT_CERTIFICATE_RECIPIENT, "recipient")):
            cancel_message_delivery(
                cur,
                gift_delivery_key(public_reference, delivery_type, token_version=gift_row["token_version"], recipient_kind=kind),
                "gift_certificate_reissued",
            )
        cur.execute("""
            UPDATE gift_access_grants
            SET token_hash = %s,
                token_version = %s,
                updated_at = NOW()
            WHERE id = %s
              AND status IN ('paid_unclaimed', 'reserved')
            RETURNING *
        """, (new_token_hash, new_token_version, gift_row["id"]))
        updated = gift_row_dict(cur, cur.fetchone())
        if not updated:
            raise ValueError("gift_not_reissuable")
        record_gift_event(cur, updated, "gift_reissued", payload.get("admin_id"), source="admin_action")
        enqueue_gift_certificate_delivery(cur, updated, updated["purchaser_telegram_id"], GIFT_CERTIFICATE_BUYER)
        enqueue_gift_admin_delivery(cur, public_reference, "gift_admin_problem", gift_admin_text("🎁 Gift certificate reissued", updated))
        conn.commit()
    finally:
        cur.close()
        conn.close()
    return {"status": "completed", "public_reference": public_reference, "gift_status": updated["status"]}


async def execute_confirmed_revoke_access(payload):
    telegram_id = int(payload["telegram_id"])
    reason = payload.get("reason") or "manual_access_revoked"
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        result = apply_manual_access_revoke(
            cur,
            telegram_id,
            reason,
            admin_id=payload.get("admin_id"),
            action_id=payload.get("action_id"),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    if result.get("status") == "failed":
        return {
            "status": "failed",
            "telegram_id": telegram_id,
            "reason": result.get("reason"),
            "admin_message": "❌ Доступ не отозван: пользователь не найден.",
        }
    return {
        **result,
        "admin_message": (
            "✅ Доступ отозван\n\n"
            f"telegram_id: {telegram_id}\n"
            f"old_expiry: {result.get('old_expiry') or 'нет'}\n"
            f"new_expiry: {result.get('new_expiry')}\n"
            f"stripe_customer_id: {safe_log_id(result.get('customer_id')) or 'нет'}\n"
            f"stripe_subscription_id: {safe_log_id(result.get('subscription_id')) or 'нет'}\n"
            f"cancelled_deliveries: {result.get('cancelled_deliveries')}\n"
            "group_removal: queued\n"
            "Stripe subscription: not modified"
        ),
    }


@router.callback_query(F.data.startswith("admin_action:confirm:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_action_confirm_callback(callback: types.CallbackQuery):
    action_id = callback.data.rsplit(":", 1)[-1]
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        claim = claim_admin_action(cur, action_id, callback.from_user.id)
        conn.commit()
    finally:
        cur.close()
        conn.close()
    if claim["status"] != "claimed":
        await callback.answer("Запрос уже обработан, истёк или не найден.", show_alert=True)
        return
    try:
        claim["payload"]["action_id"] = action_id
        result = await execute_confirmed_admin_action(claim["action_type"], claim["payload"])
    except Exception as e:
        fail_conn = get_db_conn()
        fail_cur = fail_conn.cursor()
        try:
            fail_admin_action(fail_cur, action_id)
            fail_conn.commit()
        finally:
            fail_cur.close()
            fail_conn.close()
        logging.error(
            "ADMIN_ACTION_EXECUTION_FAILED: action_id=%s, action_type=%s, admin_id=%s, error=%s",
            action_id,
            claim["action_type"],
            callback.from_user.id,
            str(e),
            exc_info=True,
        )
        await callback.answer("Ошибка выполнения. Запрос помечен failed.", show_alert=True)
        return
    result_status = result.get("status") if isinstance(result, dict) else None
    if result_status == "failed":
        fail_conn = get_db_conn()
        fail_cur = fail_conn.cursor()
        try:
            fail_admin_action(fail_cur, action_id)
            fail_conn.commit()
        finally:
            fail_cur.close()
            fail_conn.close()
        if isinstance(result, dict) and result.get("admin_message"):
            await callback.message.answer(result["admin_message"])
        else:
            await callback.message.answer(f"❌ Действие не выполнено: {result}")
        await callback.answer("Запрос помечен failed.", show_alert=True)
        return
    complete_conn = get_db_conn()
    complete_cur = complete_conn.cursor()
    try:
        complete_admin_action(complete_cur, action_id)
        complete_conn.commit()
    finally:
        complete_cur.close()
        complete_conn.close()
    if isinstance(result, dict) and result.get("admin_message"):
        await callback.message.answer(result["admin_message"])
    elif result_status == "completed_with_warning":
        await callback.message.answer(f"✅ Действие выполнено с предупреждениями: {result}")
    else:
        await callback.message.answer(f"✅ Действие выполнено: {result}")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_action:cancel:"), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def admin_action_cancel_callback(callback: types.CallbackQuery):
    action_id = callback.data.rsplit(":", 1)[-1]
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cancelled = cancel_admin_action(cur, action_id, callback.from_user.id)
        conn.commit()
    finally:
        cur.close()
        conn.close()
    await callback.answer("Отменено." if cancelled else "Запрос уже обработан, истёк или не найден.", show_alert=True)


@router.message(Command('duplicate_subscriptions'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def duplicate_subscriptions_command(message: types.Message):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT COALESCE(u.stripe_customer_id, sl.stripe_customer_id) AS customer_id
            FROM users u
            FULL OUTER JOIN stripe_links sl
              ON sl.telegram_id = u.telegram_id
            WHERE COALESCE(u.stripe_customer_id, sl.stripe_customer_id) IS NOT NULL
            ORDER BY customer_id
            LIMIT 100
        """)
        customer_ids = [row[0] for row in cur.fetchall() if row and row[0]]
        cur.close()
        conn.close()
        cur = None
        conn = None

        semaphore = asyncio.Semaphore(5)
        live_duplicates = []
        live_errors = []

        async def fetch_customer_subscriptions(customer_id):
            async with semaphore:
                try:
                    subscriptions = await asyncio.to_thread(
                        stripe.Subscription.list,
                        customer=customer_id,
                        status="all",
                        limit=20,
                    )
                    blocking = active_or_resumable_subscriptions(subscriptions)
                    if len(blocking) > 1:
                        live_duplicates.append((customer_id, blocking))
                except Exception as e:
                    live_errors.append((customer_id, str(e)))
                    logging.error(
                        "DUPLICATE_SUBSCRIPTIONS_LIVE_CHECK_FAILED: customer_id=%s, error=%s",
                        safe_log_id(customer_id),
                        str(e),
                        exc_info=True,
                    )

        await asyncio.gather(*(fetch_customer_subscriptions(customer_id) for customer_id in customer_ids))

        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT COALESCE(u.stripe_customer_id, sl.stripe_customer_id) AS customer_id,
                   array_agg(DISTINCT COALESCE(u.stripe_subscription_id, sl.stripe_subscription_id)) AS subscriptions,
                   array_agg(DISTINCT COALESCE(u.telegram_id, sl.telegram_id)) AS telegram_ids,
                   count(DISTINCT COALESCE(u.stripe_subscription_id, sl.stripe_subscription_id)) AS sub_count
            FROM users u
            FULL OUTER JOIN stripe_links sl
              ON sl.telegram_id = u.telegram_id
            WHERE COALESCE(u.stripe_customer_id, sl.stripe_customer_id) IS NOT NULL
              AND COALESCE(u.stripe_subscription_id, sl.stripe_subscription_id) IS NOT NULL
            GROUP BY COALESCE(u.stripe_customer_id, sl.stripe_customer_id)
            HAVING count(DISTINCT COALESCE(u.stripe_subscription_id, sl.stripe_subscription_id)) > 1
            ORDER BY sub_count DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        if not live_duplicates and not rows:
            await message.reply("✅ Дубли подписок среди связанных Stripe customers не найдены.")
            return
        lines = ["⚠️ Проверка customers с несколькими subscriptions:"]
        if live_duplicates:
            lines.append("")
            lines.append("Live Stripe:")
            for customer_id, subscriptions in live_duplicates:
                lines.extend([
                    "",
                    f"customer_id: {safe_log_id(customer_id)}",
                    "subscriptions: " + ", ".join(safe_log_id(getattr(sub, "id", "")) or "нет" for sub in subscriptions),
                    f"count: {len(subscriptions)}",
                ])
        if live_errors:
            lines.append("")
            lines.append(f"Ошибок live-проверки Stripe: {len(live_errors)}")
        if rows:
            lines.append("")
            lines.append("Локальные связи БД:")
        for customer_id, subscriptions, telegram_ids, count in rows:
            lines.extend([
                "",
                f"customer_id: {safe_log_id(customer_id)}",
                f"subscriptions: {', '.join(safe_log_id(x) for x in subscriptions if x)}",
                f"telegram_ids: {', '.join(str(x) for x in telegram_ids if x)}",
                f"count: {count}",
            ])
        await message.reply("\n".join(lines))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@router.message(Command('revoke_invite_links'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def revoke_invite_links_command(message: types.Message):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        invite_links = load_active_bot_invite_links(cur)
        action_id = make_action_request(
            cur,
            message.from_user.id,
            "revoke_invite_links",
            {"limit": 100, "active_count": len(invite_links)},
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    callbacks = admin_action_confirmation_keyboard(action_id)
    kb = inline_keyboard([[
        InlineKeyboardButton(text="✅ Confirm", callback_data=callbacks["confirm"]),
        InlineKeyboardButton(text="❌ Cancel", callback_data=callbacks["cancel"]),
    ]])
    await message.reply(
        "Подтвердите отзыв активных ссылок, созданных ботом.\n\n"
        f"Найдено активных ссылок: {len(invite_links)}\n"
        "До подтверждения ссылки не отзываются.",
        reply_markup=kb,
    )


@router.message(Command('resolve_checkout'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def resolve_checkout_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = (command.args or "").split()
    if len(args) != 2:
        await message.reply("⚠️ Использование: /resolve_checkout <record_id> <failed|expired>")
        return
    try:
        record_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ record_id должен быть числом.")
        return
    terminal_status = args[1].strip().lower()
    if terminal_status not in ("failed", "expired"):
        await message.reply("⚠️ terminal status должен быть failed или expired.")
        return
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, telegram_id, tariff_code, mode, status, stripe_session_id, created_at, updated_at
            FROM checkout_sessions
            WHERE id = %s
            """,
            (record_id,),
        )
        row = cur.fetchone()
        if not row:
            await message.reply("Checkout record не найден.")
            return
        action_id = make_action_request(
            cur,
            message.from_user.id,
            "resolve_checkout",
            {
                "record_id": record_id,
                "terminal_status": terminal_status,
                "admin_id": message.from_user.id,
            },
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    await send_admin_action_confirmation(
        message,
        action_id,
        "Подтвердите ручное закрытие Checkout record после проверки Stripe Dashboard.\n\n"
        f"record_id: {row[0]}\n"
        f"telegram_id: {row[1]}\n"
        f"tariff: {row[2]}\n"
        f"mode: {row[3]}\n"
        f"current_status: {row[4]}\n"
        f"session_id: {safe_log_id(row[5])}\n"
        f"created_at: {row[6]}\n"
        f"new_status: {terminal_status}\n\n"
        "До Confirm новая Checkout-ссылка для этой записи не разрешается.",
    )


@router.message(Command('stripe_conflicts'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def stripe_conflicts_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT conflict_type, stripe_id, telegram_ids, details, created_at
            FROM stripe_identity_conflicts
            WHERE resolved IS NOT TRUE
            ORDER BY created_at DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        await message.reply("✅ Неразрешённых Stripe identity conflicts нет.")
        return

    lines = ["⚠️ Неразрешённые Stripe identity conflicts:"]
    for conflict_type, stripe_id, telegram_ids, details, created_at in rows:
        source = "unknown"
        if isinstance(details, dict):
            source = details.get("source") or source
        elif isinstance(details, str):
            try:
                source = json.loads(details).get("source") or source
            except (TypeError, ValueError, json.JSONDecodeError):
                source = "unknown"
        if isinstance(telegram_ids, (list, tuple)):
            telegram_text = ", ".join(str(value) for value in telegram_ids)
        else:
            telegram_text = str(telegram_ids or "unknown")
        lines.extend([
            "",
            f"type: {conflict_type}",
            f"stripe_id: {safe_log_id(stripe_id)}",
            f"telegram_ids: {telegram_text}",
            f"source: {source}",
            f"created_at: {created_at}",
            "action: проверьте владельца в Stripe Dashboard и разберите конфликт вручную.",
        ])

    await message.reply("\n".join(lines))


@router.message(Command('link_stripe_user'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def link_stripe_user_command(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()
    if len(args) != 3:
        await message.reply("⚠️ Использование: /link_stripe_user <telegram_id> <customer_id> <subscription_id>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return

    customer_id = args[1].strip()
    subscription_id = args[2].strip()
    if not customer_id.startswith("cus_") or not subscription_id.startswith("sub_"):
        await message.reply("⚠️ customer_id должен начинаться с cus_, subscription_id должен начинаться с sub_.")
        return

    try:
        subscription = await asyncio.to_thread(stripe.Subscription.retrieve, subscription_id)
        status = getattr(subscription, "status", None)
        current_period_end = getattr(subscription, "current_period_end", None)
        cancel_at_period_end = bool(getattr(subscription, "cancel_at_period_end", False))
        stripe_customer_id = getattr(subscription, "customer", None)
        stripe_customer_id = stripe_customer_id if isinstance(stripe_customer_id, str) else customer_id
    except Exception as e:
        logging.exception("LINK_STRIPE_USER_SUBSCRIPTION_RETRIEVE_FAILED: telegram_id=%s", target_user_id)
        error_ref = safe_admin_error_reference("link_stripe_subscription_retrieve", e)
        await message.reply(f"❌ Не удалось получить Stripe subscription. ref: {error_ref}")
        return

    if stripe_customer_id != customer_id:
        await message.reply("❌ customer_id не совпадает с customer у Stripe subscription. Связка не выполнена.")
        return

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT expiry_date, first_name, username FROM users WHERE telegram_id = %s", (target_user_id,))
        user_row = cur.fetchone()
        old_expiry = user_row[0] if user_row else None
        display_name = (user_row[1] or user_row[2]) if user_row else "нет в users"
        cur.execute(
            """
            SELECT COALESCE(SUM(conflict_count), 0)
            FROM (
                SELECT COUNT(*) AS conflict_count
                FROM users
                WHERE telegram_id <> %s
                  AND (stripe_customer_id = %s OR stripe_subscription_id = %s)
                UNION ALL
                SELECT COUNT(*) AS conflict_count
                FROM stripe_links
                WHERE telegram_id <> %s
                  AND (stripe_customer_id = %s OR stripe_subscription_id = %s)
            ) conflicts
            """,
            (target_user_id, customer_id, subscription_id, target_user_id, customer_id, subscription_id),
        )
        conflict_count = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM unlinked_stripe_events WHERE stripe_customer_id = %s OR stripe_subscription_id = %s",
            (customer_id, subscription_id),
        )
        backfill_count = cur.fetchone()[0]
        new_expiry = datetime.utcfromtimestamp(current_period_end) if current_period_end else None
        effective_expiry = max([value for value in (old_expiry, new_expiry) if value], default=None)
        action_id = make_action_request(
            cur,
            message.from_user.id,
            "link_stripe_user",
            {
                "telegram_id": target_user_id,
                "stripe_customer_id": customer_id,
                "stripe_subscription_id": subscription_id,
                "admin_id": message.from_user.id,
            },
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    await send_admin_action_confirmation(
        message,
        action_id,
        "Подтвердите ручную Stripe-связку.\n\n"
        f"telegram_id: {target_user_id}\n"
        f"name: {display_name}\n"
        f"customer_id: {safe_log_id(customer_id)}\n"
        f"subscription_id: {safe_log_id(subscription_id)}\n"
        f"status: {status}; cancel_at_period_end: {cancel_at_period_end}\n"
        f"old_expiry: {old_expiry or 'нет'}\n"
        f"effective_expiry: {effective_expiry or 'нет'}\n"
        f"conflicts_now: {conflict_count}\n"
        f"payment_events_to_backfill: {backfill_count}",
    )

@router.message(Command('unban_user'), StateFilter('*'))
@admin_private_only(ADMIN_IDS)
async def unban_user(message: types.Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = (command.args or "").split()

    if len(args) != 1:
        await message.reply("⚠️ Использование: /unban_user <telegram_id>")
        return

    try:
        user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return

    try:
        await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=user_id)
        await message.reply(f"✅ Бан пользователя {user_id} снят в Telegram-группе.")
    except Exception as e:
        logging.error(f"Ошибка /unban_user для {user_id}: {e}")
        error_ref = safe_admin_error_reference("unban_user", e)
        await message.reply(f"❌ Не удалось снять бан пользователя {user_id}. ref: {error_ref}")

# --- ЗАПУСК И ВЕБХУК TELEGRAM ---
def get_telegram_webhook_path():
    return f"/webhook/{WEBHOOK_SECRET}"


def get_safe_telegram_webhook_path():
    return "/webhook/***"


async def run_scheduled_with_lock(job_name, schedule_slot, func, lease_minutes=30):
    job_key = f"{job_name}:{schedule_slot}"
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        claim = claim_scheduled_job(cur, job_key, job_name, schedule_slot, lease_minutes=lease_minutes)
        conn.commit()
    finally:
        cur.close()
        conn.close()
    if claim != "claimed":
        logging.info("SCHEDULED_JOB_SKIPPED: job_key=%s, claim=%s, owner_id=%s", job_key, claim, OWNER_ID)
        return {"status": claim, "job_key": job_key}
    try:
        result = await func()
        conn = get_db_conn()
        cur = conn.cursor()
        try:
            complete_scheduled_job(cur, job_key)
            conn.commit()
        finally:
            cur.close()
            conn.close()
        return {"status": "completed", "job_key": job_key, "result": result}
    except Exception as e:
        conn = get_db_conn()
        cur = conn.cursor()
        try:
            fail_scheduled_job(cur, job_key, e)
            conn.commit()
        finally:
            cur.close()
            conn.close()
        raise


async def scheduled_check_subscriptions_and_reminders():
    return await run_scheduled_with_lock(
        "check_subscriptions_and_reminders",
        datetime.utcnow().date().isoformat(),
        check_subscriptions_and_reminders,
        lease_minutes=120,
    )


def five_minute_schedule_slot(now=None):
    now = now or datetime.utcnow()
    minute = now.minute - (now.minute % 5)
    return now.replace(minute=minute, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M")


def hourly_schedule_slot(now=None):
    now = now or datetime.utcnow()
    return now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H")


def daily_schedule_slot(now=None):
    now = now or datetime.utcnow()
    return now.strftime("%Y-%m-%d")


async def cleanup_stale_postgres_fsm_storage():
    return await asyncio.to_thread(cleanup_postgres_fsm_storage, get_db_conn, 30)


async def scheduled_cleanup_stale_postgres_fsm_storage():
    return await run_scheduled_with_lock(
        "cleanup_stale_postgres_fsm_storage",
        daily_schedule_slot(),
        cleanup_stale_postgres_fsm_storage,
        lease_minutes=30,
    )


async def scheduled_enqueue_first_purchase_recovery_reminders():
    return await run_scheduled_with_lock(
        "enqueue_first_purchase_recovery_reminders",
        hourly_schedule_slot(),
        enqueue_due_first_purchase_recovery_reminders,
        lease_minutes=50,
    )


async def scheduled_check_auto_free_lessons():
    return await run_scheduled_with_lock(
        "check_auto_free_lessons",
        datetime.utcnow().strftime("%Y-%m-%dT%H"),
        check_auto_free_lessons,
        lease_minutes=50,
    )


async def scheduled_check_free_lesson_followups():
    return await run_scheduled_with_lock(
        "check_free_lesson_followups",
        datetime.utcnow().strftime("%Y-%m-%dT%H"),
        check_free_lesson_followups,
        lease_minutes=50,
    )


async def scheduled_send_db_backup():
    year, week, _ = datetime.utcnow().isocalendar()
    return await run_scheduled_with_lock(
        "send_db_backup",
        f"{year}-W{week:02d}",
        send_db_backup,
        lease_minutes=120,
    )


async def process_pending_message_deliveries(limit=25):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        deliveries = claim_pending_message_deliveries(cur, limit=limit)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    sent = 0
    retryable_failed = 0
    permanently_failed = 0
    blocked = 0

    for (
        delivery_key,
        telegram_id,
        delivery_type,
        payload_json,
        attempt_count,
        invite_link,
        claim_generation,
    ) in deliveries:
        sending_user_message = False
        payload = {}
        try:
            payload = json.loads(payload_json or "{}")
            if payload.get("keyboard_kind") == "billing_portal":
                check_conn = get_db_conn()
                check_cur = check_conn.cursor()
                try:
                    check_cur.execute(
                        """
                        SELECT payment_failed, grace_period_end
                        FROM users
                        WHERE telegram_id = %s
                        """,
                        (int(telegram_id),),
                    )
                    failed_row = check_cur.fetchone()
                    failed_notice_due = bool(
                        failed_row
                        and failed_row[0]
                        and failed_row[1]
                        and failed_row[1] > datetime.utcnow()
                    )
                    if not failed_notice_due:
                        cancel_result = mark_delivery_cancelled(
                            check_cur,
                            delivery_key,
                            claim_generation,
                            "failed_renewal_no_longer_due",
                        )
                        if cancel_result == "cancelled":
                            check_conn.commit()
                        else:
                            check_conn.rollback()
                            log_stale_delivery_claim(delivery_key, "mark_cancelled")
                        continue
                    check_conn.commit()
                finally:
                    check_cur.close()
                    check_conn.close()
            if delivery_type == "first_purchase_recovery_reminder":
                check_conn = get_db_conn()
                check_cur = check_conn.cursor()
                try:
                    still_due = first_purchase_recovery_reminder_still_due(
                        check_cur,
                        telegram_id,
                        current_delivery_key=delivery_key,
                    )
                    if not still_due:
                        cancel_first_purchase_recovery_delivery(
                            check_cur,
                            delivery_key,
                            "first_purchase_recovery_no_longer_due",
                        )
                        check_conn.commit()
                        logging.info(
                            "FIRST_PURCHASE_RECOVERY_REMINDER_CANCELLED: user_hash=%s, delivery_key=%s",
                            safe_delivery_hash(telegram_id),
                            safe_log_id(delivery_key),
                        )
                        continue
                    check_conn.commit()
                finally:
                    check_cur.close()
                    check_conn.close()
            if delivery_type == "free_lesson":
                result = await process_already_claimed_delivery(
                    get_db_conn,
                    delivery_key,
                    telegram_id,
                    delivery_type,
                    lambda: send_free_lesson_delivery(telegram_id, payload),
                    claim_generation,
                    blocked_exc=(TelegramForbiddenError,),
                    attempt_count=attempt_count,
                    classify_error_func=classify_delivery_error,
                    log_failure_func=log_outbox_delivery_failure,
                    terminal_error_callback=lambda error, decision, current_attempt_count: notify_terminal_free_lesson_delivery_error(
                        delivery_key,
                        delivery_type,
                        current_attempt_count,
                        error,
                        decision,
                    ),
                    retryable_error_callback=lambda error, decision, current_attempt_count, escalation: notify_retryable_outbox_failure(
                        delivery_key, delivery_type, current_attempt_count, error, decision, escalation
                    ),
                    retryable_state_func=claim_outbox_retry_escalation,
                )
                if result == "sent":
                    sent += 1
                elif result == "blocked":
                    blocked += 1
                    permanently_failed += 1
                elif result == "permanently_failed":
                    permanently_failed += 1
                elif result == "failed":
                    retryable_failed += 1
                continue
            elif delivery_type == "free_lesson_followup":
                result = await process_already_claimed_delivery(
                    get_db_conn,
                    delivery_key,
                    telegram_id,
                    delivery_type,
                    lambda: send_free_lesson_followup_delivery(telegram_id, payload),
                    claim_generation,
                    blocked_exc=(TelegramForbiddenError,),
                    success_update_sql="""
                    UPDATE users
                    SET feedback_sent = TRUE,
                        feedback_sent_at = NOW()
                    WHERE telegram_id = %s
                    """,
                    success_update_params=(int(telegram_id),),
                    attempt_count=attempt_count,
                    classify_error_func=classify_delivery_error,
                    log_failure_func=log_outbox_delivery_failure,
                    terminal_error_callback=lambda error, decision, current_attempt_count: notify_terminal_free_lesson_delivery_error(
                        delivery_key,
                        delivery_type,
                        current_attempt_count,
                        error,
                        decision,
                    ),
                    retryable_error_callback=lambda error, decision, current_attempt_count, escalation: notify_retryable_outbox_failure(
                        delivery_key, delivery_type, current_attempt_count, error, decision, escalation
                    ),
                    retryable_state_func=claim_outbox_retry_escalation,
                )
                if result == "sent":
                    sent += 1
                elif result == "blocked":
                    blocked += 1
                    permanently_failed += 1
                elif result == "permanently_failed":
                    permanently_failed += 1
                elif result == "failed":
                    retryable_failed += 1
                continue
            elif delivery_type == ACCESS_RESTORE_DELIVERY_TYPE:
                effective_expiry_raw = payload.get("effective_expiry")
                effective_expiry = datetime.fromisoformat(effective_expiry_raw) if effective_expiry_raw else None
                source = payload.get("source") or ACCESS_RESTORE_SOURCE_ADMIN
                recheck_conn = get_db_conn()
                recheck_cur = recheck_conn.cursor()
                try:
                    recheck_cur.execute("""
                        SELECT paid, expiry_date, payment_failed, grace_period_end
                        FROM users
                        WHERE telegram_id = %s
                    """, (int(telegram_id),))
                    access_row = recheck_cur.fetchone()
                    if not access_row:
                        cancel_result = mark_delivery_cancelled(
                            recheck_cur,
                            delivery_key,
                            claim_generation,
                            "access_restore_user_not_found",
                        )
                        if cancel_result == "cancelled":
                            recheck_conn.commit()
                        else:
                            recheck_conn.rollback()
                            log_stale_delivery_claim(delivery_key, "mark_cancelled")
                        continue
                    paid, db_expiry, payment_failed, grace_period_end = access_row
                    if not has_restorable_group_access(paid, db_expiry):
                        cancel_result = mark_delivery_cancelled(
                            recheck_cur,
                            delivery_key,
                            claim_generation,
                            "access_restore_inactive",
                        )
                        record_access_event_cur(
                            recheck_cur,
                            telegram_id,
                            "restore_access_cancelled_inactive",
                            source=source,
                            new_expiry=db_expiry,
                            notes=f"delivery_key={safe_delivery_hash(delivery_key)}",
                        )
                        if cancel_result == "cancelled":
                            recheck_conn.commit()
                        else:
                            recheck_conn.rollback()
                            log_stale_delivery_claim(delivery_key, "mark_cancelled")
                        continue
                    effective_expiry = db_expiry
                    recheck_conn.commit()
                finally:
                    recheck_cur.close()
                    recheck_conn.close()

                member = await bot.get_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
                member_status = getattr(member, "status", None)
                restricted_has_access = getattr(member, "is_member", True)
                membership_decision = restore_access_membership_decision(member_status, restricted_has_access)
                if membership_decision == "already_member":
                    if source == "gift_access":
                        sending_user_message = True
                        await bot.send_message(
                            int(telegram_id),
                            build_gift_redeemed_recipient_text({}, effective_expiry),
                            reply_markup=get_main_keyboard(telegram_id),
                        )
                    already_conn = get_db_conn()
                    already_cur = already_conn.cursor()
                    try:
                        record_access_event_cur(
                            already_cur,
                            telegram_id,
                            "restore_access_already_member",
                            source=source,
                            new_expiry=effective_expiry,
                            notes=f"status={member_status}; delivery_key={safe_delivery_hash(delivery_key)}",
                        )
                        sent_result = mark_delivery_sent(already_cur, delivery_key, claim_generation)
                        if sent_result == "sent":
                            already_conn.commit()
                        else:
                            already_conn.rollback()
                            log_stale_delivery_claim(delivery_key, "mark_sent")
                            continue
                    finally:
                        already_cur.close()
                        already_conn.close()
                    sent += 1
                    continue
                if membership_decision == "needs_unban_and_invite":
                    try:
                        await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
                    except TelegramBadRequest as e:
                        if not is_benign_rejoin_unban_error(e):
                            raise
                elif membership_decision != "needs_invite":
                    raise RuntimeError(f"unknown_restore_member_status:{member_status}")

                link = invite_link
                if not link:
                    options = invite_link_options("access_restore", telegram_id)
                    invite = await bot.create_chat_invite_link(chat_id=int(GROUP_ID), **options)
                    link = invite.invite_link
                    link_conn = get_db_conn()
                    link_cur = link_conn.cursor()
                    try:
                        invite_result, persisted_link = save_delivery_invite_link(
                            link_cur,
                            delivery_key,
                            claim_generation,
                            link,
                        )
                        if invite_result != "saved":
                            link_conn.rollback()
                            log_stale_delivery_claim(delivery_key, "save_invite_link")
                            continue
                        link = persisted_link
                        save_bot_invite_link(
                            link_cur,
                            link,
                            source,
                            telegram_id,
                            options.get("expire_date"),
                        )
                        link_conn.commit()
                    finally:
                        link_cur.close()
                        link_conn.close()
                sending_user_message = True
                await bot.send_message(
                    int(telegram_id),
                    (
                        build_gift_redeemed_invite_text(effective_expiry)
                        if source == "gift_access"
                        else access_restore_invite_text(effective_expiry)
                    ),
                    reply_markup=access_restore_invite_keyboard(link),
                )
            elif delivery_type in ("stripe_rejoin_invite", "stripe_rejoin_check"):
                if delivery_type == "stripe_rejoin_check":
                    member = await bot.get_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
                    member_status = getattr(member, "status", None)
                    restricted_has_access = getattr(member, "is_member", True)
                    logging.info(
                        "STRIPE_REJOIN_MEMBERSHIP_CHECKED: telegram_id=%s, delivery_key=%s, status=%s, is_member=%s",
                        safe_log_id(telegram_id),
                        safe_log_id(delivery_key),
                        member_status,
                        restricted_has_access,
                    )
                    if rejoin_delivery_should_skip_invite(member_status, restricted_has_access):
                        logging.info(
                            "STRIPE_REJOIN_CHECK_COMPLETED_WITHOUT_INVITE: telegram_id=%s, delivery_key=%s, status=%s",
                            safe_log_id(telegram_id),
                            safe_log_id(delivery_key),
                            member_status,
                        )
                    elif not rejoin_delivery_should_send_invite(member_status):
                        raise RuntimeError(f"unknown_rejoin_member_status:{member_status}")

                link = invite_link
                if delivery_type == "stripe_rejoin_invite" or rejoin_delivery_should_send_invite(member_status):
                    try:
                        await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
                    except TelegramBadRequest as e:
                        if is_benign_rejoin_unban_error(e):
                            logging.warning(
                                "STRIPE_REJOIN_UNBAN_BENIGN: telegram_id=%s, delivery_key=%s, error=%s",
                                safe_log_id(telegram_id),
                                safe_log_id(delivery_key),
                                clean_error_reason(e),
                            )
                        else:
                            logging.warning(
                                "STRIPE_REJOIN_UNBAN_RETRYABLE: telegram_id=%s, delivery_key=%s, error=%s",
                                safe_log_id(telegram_id),
                                safe_log_id(delivery_key),
                                clean_error_reason(e),
                                exc_info=True,
                            )
                            raise
                    except Exception as e:
                        logging.warning(
                            "STRIPE_REJOIN_UNBAN_RETRYABLE: telegram_id=%s, delivery_key=%s, error=%s",
                            safe_log_id(telegram_id),
                            safe_log_id(delivery_key),
                            clean_error_reason(e),
                            exc_info=True,
                        )
                        raise
                    if not link:
                        options = invite_link_options("stripe_rejoin", telegram_id)
                        invite = await bot.create_chat_invite_link(chat_id=int(GROUP_ID), **options)
                        link = invite.invite_link
                        link_conn = get_db_conn()
                        link_cur = link_conn.cursor()
                        try:
                            invite_result, persisted_link = save_delivery_invite_link(
                                link_cur,
                                delivery_key,
                                claim_generation,
                                link,
                            )
                            if invite_result != "saved":
                                link_conn.rollback()
                                log_stale_delivery_claim(delivery_key, "save_invite_link")
                                continue
                            link = persisted_link
                            save_bot_invite_link(
                                link_cur,
                                link,
                                "stripe_rejoin",
                                telegram_id,
                                options.get("expire_date"),
                            )
                            link_conn.commit()
                        finally:
                            link_cur.close()
                            link_conn.close()
                    text = payload.get("text") or (
                        "✅ Оплата прошла успешно. Доступ восстановлен.\n\n"
                        f"Ссылка для входа в группу: {link}"
                    )
                    if "{invite_link}" in text:
                        text = text.replace("{invite_link}", link)
                    sending_user_message = True
                    await bot.send_message(
                        int(telegram_id),
                        text,
                        reply_markup=await stripe_delivery_reply_markup_for_user(payload, telegram_id),
                        parse_mode=payload.get("parse_mode"),
                    )
            elif delivery_type == "gift_paid_buyer":
                public_reference = payload.get("gift_reference")
                token_version = payload.get("token_version")
                if public_reference and token_version:
                    order_conn = get_db_conn()
                    order_cur = order_conn.cursor()
                    try:
                        order_cur.execute(
                            "SELECT status FROM message_delivery_events WHERE delivery_key = %s",
                            (gift_delivery_key(
                                public_reference,
                                GIFT_CERTIFICATE_BUYER,
                                token_version=token_version,
                                recipient_kind="buyer",
                            ),),
                        )
                        certificate_delivery = order_cur.fetchone()
                        if certificate_delivery and certificate_delivery[0] not in (
                            "sent", "cancelled", "permanently_failed"
                        ):
                            deferred = mark_delivery_failed(
                                order_cur,
                                delivery_key,
                                claim_generation,
                                RuntimeError("gift_buyer_instruction_waiting_for_certificate"),
                                retry_delay_minutes=5,
                            )
                            if deferred == "failed":
                                order_conn.commit()
                                retryable_failed += 1
                            else:
                                order_conn.rollback()
                                log_stale_delivery_claim(delivery_key, "defer_gift_buyer_instruction")
                            continue
                    finally:
                        order_cur.close()
                        order_conn.close()
                text = payload.get("text")
                if not text:
                    raise ValueError("invalid_gift_paid_buyer_payload")
                sending_user_message = True
                await bot.send_message(int(telegram_id), text)
            elif delivery_type in (GIFT_CERTIFICATE_BUYER, GIFT_CERTIFICATE_RECIPIENT):
                caption = payload.get("caption")
                public_reference = payload.get("public_reference")
                token_version = payload.get("token_version")
                if not caption or not public_reference or not token_version:
                    raise ValueError("invalid_gift_certificate_payload")
                cert_conn = get_db_conn()
                cert_cur = cert_conn.cursor()
                try:
                    gift_row = fetch_gift_by_public_reference_version(
                        cert_cur,
                        public_reference,
                        int(token_version),
                        for_update=False,
                    )
                    if not gift_row or gift_row["status"] not in ("paid_unclaimed", "reserved"):
                        cancel_result = mark_delivery_cancelled(
                            cert_cur,
                            delivery_key,
                            claim_generation,
                            "gift_certificate_stale_or_unavailable",
                        )
                        if cancel_result == "cancelled":
                            cert_conn.commit()
                        else:
                            cert_conn.rollback()
                            log_stale_delivery_claim(delivery_key, "mark_cancelled")
                        continue
                    expected_hash = gift_token_hash_for_reference(public_reference, token_version)
                    if not hmac.compare_digest(str(gift_row.get("token_hash") or ""), expected_hash):
                        cancel_result = mark_delivery_cancelled(
                            cert_cur,
                            delivery_key,
                            claim_generation,
                            "gift_certificate_token_version_mismatch",
                        )
                        if cancel_result == "cancelled":
                            cert_conn.commit()
                        else:
                            cert_conn.rollback()
                            log_stale_delivery_claim(delivery_key, "mark_cancelled")
                        continue
                    cert_conn.commit()
                finally:
                    cert_cur.close()
                    cert_conn.close()
                button_url = gift_deep_link(generate_gift_token(public_reference, token_version))
                sending_user_message = True
                generated_path = None
                try:
                    template_path = certificate_template_path(gift_row["tariff_code"])
                    certificate_name = gift_row.get("certificate_name")
                    if certificate_name:
                        with tempfile.NamedTemporaryFile(prefix="gift-certificate-", suffix=".png", delete=False) as output:
                            generated_path = output.name
                        render_gift_certificate(gift_row["tariff_code"], certificate_name, generated_path)
                        photo = FSInputFile(generated_path)
                    else:
                        photo = FSInputFile(str(template_path))
                    await bot.send_photo(
                        int(telegram_id),
                        photo,
                        caption=gift_certificate_delivery_caption(caption, button_url),
                        reply_markup=inline_keyboard([[
                            InlineKeyboardButton(
                                text=payload.get("button_text") or "🎁 Активировать подарок",
                                url=button_url,
                            )
                        ]]),
                        parse_mode=payload.get("parse_mode"),
                    )
                except Exception:
                    enqueue_gift_certificate_failure_notices(public_reference, telegram_id, gift_row)
                    raise
                finally:
                    if generated_path:
                        remove_generated_certificate(generated_path)
            elif delivery_type == "gift_certificate_failed_buyer":
                public_reference = payload.get("public_reference")
                token_version = payload.get("token_version")
                if not public_reference or not token_version:
                    raise ValueError("invalid_gift_certificate_fallback_payload")
                fallback_conn = get_db_conn()
                fallback_cur = fallback_conn.cursor()
                try:
                    gift_row = fetch_gift_by_public_reference_version(
                        fallback_cur,
                        public_reference,
                        int(token_version),
                        for_update=False,
                    )
                finally:
                    fallback_cur.close()
                    fallback_conn.close()
                if not gift_row or gift_row["status"] not in ("paid_unclaimed", "reserved"):
                    raise ValueError("gift_certificate_fallback_stale")
                expected_hash = gift_token_hash_for_reference(public_reference, token_version)
                if not hmac.compare_digest(str(gift_row.get("token_hash") or ""), expected_hash):
                    raise ValueError("gift_certificate_fallback_token_version_mismatch")
                activation_url = gift_deep_link(generate_gift_token(public_reference, token_version))
                sending_user_message = True
                await bot.send_message(
                    int(telegram_id),
                    (
                        "🎁 Подарок успешно оформлен, но изображение сертификата пока не удалось отправить.\n\n"
                        "Подарок можно активировать по ссылке:\n"
                        f"{activation_url}\n\n"
                        "Мы уже сообщили администратору об ошибке сертификата."
                    ),
                )
            elif delivery_type == "stripe_admin_message":
                if int(telegram_id) not in ADMIN_IDS:
                    raise ValueError("invalid_stripe_admin_message_recipient")
                text = payload.get("text")
                if not text:
                    raise ValueError("invalid_stripe_admin_message_payload")
                sending_user_message = True
                await bot.send_message(
                    int(telegram_id),
                    text,
                    parse_mode=payload.get("parse_mode"),
                )
            elif delivery_type.startswith("gift_admin_"):
                if not gift_admin_recipient_allowed(telegram_id):
                    raise ValueError("invalid_gift_admin_message_recipient")
                text = payload.get("text")
                if not text:
                    raise ValueError("invalid_gift_admin_message_payload")
                sending_user_message = True
                await bot.send_message(
                    int(telegram_id),
                    text,
                    parse_mode=payload.get("parse_mode"),
                )
            else:
                text = payload.get("text")
                if not text:
                    raise ValueError(f"delivery text missing for {delivery_type}")
                sending_user_message = True
                await bot.send_message(
                    int(telegram_id),
                    text,
                    reply_markup=await stripe_delivery_reply_markup_for_user(payload, telegram_id),
                    parse_mode=payload.get("parse_mode"),
                )

            sent_conn = get_db_conn()
            sent_cur = sent_conn.cursor()
            try:
                if delivery_type == "free_lesson" and sending_user_message:
                    sent_cur.execute("""
                        UPDATE users
                        SET video_sent = TRUE,
                            video_sent_at = NOW()
                        WHERE telegram_id = %s
                    """, (int(telegram_id),))
                elif delivery_type == "free_lesson_followup" and sending_user_message:
                    sent_cur.execute("""
                        UPDATE users
                        SET feedback_sent = TRUE,
                            feedback_sent_at = NOW()
                        WHERE telegram_id = %s
                    """, (int(telegram_id),))
                elif delivery_type in ("stripe_rejoin_invite", "stripe_rejoin_check") and sending_user_message:
                    new_expiry_raw = payload.get("new_expiry")
                    new_expiry = datetime.fromisoformat(new_expiry_raw) if new_expiry_raw else None
                    sent_cur.execute("""
                        INSERT INTO access_events (
                            telegram_id, event_type, source, new_expiry,
                            stripe_event_id, stripe_subscription_id, notes
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        int(telegram_id),
                        "rejoin_invite_sent_after_payment",
                        payload.get("source") or "stripe_webhook",
                        new_expiry,
                        payload.get("stripe_event_id"),
                        payload.get("stripe_subscription_id"),
                        "invite link sent after payment",
                    ))
                elif delivery_type == ACCESS_RESTORE_DELIVERY_TYPE and sending_user_message:
                    new_expiry_raw = payload.get("effective_expiry")
                    new_expiry = datetime.fromisoformat(new_expiry_raw) if new_expiry_raw else None
                    sent_cur.execute("SELECT expiry_date FROM users WHERE telegram_id = %s", (int(telegram_id),))
                    row = sent_cur.fetchone()
                    effective_expiry = row[0] if row and row[0] else new_expiry
                    record_access_event_cur(
                        sent_cur,
                        telegram_id,
                        "restore_access_invite_sent",
                        source=payload.get("source") or ACCESS_RESTORE_SOURCE_ADMIN,
                        new_expiry=effective_expiry,
                        notes=f"delivery_key={safe_delivery_hash(delivery_key)}; admin_action_id={payload.get('admin_action_id') or 'none'}",
                    )
                sent_result = mark_delivery_sent(sent_cur, delivery_key, claim_generation)
                if sent_result != "sent":
                    sent_conn.rollback()
                    log_stale_delivery_claim(delivery_key, "mark_sent")
                    continue
                if delivery_type == "first_purchase_recovery_reminder" and sending_user_message:
                    enqueue_first_purchase_recovery_admin_sent_notices_safely(
                        sent_cur,
                        delivery_key,
                        int(telegram_id),
                        payload,
                    )
                sent_conn.commit()
            finally:
                sent_cur.close()
                sent_conn.close()
            sent += 1
        except Exception as e:
            if delivery_type == "stripe_admin_message" and isinstance(e, ValueError):
                decision = {
                    "blocked": False,
                    "retryable": False,
                    "permanently_failed": True,
                    "retry_delay_minutes": None,
                    "reason": str(e),
                }
            elif delivery_type == ACCESS_RESTORE_DELIVERY_TYPE and not sending_user_message and isinstance(e, (TelegramForbiddenError, TelegramBadRequest)):
                decision = {
                    "blocked": False,
                    "retryable": False,
                    "permanently_failed": True,
                    "retry_delay_minutes": None,
                    "reason": "access_restore_group_permission_failed",
                }
            else:
                decision = classify_delivery_error(e, attempt_count=attempt_count, sending_user_message=sending_user_message)
                if delivery_type == "stripe_admin_message" and decision.get("blocked"):
                    decision = {
                        **decision,
                        "blocked": False,
                        "retryable": False,
                        "permanently_failed": True,
                        "retry_delay_minutes": None,
                        "reason": "admin_recipient_unreachable",
                    }
            log_outbox_delivery_failure(delivery_key, delivery_type, attempt_count, e, decision)
            retryable_escalation = None
            fail_conn = get_db_conn()
            fail_cur = fail_conn.cursor()
            try:
                if decision.get("blocked") and delivery_type != "stripe_admin_message":
                    fail_cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (int(telegram_id),))
                    if delivery_type == ACCESS_RESTORE_DELIVERY_TYPE:
                        record_access_event_cur(
                            fail_cur,
                            telegram_id,
                            "restore_access_user_blocked",
                            source=payload.get("source") or ACCESS_RESTORE_SOURCE_ADMIN,
                            notes=f"delivery_key={safe_delivery_hash(delivery_key)}",
                        )
                retry_delay = decision.get("retry_delay_minutes")
                if retry_delay is None and not decision.get("permanently_failed", False):
                    retry_delay = 15
                failed_result = mark_delivery_failed(
                    fail_cur,
                    delivery_key,
                    claim_generation,
                    e,
                    retry_delay_minutes=retry_delay,
                    permanently_failed=decision.get("permanently_failed", False),
                )
                if failed_result in ("failed", "permanently_failed"):
                    if decision.get("retryable"):
                        retryable_escalation = claim_outbox_retry_escalation(
                            fail_cur, delivery_key, delivery_type, attempt_count
                        )
                    fail_conn.commit()
                else:
                    fail_conn.rollback()
                    log_stale_delivery_claim(delivery_key, "mark_failed")
                    continue
            finally:
                fail_cur.close()
                fail_conn.close()

            if decision.get("blocked"):
                blocked += 1
                permanently_failed += 1
            elif decision.get("permanently_failed"):
                permanently_failed += 1
            else:
                retryable_failed += 1

            if isinstance(e, MissingFreeLessonVideoError) and decision.get("permanently_failed"):
                await notify_terminal_free_lesson_delivery_error(delivery_key, delivery_type, attempt_count, e, decision)
            elif decision.get("permanently_failed") and not decision.get("blocked") and delivery_type in ("stripe_rejoin_invite", "stripe_rejoin_check"):
                await notify_admins(
                    "Stripe rejoin delivery failed before user message.\n\n"
                    f"delivery_type: {delivery_type}\n"
                    f"delivery_hash: {safe_delivery_hash(delivery_key)}\n"
                    f"stage: {'user_message' if sending_user_message else 'group_permission'}\n"
                    f"error: {type(e).__name__}",
                    alert_key=f"stripe_rejoin_group_error:{safe_delivery_hash(delivery_key)}",
                    severity="CRITICAL",
                )
            elif decision.get("permanently_failed"):
                await notify_permanent_outbox_failure(
                    delivery_key, delivery_type, attempt_count, e,
                    blocked=decision.get("blocked", False),
                    telegram_id=telegram_id,
                    payload=payload,
                    reason=decision.get("reason"),
                )
            elif decision.get("retryable"):
                await notify_retryable_outbox_failure(
                    delivery_key, delivery_type, attempt_count, e, decision, retryable_escalation,
                    telegram_id=telegram_id,
                    payload=payload,
                )

    return {
        "sent": sent,
        "retryable_failed": retryable_failed,
        "permanently_failed": permanently_failed,
        "blocked": blocked,
    }


async def scheduled_process_message_deliveries():
    return await run_scheduled_with_lock(
        "process_message_deliveries",
        five_minute_schedule_slot(),
        process_pending_message_deliveries,
        lease_minutes=20,
    )


async def apply_reserved_gifts(limit=50):
    return {"applied": 0, "skipped": 0}


def register_scheduler_jobs_once():
    global SCHEDULER_JOBS_REGISTERED
    if SCHEDULER_JOBS_REGISTERED:
        return
    scheduler.add_job(
        scheduled_check_subscriptions_and_reminders,
        'cron',
        hour=10,
        minute=0,
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1
    )

    scheduler.add_job(
        scheduled_check_auto_free_lessons,
        'cron',
        minute=15,
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1
    )

    scheduler.add_job(
        scheduled_check_free_lesson_followups,
        'cron',
        minute=30,
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1
    )

    scheduler.add_job(
        scheduled_send_db_backup,
        'cron',
        day_of_week='mon',
        hour=3,
        minute=0,
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1
    )

    scheduler.add_job(
        scheduled_process_message_deliveries,
        'cron',
        minute='*/5',
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1
    )

    scheduler.add_job(
        scheduled_enqueue_first_purchase_recovery_reminders,
        'cron',
        minute=45,
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1
    )

    scheduler.add_job(
        scheduled_cleanup_stale_postgres_fsm_storage,
        'cron',
        hour=4,
        minute=10,
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1
    )

    scheduler.add_job(
        send_weekly_admin_report,
        'cron',
        day_of_week='mon',
        hour=10,
        minute=0,
        timezone=MOSCOW_TZ,
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1
    )

    SCHEDULER_JOBS_REGISTERED = True


def start_scheduler_once():
    if not getattr(scheduler, "running", False):
        scheduler.start()
        logging.info("Scheduler started: timezone=%s", SCHEDULER_TZ.key)


async def on_startup(app):
    init_db()

    await bot.set_my_commands([
        types.BotCommand(command="start", description="Запуск бота"),
        types.BotCommand(command="menu", description="Главное меню"),
        types.BotCommand(command="profile", description="Мой профиль и подписка"),
        types.BotCommand(command="gift_status", description="Мои подарки"),
        types.BotCommand(command="ask", description="Задать вопрос"),
    ])

    domain = os.getenv("YOUR_DOMAIN")
    webhook_path = get_telegram_webhook_path()
    safe_webhook_path = get_safe_telegram_webhook_path()
    webhook_url = f"{domain}{webhook_path}"

    await bot.set_webhook(webhook_url, secret_token=WEBHOOK_SECRET)
    webhook_info = await bot.get_webhook_info()
    actual_url = getattr(webhook_info, "url", "")
    pending_update_count = getattr(webhook_info, "pending_update_count", None)
    last_error_message = getattr(webhook_info, "last_error_message", None)
    if actual_url != webhook_url:
        raise ValueError("Telegram webhook URL mismatch after set_webhook")
    logging.info(
        "Webhook установлен: path=%s, pending_update_count=%s, last_error=%s",
        safe_webhook_path,
        pending_update_count,
        last_error_message,
    )

    # register_scheduler_jobs_once keeps scheduled_process_message_deliveries registered once.
    register_scheduler_jobs_once()
    start_scheduler_once()

async def on_shutdown(app):
    if getattr(scheduler, "running", False):
        scheduler.shutdown(wait=False)
    bot_session = getattr(bot, "session", None)
    if bot_session is not None and hasattr(bot_session, "close"):
        await bot_session.close()
    elif hasattr(bot, "close"):
        await bot.close()
    await storage.close()
    close_db_pool()
    logging.info("Бот остановлен.")


async def health(request):
    return web.json_response({
        "ok": True,
        "db": db_pool_health(),
    })


def _route_exists(app, method, path):
    expected_method = method.upper()
    for route in app.router.routes():
        resource = getattr(route, "resource", None)
        canonical = getattr(resource, "canonical", None)
        if canonical == path and route.method == expected_method:
            return True
    return False


def create_app():
    app = web.Application()
    telegram_path = get_telegram_webhook_path()
    if not _route_exists(app, "POST", telegram_path):
        SimpleRequestHandler(
            dispatcher=dp,
            bot=bot,
            secret_token=WEBHOOK_SECRET,
        ).register(app, path=telegram_path)
    if not _route_exists(app, "POST", "/stripe-payment"):
        app.router.add_post('/stripe-payment', stripe_webhook)
    if not _route_exists(app, "GET", "/health"):
        app.router.add_get('/health', health)
    setup_application(app, dp, bot=bot)
    if on_startup not in app.on_startup:
        app.on_startup.append(on_startup)
    if on_shutdown not in app.on_shutdown:
        app.on_shutdown.append(on_shutdown)
    return app


if __name__ == "__main__":
    app = create_app()

    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host='0.0.0.0', port=port, access_log=None)
