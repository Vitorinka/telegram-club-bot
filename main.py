import os
import logging
import asyncio
import io
import stripe
import psycopg2
import subprocess
from datetime import datetime, timedelta, timezone
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.exceptions import BotBlocked
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
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
class PromoStates(StatesGroup):
    waiting_for_media = State()
    waiting_for_text = State()

class ContactState(StatesGroup):
    waiting_for_message = State()


class ReplyState(StatesGroup):
    waiting_for_reply = State()

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Начинаю подключение к базе данных...")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GROUP_ID = os.getenv("GROUP_ID")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
stripe.api_key = os.getenv("STRIPE_API_KEY")

if not DATABASE_URL:
    raise ValueError("Критическая ошибка: DATABASE_URL не задан!")

PHOTO_URL_INTRO = "AgACAgIAAxkBAAMPaee4TD_FGuIQ4LProdOdL5XV5EkAAiYRaxulqkBL5YKQtOj0fV4BAAMCAAN5AAM7BA"
PHOTO_URL_RULES = "AgACAgIAAxkBAAMSaee9wO7psIiqhOR3M52AQ_aRwPgAAjgRaxulqkBLRv00tJs-NW8BAAMCAAN5AAM7BA"

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler()

CHECKOUT_SESSION_COOLDOWN_SECONDS = 10 * 60
CHECKOUT_RETRY_WINDOW_SECONDS = 5 * 60
CHECKOUT_ADMIN_ALERT_COOLDOWN_SECONDS = 15 * 60
PAYMENT_RETRY_GRACE_HOURS = int(os.getenv("PAYMENT_RETRY_GRACE_HOURS", "48"))
checkout_session_cache = {}
checkout_retry_state = {}
checkout_session_cache_lock = asyncio.Lock()

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

# --- ФУНКЦИИ БАЗЫ ДАННЫХ ---
def get_db_conn():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_db():
    conn = get_db_conn()
    cur = conn.cursor()
    # Основная таблица пользователей
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            paid BOOLEAN DEFAULT FALSE,
            expiry_date TIMESTAMP,
            stripe_subscription_id TEXT,
            stripe_customer_id TEXT,
            reminder_sent BOOLEAN DEFAULT FALSE,
            payment_failed BOOLEAN DEFAULT FALSE,
            payment_failed_at TIMESTAMP,
            last_payment_succeeded_at TIMESTAMP,
            grace_period_end TIMESTAMP,
            auto_renew BOOLEAN DEFAULT TRUE,
            trial_used BOOLEAN DEFAULT FALSE,
            first_payment_done BOOLEAN DEFAULT FALSE
        );
    """)
    # Таблица для идемпотентности вебхуков Stripe
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_events (
            event_id TEXT PRIMARY KEY,
            processed BOOLEAN DEFAULT TRUE,
            processed_at TIMESTAMP DEFAULT NOW()
        );
    """)
    # История ручных действий и синхронизаций по доступу
    cur.execute("""
        CREATE TABLE IF NOT EXISTS access_events (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            event_type TEXT NOT NULL,
            source TEXT,
            old_expiry TIMESTAMP,
            new_expiry TIMESTAMP,
            stripe_event_id TEXT,
            stripe_subscription_id TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_links (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            stripe_customer_id TEXT,
            stripe_subscription_id TEXT,
            customer_email TEXT,
            status TEXT,
            current_period_end TIMESTAMP,
            is_active BOOLEAN DEFAULT FALSE,
            source TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (telegram_id, stripe_customer_id, stripe_subscription_id)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS unlinked_stripe_events (
            id SERIAL PRIMARY KEY,
            event_id TEXT UNIQUE,
            event_type TEXT,
            invoice_id TEXT,
            stripe_customer_id TEXT,
            stripe_subscription_id TEXT,
            customer_email TEXT,
            amount_paid BIGINT,
            currency TEXT,
            billing_reason TEXT,
            period_end TIMESTAMP,
            raw_summary TEXT,
            resolved BOOLEAN DEFAULT FALSE,
            resolved_by BIGINT,
            resolved_telegram_id BIGINT,
            resolved_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS payment_events (
            id BIGSERIAL PRIMARY KEY,
            stripe_event_id TEXT UNIQUE NOT NULL,
            event_type TEXT NOT NULL,
            telegram_id BIGINT,
            invoice_id TEXT,
            checkout_session_id TEXT,
            stripe_customer_id TEXT,
            stripe_subscription_id TEXT,
            payment_status TEXT NOT NULL,
            payment_kind TEXT,
            billing_reason TEXT,
            tariff_code TEXT,
            amount_paid BIGINT DEFAULT 0,
            amount_due BIGINT DEFAULT 0,
            currency TEXT,
            period_start TIMESTAMP,
            period_end TIMESTAMP,
            recovered_after_failure BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS payment_events_created_at_idx
        ON payment_events (created_at);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS payment_events_telegram_id_idx
        ON payment_events (telegram_id);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS payment_events_status_kind_idx
        ON payment_events (payment_status, payment_kind);
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weekly_report_runs (
            report_key TEXT PRIMARY KEY,
            period_start TIMESTAMP NOT NULL,
            period_end TIMESTAMP NOT NULL,
            status TEXT NOT NULL,
            sent_admin_ids TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            completed_at TIMESTAMP,
            error_text TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY,
            value_text TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    # Добавляем недостающие колонки (для старых БД)
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS payment_failed BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS payment_failed_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_payment_succeeded_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS grace_period_end TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS auto_renew BOOLEAN DEFAULT TRUE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS trial_used BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_payment_done BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS registered_at TIMESTAMP DEFAULT NOW();")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS blocked_bot BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS video_sent BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS video_sent_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS feedback_sent BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS feedback_sent_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS feedback_received BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_updated_at TIMESTAMP;")
    cur.execute("ALTER TABLE stripe_links ADD COLUMN IF NOT EXISTS customer_email TEXT;")
    cur.execute("ALTER TABLE stripe_links ADD COLUMN IF NOT EXISTS status TEXT;")
    cur.execute("ALTER TABLE stripe_links ADD COLUMN IF NOT EXISTS current_period_end TIMESTAMP;")
    cur.execute("ALTER TABLE stripe_links ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE stripe_links ADD COLUMN IF NOT EXISTS source TEXT;")
    cur.execute("ALTER TABLE stripe_links ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();")
    cur.execute("ALTER TABLE unlinked_stripe_events ADD COLUMN IF NOT EXISTS resolved BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE unlinked_stripe_events ADD COLUMN IF NOT EXISTS resolved_by BIGINT;")
    cur.execute("ALTER TABLE unlinked_stripe_events ADD COLUMN IF NOT EXISTS resolved_telegram_id BIGINT;")
    cur.execute("ALTER TABLE unlinked_stripe_events ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP;")
    cur.execute("ALTER TABLE weekly_report_runs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();")
    cur.execute("""
        INSERT INTO system_settings (key, value_text)
        VALUES ('payment_history_started_at', NOW()::TEXT)
        ON CONFLICT (key) DO NOTHING;
    """)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("--- БД ИНИЦИАЛИЗИРОВАНА И ПРОВЕРЕНА ---")

# Идемпотентность вебхуков
async def is_event_processed(event_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM stripe_events WHERE event_id = %s AND processed IS TRUE", (event_id,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists

async def claim_event_processing(event_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        claim_result = claim_stripe_event(cur, event_id)
        conn.commit()
        return claim_result
    finally:
        cur.close()
        conn.close()

async def mark_event_processed(event_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        mark_stripe_event_processed(cur, event_id)
        conn.commit()
    finally:
        cur.close()
        conn.close()

async def release_event_processing(event_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        release_stripe_event_claim(cur, event_id)
        conn.commit()
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
    if payment_kind in ("trial", "initial_subscription", "recurring", "adjustment", "out_of_band"):
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
        invite = await bot.create_chat_invite_link(chat_id=int(GROUP_ID), member_limit=1)
        return invite.invite_link
    except Exception as e:
        logging.error(f"Ошибка создания ссылки: {e}")
        return None

def get_tariffs_keyboard(show_trial=True):
    kb = InlineKeyboardMarkup(row_width=1)
    if show_trial:
        kb.add(InlineKeyboardButton("🌟 Пробная неделя", callback_data="sub_trial"))
    kb.add(
        InlineKeyboardButton("💳 1 месяц (50€)", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев (240€)", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев (410€)", callback_data="sub_12")
    )
    return kb

def get_cancel_subscription_keyboard():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("❌ Отменить подписку", callback_data="cancel_subscription"))
    return kb


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
    now_timestamp = datetime.utcnow().timestamp()
    retry_state = checkout_retry_state.setdefault(
        user_id,
        {"attempts": [], "last_admin_alert_at": None}
    )
    retry_state["attempts"] = [
        attempt
        for attempt in retry_state["attempts"]
        if now_timestamp - attempt["timestamp"] < CHECKOUT_RETRY_WINDOW_SECONDS
    ]
    retry_state["attempts"].append({"timestamp": now_timestamp, "sub_type": sub_type})
    retry_state["username"] = telegram_user.username
    retry_state["first_name"] = telegram_user.first_name
    retry_state["last_name"] = telegram_user.last_name

    attempt_count = len(retry_state["attempts"])
    if attempt_count >= 2:
        logging.warning(
            f"Checkout retry detected: user_id={user_id}, sub_type={sub_type}, "
            f"attempts_in_window={attempt_count}, window_seconds={CHECKOUT_RETRY_WINDOW_SECONDS}"
        )

    return attempt_count, now_timestamp


async def notify_admins_about_checkout_retry(user_id, sub_type, attempt_count, session_id, attempt_timestamp):
    retry_state = checkout_retry_state.get(int(user_id))
    if not retry_state or attempt_count < 2:
        return

    if not ADMIN_IDS:
        logging.warning(
            f"Checkout retry admin alert skipped: ADMIN_IDS не настроен, user_id={user_id}, "
            f"sub_type={sub_type}, attempts={attempt_count}"
        )
        return

    last_admin_alert_at = retry_state.get("last_admin_alert_at")
    if last_admin_alert_at and attempt_timestamp - last_admin_alert_at < CHECKOUT_ADMIN_ALERT_COOLDOWN_SECONDS:
        return

    username = retry_state.get("username")
    username_text = f"@{username}" if username else "нет"
    name_parts = [retry_state.get("first_name"), retry_state.get("last_name")]
    name_text = " ".join(part for part in name_parts if part) or "нет"
    attempt_time_text = datetime.utcfromtimestamp(attempt_timestamp).strftime("%d.%m.%Y %H:%M:%S UTC")

    await notify_admins(
        "Возможная проблема с оплатой\n\n"
        "Пользователь несколько раз открыл оплату, но успешной оплаты пока нет.\n\n"
        f"Telegram ID: {user_id}\n"
        f"Username: {username_text}\n"
        f"Имя: {name_text}\n"
        f"Тариф: {sub_type}\n"
        f"Попыток за последние 5 минут: {attempt_count}\n"
        f"Последняя session_id: {session_id}\n"
        f"Время последней попытки: {attempt_time_text}\n\n"
        "Возможная причина: Stripe Checkout сбрасывается во встроенном браузере Telegram. "
        "Пользователю отправлена инструкция открыть оплату во внешнем браузере."
    )
    retry_state["last_admin_alert_at"] = attempt_timestamp
    logging.info(
        f"Admin checkout issue alert sent: user_id={user_id}, sub_type={sub_type}, "
        f"attempts={attempt_count}, session_id={session_id}"
    )


def reset_checkout_retry_state_after_success(user_id, source):
    user_id = int(user_id)
    clear_cached_checkout_sessions_for_user(user_id)
    checkout_retry_state.pop(user_id, None)
    logging.info(
        f"Checkout retry state reset after successful payment: user_id={user_id}, source={source}"
    )


async def send_checkout_open_instruction(callback, checkout_url, user_id, session_id, sub_type, mode, reused=False):
    payment_keyboard = InlineKeyboardMarkup(row_width=1).add(
        InlineKeyboardButton("💳 Перейти к оплате", url=checkout_url),
        InlineKeyboardButton("🔙 Назад к тарифам", callback_data="back_to_tariffs")
    )
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

async def notify_admins(text: str):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"⚠️ {text}")
        except Exception:
            pass


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
    return InlineKeyboardMarkup(row_width=1).add(
        InlineKeyboardButton("📄 Скачать CSV покупок", callback_data=f"weekly_csv:{key}"),
        InlineKeyboardButton("🔄 Обновить отчёт", callback_data=f"weekly_refresh:{key}"),
    )


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


def complete_weekly_report_run(cur, key, sent_admin_ids):
    cur.execute("""
        UPDATE weekly_report_runs
        SET status = 'completed',
            sent_admin_ids = %s,
            updated_at = NOW(),
            completed_at = NOW(),
            error_text = NULL
        WHERE report_key = %s
    """, (",".join(str(admin_id) for admin_id in sent_admin_ids), key))


def fail_weekly_report_run(cur, key, error_text):
    cur.execute("""
        UPDATE weekly_report_runs
        SET status = 'failed',
            updated_at = NOW(),
            completed_at = NOW(),
            error_text = %s
        WHERE report_key = %s
    """, (str(error_text)[:500], key))


def save_weekly_report_sent_admin(cur, key, sent_admin_ids):
    cur.execute("""
        UPDATE weekly_report_runs
        SET sent_admin_ids = %s,
            updated_at = NOW()
        WHERE report_key = %s
    """, (",".join(str(admin_id) for admin_id in sent_admin_ids), key))


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
            "errors": [],
        }

    sent_admin_ids = list(claim_result.get("sent_admin_ids", []))
    errors = []
    try:
        text, _ = await build_weekly_admin_report(period_start, period_end)
        keyboard = _weekly_report_keyboard(key)
        for admin_id in ADMIN_IDS:
            if admin_id in sent_admin_ids:
                continue
            try:
                await bot.send_message(admin_id, text, reply_markup=keyboard)
                sent_admin_ids.append(admin_id)
                conn = get_db_conn()
                cur = conn.cursor()
                try:
                    save_weekly_report_sent_admin(cur, key, sent_admin_ids)
                    conn.commit()
                finally:
                    cur.close()
                    conn.close()
            except Exception as e:
                logging.error("WEEKLY_ADMIN_REPORT_SEND_FAILED: admin_id=%s, report_key=%s, error=%s", admin_id, key, e)
                errors.append(f"{admin_id}: {e}")
    except Exception as e:
        logging.exception("WEEKLY_ADMIN_REPORT_BUILD_FAILED: report_key=%s, error=%s", key, e)
        errors.append(str(e))

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        if sent_admin_ids:
            complete_weekly_report_run(cur, key, sent_admin_ids)
        else:
            fail_weekly_report_run(cur, key, "; ".join(errors) or "unknown error")
        conn.commit()
    finally:
        cur.close()
        conn.close()
    status = "failed"
    if sent_admin_ids and errors:
        status = "partial"
    elif sent_admin_ids:
        status = "completed"
    return {
        "status": status,
        "report_key": key,
        "sent_admin_ids": sent_admin_ids,
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


async def send_rejoin_invite_after_payment(telegram_id, expiry_date, source, stripe_event_id=None, stripe_subscription_id=None):
    try:
        try:
            await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
        except Exception as e:
            if "administrator" in str(e).lower():
                logging.warning(f"Не удалось разбанить админа {telegram_id}: {e}")
            else:
                logging.warning(
                    "ACCESS_REJOIN_UNBAN_FAILED_AFTER_PAYMENT: telegram_id=%s, source=%s, "
                    "stripe_event_id=%s, stripe_subscription_id=%s, error=%s",
                    telegram_id,
                    source,
                    safe_log_id(stripe_event_id),
                    safe_log_id(stripe_subscription_id),
                    str(e),
                    exc_info=True,
                )

        invite_link = await generate_invite_link()
        if not invite_link:
            raise RuntimeError("invite_link_not_created")

        expiry_text = expiry_date.strftime("%d.%m.%Y") if expiry_date else "активен"
        await bot.send_message(
            int(telegram_id),
            "✅ Оплата прошла успешно, доступ восстановлен.\n\n"
            f"Ваш доступ активен до {expiry_text}.\n\n"
            "Вот новая ссылка для входа в клуб:\n"
            f"{invite_link}"
        )
        logging.info(
            "ACCESS_REJOIN_INVITE_SENT_AFTER_PAYMENT: telegram_id=%s, source=%s, "
            "stripe_event_id=%s, stripe_subscription_id=%s, expiry_date=%s",
            telegram_id,
            source,
            safe_log_id(stripe_event_id),
            safe_log_id(stripe_subscription_id),
            expiry_date,
        )
        await log_access_event(
            telegram_id,
            "rejoin_invite_sent_after_payment",
            source=source,
            new_expiry=expiry_date,
            stripe_event_id=stripe_event_id,
            stripe_subscription_id=stripe_subscription_id,
            notes="invite link sent after payment"
        )
        return True
    except BotBlocked as e:
        conn = get_db_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                (int(telegram_id),)
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        logging.error(
            "ACCESS_REJOIN_INVITE_FAILED_AFTER_PAYMENT: telegram_id=%s, source=%s, "
            "stripe_event_id=%s, stripe_subscription_id=%s, error=%s",
            telegram_id,
            source,
            safe_log_id(stripe_event_id),
            safe_log_id(stripe_subscription_id),
            "BotBlocked",
            exc_info=True,
        )
        await notify_admins(
            "Оплата прошла, но не удалось отправить пользователю ссылку для входа.\n\n"
            f"telegram_id: {telegram_id}\n"
            f"source: {source}\n"
            f"subscription_id: {stripe_subscription_id or 'нет'}\n"
            f"ошибка: BotBlocked"
        )
        return False
    except Exception as e:
        logging.error(
            "ACCESS_REJOIN_INVITE_FAILED_AFTER_PAYMENT: telegram_id=%s, source=%s, "
            "stripe_event_id=%s, stripe_subscription_id=%s, error=%s",
            telegram_id,
            source,
            safe_log_id(stripe_event_id),
            safe_log_id(stripe_subscription_id),
            str(e),
            exc_info=True,
        )
        await notify_admins(
            "Оплата прошла, но не удалось отправить пользователю ссылку для входа.\n\n"
            f"telegram_id: {telegram_id}\n"
            f"source: {source}\n"
            f"subscription_id: {stripe_subscription_id or 'нет'}\n"
            f"ошибка: {e}"
        )
        return False


def is_undeliverable_user_error(error):
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
        logging.error(
            "OPEN_INVOICE_LOOKUP_FAILED: stripe_subscription_id=%s, error=%s",
            safe_log_id(stripe_subscription_id),
            str(e),
            exc_info=True,
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
        logging.error(
            "BILLING_PORTAL_CREATE_FAILED: stripe_customer_id=%s, error=%s",
            safe_log_id(stripe_customer_id),
            str(e),
            exc_info=True,
        )
        return None


async def send_existing_subscription_action(callback, user_id, stripe_subscription_id, stripe_customer_id, status, current_period_end=None):
    invoice_url, invoice_id = await get_open_invoice_url_for_subscription(stripe_subscription_id)
    if invoice_url:
        kb = InlineKeyboardMarkup(row_width=1).add(
            InlineKeyboardButton("💳 Оплатить открытый счёт", url=invoice_url)
        )
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
        kb = InlineKeyboardMarkup(row_width=1).add(
            InlineKeyboardButton("💳 Управлять оплатой", url=portal_url)
        )
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
        f"stripe_customer_id: {stripe_customer_id or 'нет'}\n"
        f"stripe_subscription_id: {stripe_subscription_id or 'нет'}\n"
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


async def refresh_active_stripe_subscription(telegram_id, stripe_subscription_id, cur):
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
                f"telegram_id={telegram_id}, stripe_subscription_id={stripe_subscription_id}"
            )
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
            return "STRIPE_ACTIVE"

        if status in ('active', 'trialing') and current_period_end:
            new_expiry = datetime.utcfromtimestamp(current_period_end)

            if new_expiry > datetime.utcnow():
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

                logging.info(
                    f"Пользователь {telegram_id} не удален: Stripe подписка активна до {new_expiry} UTC."
                )
                return "STRIPE_ACTIVE"

    except Exception as e:
        logging.error(
            f"Не удалось перепроверить Stripe-подписку {safe_log_id(stripe_subscription_id)} "
            f"для {telegram_id}: {e}"
        )
        await notify_admins(
            f"Не смогла перепроверить Stripe перед удалением пользователя {telegram_id}.\n"
            f"subscription_id: {stripe_subscription_id}\n"
            f"Ошибка: {e}\n\n"
            "Пользователь пока НЕ удален автоматически. Проверьте вручную."
        )
        return "STRIPE_CHECK_FAILED"

    return False

async def ban_user_logic(telegram_id, cur):
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
    user = cur.fetchone()

    if not user:
        logging.warning(
            "USER_REMOVE_SKIPPED_SAFETY_CHECK: telegram_id=%s, reason=%s, paid=%s, "
            "expiry_date=%s, grace=%s, auto_renew=%s, stripe_subscription_id=%s",
            telegram_id, "user_not_found", None, None, None, None, None
        )
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
    now = datetime.utcnow()
    reason = "subscription_expired"
    grace = grace_period_end

    if paid and expiry_date and expiry_date > now:
        logging.warning(
            "USER_REMOVE_SKIPPED_SAFETY_CHECK: telegram_id=%s, reason=%s, paid=%s, "
            "expiry_date=%s, grace=%s, auto_renew=%s, stripe_subscription_id=%s",
            telegram_id, "active_access_in_db", paid, expiry_date, grace, auto_renew, stripe_subscription_id
        )
        return "active_in_db"

    if payment_failed and payment_failed_at:
        retry_until = payment_failed_at + timedelta(hours=PAYMENT_RETRY_GRACE_HOURS)
        if now < retry_until:
            logging.warning(
                "USER_REMOVE_SKIPPED_RECENT_PAYMENT_FAILURE: telegram_id=%s, email=%s, "
                "payment_failed_at=%s, grace_until=%s, expiry_date=%s, stripe_subscription_id=%s",
                telegram_id, None, payment_failed_at, retry_until, expiry_date, stripe_subscription_id
            )
            return "recent_payment_failure"

    if grace_period_end and now < grace_period_end:
        logging.warning(
            "USER_REMOVE_SKIPPED_SAFETY_CHECK: telegram_id=%s, reason=%s, paid=%s, "
            "expiry_date=%s, grace=%s, auto_renew=%s, stripe_subscription_id=%s",
            telegram_id, "grace_period_active", paid, expiry_date, grace_period_end, auto_renew, stripe_subscription_id
        )
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
            f"stripe_customer_id: {stripe_customer_id or 'нет'}\n"
            f"stripe_subscription_id: {stripe_subscription_id or 'нет'}\n\n"
            "Нужно вручную проверить Stripe и связать пользователя командой "
            "/link_stripe_user <telegram_id> <customer_id> <subscription_id>."
        )
        return "STRIPE_UNLINKED_REVIEW"

    if auto_renew and has_valid_stripe_subscription_id(stripe_subscription_id):
        logging.warning(
            "USER_REMOVE_STRIPE_RECHECK_REQUIRED: telegram_id=%s, reason=%s, paid=%s, "
            "expiry_date=%s, grace=%s, auto_renew=%s, stripe_subscription_id=%s",
            telegram_id, "auto_renew_with_stripe_subscription_needs_recheck", paid, expiry_date,
            grace_period_end, auto_renew, stripe_subscription_id
        )

    stripe_guard_status = await refresh_active_stripe_subscription(telegram_id, stripe_subscription_id, cur)
    if stripe_guard_status:
        return stripe_guard_status

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
        await bot.kick_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
        try:
            await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
            unban_result = "unban_success"
        except Exception as e:
            unban_result = f"unban_failed: {e}"
            logging.error(
                "USER_UNBAN_AFTER_REMOVE_FAILED: telegram_id=%s, username=%s, chat_id=%s, reason=%s, error=%s",
                telegram_id, None, GROUP_ID, reason, str(e), exc_info=True
            )
            await notify_admins(
                f"Пользователь {telegram_id} удален из группы, но не удалось снять бан.\n"
                f"Ошибка: {e}"
            )
        logging.warning(
            "USER_REMOVED_FROM_GROUP: telegram_id=%s, username=%s, chat_id=%s, reason=%s, result=%s",
            telegram_id, None, GROUP_ID, reason, unban_result
        )
        await notify_admins(
            "Пользователь удалён из группы ботом.\n\n"
            f"Telegram ID: {telegram_id}\n"
            f"Username: нет\n"
            f"Причина: {reason}\n"
            f"Подписка до: {expiry_date or 'нет'}\n"
            f"Grace: {grace or 'нет'}\n"
            f"Auto-renew: {auto_renew}\n"
            f"Stripe subscription: {stripe_subscription_id or 'нет'}"
        )
    except Exception as e:
        logging.error(
            "USER_REMOVE_FAILED: telegram_id=%s, username=%s, chat_id=%s, reason=%s, error=%s",
            telegram_id, None, GROUP_ID, reason, str(e), exc_info=True
        )
        await notify_admins(
            f"Не удалось удалить пользователя {telegram_id} из группы.\n"
            f"Ошибка: {e}\n\n"
            "Пользователь мог остаться в группе. Проверьте вручную."
        )
        status = "kick_failed"

    # 2. В любом случае закрываем доступ в базе
    cur.execute("""
        UPDATE users 
        SET paid = FALSE,
            payment_failed = FALSE,
            payment_failed_at = NULL,
            grace_period_end = NULL,
            reminder_sent = FALSE
        WHERE telegram_id = %s
    """, (int(telegram_id),))

    # 3. Пытаемся уведомить пользователя
    try:
        await bot.send_message(
            int(telegram_id),
            "⚠️ Ваша подписка истекла. Доступ закрыт.\n"
            "Вы можете оформить новую подписку в любое время.",
            reply_markup=get_tariffs_keyboard(show_trial=False)
        )
    except BotBlocked:
        cur.execute(
            "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
            (int(telegram_id),)
        )
        logging.info(
            f"Пользователь {telegram_id} заблокировал бота: сообщение об окончании доступа "
            "не отправлено, но доступ уже закрыт в БД."
        )
        return status
    except Exception as e:
        logging.error(f"Не удалось отправить сообщение об окончании доступа пользователю {telegram_id}: {e}")
        if is_undeliverable_user_error(e):
            cur.execute(
                "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                (int(telegram_id),)
            )
            logging.info(
                f"Пользователь {telegram_id}: сообщение об окончании доступа недоставляемо, "
                "но доступ уже закрыт в БД."
            )
            return status

        await notify_critical_delivery_failed(
            telegram_id,
            "subscription_expired",
            "сообщение об окончании подписки",
            e,
            "paid = FALSE; доступ закрыт в БД"
        )

    return status
        
async def check_subscriptions_and_reminders():
    logging.info("--- Запуск ежедневной проверки подписок ---")
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT telegram_id, expiry_date, payment_failed, payment_failed_at, grace_period_end, auto_renew, reminder_sent, trial_used, stripe_subscription_id, stripe_customer_id
        FROM users
        WHERE paid = TRUE
          AND expiry_date IS NOT NULL
          AND (blocked_bot IS NOT TRUE)
          AND (
              (expiry_date > NOW() AND expiry_date < NOW() + INTERVAL '2 days')
              OR (
                  expiry_date < NOW()
                  AND expiry_date > NOW() - INTERVAL '2 days'
                  AND (grace_period_end IS NULL OR grace_period_end > NOW())
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
          AND (
              (grace_period_end IS NOT NULL AND grace_period_end < NOW())
              OR (
                  grace_period_end IS NULL
                  AND expiry_date <= NOW() - INTERVAL '2 days'
              )
          )
    """)
    removal_users = cur.fetchall()
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
                "reason": "expired_after_grace_or_2_days",
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
                f"   stripe_customer_id: {user_info.get('stripe_customer_id') or 'нет'}",
                f"   stripe_subscription_id: {user_info.get('stripe_subscription_id') or 'нет'}",
            ])
            if user_info.get("reason"):
                lines.append(f"   причина: {user_info['reason']}")

        if len(users) > 10:
            lines.append(f"...и еще {len(users) - 10} пользователей")

        return "\n".join(lines)

    for (telegram_id, expiry, payment_failed, payment_failed_at, grace_end, auto_renew, reminder_sent, _, stripe_subscription_id, stripe_customer_id) in reminder_users:
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
                logging.info(
                    f"GRACE_USER: telegram_id={telegram_id} пропущен из-за активного grace_period_end={fmt_report_dt(grace_end)}"
                )
                continue

            # Общий льготный период 2 дня
            if -time_left.total_seconds() < 2 * 86400:
                grace_total += 1
                grace_user = build_report_user(
                    telegram_id,
                    expiry,
                    stripe_subscription_id,
                    stripe_customer_id,
                    "пользователь находится в 2-дневном льготном периоде"
                )
                grace_user_details.append(grace_user)
                log_report_user("GRACE_USER", grace_user)

                if auto_renew and has_valid_stripe_subscription_id(stripe_subscription_id):
                    stripe_guard_status = await refresh_active_stripe_subscription(telegram_id, stripe_subscription_id, cur)
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
                            "⏳ Ваша подписка истекла, но у вас есть 2 дня, чтобы продлить доступ без потери истории.\n"
                            "Пожалуйста, продлите подписку как можно скорее.",
                            reply_markup=get_tariffs_keyboard(show_trial=False))
                        cur.execute("UPDATE users SET reminder_sent = TRUE WHERE telegram_id = %s", (telegram_id,))
                        reminders_sent += 1
                    except Exception as e:
                        reminder_errors += 1
                        telegram_errors += 1
                        logging.warning(f"Не удалось отправить сообщение пользователю {telegram_id}: {e}")
                        if is_undeliverable_user_error(e):
                            cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (telegram_id,))

        # ----- Напоминание за 48 часов -----
        elif timedelta(0) < time_left < timedelta(days=2):
            if auto_renew and stripe_subscription_id:
                logging.info(
                    f"Пользователь {telegram_id}: напоминание за 48 часов пропущено, потому что включено auto_renew."
                )
            elif not reminder_sent and not auto_renew:
                text = "⏳ Ваша подписка заканчивается через 48 часов. Продлите доступ, чтобы не потерять связь с клубом."
                try:
                    await bot.send_message(telegram_id, text, reply_markup=get_tariffs_keyboard(show_trial=False))
                    cur.execute("UPDATE users SET reminder_sent = TRUE WHERE telegram_id = %s", (telegram_id,))
                    reminders_sent += 1
                except Exception as e:
                    reminder_errors += 1
                    telegram_errors += 1
                    logging.warning(f"Не удалось отправить напоминание пользователю {telegram_id}: {e}")
                    if is_undeliverable_user_error(e):
                        cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (telegram_id,))

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

        if has_valid_stripe_subscription_id(stripe_subscription_id):
            removal_reason = "STRIPE_INACTIVE_OR_EXPIRED — proceed to removal"
            stripe_guard_status = await refresh_active_stripe_subscription(telegram_id, stripe_subscription_id, cur)
            if stripe_guard_status:
                cur.execute(
                    "SELECT expiry_date, stripe_customer_id FROM users WHERE telegram_id = %s",
                    (telegram_id,)
                )
                row = cur.fetchone()
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
        else:
            logging.info(
                f"NO_STRIPE_SUBSCRIPTION_ID — proceed to removal. telegram_id={telegram_id}, "
                f"stripe_subscription_id={stripe_subscription_id or 'нет'}"
            )

        ban_status = await ban_user_logic(telegram_id, cur)

        if ban_status == "active_in_db":
            active_in_db_skipped += 1
        elif ban_status in ("STRIPE_ACTIVE", "STRIPE_CHECK_FAILED", "STRIPE_UNLINKED_REVIEW"):
            cur.execute(
                "SELECT expiry_date, stripe_customer_id FROM users WHERE telegram_id = %s",
                (telegram_id,)
            )
            row = cur.fetchone()
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
        elif ban_status in ("recent_payment_failure", "grace_active"):
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
        elif ban_status in ("removed", "kick_failed"):
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
            if ban_status == "kick_failed":
                telegram_errors += 1
                logging.error(f"DELETED_USER: не получилось удалить из группы telegram_id={telegram_id}")

    conn.commit()
    cur.close()
    conn.close()

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
            f"Ошибки Telegram: {telegram_errors}"
        )

    report_text += format_report_section("🛡 Защищены через Stripe / ошибку Stripe", protected_user_details)
    report_text += format_report_section("⏳ В льготном периоде", grace_user_details)
    report_text += format_report_section("⚠️ Просроченные пользователи", expired_user_details)
    report_text += format_report_section("🚪 Удалены / закрыт доступ", deleted_user_details)

    try:
        await notify_admins(report_text)
    except Exception as e:
        logging.error(f"Не удалось отправить отчет проверки подписок: {e}")

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

        sent = 0
        blocked = 0
        failed = 0

        for (user_id,) in users:
            try:
                await send_free_lesson_followup(user_id, cur)
                sent += 1
            except BotBlocked:
                blocked += 1
                cur.execute(
                    "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                    (int(user_id),)
                )
            except Exception as e:
                failed += 1
                logging.error(f"Ошибка follow-up после бесплатного урока для {user_id}: {e}")

        conn.commit()

        logging.info(
            f"Follow-up после бесплатного урока: отправлено={sent}, "
            f"заблокировали={blocked}, ошибки={failed}"
        )

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка check_free_lesson_followups: {e}")

    finally:
        cur.close()
        conn.close()

# --- БЭКАП БАЗЫ ДАННЫХ ---
async def send_db_backup():
    filename = f"backup_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.sql"
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        await notify_admins("❌ Ошибка бэкапа: DATABASE_URL не задан!")
        return

    # Добавляем sslmode=require для Railway
    conn_string = db_url + "?sslmode=require"

    try:
        process = await asyncio.create_subprocess_exec(
            'pg_dump', conn_string,
            '--no-owner', '--no-privileges',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_msg = stderr.decode('utf-8')
            logging.error(f"pg_dump failed (code {process.returncode}): {error_msg}")
            await notify_admins(f"❌ Ошибка дампа БД. Код: {process.returncode}. Подробности в логах.")
            return

        # Записываем дамп в файл
        with open(filename, 'wb') as f:
            f.write(stdout)

        logging.info(f"Бэкап создан: {filename} (размер: {len(stdout)} байт)")

        # Отправляем файл каждому админу
        for admin_id in ADMIN_IDS:
            try:
                with open(filename, 'rb') as f:
                    await bot.send_document(admin_id, f, caption=f"📦 Бэкап БД от {datetime.now().strftime('%d.%m.%Y %H:%M')}")
            except Exception as e:
                logging.error(f"Не удалось отправить бэкап админу {admin_id}: {e}")

    except Exception as e:
        logging.exception(f"Критическая ошибка бэкапа: {e}")
        await notify_admins(f"❌ Непредвиденная ошибка бэкапа: {e}")
    finally:
        if os.path.exists(filename):
            os.remove(filename)

@dp.message_handler(content_types=['video'], state=None)
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

@dp.message_handler(content_types=['photo'], state=None)
async def reply_with_photo_id(message: types.Message):
    if message.chat.type != 'private':
        return
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Эта команда только для администратора.")
        return
    file_id = message.photo[-1].file_id
    await message.reply(f"Ваш photo file_id:\n`{file_id}`", parse_mode="Markdown")

@dp.message_handler(commands=['promo_trial'], state='*')
async def promo_trial(message: types.Message, state: FSMContext):
    await state.finish()
    logging.info(f"Команда promo_trial от {message.from_user.id}")
    if message.from_user.id not in ADMIN_IDS:
        logging.warning(f"Отказано {message.from_user.id}")
        return
    await PromoStates.waiting_for_media.set()
    await message.reply("📎 Отправьте фото или видео, которое будет в рассылке.\n\n"
                        "Чтобы отменить, отправьте /cancel")

@dp.message_handler(commands=['cancel'], state='*')
async def cancel_handler(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.reply("Нет активного действия для отмены.")
        return
    await state.finish()
    await message.reply("✅ Действие отменено. Можете начать заново.")

@dp.message_handler(content_types=['photo', 'video'], state=PromoStates.waiting_for_media)
async def promo_get_media(message: types.Message, state: FSMContext):
    if message.photo:
        file_id = message.photo[-1].file_id
        media_type = 'photo'
    else:
        file_id = message.video.file_id
        media_type = 'video'
    await state.update_data(media_type=media_type, file_id=file_id)
    await PromoStates.waiting_for_text.set()
    await message.reply("✏️ Теперь отправьте текст сообщения.\n\n"
                        "Можно использовать HTML-разметку (<b>жирный</b>, <i>курсив</i>).")

@dp.message_handler(state=PromoStates.waiting_for_text, content_types=types.ContentTypes.TEXT)
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

    kb = InlineKeyboardMarkup(row_width=2).add(
        InlineKeyboardButton("✅ Да, отправить", callback_data="confirm_promo"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_promo")
    )

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
        
@dp.callback_query_handler(text="confirm_promo", state=PromoStates.waiting_for_text)
async def promo_send(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data['text']
    media_type = data['media_type']
    file_id = data['file_id']

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT telegram_id FROM users WHERE paid = FALSE AND (blocked_bot IS NOT TRUE)")
    users = cur.fetchall()

    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("Начать пробную неделю", callback_data="sub_trial"))

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
        except BotBlocked:
            blocked += 1
            cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (user_id,))
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка промо-рассылки для {user_id}: {e}")

    conn.commit()
    cur.close()
    conn.close()

    await callback.message.answer(
        f"✅ Рассылка завершена.\n"
        f"📨 Успешно: {success}\n"
        f"🚫 Заблокировали бота: {blocked}\n"
        f"⚠️ Другие ошибки: {failed}"
    )
    await state.finish()
    await callback.answer()

@dp.callback_query_handler(text="cancel_promo", state=PromoStates.waiting_for_text)
async def promo_cancel(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Рассылка отменена.")
    await state.finish()
    await callback.answer()

def get_main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(KeyboardButton("🎁 Бесплатный урок"))
    kb.add(
        KeyboardButton("💬 Задать вопрос"),
        KeyboardButton("🆘 Правила клуба")
    )
    kb.add(KeyboardButton("👤 Профиль и подписка"))
    return kb


@dp.message_handler(commands=['menu'], state='*')
async def show_menu(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(
        "Главное меню\n\nВыберите нужный раздел:",
        reply_markup=get_main_keyboard()
    )


@dp.message_handler(text="👤 Профиль и подписка", state='*')
async def profile_button_handler(message: types.Message, state: FSMContext):
    await state.finish()
    await profile(message)


@dp.message_handler(text="🆘 Правила клуба", state='*')
async def rules_button_handler(message: types.Message, state: FSMContext):
    await state.finish()

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

    await message.answer(rules_text, parse_mode="HTML", reply_markup=get_main_keyboard())


@dp.message_handler(text="💬 Задать вопрос", state='*')
async def ask_question_button(message: types.Message, state: FSMContext):
    await state.finish()

    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    kb.add(KeyboardButton("❌ Отмена"))

    await message.answer(
        "💬 Напишите ваш вопрос одним сообщением.\n\n"
        "Я передам его администратору, и вам ответят здесь, в этом чате.",
        reply_markup=kb
    )

    await ContactState.waiting_for_message.set()

@dp.message_handler(state=ContactState.waiting_for_message, content_types=types.ContentTypes.ANY)
async def forward_question_to_admin(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.finish()
        await message.answer(
            "Отправка вопроса отменена.",
            reply_markup=get_main_keyboard()
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

            kb = InlineKeyboardMarkup(row_width=1).add(
                InlineKeyboardButton("✍️ Ответить", callback_data=f"reply_to_{user.id}")
            )

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
            reply_markup=get_main_keyboard()
        )

    except Exception as e:
        logging.error(f"Ошибка отправки вопроса админу от {user.id}: {e}")
        await message.answer(
            "❌ Не удалось отправить вопрос. Попробуйте позже или напишите @re_tasha.",
            reply_markup=get_main_keyboard()
        )

    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("reply_to_"), state='*')
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
    await ReplyState.waiting_for_reply.set()

    await callback.message.answer(
        f"✍️ Отправьте ответ для пользователя {target_user_id} одним сообщением.\n\n"
        f"Можно отправить текст, фото, видео, голосовое или документ.\n"
        f"Чтобы отменить, отправьте /cancel."
    )

    await callback.answer()

@dp.message_handler(state=ReplyState.waiting_for_reply, content_types=types.ContentTypes.ANY)
async def send_admin_reply(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    if message.text in ["/cancel", "❌ Отмена"]:
        await state.finish()
        await message.answer("Ответ отменен.")
        return

    data = await state.get_data()
    target_user_id = data.get("reply_to_user")

    if not target_user_id:
        await state.finish()
        await message.answer("❌ Не найден пользователь для ответа.")
        return

    try:
        await bot.send_message(
            int(target_user_id),
            "💬 Ответ администратора:"
        )

        await bot.copy_message(
            chat_id=int(target_user_id),
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )

        await message.answer(f"✅ Ответ отправлен пользователю {target_user_id}.")
        await state.finish()

    except BotBlocked:
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
        await state.finish()

    except Exception as e:
        logging.error(f"Ошибка отправки ответа пользователю {target_user_id}: {e}")
        await message.answer(f"❌ Не удалось отправить ответ: {e}")
        await state.finish()

@dp.message_handler(commands=['ask'], state='*')
async def ask_command(message: types.Message, state: FSMContext):
    await ask_question_button(message, state)

@dp.callback_query_handler(text="feedback_join", state='*')
async def feedback_join(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

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

        await RegistrationStates.choice.set()

        await callback.message.answer(
            "Отлично. Выберите удобный формат участия:",
            reply_markup=get_tariffs_keyboard(show_trial=show_trial)
        )

        await callback.answer()

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка feedback_join для {user_id}: {e}")
        await callback.answer("Не удалось открыть тарифы. Попробуйте /start.", show_alert=True)

    finally:
        cur.close()
        conn.close()


@dp.callback_query_handler(text="feedback_question", state='*')
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

    await state.finish()

    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    kb.add(KeyboardButton("❌ Отмена"))

    await callback.message.answer(
        "💬 Напишите ваш вопрос одним сообщением.\n\n"
        "Я передам его администратору, и вам ответят здесь, в этом чате.",
        reply_markup=kb
    )

    await ContactState.waiting_for_message.set()
    await callback.answer()


@dp.callback_query_handler(text="feedback_think", state='*')
async def feedback_think(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()

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

        await callback.message.answer(
            "Хорошо, возвращайтесь, когда будет удобно.\n\n"
            "В меню ниже можно открыть тарифы, задать вопрос или посмотреть профиль.",
            reply_markup=get_main_keyboard()
        )

        await callback.answer()

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка feedback_think для {user_id}: {e}")
        await callback.answer("Ошибка. Попробуйте позже.", show_alert=True)

    finally:
        cur.close()
        conn.close()

@dp.message_handler(text="🎁 Бесплатный урок", state='*')
async def free_lesson_button(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

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

        if video_sent:
            conn.commit()
            kb = get_tariffs_keyboard(show_trial=show_trial)

            await message.answer(
                "✅ Вы уже получали бесплатный урок.\n\n"
                "Если вам понравился формат, вы можете оформить доступ к клубу и продолжить занятия:",
                reply_markup=kb
            )
            return

        video_id = os.getenv("FREE_LESSON_VIDEO_ID")

        if not video_id:
            conn.commit()
            await message.answer(
                "🎁 Бесплатный урок скоро появится здесь.\n\n"
                "Пока вы можете посмотреть тарифы и выбрать удобный формат участия.",
                reply_markup=get_tariffs_keyboard(show_trial=show_trial)
            )
            await notify_admins("FREE_LESSON_VIDEO_ID не задан в Railway Variables.")
            return

        caption_text = """<b>Чтобы почувствовать изменения в теле и самочувствии, не нужно усложнять.</b>

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

        kb = InlineKeyboardMarkup(row_width=1).add(
            InlineKeyboardButton("Хочу в клуб", callback_data="sub_trial")
        )

        await bot.send_video(
            chat_id=message.chat.id,
            video=video_id,
            caption=caption_text,
            reply_markup=kb,
            parse_mode="HTML"
        )

        cur.execute("""
            UPDATE users
            SET video_sent = TRUE,
                video_sent_at = NOW()
            WHERE telegram_id = %s
        """, (user_id,))

        conn.commit()

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка free_lesson_button для {user_id}: {e}")
        await message.answer(
            "❌ Не удалось отправить бесплатный урок. Попробуйте позже или напишите @re_tasha.",
            reply_markup=get_main_keyboard()
        )

    finally:
        cur.close()
        conn.close()

def get_free_lesson_feedback_keyboard():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("Хочу в клуб", callback_data="feedback_join"),
        InlineKeyboardButton("Задать вопрос", callback_data="feedback_question"),
        InlineKeyboardButton("Пока думаю", callback_data="feedback_think")
    )
    return kb

async def send_auto_free_lesson(user_id, cur):
    video_id = os.getenv("FREE_LESSON_VIDEO_ID")

    if not video_id:
        await notify_admins("FREE_LESSON_VIDEO_ID не задан в Railway Variables. Автоурок не отправлен.")
        return False

    caption_text = """<b>Я подготовила для вас бесплатную пробную тренировку.</b>

Иногда, чтобы почувствовать больше легкости, подвижности и контакта с телом, не нужен зал, сложное оборудование и час свободного времени. Достаточно коврика и 15 минут правильного движения.

Эта тренировка поможет мягко включиться в практику и почувствовать формат клуба.

<b>Она подойдет, если вы:</b>
— только начинаете тренироваться;
— устали от жестких нагрузок;
— хотите чувствовать тело лучше без перегрузки.

Если вам понравится такой подход, вы сможете попробовать онлайн-клуб и получить доступ к полноценным тренировкам, зарядкам, дыхательным практикам, рецептам и поддержке."""

    kb = InlineKeyboardMarkup(row_width=1).add(
        InlineKeyboardButton("Хочу в клуб", callback_data="sub_trial")
    )

    await bot.send_video(
        chat_id=int(user_id),
        video=video_id,
        caption=caption_text,
        reply_markup=kb,
        parse_mode="HTML"
    )

    cur.execute("""
        UPDATE users
        SET video_sent = TRUE,
            video_sent_at = NOW()
        WHERE telegram_id = %s
    """, (int(user_id),))

    return True

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

        sent = 0
        blocked = 0
        failed = 0

        for (user_id,) in users:
            try:
                was_sent = await send_auto_free_lesson(user_id, cur)
                if was_sent:
                    sent += 1
            except BotBlocked:
                blocked += 1
                cur.execute(
                    "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                    (int(user_id),)
                )
            except Exception as e:
                if is_undeliverable_user_error(e):
                    blocked += 1
                    cur.execute(
                        "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                        (int(user_id),)
                    )
                    logging.info(f"Пользователь {user_id} помечен blocked_bot после ошибки автоурока: {e}")
                else:
                    failed += 1
                    logging.error(f"Ошибка автоотправки бесплатного урока для {user_id}: {e}")

        conn.commit()

        logging.info(
            f"Автоурок: отправлено={sent}, заблокировали={blocked}, ошибки={failed}"
        )

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка check_auto_free_lessons: {e}")

    finally:
        cur.close()
        conn.close()


async def send_free_lesson_followup(user_id, cur):
    text = (
        "Как ощущения после пробной тренировки?\n\n"
        "Удалось почувствовать больше легкости, подвижности или контакта с телом?\n\n"
        "Если вам откликнулся такой формат, вы можете продолжить занятия в клубе: "
        "там собраны тренировки, зарядки, дыхательные практики, рецепты и поддержка.\n\n"
        "Выберите, что вам сейчас ближе:"
    )

    await bot.send_message(
        int(user_id),
        text,
        reply_markup=get_free_lesson_feedback_keyboard()
    )

    cur.execute("""
        UPDATE users
        SET feedback_sent = TRUE,
            feedback_sent_at = NOW()
        WHERE telegram_id = %s
    """, (int(user_id),))

@dp.message_handler(
    content_types=[
        types.ContentType.NEW_CHAT_MEMBERS,
        types.ContentType.LEFT_CHAT_MEMBER
    ],
    state='*'
)
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
        if not service_user_id or getattr(service_user, "is_bot", False):
            continue
        save_telegram_user_profile(service_user)
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
@dp.message_handler(commands=['start'], state='*')
async def start(message: types.Message, state: FSMContext):
    await state.finish()
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

    # Отправка приветствия
    await RegistrationStates.intro.set()
    text = """<b>Добро пожаловать в закрытый клуб Натальи Ребковец.</b>

Здесь тренировки построены на современных знаниях о движении, нейрофизиологии и работе тела.

Силовые тренировки, йога, пилатес, кинезиологические упражнения, работа с дыханием, мобильностью и двигательными паттернами — для сильного, здорового и функционального тела без перегрузки. 

<b>Готовы начать путь к здоровому и сильному телу? Тогда — поехали!</b>"""
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_desc"))
    await bot.send_photo(message.chat.id, PHOTO_URL_INTRO, caption=text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query_handler(text="to_desc", state=RegistrationStates.intro)
async def show_description(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.description.set()
    text = """<b>Внутри клуба вас ждёт:</b>
    
🧠 <b>Библиотека тренировок</b> — 50+ уроков с системным подходом: осанка, сила, мобильность, стопы, гибкость и работа с движением. База регулярно пополняется.

🔋 <b>Короткие зарядки</b> — 10–15 минут для энергии, снятия напряжения и уменьшения отёков.

🧘🏽‍♀️ <b>Медитации и дыхательные практики</b> — для расслабления, восстановления и работы с нервной системой.

🩹 <b>Фитнес-аптечка</b> — короткие уроки для быстрой помощи при боли, напряжении и дискомфорте в теле.

🥗 <b>Раздел с рецептами</b> и обратной связью от врача-нутрициолога.

👩🏽‍💻 <b>Живые Zoom-уроки 2–4 раза в месяц</b> — разбор техники, двигательных паттернов, перекосов и индивидуальная коррекция в формате группы.

💬 <b>Закрытый чат поддержки,</b> — где я лично отвечаю на вопросы."""
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_rules"))
    
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
    
@dp.callback_query_handler(text="to_rules", state=RegistrationStates.description)
async def show_rules(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.rules.set()
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
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_choice"))
    await bot.send_message(callback.message.chat.id, text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query_handler(text="to_choice", state=RegistrationStates.rules)
async def show_choice(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.choice.set()
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
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('sub_'), state='*')
async def process_payment(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("⏳ Проверяем...")
    sub_type = callback.data
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
            stripe_customer_id
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
        await state.finish()
        return

    mode = 'payment' if sub_type == "sub_trial" else 'subscription'

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
                await state.finish()
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
                            expiry_date = %s,
                            payment_failed = FALSE,
                            payment_failed_at = NULL,
                            last_payment_succeeded_at = NOW(),
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            stripe_customer_id = COALESCE(%s, stripe_customer_id),
                            blocked_bot = FALSE
                        WHERE telegram_id = %s
                    """, (new_expiry, customer_id, user_id))
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
                        stripe_subscription_id,
                        customer_id,
                        status,
                        "active_subscription_no_checkout",
                    )
                    cur.close()
                    conn.close()
                    await callback.message.answer(
                        f"✅ У вас уже есть активная подписка до {new_expiry.strftime('%d.%m.%Y %H:%M')}.\n"
                        "Повторная оплата не нужна."
                    )
                    await state.finish()
                    return

            if status in ('active', 'trialing') and not current_period_end:
                cur.execute("""
                    UPDATE users
                    SET stripe_subscription_id = %s,
                        stripe_customer_id = COALESCE(%s, stripe_customer_id),
                        auto_renew = TRUE,
                        payment_failed = FALSE,
                        payment_failed_at = NULL,
                        last_payment_succeeded_at = NOW(),
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
                    f"user_id={user_id}, stripe_subscription_id={stripe_subscription_id}, customer_id={customer_id}"
                )
                logging.warning(
                    "EXISTING_STRIPE_SUBSCRIPTION_FOUND_CHECKOUT_BLOCKED: telegram_id=%s, "
                    "stripe_subscription_id=%s, stripe_customer_id=%s, status=%s, action=%s",
                    user_id,
                    stripe_subscription_id,
                    customer_id,
                    status,
                    "active_subscription_period_unknown_no_checkout",
                )
                cur.close()
                conn.close()
                await callback.message.answer(
                    "✅ У вас уже есть активная подписка.\n"
                    "Повторная оплата не нужна. Если доступ не обновился, напишите администратору."
                )
                await state.finish()
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
                await state.finish()
                return
        except Exception as e:
            logging.error(f"Не удалось проверить Stripe перед Checkout для пользователя {user_id}: {e}")

    cur.close()
    conn.close()

    # Если нажата кнопка пробной недели
    if sub_type == "sub_trial":
        # Если пробный период уже использован ИЛИ у пользователя есть активная подписка
        if trial_used or paid:
            # Показываем клавиатуру с обычными тарифами (без пробного)
            await state.finish()
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

        # Иначе (пробный период не использован) – продолжаем создание оплаты пробной недели
        # (весь код ниже для sub_trial, но он такой же, как для остальных тарифов, поэтому вынесем общую логику)

    # Обработка всех тарифов (включая sub_trial, если прошли проверку)
    price_map = {
        "sub_trial": "PRICE_TRIAL",
        "sub_1": "PRICE_1M",
        "sub_6": "PRICE_6M",
        "sub_12": "PRICE_12M"
    }
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

        async with checkout_session_cache_lock:
            cached_session = get_reusable_checkout_session(cache_key)

            if cached_session:
                reused = True
                session_id = cached_session["session_id"]
                checkout_url = cached_session["checkout_url"]
                cache_age = int(datetime.utcnow().timestamp() - cached_session["cached_at"])
                logging.info(
                    f"New Checkout Session blocked by cooldown: user_id={user_id}, "
                    f"sub_type={sub_type}, existing_session_id={session_id}, cache_age_seconds={cache_age}"
                )
                logging.info(
                    f"Reusing existing Checkout Session: user_id={user_id}, session_id={session_id}, "
                    f"sub_type={sub_type}, mode={mode}"
                )
            else:
                logging.info(
                    f"Создаю Checkout Session: user_id={user_id}, sub_type={sub_type}, "
                    f"mode={mode}, paid={paid}, expiry_date={expiry_date}, "
                    f"stripe_subscription_id={stripe_subscription_id or 'нет'}"
                )
                session = stripe.checkout.Session.create(**session_params)
                session_id = session.id
                checkout_url = session.url

                if not checkout_url:
                    raise ValueError(f"Stripe Checkout Session {session_id} не содержит url")

                checkout_session_cache[cache_key] = {
                    "session_id": session_id,
                    "checkout_url": checkout_url,
                    "cached_at": datetime.utcnow().timestamp(),
                    "expires_at": getattr(session, 'expires_at', None),
                }
                logging.info(
                    f"Checkout Session создана: user_id={user_id}, session_id={session_id}, "
                    f"sub_type={sub_type}, mode={mode}"
                )

        await send_checkout_open_instruction(
            callback,
            checkout_url,
            user_id,
            session_id,
            sub_type,
            mode,
            reused=reused
        )
        await notify_admins_about_checkout_retry(
            user_id,
            sub_type,
            attempt_count,
            session_id,
            attempt_timestamp
        )
        await state.finish()
    except Exception as e:
        logging.exception(
            f"Ошибка создания или отправки Stripe Checkout: user_id={user_id}, "
            f"sub_type={sub_type}, mode={mode}: {e}"
        )
        await callback.answer(
            "Техническая ошибка. Попробуйте позже или напишите @re_tasha",
            show_alert=True
        )

@dp.callback_query_handler(text="retry_payment", state='*')
async def retry_payment(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.choice.set()

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

@dp.callback_query_handler(text="back_to_tariffs", state='*')
async def back_to_tariffs(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.choice.set()
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

@dp.callback_query_handler(text="cancel_subscription", state='*')
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
        stripe.Subscription.modify(sub_id, cancel_at_period_end=True)
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

@dp.message_handler(commands=['profile'], state='*')
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

        kb = InlineKeyboardMarkup(row_width=1)

        if not user:
            kb.add(InlineKeyboardButton("💳 Выбрать тариф", callback_data="retry_payment"))
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
                kb.add(InlineKeyboardButton("❌ Отменить автопродление", callback_data="cancel_subscription"))
            else:
                kb.add(InlineKeyboardButton("💳 Продлить доступ", callback_data="show_renew_options"))

        elif paid and expiry_date and expiry_date <= now:
            delta = now - expiry_date

            if delta < timedelta(days=2):
                status_text = "⏳ Подписка истекла, идет льготный период"
                time_text = f"истекла {delta.days} дн. назад"
            else:
                status_text = "⚠️ Подписка истекла"
                time_text = f"истекла {delta.days} дн. назад"

            kb.add(InlineKeyboardButton("💳 Продлить доступ", callback_data="show_renew_options"))

        else:
            status_text = "❌ Активной подписки нет"
            time_text = "нет активного доступа"
            kb.add(InlineKeyboardButton("💳 Выбрать тариф", callback_data="retry_payment"))

        text = (
            "👤 Ваш профиль\n\n"
            f"{status_text}\n"
            f"📅 Действует до: {expiry_text}\n"
            f"⏳ Срок: {time_text}\n"
            f"🔁 Автопродление: {auto_renew_text}\n\n"
            "Вы можете управлять доступом ниже."
        )

        await message.answer(text, reply_markup=kb)

    except Exception as e:
        logging.error(f"Ошибка profile: {e}")
        await message.answer("❌ Не удалось загрузить профиль. Попробуйте позже или напишите @re_tasha.")

    finally:
        cur.close()
        conn.close()

@dp.callback_query_handler(text="show_renew_options", state='*')
async def show_renew_options(callback: types.CallbackQuery):
    kb = get_tariffs_keyboard(show_trial=False)
    await callback.message.edit_text("Выберите тариф для продления доступа:", reply_markup=kb)
    await callback.answer()

@dp.message_handler(commands=['send_user'], state='*')
async def send_user_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split(maxsplit=1)

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

    except BotBlocked:
        cur.execute(
            "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
            (target_user_id,)
        )
        conn.commit()

        await message.answer("⚠️ Пользователь заблокировал бота. Сообщение не отправлено.")

    except Exception as e:
        logging.error(f"Ошибка send_user для {target_user_id}: {e}")
        await message.answer(
            f"❌ Не удалось отправить сообщение пользователю {target_user_id}.\n\n"
            f"Ошибка: {e}"
        )

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['broadcast'], state='*')
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

    success = 0
    blocked = 0
    failed = 0

    for (user_id,) in users:
        try:
            await bot.send_message(user_id, text)
            success += 1
        except BotBlocked:
            blocked += 1
            cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (user_id,))
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка broadcast для {user_id}: {e}")

    conn.commit()
    cur.close()
    conn.close()

    await message.answer(
        f"Рассылка завершена.\n"
        f"Успешно: {success}\n"
        f"Заблокировали бота: {blocked}\n"
        f"Другие ошибки: {failed}"
    )

@dp.message_handler(commands=['give_access'], state='*')
async def give_access_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()

    if len(args) < 1 or len(args) > 2:
        await message.reply("⚠️ Использование: /give_access <telegram_id> [дней]")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ telegram_id должен быть числом.")
        return

    if len(args) == 2:
        try:
            days = int(args[1])
        except ValueError:
            await message.reply("⚠️ Количество дней должно быть числом.")
            return
    else:
        days = 30

    if days <= 0:
        await message.reply("⚠️ Количество дней должно быть больше 0.")
        return

    if days > 730:
        await message.reply("⚠️ Нельзя выдать доступ больше чем на 730 дней одной командой.")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute(
            "SELECT expiry_date FROM users WHERE telegram_id = %s",
            (target_user_id,)
        )
        row = cur.fetchone()
        old_expiry = row[0] if row else None

        cur.execute("""
            INSERT INTO users (telegram_id, paid, expiry_date, auto_renew)
            VALUES (%s, TRUE, NOW() + INTERVAL '%s days', FALSE)
            ON CONFLICT (telegram_id) DO UPDATE 
            SET paid = TRUE, 
                expiry_date = CASE 
                    WHEN users.expiry_date > NOW() THEN users.expiry_date + INTERVAL '%s days'
                    ELSE NOW() + INTERVAL '%s days'
                END,
                payment_failed = FALSE,
                payment_failed_at = NULL,
                last_payment_succeeded_at = NOW(),
                grace_period_end = NULL,
                blocked_bot = FALSE,
                auto_renew = CASE
                    WHEN users.stripe_subscription_id IS NULL THEN FALSE
                    ELSE users.auto_renew
                END;
        """, (target_user_id, days, days, days))

        cur.execute(
            "SELECT expiry_date FROM users WHERE telegram_id = %s",
            (target_user_id,)
        )
        row = cur.fetchone()
        new_expiry = row[0] if row else None

        conn.commit()

        await log_access_event(
            target_user_id,
            "manual_give_access",
            source="admin_command",
            old_expiry=old_expiry,
            new_expiry=new_expiry,
            notes=f"days={days}; admin_id={message.from_user.id}"
        )

        try:
            await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=target_user_id)
        except Exception as e:
            if "administrator" in str(e).lower():
                logging.warning(f"Не удалось разбанить админа {target_user_id}: {e}")
            else:
                logging.error(f"Ошибка разбана {target_user_id}: {e}")

        link = await generate_invite_link()

        try:
            if link:
                await bot.send_message(
                    target_user_id,
                    f"✅ Администратор предоставил вам доступ на {days} дней!\nСсылка: {link}"
                )
            else:
                await bot.send_message(
                    target_user_id,
                    f"✅ Администратор предоставил вам доступ на {days} дней. Добро пожаловать!"
                )

            await message.answer(f"✅ Доступ пользователю {target_user_id} предоставлен.")

        except BotBlocked:
            cur.execute(
                "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                (target_user_id,)
            )
            conn.commit()
            await notify_critical_delivery_failed(
                target_user_id,
                "give_access",
                "сообщение о вручную выданном доступе",
                "BotBlocked",
                f"Доступ выдан на {days} дней; blocked_bot = TRUE"
            )
            await message.answer("⚠️ Доступ обновлен, но пользователь заблокировал бота.")
        except Exception as e:
            logging.error(f"Не удалось отправить сообщение после /give_access пользователю {target_user_id}: {e}")
            await notify_critical_delivery_failed(
                target_user_id,
                "give_access",
                "сообщение о вручную выданном доступе",
                e,
                f"Доступ выдан на {days} дней"
            )
            await message.answer(
                f"⚠️ Доступ выдан, но не удалось отправить сообщение пользователю {target_user_id}.\n\n"
                f"Ошибка: {e}"
            )

    except Exception as e:
        conn.rollback()
        await message.answer(f"❌ Ошибка: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['set_expiry'], state='*')
async def set_expiry_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()

    if len(args) not in (2, 3):
        await message.reply("⚠️ Использование: /set_expiry <telegram_id> <dd.mm.yyyy> [hh:mm]")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ Использование: /set_expiry <telegram_id> <dd.mm.yyyy> [hh:mm]")
        return

    date_text = args[1]
    time_text = args[2] if len(args) == 3 else "23:59"

    try:
        expiry_date = datetime.strptime(f"{date_text} {time_text}", "%d.%m.%Y %H:%M")
    except ValueError:
        await message.reply("⚠️ Неверный формат даты. Пример: /set_expiry 901812366 06.07.2026 23:59")
        return

    if expiry_date <= datetime.utcnow():
        await message.reply("⚠️ Дата окончания должна быть в будущем.")
        return

    conn = get_db_conn()
    cur = conn.cursor()
    expiry_text = expiry_date.strftime("%d.%m.%Y %H:%M")

    try:
        cur.execute(
            "SELECT expiry_date FROM users WHERE telegram_id = %s",
            (target_user_id,)
        )
        row = cur.fetchone()
        old_expiry = row[0] if row else None

        cur.execute("""
            INSERT INTO users (
                telegram_id,
                paid,
                expiry_date,
                payment_failed,
                grace_period_end,
                reminder_sent,
                blocked_bot,
                auto_renew
            )
            VALUES (%s, TRUE, %s, FALSE, NULL, FALSE, FALSE, FALSE)
            ON CONFLICT (telegram_id) DO UPDATE
            SET paid = TRUE,
                expiry_date = EXCLUDED.expiry_date,
                payment_failed = FALSE,
                payment_failed_at = NULL,
                last_payment_succeeded_at = NOW(),
                grace_period_end = NULL,
                reminder_sent = FALSE,
                blocked_bot = FALSE,
                auto_renew = CASE
                    WHEN users.stripe_subscription_id IS NULL THEN FALSE
                    ELSE users.auto_renew
                END
        """, (target_user_id, expiry_date))

        conn.commit()

        await log_access_event(
            target_user_id,
            "manual_set_expiry",
            source="admin_command",
            old_expiry=old_expiry,
            new_expiry=expiry_date,
            notes=f"admin_id={message.from_user.id}"
        )

        try:
            await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=target_user_id)
        except Exception as e:
            logging.error(f"Ошибка разбана после /set_expiry для {target_user_id}: {e}")

        link = await generate_invite_link()
        user_text = f"✅ Администратор обновил ваш доступ до {expiry_text}."

        if link:
            user_text += f"\nСсылка для входа в клуб: {link}"

        try:
            await bot.send_message(target_user_id, user_text)
        except BotBlocked:
            cur.execute(
                "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                (target_user_id,)
            )
            conn.commit()
            await notify_critical_delivery_failed(
                target_user_id,
                "set_expiry",
                "сообщение об обновлении точной даты доступа",
                "BotBlocked",
                f"expiry_date = {expiry_text}; blocked_bot = TRUE"
            )
            await message.answer("⚠️ Дата обновлена, но пользователь заблокировал бота.")
            return
        except Exception as e:
            logging.error(f"Не удалось отправить сообщение после /set_expiry пользователю {target_user_id}: {e}")
            await notify_critical_delivery_failed(
                target_user_id,
                "set_expiry",
                "сообщение об обновлении точной даты доступа",
                e,
                f"expiry_date = {expiry_text}"
            )
            await message.answer(
                f"⚠️ Дата обновлена, но не удалось отправить сообщение пользователю {target_user_id}.\n\n"
                f"Ошибка: {e}"
            )
            return

        await message.answer(f"✅ Доступ пользователя {target_user_id} установлен до {expiry_text}.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка /set_expiry для {args[0]}: {e}")
        await message.answer(f"❌ Ошибка: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['sync_stripe_user'], state='*')
async def sync_stripe_user_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()

    if len(args) != 1:
        await message.reply("⚠️ Использование: /sync_stripe_user <telegram_id>")
        return

    try:
        target_user_id = int(args[0])
    except ValueError:
        await message.reply("⚠️ Использование: /sync_stripe_user <telegram_id>")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
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

        user = cur.fetchone()

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
            await message.reply(f"❌ Не удалось получить подписку из Stripe: {e}")
            return

        def sync_stripe_value(obj, *path):
            current = obj
            for key in path:
                if current is None:
                    return None
                if isinstance(current, dict):
                    current = current.get(key)
                else:
                    current = getattr(current, key, None)
            return current

        status = sync_stripe_value(subscription, 'status')
        current_period_end = sync_stripe_value(subscription, 'current_period_end')
        period_source = "subscription.current_period_end"
        customer = sync_stripe_value(subscription, 'customer')
        cancel_at_period_end = bool(sync_stripe_value(subscription, 'cancel_at_period_end'))
        customer_id = customer if isinstance(customer, str) else getattr(customer, 'id', None)
        auto_renew = not cancel_at_period_end
        period_end_text = "нет"

        if not current_period_end:
            try:
                invoices = await asyncio.to_thread(
                    stripe.Invoice.list,
                    subscription=stripe_subscription_id,
                    limit=5
                )

                invoice_data = sync_stripe_value(invoices, 'data') or []
                for invoice in invoice_data:
                    invoice_status = sync_stripe_value(invoice, 'status')
                    if invoice_status != 'paid':
                        continue

                    lines_data = sync_stripe_value(invoice, 'lines', 'data') or []
                    first_line = lines_data[0] if lines_data else None
                    period_end = sync_stripe_value(first_line, 'period', 'end')

                    if period_end:
                        current_period_end = period_end
                        period_source = "invoice.lines.data[0].period.end"
                        break
            except Exception as e:
                logging.error(f"Не удалось получить invoices Stripe для /sync_stripe_user {target_user_id}: {e}")

        if current_period_end:
            period_end_dt = datetime.utcfromtimestamp(current_period_end)
            period_end_text = period_end_dt.strftime("%d.%m.%Y %H:%M")

        if status in ('active', 'trialing') and current_period_end:
            new_expiry = datetime.utcfromtimestamp(current_period_end)

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
            """, (new_expiry, customer_id, auto_renew, target_user_id))

            conn.commit()

            await log_access_event(
                target_user_id,
                "manual_stripe_sync",
                source="admin_command",
                old_expiry=expiry_date,
                new_expiry=new_expiry,
                stripe_subscription_id=stripe_subscription_id,
                notes=f"status={status}; auto_renew={auto_renew}; period_source={period_source}; admin_id={message.from_user.id}"
            )

            await message.reply(
                "✅ Stripe-синхронизация выполнена\n\n"
                f"telegram_id: {target_user_id}\n"
                f"status: {status}\n"
                "paid: TRUE\n"
                f"expiry_date: {new_expiry.strftime('%d.%m.%Y %H:%M')}\n"
                f"auto_renew: {auto_renew}\n"
                f"period_source: {period_source}\n"
                f"stripe_subscription_id: {stripe_subscription_id}\n"
                f"stripe_customer_id: {customer_id or 'нет'}"
            )
            return

        if status in ('active', 'trialing') and not current_period_end:
            cur.execute("""
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
            """, (stripe_subscription_id, customer_id, auto_renew, target_user_id))
            conn.commit()

            await message.reply(
                "⚠️ Подписка активна, customer_id обновлен, но current_period_end не найден. expiry_date не меняла.\n\n"
                f"telegram_id: {target_user_id}\n"
                f"status: {status}\n"
                f"auto_renew: {auto_renew}\n"
                f"stripe_subscription_id: {stripe_subscription_id}\n"
                f"stripe_customer_id: {customer_id or 'нет'}"
            )
            return

        await message.reply(
            "⚠️ Подписка в Stripe не активна\n\n"
            f"telegram_id: {target_user_id}\n"
            f"status: {status}\n"
            f"current_period_end: {period_end_text}\n"
            f"cancel_at_period_end: {cancel_at_period_end}\n\n"
            "БД автоматически не обновлена до paid=True."
        )

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка /sync_stripe_user для {args[0]}: {e}")
        await message.reply(f"❌ Ошибка: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['expired_users'], state='*')
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
        await message.answer(f"❌ Ошибка получения списка: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['user'], state='*')
async def user_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()

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

        stripe_text = stripe_subscription_id if stripe_subscription_id else "нет"
        stripe_customer_text = stripe_customer_id if stripe_customer_id else "нет"

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
        await message.answer(f"❌ Ошибка получения пользователя: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['access_history'], state='*')
async def access_history_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()

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
                f"stripe_event_id: {stripe_event_id or 'нет'}",
                f"stripe_subscription_id: {stripe_subscription_id or 'нет'}",
                f"notes: {notes or 'нет'}",
                ""
            ])

        text = "\n".join(lines).strip()

        if len(text) > 4000:
            text = text[:3997] + "..."

        await message.answer(text)

    except Exception as e:
        logging.error(f"Ошибка access_history_command для {args[0]}: {e}")
        await message.answer(f"❌ Ошибка получения истории доступа: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['recent_access_events'], state='*')
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
                f"stripe_event_id: {stripe_event_id or 'нет'}",
                f"stripe_subscription_id: {stripe_subscription_id or 'нет'}",
                f"notes: {notes or 'нет'}",
                ""
            ])

        text = "\n".join(lines).strip()

        if len(text) > 4000:
            text = text[:3997] + "..."

        await message.answer(text)

    except Exception as e:
        logging.error(f"Ошибка recent_access_events_command: {e}")
        await message.answer(f"❌ Ошибка получения последних событий доступа: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['find_by_stripe'], state='*')
async def find_by_stripe_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()

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
            await message.answer(f"Ничего не найдено по Stripe ID:\n{query_id}")
            return

        lines = [f"🔎 Найдено по Stripe ID: {query_id}\n"]

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
                    f"stripe_subscription_id: {stripe_subscription_id or 'нет'}",
                    f"stripe_customer_id: {stripe_customer_id or 'нет'}",
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
                    f"stripe_event_id: {stripe_event_id or 'нет'}",
                    f"stripe_subscription_id: {stripe_subscription_id or 'нет'}",
                    f"notes: {notes or 'нет'}",
                    ""
                ])

        text = "\n".join(lines).strip()

        if len(text) > 4000:
            text = text[:3997] + "..."

        await message.answer(text)

    except Exception as e:
        logging.error(f"Ошибка find_by_stripe_command для {query_id}: {e}")
        await message.answer(f"❌ Ошибка поиска по Stripe ID: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['bot_health'], state='*')
async def bot_health_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    def fmt_dt(value):
        return value.strftime("%d.%m.%Y %H:%M") if value else "нет"

    env_names = [
        "BOT_TOKEN",
        "DATABASE_URL",
        "GROUP_ID",
        "ADMIN_IDS",
        "STRIPE_API_KEY",
        "STRIPE_WEBHOOK_SECRET"
    ]
    env_lines = [f"{name}: {'OK' if os.getenv(name) else 'MISSING'}" for name in env_names]

    db_status = "OK"
    user_stats = {
        "total": "нет",
        "paid": "нет",
        "active": "нет",
        "expired_paid": "нет",
        "payment_failed": "нет",
        "grace": "нет",
        "blocked": "нет"
    }
    access_stats = {
        "total": "нет",
        "last_24h": "нет",
        "last_event": "нет"
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
    except Exception as e:
        db_status = f"ERROR: {e}"
        logging.error(f"Ошибка bot_health_command: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    text = (
        "🩺 Bot health\n\n"
        f"UTC now: {datetime.utcnow().strftime('%d.%m.%Y %H:%M')}\n\n"
        "ENV:\n"
        f"{chr(10).join(env_lines)}\n\n"
        f"DB: {db_status}\n\n"
        "Users:\n"
        f"Всего пользователей: {user_stats['total']}\n"
        f"paid=True: {user_stats['paid']}\n"
        f"Активных по expiry_date: {user_stats['active']}\n"
        f"Истекли, но paid=True: {user_stats['expired_paid']}\n"
        f"payment_failed=True: {user_stats['payment_failed']}\n"
        f"В grace period: {user_stats['grace']}\n"
        f"Заблокировали бота: {user_stats['blocked']}\n\n"
        "Access events:\n"
        f"Всего: {access_stats['total']}\n"
        f"За 24ч: {access_stats['last_24h']}\n"
        f"Последнее событие: {access_stats['last_event']}"
    )

    if len(text) > 4000:
        text = text[:3997] + "..."

    await message.answer(text)

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
            "/unlinked_stripe — показать Stripe оплаты без пользователя",
            "/stripe_links <telegram_id> — показать Stripe связи пользователя",
            "/link_stripe_user <telegram_id> <customer_id> <subscription_id> — связать Stripe с пользователем",
            "/send_invite_link <telegram_id> — отправить invite link",
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
            "/find_by_stripe <stripe_id>",
        ],
    },
    "tech": {
        "button": "🛠 Тех. функции",
        "title": "🛠 Тех. функции",
        "danger": True,
        "commands": [
            "/test_backup — backup",
            "Отправь боту фото или видео от имени админа — бот ответит file_id",
            "/admin_help — список всех команд",
        ],
    },
}


def get_admin_menu_keyboard():
    kb = InlineKeyboardMarkup(row_width=1)
    for section_key, section in ADMIN_MENU_SECTIONS.items():
        kb.add(InlineKeyboardButton(section["button"], callback_data=f"admin_menu:{section_key}"))
    return kb


def get_admin_back_keyboard():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("⬅️ Назад в админ-меню", callback_data="admin_menu:back"))
    return kb


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


@dp.message_handler(commands=['admin'], state='*')
async def admin_menu_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Недостаточно прав.")
        return

    await message.answer(get_admin_menu_text(), reply_markup=get_admin_menu_keyboard())


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("admin_menu:"), state='*')
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


@dp.message_handler(commands=['admin_help'], state='*')
async def admin_help_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer(get_admin_help_text())

@dp.message_handler(commands=['expiring_users'], state='*')
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
        await message.answer(f"❌ Ошибка получения списка: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['test_followup'], state='*')
async def test_followup_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()

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

    try:
        cur.execute("""
            INSERT INTO users (telegram_id, paid)
            VALUES (%s, FALSE)
            ON CONFLICT (telegram_id) DO NOTHING
        """, (target_user_id,))

        await send_free_lesson_followup(target_user_id, cur)
        conn.commit()

        await message.answer(f"✅ Тестовый follow-up отправлен пользователю {target_user_id}.")

    except BotBlocked:
        cur.execute(
            "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
            (target_user_id,)
        )
        conn.commit()
        await message.answer("⚠️ Пользователь заблокировал бота.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка test_followup для {target_user_id}: {e}")
        await message.answer(f"❌ Ошибка отправки тестового follow-up: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['help'], state='*')
async def help_command(message: types.Message):
    await message.answer("По всем вопросам @re_tasha")

@dp.message_handler(commands=['stats'], state='*')
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

        await message.answer(text)

    except Exception as e:
        logging.error(f"Ошибка stats: {e}")
        await message.answer(f"❌ Ошибка получения статистики: {e}")

    finally:
        cur.close()
        conn.close()


@dp.message_handler(commands=['weekly_report'], state='*')
async def weekly_report_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    period_start, period_end = get_last_completed_week_bounds()
    await send_weekly_report_to_admin(message, period_start, period_end)


@dp.message_handler(commands=['weekly_report_current'], state='*')
async def weekly_report_current_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    period_start, period_end = get_current_week_bounds()
    await send_weekly_report_to_admin(message, period_start, period_end, with_actions=False)


@dp.message_handler(commands=['weekly_report_send'], state='*')
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


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("weekly_csv:"), state='*')
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


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("weekly_refresh:"), state='*')
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


@dp.message_handler(commands=['test_expiry'])
async def test_expiry(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        await message.answer("Запускаю проверку подписок...")
        await check_subscriptions_and_reminders()
        await message.answer("Проверка завершена.")
    else:
        await message.answer("Нет прав.")

@dp.message_handler(commands=['test_grace'])
async def test_grace(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.get_args().split()
    if len(args) != 1:
        await message.reply("Использование: /test_grace <user_id>")
        return
    user_id = args[0]
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE users 
            SET payment_failed = TRUE, 
                payment_failed_at = COALESCE(payment_failed_at, NOW()),
                grace_period_end = NOW() + INTERVAL '1 day'
            WHERE telegram_id = %s
        """, (int(user_id),))
        conn.commit()
        await message.reply(f"✅ Установлен grace period для {user_id} на 24 часа.")
        # Отправим уведомление пользователю
        await bot.send_message(int(user_id), "⚠️ Тестовое: не удалось списать оплату. У вас есть 24 часа для исправления.")
    except Exception as e:
        await message.reply(f"Ошибка: {e}")
    finally:
        cur.close()
        conn.close()

async def stripe_webhook(request):
    payload = await request.read()
    sig_header = request.headers.get('Stripe-Signature')
    webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET")
    logging.info(
        f"Stripe webhook received: path={request.path}, payload_bytes={len(payload)}, "
        f"signature_present={bool(sig_header)}, webhook_secret_configured={bool(webhook_secret)}"
    )

    if not webhook_secret:
        logging.error("Stripe webhook rejected: STRIPE_WEBHOOK_SECRET не задан.")
        return web.Response(status=500)

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, webhook_secret
        )
    except Exception as e:
        logging.exception(f"Ошибка проверки подписи Stripe webhook: {e}")
        return web.Response(status=400)

    event_id = event['id']
    event_type = event['type']
    logging.info(f"Stripe webhook event: event_id={safe_log_id(event_id)}, event.type={event_type}")

    claim_result = await claim_event_processing(event_id)
    if claim_result != "claimed":
        logging.info(
            "Stripe webhook event already claimed: event_id=%s, event.type=%s, claim_result=%s",
            safe_log_id(event_id),
            event_type,
            claim_result,
        )
        return web.Response(status=200)

    try:

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
            await notify_admins(
                "Оплата Stripe прошла, но пользователь не найден в БД. Нужно вручную связать "
                "Stripe customer/subscription с Telegram ID.\n\n"
                f"invoice_id: {invoice_id}\n"
                f"event_id: {event_id}\n"
                f"subscription_id: {subscription_id or 'нет'}\n"
                f"billing_reason: {billing_reason}\n"
                f"customer_id: {customer_id}\n"
                f"customer_email: {customer_email}\n"
                f"amount_paid: {amount_paid if amount_paid is not None else 'нет'}\n"
                f"period_end: {period_end or 'нет'}\n"
                f"Пустые subscription-поля: {empty_subscription_fields_text(invoice)}\n\n"
                "Доступ автоматически НЕ выдан. Используйте команду:\n"
                "/link_stripe_user <telegram_id> <customer_id> <subscription_id>"
            )

        # ---------- 1. ОПЛАТА ЧЕРЕЗ CHECKOUT (ПЕРВИЧНАЯ ИЛИ ПРОДЛЕНИЕ) ----------
        if event['type'] == 'checkout.session.completed':
            session = event['data']['object']
            user_id = getattr(session, 'client_reference_id', None)
            metadata_obj = stripe_value(session, 'metadata') or {}
            metadata_keys = list(metadata_obj.keys()) if isinstance(metadata_obj, dict) else []
            logging.info(
                "Stripe checkout.session.completed data: "
                f"event_id={safe_log_id(event_id)}, session_id={safe_log_id(stripe_value(session, 'id'))}, "
                f"user_id={user_id}, metadata_telegram_id={stripe_value(session, 'metadata', 'telegram_id')}, "
                f"metadata_keys={metadata_keys}, "
                f"mode={stripe_value(session, 'mode')}, payment_status={stripe_value(session, 'payment_status')}, "
                f"customer_id={safe_log_id(stripe_object_id(stripe_value(session, 'customer')))}, "
                f"customer_email={safe_log_email(stripe_value(session, 'customer_details', 'email') or stripe_value(session, 'customer_email'))}"
            )
            if not user_id:
                logging.error(
                    f"checkout.session.completed пропущен: client_reference_id отсутствует, "
                    f"event_id={safe_log_id(event_id)}, session_id={safe_log_id(stripe_value(session, 'id'))}"
                )
                await mark_event_processed(event_id)
                return web.Response(status=200)

            sub_id = stripe_object_id(stripe_value(session, 'subscription'))
            customer_id = stripe_object_id(stripe_value(session, 'customer'))
            customer_email = stripe_value(session, 'customer_details', 'email') or stripe_value(session, 'customer_email')
            checkout_mode = stripe_value(session, 'mode') or getattr(session, 'mode', None)
            checkout_action = checkout_completion_action(checkout_mode, sub_id)
            if checkout_action == "link_only":
                session_id = stripe_value(session, 'id')
                if not sub_id and session_id:
                    try:
                        session = stripe.checkout.Session.retrieve(
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
                        await notify_admins(
                            "Stripe Checkout subscription завершился без subscription_id.\n\n"
                            f"user_id: {user_id}\n"
                            f"event_id: {event_id}\n"
                            f"session_id: {session_id or 'нет'}\n"
                            f"customer_id: {customer_id or 'нет'}\n\n"
                            "Доступ НЕ выдан. Webhook вернул 500, Stripe повторит событие."
                        )
                        await release_event_processing(event_id)
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
                    await mark_event_processed(event_id)
                    return web.Response(status=200)
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
                    await notify_admins(
                        f"Ошибка связывания подписочного Checkout.\n\n"
                        f"user_id: {user_id}\n"
                        f"event_id: {event_id}\n"
                        f"subscription_id: {sub_id or 'нет'}\n"
                        f"Ошибка: {e}"
                    )
                    await release_event_processing(event_id)
                    return web.Response(status=500)
                finally:
                    cur.close()
                    conn.close()

            days_to_add = 0
            metadata_raw = getattr(session, 'metadata', None)
            if metadata_raw is not None:
                try:
                    days_to_add = int(metadata_raw['days'])
                except (KeyError, TypeError, ValueError):
                    try:
                        days_val = getattr(metadata_raw, 'days', None)
                        if days_val is not None:
                            days_to_add = int(days_val)
                    except:
                        pass
            logging.info(f"WEBHOOK DEBUG: user={user_id}, days={days_to_add}, mode={getattr(session, 'mode', '?')}")
            if days_to_add <= 0:
                logging.error(f"Не удалось получить days для {user_id}")
                await mark_event_processed(event_id)
                return web.Response(status=200)

            is_trial = (days_to_add == 7)
            has_subscription = bool(sub_id)
            conn = get_db_conn()
            cur = conn.cursor()
            try:
                cur.execute("SELECT paid, expiry_date, first_payment_done FROM users WHERE telegram_id = %s", (int(user_id),))
                row = cur.fetchone()
                now = datetime.utcnow()
                old_expiry = row[1] if row else None

                if row and row[0] and row[1] and row[1] > now:
                    new_expiry = row[1] + timedelta(days=days_to_add)
                else:
                    new_expiry = now + timedelta(days=days_to_add)

                # Нужна ли ссылка? Да, если нет активной подписки (paid=False или expiry_date < now)
                needs_link = (row is None) or (not row[0]) or (row[1] is not None and row[1] < now)
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
                    source="checkout.session.completed",
                )
                insert_payment_event(
                    cur,
                    event_id,
                    event_type,
                    "succeeded",
                    telegram_id=user_id,
                    checkout_session_id=stripe_value(session, 'id'),
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
                conn.commit()
                logging.info(
                    f"Checkout Session marked completed: user_id={user_id}, "
                    f"session_id={safe_log_id(stripe_value(session, 'id'))}, event_id={safe_log_id(event_id)}"
                )
                reset_checkout_retry_state_after_success(user_id, "checkout.session.completed")
                logging.info(
                    "User access activated: source=checkout.session.completed, event_id=%s, event.type=%s, "
                    "user_id=%s, customer_id=%s, customer_email=%s, paid=True, expiry_date=%s, "
                    "stripe_subscription_id=%s, blocked_bot=False",
                    safe_log_id(event_id),
                    event_type,
                    user_id,
                    safe_log_id(customer_id),
                    safe_log_email(customer_email),
                    new_expiry,
                    safe_log_id(sub_id),
                )

                await log_access_event(
                    user_id,
                    "stripe_checkout_completed",
                    source="stripe_webhook",
                    old_expiry=old_expiry,
                    new_expiry=new_expiry,
                    stripe_event_id=event_id,
                    stripe_subscription_id=sub_id,
                    notes=f"days={days_to_add}; customer_id={safe_log_id(customer_id)}"
                )

                if await payment_needs_rejoin_invite(
                    user_id,
                    old_expiry,
                    "checkout.session.completed",
                    stripe_event_id=event_id,
                ):
                    await send_rejoin_invite_after_payment(
                        user_id,
                        new_expiry,
                        "checkout.session.completed",
                        stripe_event_id=event_id,
                        stripe_subscription_id=sub_id,
                    )
                else:
                    msg = f"✅ Ваша подписка продлена до {new_expiry.strftime('%d.%m.%Y')}. Спасибо! ❤️"
                    reply_markup = get_cancel_subscription_keyboard() if has_subscription else None

                    if has_subscription:
                        msg += (
                            "\n\nОплата будет списываться автоматически до момента, пока вы не отмените подписку.\n\n"
                            "Вы можете отменить автопродление в любой момент по кнопке ниже или через /profile."
                        )

                    try:
                        await bot.send_message(int(user_id), msg, reply_markup=reply_markup)
                    except BotBlocked:
                        cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (user_id,))
                        conn.commit()
                        await notify_critical_delivery_failed(
                            user_id,
                            "checkout.session.completed",
                            "сообщение об успешной оплате/продлении",
                            "BotBlocked",
                            f"paid = TRUE; expiry_date = {new_expiry.strftime('%d.%m.%Y %H:%M')}; blocked_bot = TRUE"
                        )
                    except Exception as e:
                        logging.error(f"Не удалось отправить сообщение после checkout пользователю {user_id}: {e}")
                        await notify_critical_delivery_failed(
                            user_id,
                            "checkout.session.completed",
                            "сообщение об успешной оплате/продлении",
                            e,
                            f"paid = TRUE; expiry_date = {new_expiry.strftime('%d.%m.%Y %H:%M')}"
                        )
            except Exception as e:
                conn.rollback()
                logging.exception(
                    f"Ошибка обработки checkout.session.completed: event_id={safe_log_id(event_id)}, "
                    f"user_id={user_id}, session_id={safe_log_id(stripe_value(session, 'id'))}: {e}"
                )
                await notify_admins(
                    f"Ошибка обработки checkout.session.completed.\n\n"
                    f"user_id: {user_id}\n"
                    f"event_id: {event_id}\n"
                    f"Ошибка: {e}"
                )
                await release_event_processing(event_id)
                return web.Response(status=500)
            finally:
                cur.close()
                conn.close()

        # ---------- 2. УСПЕШНОЕ АВТОПРОДЛЕНИЕ (invoice.payment_succeeded) ----------
            # ---------- 2. УСПЕШНОЕ АВТОПРОДЛЕНИЕ (invoice.payment_succeeded) ----------
        elif event['type'] == 'invoice.payment_succeeded':
            invoice = event['data']['object']
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
                    invoice = stripe.Invoice.retrieve(
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
                    await mark_event_processed(event_id)
                    return web.Response(status=200)

                subscription = stripe.Subscription.retrieve(sub_id)
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
                        invoice = stripe.Invoice.retrieve(
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
                        await notify_admins(
                            "Stripe прислал успешный invoice, но бот не смог проверить payment records.\n\n"
                            f"event_id: {event_id}\n"
                            f"invoice_id: {invoice_id}\n"
                            f"subscription_id: {sub_id}\n"
                            f"customer_id: {customer_id or 'нет'}\n"
                            f"Ошибка: {e}\n\n"
                            "Webhook вернул 500, Stripe повторит событие. Доступ в БД не менялся."
                        )
                        conn.rollback()
                        await release_event_processing(event_id)
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
                    await mark_event_processed(event_id)
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
                        await mark_event_processed(event_id)
                        return web.Response(status=200)

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
                        await mark_event_processed(event_id)
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
                    await mark_event_processed(event_id)
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
                        f"invoice.payment_succeeded: у subscription нет current_period_end. "
                        f"subscription_id={sub_id}, customer_id={customer_id}, invoice_id={invoice_id}, event={event_id}"
                    )
                    await notify_admins(
                        "Stripe прислал успешную оплату, но у подписки нет current_period_end.\n\n"
                        f"event_id: {event_id}\n"
                        f"subscription_id: {sub_id}\n"
                        f"customer_id: {customer_id or 'нет'}\n"
                        f"invoice_id: {invoice_id}\n\n"
                        "Webhook не упал, но доступ автоматически не обновлен. Проверьте подписку вручную."
                    )
                    conn.commit()
                    await mark_event_processed(event_id)
                    return web.Response(status=200)

                new_expiry = datetime.utcfromtimestamp(current_period_end)
                old_expiry = None
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

                if metadata_telegram_id:
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
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            auto_renew = TRUE,
                            blocked_bot = FALSE,
                            first_payment_done = CASE WHEN %s THEN TRUE ELSE users.first_payment_done END
                        FROM target
                        WHERE users.telegram_id = target.telegram_id
                        RETURNING users.telegram_id, target.old_expiry, target.was_payment_failed
                    """, (
                        metadata_telegram_id,
                        new_expiry,
                        new_expiry,
                        sub_id,
                        customer_id,
                        payment_kind == "initial_subscription",
                    ))

                    row = cur.fetchone()
                    if row:
                        old_expiry = row[1]
                        was_payment_failed = row[2]

                if not row:
                    cur.execute("""
                        WITH target AS (
                            SELECT telegram_id, expiry_date AS old_expiry, payment_failed AS was_payment_failed
                            FROM users
                            WHERE stripe_subscription_id = %s
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
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            auto_renew = TRUE,
                            blocked_bot = FALSE,
                            first_payment_done = CASE WHEN %s THEN TRUE ELSE users.first_payment_done END
                        FROM target
                        WHERE users.telegram_id = target.telegram_id
                        RETURNING users.telegram_id, target.old_expiry, target.was_payment_failed
                    """, (
                        sub_id,
                        new_expiry,
                        new_expiry,
                        sub_id,
                        customer_id,
                        payment_kind == "initial_subscription",
                    ))

                    row = cur.fetchone()
                    if row:
                        old_expiry = row[1]
                        was_payment_failed = row[2]

                if not row and customer_id:
                    cur.execute("""
                        WITH target AS (
                            SELECT telegram_id, expiry_date AS old_expiry, payment_failed AS was_payment_failed
                            FROM users
                            WHERE stripe_customer_id = %s
                        )
                        UPDATE users
                        SET expiry_date = CASE
                                WHEN users.expiry_date IS NOT NULL AND users.expiry_date >= %s THEN users.expiry_date
                                ELSE %s
                            END,
                            paid = TRUE,
                            stripe_subscription_id = %s,
                            stripe_customer_id = %s,
                            payment_failed = FALSE,
                            payment_failed_at = NULL,
                            last_payment_succeeded_at = NOW(),
                            grace_period_end = NULL,
                            reminder_sent = FALSE,
                            auto_renew = TRUE,
                            blocked_bot = FALSE,
                            first_payment_done = CASE WHEN %s THEN TRUE ELSE users.first_payment_done END
                        FROM target
                        WHERE users.telegram_id = target.telegram_id
                        RETURNING users.telegram_id, target.old_expiry, target.was_payment_failed
                    """, (
                        customer_id,
                        new_expiry,
                        new_expiry,
                        sub_id,
                        customer_id,
                        payment_kind == "initial_subscription",
                    ))

                    row = cur.fetchone()
                    if row:
                        old_expiry = row[1]
                        was_payment_failed = row[2]

                if not row:
                    linked_telegram_id, link_source = find_telegram_id_for_stripe(
                        cur,
                        stripe_subscription_id=sub_id,
                        stripe_customer_id=customer_id,
                    )
                    if linked_telegram_id:
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
                                grace_period_end = NULL,
                                reminder_sent = FALSE,
                                auto_renew = TRUE,
                                blocked_bot = FALSE,
                                first_payment_done = CASE WHEN %s THEN TRUE ELSE users.first_payment_done END
                            FROM target
                            WHERE users.telegram_id = target.telegram_id
                            RETURNING users.telegram_id, target.old_expiry, target.was_payment_failed
                        """, (
                            linked_telegram_id,
                            new_expiry,
                            new_expiry,
                            sub_id,
                            customer_id,
                            payment_kind == "initial_subscription",
                        ))
                        row = cur.fetchone()
                        if row:
                            old_expiry = row[1]
                            was_payment_failed = row[2]
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
                    await mark_event_processed(event_id)
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
                    period_end=period_end or new_expiry,
                    recovered_after_failure=was_payment_failed,
                )
                conn.commit()

                reset_checkout_retry_state_after_success(telegram_id, "invoice.payment_succeeded")
                if payment_kind == "out_of_band":
                    logging.info(
                        "MANUAL_OUT_OF_BAND_PAYMENT_PROCESSED: event_id=%s, event.type=%s, invoice_id=%s, "
                        "telegram_id=%s, subscription_id=%s, customer_id=%s, billing_reason=%s, "
                        "amount_paid=%s, amount_due=%s, invoice_status=%s, new_expiry=%s",
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
                        new_expiry,
                    )
                elif payment_kind == "initial_subscription":
                    logging.info(
                        "INITIAL_SUBSCRIPTION_PAYMENT_PROCESSED: event_id=%s, event.type=%s, invoice_id=%s, "
                        "telegram_id=%s, subscription_id=%s, customer_id=%s, billing_reason=%s, "
                        "amount_paid=%s, amount_due=%s, invoice_status=%s, new_expiry=%s",
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
                        new_expiry,
                    )
                elif payment_kind == "recurring":
                    logging.info(
                        "REAL_RECURRING_PAYMENT_PROCESSED: event_id=%s, event.type=%s, invoice_id=%s, "
                        "telegram_id=%s, subscription_id=%s, customer_id=%s, billing_reason=%s, "
                        "amount_paid=%s, amount_due=%s, invoice_status=%s, new_expiry=%s",
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
                        new_expiry,
                    )
                else:
                    logging.info(
                        "SUBSCRIPTION_PAYMENT_ADJUSTMENT_PROCESSED: event_id=%s, event.type=%s, invoice_id=%s, "
                        "telegram_id=%s, subscription_id=%s, customer_id=%s, billing_reason=%s, "
                        "amount_paid=%s, amount_due=%s, invoice_status=%s, new_expiry=%s",
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
                        new_expiry,
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
                    new_expiry,
                )
                if was_payment_failed:
                    logging.info(
                        "PAYMENT_RECOVERED_AFTER_FAILURE: telegram_id=%s, customer_id=%s, email=%s, "
                        "subscription_id=%s, invoice_id=%s, new_expiry_date=%s",
                        telegram_id,
                        safe_log_id(customer_id),
                        safe_log_email(customer_email),
                        safe_log_id(sub_id),
                        safe_log_id(invoice_id),
                        new_expiry,
                    )

                should_send_invoice_rejoin_invite = await payment_needs_rejoin_invite(
                    telegram_id,
                    old_expiry,
                    "invoice.payment_succeeded",
                    stripe_event_id=event_id,
                )

                if should_skip_invoice_notice_for_current_expiry(payment_kind, old_expiry, new_expiry):
                    logging.info(
                        f"invoice.payment_succeeded: срок уже актуален, пропускаю повторное уведомление. "
                        f"telegram_id={telegram_id}, old_expiry={old_expiry}, new_expiry={new_expiry}, event={safe_log_id(event_id)}"
                    )
                    if should_send_invoice_rejoin_invite:
                        await send_rejoin_invite_after_payment(
                            telegram_id,
                            new_expiry,
                            "invoice.payment_succeeded",
                            stripe_event_id=event_id,
                            stripe_subscription_id=sub_id,
                        )
                    await mark_event_processed(event_id)
                    return web.Response(status=200)

                if should_send_invoice_rejoin_invite:
                    await send_rejoin_invite_after_payment(
                        telegram_id,
                        new_expiry,
                        "invoice.payment_succeeded",
                        stripe_event_id=event_id,
                        stripe_subscription_id=sub_id,
                    )
                    await mark_event_processed(event_id)
                    return web.Response(status=200)

                if payment_kind == "out_of_band":
                    logging.info(
                        "AUTO_RENEW_NOTICE_SKIPPED_OUT_OF_BAND: telegram_id=%s, invoice_id=%s, "
                        "event_id=%s, new_expiry=%s",
                        telegram_id,
                        safe_log_id(invoice_id),
                        safe_log_id(event_id),
                        new_expiry,
                    )
                    await mark_event_processed(event_id)
                    return web.Response(status=200)

                await log_access_event(
                    telegram_id,
                    "stripe_invoice_paid",
                    source="stripe_webhook",
                    old_expiry=old_expiry,
                    new_expiry=new_expiry,
                    stripe_event_id=event_id,
                    stripe_subscription_id=sub_id,
                    notes=f"customer_id={safe_log_id(customer_id)}; invoice_id={safe_log_id(invoice_id)}; period_source={period_source}"
                )

                try:
                    if payment_kind == "initial_subscription":
                        message_text = (
                            f"✅ Оплата прошла успешно! Доступ активен до {new_expiry.strftime('%d.%m.%Y')}.\n\n"
                            "Оплата будет списываться автоматически до момента, пока вы не отмените подписку.\n\n"
                            "Вы можете отменить автопродление в любой момент по кнопке ниже или через /profile."
                        )
                        notice_marker = "INITIAL_SUBSCRIPTION_NOTICE_SENT"
                    elif payment_kind == "recurring":
                        message_text = (
                            f"✅ Автопродление успешно! Доступ продлен до {new_expiry.strftime('%d.%m.%Y')}.\n\n"
                            "Оплата будет списываться автоматически до момента, пока вы не отмените подписку."
                        )
                        notice_marker = "AUTO_RENEW_NOTICE_SENT"
                    else:
                        message_text = (
                            f"✅ Оплата прошла успешно! Доступ активен до {new_expiry.strftime('%d.%m.%Y')}.\n\n"
                            "Спасибо! ❤️"
                        )
                        notice_marker = "SUBSCRIPTION_PAYMENT_NOTICE_SENT"
                    await bot.send_message(
                        int(telegram_id),
                        message_text,
                        reply_markup=get_cancel_subscription_keyboard()
                    )
                    logging.info(
                        "%s: telegram_id=%s, invoice_id=%s, new_expiry=%s",
                        notice_marker,
                        telegram_id,
                        safe_log_id(invoice_id),
                        new_expiry,
                    )
                except BotBlocked:
                    cur.execute(
                        "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                        (int(telegram_id),)
                    )
                    conn.commit()
                    failed_action = (
                        "сообщение об успешном автопродлении"
                        if payment_kind == "recurring"
                        else "сообщение об успешной оплате"
                    )
                    await notify_critical_delivery_failed(
                        telegram_id,
                        "invoice.payment_succeeded",
                        failed_action,
                        "BotBlocked",
                        f"paid = TRUE; expiry_date = {new_expiry.strftime('%d.%m.%Y %H:%M')}; blocked_bot = TRUE"
                    )
                except Exception as e:
                    failed_action = (
                        "сообщение об успешном автопродлении"
                        if payment_kind == "recurring"
                        else "сообщение об успешной оплате"
                    )
                    logging.error(f"Не удалось отправить {failed_action} {telegram_id}: {e}")
                    await notify_critical_delivery_failed(
                        telegram_id,
                        "invoice.payment_succeeded",
                        failed_action,
                        e,
                        f"paid = TRUE; expiry_date = {new_expiry.strftime('%d.%m.%Y %H:%M')}"
                    )

            except Exception as e:
                conn.rollback()
                logging.exception(
                    f"Ошибка invoice.payment_succeeded: event_id={safe_log_id(event_id)}, "
                    f"subscription_id={safe_log_id(sub_id)}, customer_id={safe_log_id(customer_id)}: {e}"
                )
                await notify_admins(
                    f"Ошибка обработки успешной оплаты Stripe.\n\n"
                    f"subscription_id: {sub_id}\n"
                    f"event_id: {event_id}\n"
                    f"Ошибка: {e}"
                )
                await release_event_processing(event_id)
                return web.Response(status=500)

            finally:
                cur.close()
                conn.close()
    
        # ---------- 3. ОШИБКА ОПЛАТЫ (invoice.payment_failed) – GRACE PERIOD ----------
        elif event['type'] == 'invoice.payment_failed':
            invoice = event['data']['object']
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
                await notify_admins(
                    "Stripe прислал ошибку оплаты, но subscription_id не найден.\n\n"
                    f"event_id: {event_id}\n"
                    f"invoice_id: {invoice_id}\n"
                    f"customer_id: {customer_id}\n\n"
                    "payment_failed в БД не обновлен. Проверьте вручную."
                )
                await mark_event_processed(event_id)
                return web.Response(status=200)

            if sub_id:
                try:
                    subscription = stripe.Subscription.retrieve(sub_id)
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
                    await notify_admins(
                        "Stripe прислал ошибку оплаты, но бот не смог проверить актуальный статус подписки.\n\n"
                        f"event_id: {event_id}\n"
                        f"invoice_id: {invoice_id}\n"
                        f"subscription_id: {sub_id}\n"
                        f"customer_id: {customer_id}\n"
                        f"Ошибка: {e}\n\n"
                        "Webhook вернул 500, Stripe повторит событие. Доступ в БД не менялся."
                    )
                    await release_event_processing(event_id)
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
                    try:
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
                                RETURNING telegram_id, expiry_date
                            """, (
                                trial_expiry,
                                trial_expiry,
                                not cancel_at_period_end,
                                sub_id,
                                customer_id_for_db,
                                customer_id_for_db,
                            ))
                            row = cur.fetchone()

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
                        await notify_admins(
                            "Stripe прислал ошибку оплаты по старому invoice, подписка сейчас trialing, "
                            "но бот не смог синхронизировать trial в БД.\n\n"
                            f"event_id: {event_id}\n"
                            f"invoice_id: {invoice_id}\n"
                            f"subscription_id: {sub_id}\n"
                            f"customer_id: {customer_id}\n"
                            f"trial_end: {trial_end}\n"
                            f"Ошибка: {e}\n\n"
                            "Webhook вернул 500, Stripe повторит событие."
                        )
                        await release_event_processing(event_id)
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
                    await mark_event_processed(event_id)
                    return web.Response(status=200)

                conn = get_db_conn()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE users
                    SET payment_failed = TRUE,
                        payment_failed_at = COALESCE(payment_failed_at, NOW()),
                        grace_period_end = GREATEST(
                            COALESCE(grace_period_end, NOW()),
                            NOW() + (%s * INTERVAL '1 hour')
                        ),
                        stripe_customer_id = COALESCE(%s, stripe_customer_id)
                    WHERE stripe_subscription_id = %s
                    RETURNING telegram_id, paid, expiry_date, payment_failed_at, grace_period_end
                """, (PAYMENT_RETRY_GRACE_HOURS, customer_id_for_db, sub_id))
                row = cur.fetchone()
                if not row and customer_id_for_db:
                    cur.execute("""
                        UPDATE users
                        SET payment_failed = TRUE,
                            payment_failed_at = COALESCE(payment_failed_at, NOW()),
                            grace_period_end = GREATEST(
                                COALESCE(grace_period_end, NOW()),
                                NOW() + (%s * INTERVAL '1 hour')
                            ),
                            stripe_subscription_id = COALESCE(%s, stripe_subscription_id),
                            stripe_customer_id = COALESCE(%s, stripe_customer_id)
                        WHERE stripe_customer_id = %s
                        RETURNING telegram_id, paid, expiry_date, payment_failed_at, grace_period_end
                    """, (PAYMENT_RETRY_GRACE_HOURS, sub_id, customer_id_for_db, customer_id_for_db))
                    row = cur.fetchone()
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
                conn.commit()
                cur.close()
                conn.close()
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
                    try:
                        await bot.send_message(telegram_id,
                            f"⚠️ Не удалось списать оплату за подписку. У вас есть {PAYMENT_RETRY_GRACE_HOURS} часов, чтобы пополнить карту или связаться с администратором.\n"
                            "После устранения проблемы доступ восстановится автоматически.")
                    except BotBlocked:
                        conn = get_db_conn()
                        cur = conn.cursor()
                        try:
                            cur.execute(
                                "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                                (int(telegram_id),)
                            )
                            conn.commit()
                        finally:
                            cur.close()
                            conn.close()
                    except Exception as e:
                        logging.error(f"Не удалось отправить сообщение о неудачной оплате пользователю {telegram_id}: {e}")
                        await notify_critical_delivery_failed(
                            telegram_id,
                            "invoice.payment_failed",
                            "сообщение о неудачном списании",
                            e,
                            "payment_failed = TRUE; grace_period_end установлен"
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

        # ---------- 4. ПОЛЬЗОВАТЕЛЬ ОТМЕНИЛ ПОДПИСКУ (customer.subscription.deleted) ----------
        elif event['type'] == 'customer.subscription.deleted':
            sub = event['data']['object']
            sub_id = stripe_object_id(stripe_value(sub, 'id'))
            customer_id = stripe_object_id(stripe_value(sub, 'customer'))
            status = stripe_value(sub, 'status')
            if sub_id:
                conn = get_db_conn()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE users
                    SET paid = CASE
                            WHEN expiry_date IS NOT NULL AND expiry_date > NOW() THEN paid
                            ELSE FALSE
                        END,
                        auto_renew = FALSE,
                        stripe_subscription_id = NULL,
                        stripe_customer_id = COALESCE(%s, stripe_customer_id)
                    WHERE stripe_subscription_id = %s
                    RETURNING telegram_id, paid, expiry_date
                """, (customer_id, sub_id))
                row = cur.fetchone()
                conn.commit()
                cur.close()
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
        elif event['type'] == 'customer.subscription.updated':
            sub = event['data']['object']
            sub_id = stripe_object_id(stripe_value(sub, 'id'))
            cancel_at_period_end = bool(stripe_value(sub, 'cancel_at_period_end'))
            status = stripe_value(sub, 'status')
            customer_id = stripe_object_id(stripe_value(sub, 'customer'))
            current_period_end = stripe_value(sub, 'current_period_end')
            trial_end = stripe_value(sub, 'trial_end')
            period_value, period_source = subscription_update_period(status, current_period_end, trial_end)
            subscription_expiry = datetime.utcfromtimestamp(period_value) if period_value else None
            if sub_id:
                conn = get_db_conn()
                cur = conn.cursor()
                old_auto_renew = None
                cur.execute(
                    "SELECT auto_renew FROM users WHERE stripe_subscription_id = %s",
                    (sub_id,)
                )
                old_auto_row = cur.fetchone()
                if old_auto_row:
                    old_auto_renew = old_auto_row[0]
                if status in ("past_due", "unpaid"):
                    cur.execute("""
                        UPDATE users
                        SET auto_renew = %s,
                            payment_failed = TRUE,
                            payment_failed_at = COALESCE(payment_failed_at, NOW()),
                            grace_period_end = GREATEST(
                                COALESCE(grace_period_end, NOW()),
                                NOW() + (%s * INTERVAL '1 hour')
                            ),
                            stripe_customer_id = COALESCE(%s, stripe_customer_id)
                        WHERE stripe_subscription_id = %s
                        RETURNING telegram_id, paid, expiry_date, payment_failed_at, grace_period_end
                    """, (not cancel_at_period_end, PAYMENT_RETRY_GRACE_HOURS, customer_id, sub_id))
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
                elif status in ("active", "trialing"):
                    if subscription_expiry:
                        cur.execute("""
                            UPDATE users
                            SET expiry_date = CASE
                                    WHEN users.expiry_date IS NOT NULL AND users.expiry_date >= %s THEN users.expiry_date
                                    ELSE %s
                                END,
                                reminder_sent = FALSE,
                                auto_renew = %s,
                                stripe_customer_id = COALESCE(%s, stripe_customer_id)
                            WHERE stripe_subscription_id = %s
                            RETURNING telegram_id, paid, expiry_date
                        """, (
                            subscription_expiry,
                            subscription_expiry,
                            not cancel_at_period_end,
                            customer_id,
                            sub_id,
                        ))
                    else:
                        cur.execute("""
                            UPDATE users
                            SET auto_renew = %s,
                                stripe_customer_id = COALESCE(%s, stripe_customer_id)
                            WHERE stripe_subscription_id = %s
                            RETURNING telegram_id, paid, expiry_date
                        """, (not cancel_at_period_end, customer_id, sub_id))
                    row = cur.fetchone()
                    if row:
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
                        await notify_admins(
                            "Stripe subscription active/trialing, но пользователь не найден в БД.\n\n"
                            f"event_id: {event_id}\n"
                            f"customer_id: {customer_id or 'нет'}\n"
                            f"subscription_id: {sub_id or 'нет'}\n"
                            f"status: {status or 'нет'}\n"
                            f"current_period_end: {current_period_end or 'нет'}\n\n"
                            "Нужно вручную связать Stripe с Telegram ID:\n"
                            "/link_stripe_user <telegram_id> <customer_id> <subscription_id>"
                        )
                else:
                    cur.execute("""
                        UPDATE users
                        SET auto_renew = %s,
                            stripe_customer_id = COALESCE(%s, stripe_customer_id)
                        WHERE stripe_subscription_id = %s
                        RETURNING telegram_id, paid, expiry_date
                    """, (not cancel_at_period_end, customer_id, sub_id))
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
                        upsert_stripe_link(
                            cur,
                            row[0],
                            stripe_customer_id=customer_id,
                            stripe_subscription_id=sub_id,
                            status=status,
                            current_period_end=current_period_end,
                            is_active=status in ("active", "trialing"),
                            source="customer.subscription.updated",
                        )
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
                conn.commit()
                cur.close()
                conn.close()

        # ---------- 5. СЕССИЯ ОПЛАТЫ ИСТЕКЛА ИЛИ НЕ УДАЛАСЬ ----------
        elif event['type'] in ('checkout.session.expired', 'checkout.session.async_payment_failed'):
            session = event['data']['object']
            user_id = getattr(session, 'client_reference_id', None)

            if user_id:
                clear_cached_checkout_sessions_for_user(user_id)
                kb = InlineKeyboardMarkup(row_width=1).add(
                    InlineKeyboardButton("🔁 Выбрать тариф заново", callback_data="retry_payment")
                )

                try:
                    await bot.send_message(
                        int(user_id),
                        "Похоже, оформление доступа не завершилось.\n\n"
                        "Вы можете выбрать тариф еще раз или написать администратору, если нужна помощь.",
                        reply_markup=kb
                    )
                except BotBlocked:
                    conn = get_db_conn()
                    cur = conn.cursor()
                    try:
                        cur.execute(
                            "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                            (int(user_id),)
                        )
                        conn.commit()
                    finally:
                        cur.close()
                        conn.close()
                except Exception as e:
                    logging.error(f"Не удалось отправить сообщение о неудачной оплате пользователю {user_id}: {e}")

        await mark_event_processed(event_id)
        return web.Response(status=200)
    except Exception as e:
        await release_event_processing(event_id)
        logging.exception(
            "STRIPE_WEBHOOK_UNHANDLED_EXCEPTION: event_id=%s, event.type=%s, error=%s",
            safe_log_id(event_id),
            event_type,
            e,
        )
        return web.Response(status=500)

@dp.message_handler(commands=['test_auto_lesson'], state='*')
async def test_auto_lesson_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()

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

    except BotBlocked:
        cur.execute(
            "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
            (target_user_id,)
        )
        conn.commit()
        await message.answer("⚠️ Пользователь заблокировал бота.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка test_auto_lesson для {target_user_id}: {e}")
        await message.answer(f"❌ Ошибка отправки тестового урока: {e}")

    finally:
        cur.close()
        conn.close()

@dp.message_handler(commands=['test_backup'])
async def test_backup(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет прав.")
        return
    await message.answer("🔄 Запускаю бэкап...")
    await send_db_backup()
    await message.answer("✅ Бэкап завершён. Проверьте личные сообщения от бота (файл должен прийти админам).")

@dp.message_handler(commands=['unblock_user'], state='*')
async def unblock_user(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.get_args().split()
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

@dp.message_handler(commands=['send_invite_link'], state='*')
async def send_invite_link_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()

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

        if not user:
            await message.reply("❌ Пользователь не найден в базе.")
            return

        paid, expiry_date, blocked_bot = user

        if not paid or not expiry_date or expiry_date <= datetime.utcnow():
            await message.reply("⚠️ У пользователя нет активного доступа.")
            return

        try:
            await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=target_user_id)
        except Exception as e:
            logging.error(f"Ошибка разбана перед /send_invite_link для {target_user_id}: {e}")

        invite = await bot.create_chat_invite_link(
            chat_id=int(GROUP_ID),
            name=f"manual_invite_{target_user_id}",
            expire_date=datetime.utcnow() + timedelta(hours=24),
            member_limit=1
        )
        invite_link = invite.invite_link
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
        except BotBlocked:
            cur.execute(
                "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                (target_user_id,)
            )
            conn.commit()
            await message.answer(
                "⚠️ Ссылка создана, но пользователь заблокировал бота.\n\n"
                f"telegram_id: {target_user_id}\n"
                f"Ссылка для ручной отправки: {invite_link}"
            )
            return
        except Exception as e:
            logging.error(f"Не удалось отправить invite link пользователю {target_user_id}: {e}")
            await message.answer(
                "⚠️ Ссылка создана, но не удалось отправить ее пользователю.\n\n"
                f"telegram_id: {target_user_id}\n"
                f"Ошибка: {e}\n"
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
        await message.answer(f"❌ Ошибка отправки ссылки: {e}")

    finally:
        cur.close()
        conn.close()


@dp.message_handler(commands=['unlinked_stripe'], state='*')
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
                f"{index}) event_id: {event_id}",
                f"event_type: {event_type or 'нет'}",
                f"invoice_id: {invoice_id or 'нет'}",
                f"customer: {customer_id or 'нет'}",
                f"subscription: {subscription_id or 'нет'}",
                f"email: {customer_email or 'нет'}",
                f"amount: {amount_paid if amount_paid is not None else 'нет'} {currency or ''}".strip(),
                f"billing_reason: {billing_reason or 'нет'}",
                f"period_end: {period_end or 'нет'}",
                f"created_at: {created_at}",
                "Связать: /link_stripe_user <telegram_id> "
                f"{customer_id or '<customer_id>'} {subscription_id or '<subscription_id>'}",
            ])

        await message.reply("\n".join(lines))
    except Exception as e:
        logging.error("UNLINKED_STRIPE_COMMAND_FAILED: error=%s", str(e), exc_info=True)
        await message.reply(f"❌ Ошибка /unlinked_stripe: {e}")
    finally:
        cur.close()
        conn.close()


@dp.message_handler(commands=['stripe_links'], state='*')
async def stripe_links_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()
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
                f"{index}) customer_id: {customer_id or 'нет'}",
                f"subscription_id: {subscription_id or 'нет'}",
                f"email: {customer_email or 'нет'}",
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
        await message.reply(f"❌ Ошибка /stripe_links: {e}")
    finally:
        cur.close()
        conn.close()


@dp.message_handler(commands=['link_stripe_user'], state='*')
async def link_stripe_user_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()
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
    except Exception as e:
        logging.error(
            "LINK_STRIPE_USER_SUBSCRIPTION_RETRIEVE_FAILED: telegram_id=%s, customer_id=%s, "
            "subscription_id=%s, error=%s",
            target_user_id,
            customer_id,
            subscription_id,
            str(e),
            exc_info=True,
        )
        await message.reply(f"❌ Не удалось получить Stripe subscription: {e}")
        return

    prepared_payment_events = await prepare_manual_link_payment_events(customer_id, subscription_id)

    conn = get_db_conn()
    cur = conn.cursor()
    old_expiry = None
    new_expiry = None
    access_active = False
    status = None

    try:
        cur.execute("SELECT expiry_date FROM users WHERE telegram_id = %s", (target_user_id,))
        row = cur.fetchone()
        if not row:
            await message.reply("❌ Пользователь не найден в базе.")
            return

        old_expiry = row[0]

        status = getattr(subscription, "status", None)
        current_period_end = getattr(subscription, "current_period_end", None)
        cancel_at_period_end = bool(getattr(subscription, "cancel_at_period_end", False))
        stripe_customer_id = getattr(subscription, "customer", None)
        stripe_customer_id = stripe_customer_id if isinstance(stripe_customer_id, str) else customer_id

        if stripe_customer_id and stripe_customer_id != customer_id:
            logging.warning(
                "LINK_STRIPE_USER_CUSTOMER_MISMATCH: telegram_id=%s, provided_customer_id=%s, "
                "subscription_customer_id=%s, subscription_id=%s",
                target_user_id,
                customer_id,
                stripe_customer_id,
                subscription_id,
            )

        if status in ("active", "trialing") and current_period_end:
            new_expiry = datetime.utcfromtimestamp(current_period_end)
            cur.execute("""
                UPDATE users
                SET stripe_customer_id = %s,
                    stripe_subscription_id = %s,
                    paid = TRUE,
                    expiry_date = %s,
                    payment_failed = FALSE,
                    payment_failed_at = NULL,
                    last_payment_succeeded_at = NOW(),
                    grace_period_end = NULL,
                    reminder_sent = FALSE,
                    auto_renew = TRUE,
                    blocked_bot = FALSE
                WHERE telegram_id = %s
            """, (customer_id, subscription_id, new_expiry, target_user_id))
            access_active = True
        else:
            cur.execute("""
                UPDATE users
                SET stripe_customer_id = %s,
                    stripe_subscription_id = %s,
                    auto_renew = %s
                WHERE telegram_id = %s
            """, (customer_id, subscription_id, not cancel_at_period_end, target_user_id))

        upsert_stripe_link(
            cur,
            target_user_id,
            stripe_customer_id=customer_id,
            stripe_subscription_id=subscription_id,
            status=status,
            current_period_end=current_period_end,
            is_active=access_active,
            source="manual_link_stripe_user",
        )
        backfilled_payment_events = backfill_payment_events_for_manual_link(
            cur,
            target_user_id,
            prepared_payment_events,
        )
        cur.execute("""
            UPDATE unlinked_stripe_events
            SET resolved = TRUE,
                resolved_by = %s,
                resolved_telegram_id = %s,
                resolved_at = NOW()
            WHERE resolved IS NOT TRUE
              AND (
                  stripe_customer_id = %s
                  OR stripe_subscription_id = %s
              )
        """, (message.from_user.id, target_user_id, customer_id, subscription_id))

        conn.commit()

        await log_access_event(
            target_user_id,
            "manual_link_stripe_user",
            source="admin_command",
            old_expiry=old_expiry,
            new_expiry=new_expiry,
            stripe_subscription_id=subscription_id,
            notes=(
                f"admin_id={message.from_user.id}; customer_id={customer_id}; "
                f"status={status}; current_period_end={current_period_end}; "
                f"backfilled_payment_events={backfilled_payment_events}"
            )
        )

        invite_sent = False
        if access_active:
            invite_sent = await send_rejoin_invite_after_payment(
                target_user_id,
                new_expiry,
                "manual_link_stripe_user",
                stripe_subscription_id=subscription_id,
            )

        logging.info(
            "LINK_STRIPE_USER_COMPLETED: telegram_id=%s, customer_id=%s, subscription_id=%s, "
            "status=%s, expiry_date=%s, invite_sent=%s, backfilled_payment_events=%s",
            target_user_id,
            customer_id,
            subscription_id,
            status,
            new_expiry,
            invite_sent,
            backfilled_payment_events,
        )
        await message.reply(
            "✅ Stripe пользователь связан.\n\n"
            f"telegram_id: {target_user_id}\n"
            f"customer_id: {customer_id}\n"
            f"subscription_id: {subscription_id}\n"
            f"status: {status or 'нет'}\n"
            f"expiry_date: {new_expiry or 'не обновлена'}\n"
            f"invite_sent: {invite_sent}\n"
            f"payment_events_added: {backfilled_payment_events}"
        )

    except Exception as e:
        conn.rollback()
        logging.error(
            "LINK_STRIPE_USER_FAILED: telegram_id=%s, customer_id=%s, subscription_id=%s, error=%s",
            target_user_id,
            customer_id,
            subscription_id,
            str(e),
            exc_info=True,
        )
        await message.reply(f"❌ Ошибка /link_stripe_user: {e}")
    finally:
        cur.close()
        conn.close()


@dp.message_handler(commands=['unban_user'], state='*')
async def unban_user(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.get_args().split()

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
        await message.reply(f"❌ Не удалось снять бан пользователя {user_id}: {e}")
    
# --- ЗАПУСК И ВЕБХУК TELEGRAM ---
def get_telegram_webhook_path():
    secret = os.getenv("WEBHOOK_SECRET")
    if secret:
        return f"/webhook/{secret}"
    return "/webhook"


def get_safe_telegram_webhook_path():
    secret = os.getenv("WEBHOOK_SECRET")
    if secret:
        return "/webhook/***"
    return "/webhook"


async def on_startup(app):
    init_db()
    await bot.delete_webhook()
    
    await bot.set_my_commands([
        types.BotCommand("start", "Запуск бота"),
        types.BotCommand("menu", "Главное меню"),
        types.BotCommand("profile", "Мой профиль и подписка"),
        types.BotCommand("ask", "Задать вопрос"),
    ])

    domain = os.getenv("YOUR_DOMAIN")

    if not domain:
        logging.error("YOUR_DOMAIN не задан! Вебхук Telegram не установлен.")
    else:
        webhook_path = get_telegram_webhook_path()
        safe_webhook_path = get_safe_telegram_webhook_path()

        webhook_url = f"{domain}{webhook_path}"
        safe_webhook_url = f"{domain}{safe_webhook_path}"

        await bot.set_webhook(webhook_url)
        logging.info(f"Webhook установлен: {safe_webhook_url}")

    scheduler.add_job(
        check_subscriptions_and_reminders,
        'cron',
        hour=10,
        minute=0,
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1
    )

    scheduler.add_job(
        check_auto_free_lessons,
        'cron',
        minute=15,
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1
    )

    scheduler.add_job(
        check_free_lesson_followups,
        'cron',
        minute=30,
        misfire_grace_time=300,
        coalesce=True,
        max_instances=1
    )

    scheduler.add_job(
        send_db_backup,
        'cron',
        day_of_week='mon',
        hour=3,
        minute=0,
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

    scheduler.start()

async def on_shutdown(app):
    await bot.close()
    logging.info("Бот остановлен.")


if __name__ == "__main__":
    from aiogram.dispatcher.webhook import get_new_configured_app

    app = get_new_configured_app(dispatcher=dp, path=get_telegram_webhook_path())
    app.router.add_post('/stripe-payment', stripe_webhook)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host='0.0.0.0', port=port, access_log=None)
