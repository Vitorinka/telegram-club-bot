import os
import logging
import asyncio
import stripe
import psycopg2
import subprocess
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.exceptions import BotBlocked
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
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
    # Добавляем недостающие колонки (для старых БД)
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS payment_failed BOOLEAN DEFAULT FALSE;")
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
    conn.commit()
    cur.close()
    conn.close()
    logging.info("--- БД ИНИЦИАЛИЗИРОВАНА И ПРОВЕРЕНА ---")

# Идемпотентность вебхуков
async def is_event_processed(event_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM stripe_events WHERE event_id = %s", (event_id,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists

async def mark_event_processed(event_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO stripe_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING", (event_id,))
    conn.commit()
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
async def refresh_active_stripe_subscription(telegram_id, stripe_subscription_id, cur):
    if not stripe_subscription_id:
        return False

    try:
        subscription = await asyncio.to_thread(stripe.Subscription.retrieve, stripe_subscription_id)
        status = getattr(subscription, 'status', None)
        current_period_end = getattr(subscription, 'current_period_end', None)

        if status in ('active', 'trialing') and current_period_end:
            new_expiry = datetime.utcfromtimestamp(current_period_end)

            if new_expiry > datetime.utcnow():
                cur.execute("""
                    UPDATE users
                    SET paid = TRUE,
                        expiry_date = %s,
                        payment_failed = FALSE,
                        grace_period_end = NULL,
                        reminder_sent = FALSE
                    WHERE telegram_id = %s
                """, (new_expiry, int(telegram_id)))

                logging.info(
                    f"Пользователь {telegram_id} не удален: Stripe подписка активна до {new_expiry} UTC."
                )
                return True

    except Exception as e:
        logging.error(f"Не удалось перепроверить Stripe-подписку {stripe_subscription_id} для {telegram_id}: {e}")
        await notify_admins(
            f"Не смогла перепроверить Stripe перед удалением пользователя {telegram_id}.\n"
            f"subscription_id: {stripe_subscription_id}\n"
            f"Ошибка: {e}\n\n"
            "Пользователь пока НЕ удален автоматически. Проверьте вручную."
        )
        return True

    return False

async def ban_user_logic(telegram_id, cur):
    cur.execute("""
        SELECT paid, expiry_date, stripe_subscription_id
        FROM users
        WHERE telegram_id = %s
    """, (int(telegram_id),))
    user = cur.fetchone()

    if not user:
        logging.warning(f"Пользователь {telegram_id} не удален: пользователь не найден в БД.")
        return "not_found"

    paid, expiry_date, stripe_subscription_id = user
    now = datetime.utcnow()

    if paid and expiry_date and expiry_date > now:
        logging.info("Пользователь не удален: доступ уже активен в БД")
        return "active_in_db"

    if await refresh_active_stripe_subscription(telegram_id, stripe_subscription_id, cur):
        return "stripe_protected"

    # 1. Пытаемся удалить пользователя из группы
    status = "removed"
    try:
        await bot.kick_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
        try:
            await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(telegram_id))
        except Exception as e:
            logging.error(f"Пользователь {telegram_id} удален, но не удалось снять бан: {e}")
            await notify_admins(
                f"Пользователь {telegram_id} удален из группы, но не удалось снять бан.\n"
                f"Ошибка: {e}"
            )
        logging.info(f"Пользователь {telegram_id} удален из группы из-за истечения подписки.")
    except Exception as e:
        logging.error(f"Не удалось удалить пользователя {telegram_id} из группы: {e}")
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
        logging.info(f"Пользователь {telegram_id} заблокировал бота.")
        await notify_critical_delivery_failed(
            telegram_id,
            "subscription_expired",
            "сообщение об окончании подписки",
            "BotBlocked",
            "paid = FALSE; доступ закрыт в БД"
        )
    except Exception as e:
        logging.error(f"Не удалось отправить сообщение об окончании доступа пользователю {telegram_id}: {e}")
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
        SELECT telegram_id, expiry_date, payment_failed, grace_period_end, auto_renew, reminder_sent, trial_used, stripe_subscription_id
        FROM users
        WHERE paid = TRUE
          AND expiry_date IS NOT NULL
          AND (blocked_bot IS NOT TRUE)
    """)
    users = cur.fetchall()
    now = datetime.utcnow()
    checked_total = len(users)
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

    for (telegram_id, expiry, payment_failed, grace_end, auto_renew, reminder_sent, _, stripe_subscription_id) in users:
        time_left = expiry - now

        # ----- Истекший доступ -----
        if time_left.total_seconds() < 0:
            expired_total += 1

            if payment_failed and grace_end and now < grace_end:
                continue

            # Общий льготный период 2 дня
            if -time_left.total_seconds() < 2 * 86400:
                grace_total += 1

                if auto_renew and stripe_subscription_id:
                    if await refresh_active_stripe_subscription(telegram_id, stripe_subscription_id, cur):
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
                continue
            else:
                if await refresh_active_stripe_subscription(telegram_id, stripe_subscription_id, cur):
                    cur.execute(
                        "SELECT expiry_date FROM users WHERE telegram_id = %s",
                        (telegram_id,)
                    )
                    row = cur.fetchone()
                    refreshed_expiry = row[0] if row else None
                    pending_access_events.append({
                        "telegram_id": telegram_id,
                        "event_type": "auto_stripe_protected_before_removal",
                        "source": "auto_check",
                        "old_expiry": expiry,
                        "new_expiry": refreshed_expiry,
                        "stripe_subscription_id": stripe_subscription_id,
                        "notes": "Stripe subscription active during expired-user check"
                    })
                    stripe_protected += 1
                    continue

                ban_status = await ban_user_logic(telegram_id, cur)

                if ban_status == "active_in_db":
                    active_in_db_skipped += 1
                elif ban_status == "stripe_protected":
                    cur.execute(
                        "SELECT expiry_date FROM users WHERE telegram_id = %s",
                        (telegram_id,)
                    )
                    row = cur.fetchone()
                    refreshed_expiry = row[0] if row else None
                    pending_access_events.append({
                        "telegram_id": telegram_id,
                        "event_type": "auto_stripe_protected_before_removal",
                        "source": "auto_check",
                        "old_expiry": expiry,
                        "new_expiry": refreshed_expiry,
                        "stripe_subscription_id": stripe_subscription_id,
                        "notes": "Stripe subscription protected user inside ban_user_logic"
                    })
                    stripe_protected += 1
                elif ban_status == "not_found":
                    not_found_total += 1
                elif ban_status in ("removed", "kick_failed"):
                    pending_access_events.append({
                        "telegram_id": telegram_id,
                        "event_type": "auto_access_closed_expired",
                        "source": "auto_check",
                        "old_expiry": expiry,
                        "new_expiry": None,
                        "stripe_subscription_id": stripe_subscription_id,
                        "notes": f"ban_status={ban_status}"
                    })
                    removed_total += 1
                    if ban_status == "kick_failed":
                        telegram_errors += 1

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
                    if "ChatNotFound" in str(e) or "bot was blocked" in str(e):
                        cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (telegram_id,))

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

    try:
        await message.delete()
        logging.info(f"Удалено системное сообщение о входе/выходе пользователя в группе {message.chat.id}")
    except Exception as e:
        logging.warning(f"Не удалось удалить системное сообщение в группе {message.chat.id}: {e}")

# --- ХЕНДЛЕРЫ КОМАНД И КОЛБЭКОВ ---
@dp.message_handler(commands=['start'], state='*')
async def start(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = message.from_user.id

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

    # Получаем данные пользователя из БД
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT trial_used, paid FROM users WHERE telegram_id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    trial_used = row[0] if row else False
    paid = row[1] if row else False

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

    mode = 'payment' if sub_type == "sub_trial" else 'subscription'

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

        session = stripe.checkout.Session.create(**session_params)
        new_kb = InlineKeyboardMarkup(row_width=1).add(
            InlineKeyboardButton("💳 Перейти к оплате", url=session.url),
            InlineKeyboardButton("🔙 Назад к тарифам", callback_data="back_to_tariffs")
        )
        # Меняем клавиатуру исходного сообщения (безопасно)
        await callback.message.edit_reply_markup(reply_markup=new_kb)
        await state.finish()
    except Exception as e:
        logging.error(f"Stripe ошибка: {e}")
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
        logging.error(f"Ошибка отмены подписки {sub_id}: {e}")
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
            kb.add(InlineKeyboardButton("💳 Продлить доступ", callback_data="show_renew_options"))

            if stripe_subscription_id and auto_renew:
                kb.add(InlineKeyboardButton("❌ Отменить автопродление", callback_data="cancel_subscription"))

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
            INSERT INTO users (telegram_id, paid, expiry_date)
            VALUES (%s, TRUE, NOW() + INTERVAL '%s days')
            ON CONFLICT (telegram_id) DO UPDATE 
            SET paid = TRUE, 
                expiry_date = CASE 
                    WHEN users.expiry_date > NOW() THEN users.expiry_date + INTERVAL '%s days'
                    ELSE NOW() + INTERVAL '%s days'
                END,
                payment_failed = FALSE,
                grace_period_end = NULL,
                blocked_bot = FALSE;
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
                blocked_bot
            )
            VALUES (%s, TRUE, %s, FALSE, NULL, FALSE, FALSE)
            ON CONFLICT (telegram_id) DO UPDATE
            SET paid = TRUE,
                expiry_date = EXCLUDED.expiry_date,
                payment_failed = FALSE,
                grace_period_end = NULL,
                reminder_sent = FALSE,
                blocked_bot = FALSE
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

        status = getattr(subscription, 'status', None)
        current_period_end = getattr(subscription, 'current_period_end', None)
        customer = getattr(subscription, 'customer', None)
        cancel_at_period_end = bool(getattr(subscription, 'cancel_at_period_end', False))
        customer_id = customer if isinstance(customer, str) else getattr(customer, 'id', None)
        auto_renew = not cancel_at_period_end
        period_end_text = "нет"

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
                    grace_period_end = NULL,
                    reminder_sent = FALSE,
                    auto_renew = %s
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
                notes=f"status={status}; auto_renew={auto_renew}; admin_id={message.from_user.id}"
            )

            await message.reply(
                "✅ Stripe-синхронизация выполнена\n\n"
                f"telegram_id: {target_user_id}\n"
                f"status: {status}\n"
                "paid: TRUE\n"
                f"expiry_date: {new_expiry.strftime('%d.%m.%Y %H:%M')}\n"
                f"auto_renew: {auto_renew}\n"
                f"stripe_subscription_id: {stripe_subscription_id}\n"
                f"stripe_customer_id: {customer_id or 'нет'}"
            )
            return

        if status in ('active', 'trialing') and not current_period_end:
            await message.reply(
                "⚠️ Подписка в Stripe активна, но у нее нет current_period_end.\n\n"
                f"telegram_id: {target_user_id}\n"
                f"status: {status}\n"
                f"stripe_subscription_id: {stripe_subscription_id}\n"
                f"stripe_customer_id: {customer_id or 'нет'}\n\n"
                "БД автоматически не обновлена. Проверьте подписку вручную в Stripe."
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

@dp.message_handler(commands=['admin_help'], state='*')
async def admin_help_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    text = (
        "🛠 Админ-команды бота\n\n"
        "/stats — общая статистика\n"
        "/user <telegram_id> — карточка пользователя\n"
        "/expired_users — пользователи с истекшей датой, но paid=True\n"
        "/give_access <telegram_id> [дней] — выдать доступ вручную\n"
        "/set_expiry <telegram_id> <dd.mm.yyyy> [hh:mm] — установить точную дату окончания доступа\n"
        "/sync_stripe_user <telegram_id> — вручную синхронизировать пользователя со Stripe\n"
        "/access_history <telegram_id> — история действий по доступу\n"
        "/recent_access_events — последние события по доступу\n"
        "/find_by_stripe <id> — найти пользователя по Stripe ID\n"
        "/bot_health — диагностика бота и базы\n"
        "/broadcast текст — текстовая рассылка всем пользователям\n"
        "/promo_trial — промо-рассылка с фото/видео и кнопкой триала для тех кого еще нет в клубе\n"
        "/test_expiry — вручную запустить проверку подписок\n"
        "/test_grace <telegram_id> — тестово поставить grace period на 24 часа\n"
        "/test_backup — вручную запустить бэкап базы\n"
        "/unblock_user <telegram_id> — снять blocked_bot в базе\n\n"
        "/unban_user <telegram_id> — снять бан пользователя в Telegram-группе\n"
        "/expiring_users — пользователи, у которых подписка заканчивается в ближайшие 48 часов\n"
        "/test_followup <telegram_id> — тестово отправить follow-up после бесплатного урока\n"
        "/test_auto_lesson <telegram_id> — тестово отправить бесплатный урок\n"
        "/send_user <telegram_id> текст — написать конкретному пользователю\n"
        "⚠️ Важно: команды с доступом и рассылками используй аккуратно."
    )

    await message.answer(text)

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
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, os.getenv("STRIPE_WEBHOOK_SECRET")
        )
    except Exception as e:
        logging.error(f"Ошибка подписи вебхука: {e}")
        return web.Response(status=400)

    event_id = event['id']
    if await is_event_processed(event_id):
        return web.Response(status=200)

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
                key: safe_stripe_repr(val)
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
            'event_id': event_id,
            'invoice_id': stripe_value(invoice, 'id'),
            'billing_reason': stripe_value(invoice, 'billing_reason'),
            'status': stripe_value(invoice, 'status'),
            'amount_paid': stripe_value(invoice, 'amount_paid'),
            'currency': stripe_value(invoice, 'currency'),
            'customer': stripe_object_id(stripe_value(invoice, 'customer')),
            'customer_email': stripe_value(invoice, 'customer_email'),
            'subscription': stripe_object_id(stripe_value(invoice, 'subscription')),
            'parent_subscription': stripe_object_id(stripe_value(invoice, 'parent', 'subscription_details', 'subscription')),
            'resolved_subscription_id': subscription_id,
            'payment_intent': stripe_object_id(stripe_value(invoice, 'payment_intent')),
            'hosted_invoice_url': stripe_value(invoice, 'hosted_invoice_url'),
            'metadata': safe_stripe_repr(stripe_value(invoice, 'metadata')),
            'lines_count': len(lines_data),
            'first_line': {
                'id': stripe_value(first_line, 'id'),
                'price_id': stripe_object_id(stripe_value(first_line, 'price')),
                'subscription': stripe_object_id(stripe_value(first_line, 'subscription')),
                'period_start': stripe_value(first_line, 'period', 'start'),
                'period_end': stripe_value(first_line, 'period', 'end'),
            } if first_line else None,
        }
        logging.info(f"STRIPE INVOICE DEBUG: {debug_payload}")

    async def notify_unlinked_invoice(invoice, subscription_id=None):
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
        hosted_invoice_url = stripe_value(invoice, 'hosted_invoice_url') or "нет"

        await notify_admins(
            "Stripe прислал успешную оплату, но пользователя в БД не удалось надежно определить.\n\n"
            f"invoice_id: {invoice_id}\n"
            f"event_id: {event_id}\n"
            f"subscription_id: {subscription_id or 'нет'}\n"
            f"billing_reason: {billing_reason}\n"
            f"customer_id: {customer_id}\n"
            f"customer_email: {customer_email}\n"
            f"amount_paid: {amount_paid if amount_paid is not None else 'нет'}\n"
            f"hosted_invoice_url: {hosted_invoice_url}\n\n"
            f"Пустые subscription-поля: {empty_subscription_fields_text(invoice)}\n\n"
            "Доступ автоматически НЕ выдан. Проверьте оплату вручную."
        )

    # ---------- 1. ОПЛАТА ЧЕРЕЗ CHECKOUT (ПЕРВИЧНАЯ ИЛИ ПРОДЛЕНИЕ) ----------
    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        user_id = getattr(session, 'client_reference_id', None)
        if not user_id:
            await mark_event_processed(event_id)
            return web.Response(status=200)

        sub_id = stripe_object_id(stripe_value(session, 'subscription'))
        customer_id = stripe_object_id(stripe_value(session, 'customer'))
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
                INSERT INTO users (telegram_id, paid, expiry_date, stripe_subscription_id, stripe_customer_id, auto_renew, trial_used, payment_failed, grace_period_end, first_payment_done)
                VALUES (%s, TRUE, %s, %s, %s, TRUE, %s, FALSE, NULL, FALSE)
                ON CONFLICT (telegram_id) DO UPDATE SET
                    paid = TRUE,
                    expiry_date = EXCLUDED.expiry_date,
                    stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, users.stripe_subscription_id),
                    stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, users.stripe_customer_id),
                    trial_used = CASE WHEN EXCLUDED.trial_used = TRUE THEN TRUE ELSE users.trial_used END,
                    payment_failed = FALSE,
                    grace_period_end = NULL,
                    auto_renew = TRUE,
                    reminder_sent = FALSE,
                    first_payment_done = CASE WHEN %s THEN FALSE ELSE COALESCE(users.first_payment_done, FALSE) END
            """, (int(user_id), new_expiry, sub_id, customer_id, is_trial, needs_link))
            conn.commit()

            await log_access_event(
                user_id,
                "stripe_checkout_completed",
                source="stripe_webhook",
                old_expiry=old_expiry,
                new_expiry=new_expiry,
                stripe_event_id=event_id,
                stripe_subscription_id=sub_id,
                notes=f"days={days_to_add}; customer_id={customer_id or 'нет'}"
            )

            if needs_link:
                link = await generate_invite_link()
                msg = f"✅ Оплата прошла успешно! Доступ до {new_expiry.strftime('%d.%m.%Y')}.\nСсылка для вступления: {link}\n\nДобро пожаловать!"
            else:
                msg = f"✅ Ваша подписка продлена до {new_expiry.strftime('%d.%m.%Y')}. Спасибо! ❤️"
            try:
                await bot.send_message(int(user_id), msg)
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
            try:
                await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(user_id))
            except Exception as e:
                if "administrator" in str(e).lower():
                    logging.warning(f"Не удалось разбанить админа {user_id}: {e}")
                else:
                    logging.error(f"Ошибка разбана {user_id}: {e}")
        except Exception as e:
            conn.rollback()
            logging.error(f"Ошибка checkout: {e}")
        finally:
            cur.close()
            conn.close()

    # ---------- 2. УСПЕШНОЕ АВТОПРОДЛЕНИЕ (invoice.payment_succeeded) ----------
        # ---------- 2. УСПЕШНОЕ АВТОПРОДЛЕНИЕ (invoice.payment_succeeded) ----------
    elif event['type'] == 'invoice.payment_succeeded':
        invoice = event['data']['object']
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
                logging.error(f"Не удалось повторно получить invoice {stripe_value(invoice, 'id')}: {e}")

        log_invoice_debug(invoice, subscription_id=sub_id)

        conn = get_db_conn()
        cur = conn.cursor()

        try:
            if not sub_id:
                logging.error(f"invoice.payment_succeeded: не найден subscription_id, event={event_id}")
                await notify_unlinked_invoice(invoice)
                conn.commit()
                await mark_event_processed(event_id)
                return web.Response(status=200)

            subscription = stripe.Subscription.retrieve(sub_id)
            customer_id = customer_id or stripe_object_id(stripe_value(subscription, 'customer'))
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

            cur.execute("""
                WITH target AS (
                    SELECT telegram_id, expiry_date AS old_expiry
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
                    grace_period_end = NULL,
                    reminder_sent = FALSE,
                    auto_renew = TRUE
                FROM target
                WHERE users.telegram_id = target.telegram_id
                RETURNING users.telegram_id, target.old_expiry
            """, (sub_id, new_expiry, new_expiry, sub_id, customer_id))

            row = cur.fetchone()
            if row:
                old_expiry = row[1]

            if not row:
                metadata_telegram_id = stripe_value(subscription, 'metadata', 'telegram_id')

                if metadata_telegram_id:
                    try:
                        metadata_telegram_id = int(metadata_telegram_id)
                    except (TypeError, ValueError):
                        logging.error(
                            f"invoice.payment_succeeded: некорректный metadata.telegram_id={metadata_telegram_id}, "
                            f"subscription_id={sub_id}, event={event_id}"
                        )
                    else:
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
                                grace_period_end = NULL,
                                reminder_sent = FALSE,
                                auto_renew = TRUE
                            FROM target
                            WHERE users.telegram_id = target.telegram_id
                            RETURNING users.telegram_id, target.old_expiry
                        """, (metadata_telegram_id, new_expiry, new_expiry, sub_id, customer_id))

                        row = cur.fetchone()
                        if row:
                            old_expiry = row[1]

            if not row and customer_id:
                cur.execute("""
                    WITH target AS (
                        SELECT telegram_id, expiry_date AS old_expiry
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
                        grace_period_end = NULL,
                        reminder_sent = FALSE,
                        auto_renew = TRUE
                    FROM target
                    WHERE users.telegram_id = target.telegram_id
                    RETURNING users.telegram_id, target.old_expiry
                """, (customer_id, new_expiry, new_expiry, sub_id, customer_id))

                row = cur.fetchone()
                if row:
                    old_expiry = row[1]

            conn.commit()

            if not row:
                logging.error(
                    f"invoice.payment_succeeded: пользователь не найден. "
                    f"subscription_id={sub_id}, customer_id={customer_id}, event={event_id}"
                )

                await notify_unlinked_invoice(invoice, subscription_id=sub_id)
                await mark_event_processed(event_id)
                return web.Response(status=200)

            telegram_id = row[0]
            invoice_id = stripe_value(invoice, 'id') or "нет"

            if old_expiry and old_expiry >= new_expiry:
                logging.info(
                    f"invoice.payment_succeeded: срок уже актуален, пропускаю повторное уведомление. "
                    f"telegram_id={telegram_id}, old_expiry={old_expiry}, new_expiry={new_expiry}, event={event_id}"
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
                notes=f"customer_id={customer_id or 'нет'}; invoice_id={invoice_id}; period_source={period_source}"
            )

            try:
                await bot.send_message(
                    int(telegram_id),
                    f"✅ Автопродление успешно! Доступ продлен до {new_expiry.strftime('%d.%m.%Y')}. Хорошего дня!"
                )
            except BotBlocked:
                cur.execute(
                    "UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s",
                    (int(telegram_id),)
                )
                conn.commit()
                await notify_critical_delivery_failed(
                    telegram_id,
                    "invoice.payment_succeeded",
                    "сообщение об успешном автопродлении",
                    "BotBlocked",
                    f"paid = TRUE; expiry_date = {new_expiry.strftime('%d.%m.%Y %H:%M')}; blocked_bot = TRUE"
                )
            except Exception as e:
                logging.error(f"Не удалось отправить сообщение об автопродлении {telegram_id}: {e}")
                await notify_critical_delivery_failed(
                    telegram_id,
                    "invoice.payment_succeeded",
                    "сообщение об успешном автопродлении",
                    e,
                    f"paid = TRUE; expiry_date = {new_expiry.strftime('%d.%m.%Y %H:%M')}"
                )

        except Exception as e:
            conn.rollback()
            logging.error(f"Ошибка invoice.payment_succeeded: {e}")
            await notify_admins(
                f"Ошибка обработки успешной оплаты Stripe.\n\n"
                f"subscription_id: {sub_id}\n"
                f"event_id: {event_id}\n"
                f"Ошибка: {e}"
            )

        finally:
            cur.close()
            conn.close()
    
    # ---------- 3. ОШИБКА ОПЛАТЫ (invoice.payment_failed) – GRACE PERIOD ----------
    elif event['type'] == 'invoice.payment_failed':
        invoice = event['data']['object']
        sub_id = getattr(invoice, 'subscription', None)
        if sub_id:
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute("""
                UPDATE users 
                SET payment_failed = TRUE, 
                    grace_period_end = NOW() + INTERVAL '1 day' 
                WHERE stripe_subscription_id = %s
            """, (sub_id,))
            conn.commit()
            cur.execute("SELECT telegram_id FROM users WHERE stripe_subscription_id = %s", (sub_id,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                try:
                    await bot.send_message(row[0], 
                        "⚠️ Не удалось списать оплату за подписку. У вас есть 24 часа, чтобы пополнить карту или связаться с администратором.\n"
                        "После устранения проблемы доступ восстановится автоматически.")
                except BotBlocked:
                    pass

    # ---------- 4. ПОЛЬЗОВАТЕЛЬ ОТМЕНИЛ ПОДПИСКУ (customer.subscription.deleted) ----------
    elif event['type'] == 'customer.subscription.deleted':
        sub = event['data']['object']
        sub_id = getattr(sub, 'id', None)
        if sub_id:
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute("""
                UPDATE users 
                SET paid = FALSE, 
                    stripe_subscription_id = NULL 
                WHERE stripe_subscription_id = %s
            """, (sub_id,))
            conn.commit()
            cur.close()
            conn.close()

    # ---------- 4.1. ОБНОВЛЕНИЕ ПОДПИСКИ (customer.subscription.updated) ----------
    elif event['type'] == 'customer.subscription.updated':
        sub = event['data']['object']
        sub_id = stripe_object_id(stripe_value(sub, 'id'))
        cancel_at_period_end = bool(stripe_value(sub, 'cancel_at_period_end'))
        if sub_id:
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute("""
                UPDATE users 
                SET auto_renew = %s 
                WHERE stripe_subscription_id = %s
            """, (not cancel_at_period_end, sub_id))
            conn.commit()
            cur.close()
            conn.close()

    # ---------- 5. СЕССИЯ ОПЛАТЫ ИСТЕКЛА ИЛИ НЕ УДАЛАСЬ ----------
    elif event['type'] in ('checkout.session.expired', 'checkout.session.async_payment_failed'):
        session = event['data']['object']
        user_id = getattr(session, 'client_reference_id', None)

        if user_id:
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
