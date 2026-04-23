import os
import logging
import asyncio
import stripe
import psycopg2
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.exceptions import BotBlocked
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = os.getenv("GROUP_ID") 
stripe.api_key = os.getenv("STRIPE_API_KEY")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# --- БАЗА ДАННЫХ ---
def init_db():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
    cur = conn.cursor()
    
    # 1. Создаем таблицу, если её нет
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            paid BOOLEAN DEFAULT FALSE,
            expiry_date TIMESTAMP
        );
    """)
    
    # 2. Добавляем колонку для подписки, если её нет (это и есть то самое обновление)
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT;")
    
    conn.commit()
    cur.close()
    conn.close()
    logging.info("--- БД ИНИЦИАЛИЗИРОВАНА И ПРОВЕРЕНА ---")

def save_user_to_db(user_id):
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (telegram_id, paid, expiry_date)
            VALUES (%s, TRUE, NOW() + INTERVAL '30 days')
            ON CONFLICT (telegram_id) DO UPDATE 
            SET paid = TRUE, expiry_date = NOW() + INTERVAL '30 days';
        """, (int(user_id),))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Ошибка сохранения в БД: {e}")

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
async def generate_invite_link():
    try:
        invite = await bot.create_chat_invite_link(chat_id=int(GROUP_ID), member_limit=1)
        return invite.invite_link
    except Exception as e:
        logging.error(f"Ошибка ссылки: {e}")
        return None

async def send_renewal_reminders():
    # Логика напоминаний (заглушка, которую вы можете дописать)
    logging.info("Проверка подписок для напоминаний...")

# --- ХЕНДЛЕРЫ ---
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    # Приветствие
    await bot.send_photo(
        chat_id=message.chat.id,
        photo="AgACAgIAAxkBAAMPaee4TD_FGuIQ4LProdOdL5XV5EkAAiYRaxulqkBL5YKQtOj0fV4BAAMCAAN5AAM7BA",
        caption="Добро пожаловать в обновлённую версию онлайн-клуба! Это пространство про осознанную работу с телом..."
    )
    await message.answer("Основные правила нашего клуба:\n1. Клуб закрытый...\n2. Запись обязательна...")
    await message.answer("Что входит в абонемент:\n- База тренировок\n- Живые тренировки\n- Обратная связь")
    
    # Кнопки подписки
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("💳 1 месяц", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев", callback_data="sub_12")
    )
    await bot.send_photo(
        chat_id=message.chat.id,
        photo="AgACAgIAAxkBAAMSaee9wO7psIiqhOR3M52AQ_aRwPgAAjgRaxulqkBLRv00tJs-NW8BAAMCAAN5AAM7BA",
        caption="Готова начать? Выбирай формат участия:",
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('sub_'))
async def process_payment(callback_query: types.CallbackQuery):
    price_map = {"sub_1": "PRICE_1M", "sub_6": "PRICE_6M", "sub_12": "PRICE_12M"}
    price_env_var = price_map.get(callback_query.data)
    
    session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        line_items=[{'price': os.getenv(price_env_var), 'quantity': 1}],
        mode='subscription',
        success_url='https://t.me/Natalia_SoulFit_bot',
        client_reference_id=str(callback_query.from_user.id)
    )
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("💳 Оплатить", url=session.url))
    await callback_query.message.edit_caption(caption="Отлично! Переходите по ссылке для оплаты:", reply_markup=kb)
    await callback_query.answer()

# --- WEBHOOK STRIPE ---
async def stripe_webhook(request):
    payload = await request.read()
    sig_header = request.headers.get('Stripe-Signature')
    
    # 1. Проверка подписи
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, os.getenv("STRIPE_WEBHOOK_SECRET"))
    except Exception as e:
        logging.error(f"Ошибка проверки подписи Stripe: {e}")
        return web.Response(status=400)

    # 2. Обработка успешного платежа
    if event.type == 'checkout.session.completed':
        session = event.data.object
        user_id = session.client_reference_id
        sub_id = session.subscription  # <-- ВОТ ЭТОТ ID МЫ ПОЛУЧАЕМ
    
    if user_id:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
        cur = conn.cursor()
        # Обновляем БД, сохраняя sub_id
        cur.execute("""
            UPDATE users SET paid = TRUE, expiry_date = NOW() + INTERVAL '30 days', stripe_subscription_id = %s
            WHERE telegram_id = %s;
        """, (sub_id, int(user_id)))
        conn.commit()
        cur.close()
        conn.close()
            
            link = await generate_invite_link()
            if link:
                try:
                    await bot.send_message(user_id, f"✅ Оплата прошла успешно! Ваша ссылка в клуб: {link}")
                except Exception as e:
                    logging.error(f"Не удалось отправить ссылку: {e}")
            else:
                logging.error("Не удалось сгенерировать ссылку-приглашение")
    
    return web.Response(status=200)
    # ... внутри stripe_webhook, там где сохраняем пользователя:
    if event.type == 'checkout.session.completed':
        session = event.data.object
        user_id = session.client_reference_id
        
        if user_id:
            # 1. Сначала разбаниваем (чтобы он мог войти по новой ссылке)
            try:
                await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(user_id), only_if_banned=True)
            except:
                pass 
            
            # 2. Сохраняем в БД
            await asyncio.to_thread(save_user_to_db, int(user_id))
            
            # 3. Генерируем ссылку
            link = await generate_invite_link()
            # ... далее отправка ссылки ...

async def send_renewal_reminders():
    logging.info("Запуск проверки истекших подписок...")
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
        cur = conn.cursor()
        # Ищем тех, у кого срок истек
        cur.execute("SELECT telegram_id FROM users WHERE expiry_date < NOW() AND paid = TRUE")
        expired_users = cur.fetchall()

        for user in expired_users:
            telegram_id = user[0]
            try:
                # 1. Кикаем из группы (ban в Telegram удаляет пользователя и запрещает вход)
                await bot.ban_chat_member(chat_id=int(GROUP_ID), user_id=telegram_id)
                # 2. Обновляем статус в БД
                cur.execute("UPDATE users SET paid = FALSE WHERE telegram_id = %s", (telegram_id,))
                await bot.send_message(telegram_id, "Ваша подписка истекла. Доступ в клуб ограничен.")
                logging.info(f"Пользователь {telegram_id} исключен.")
            except Exception as e:
                logging.error(f"Не удалось исключить {telegram_id}: {e}")
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Ошибка проверки подписок: {e}")

@dp.message_handler(commands=['profile'])
async def profile(message: types.Message):
    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
    cur = conn.cursor()
    cur.execute("SELECT paid, expiry_date FROM users WHERE telegram_id = %s", (message.from_user.id,))
    user = cur.fetchone()
    cur.close()
    conn.close()

    if not user or not user[0]:
        await message.answer("У вас пока нет активной подписки. Нажмите /start, чтобы оформить её.")
    else:
        expiry = user[1].strftime('%d.%m.%Y')
        text = f"Ваша подписка активна до: {expiry}\n\nХотите отменить автопродление?"
        
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("❌ Отменить подписку", callback_data="cancel_subscription"))
        
        await message.answer(text, reply_markup=keyboard)

@dp.callback_query_handler(text="cancel_subscription")
async def cancel_subscription(callback: types.CallbackQuery):
    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
    cur = conn.cursor()
    cur.execute("SELECT stripe_subscription_id FROM users WHERE telegram_id = %s", (callback.from_user.id,))
    result = cur.fetchone()
    
    if result and result[0]:
        sub_id = result[0]
        try:
            # Отменяем в Stripe
            stripe.Subscription.delete(sub_id)
            # Обновляем в БД
            cur.execute("UPDATE users SET paid = FALSE WHERE telegram_id = %s", (callback.from_user.id,))
            conn.commit()
            await callback.message.edit_text("✅ Подписка успешно отменена. Доступ сохранится до конца оплаченного периода.")
        except Exception as e:
            await callback.answer("Ошибка при отмене подписки. Напишите администратору.")
            logging.error(f"Ошибка Stripe: {e}")
    else:
        await callback.answer("Не удалось найти подписку.")
    
    cur.close()
    conn.close()

# --- ЗАПУСК ---
async def on_startup(app):
    init_db()
    # Планировщик
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_renewal_reminders, 'cron', hour=10)
    scheduler.start()
    # Вебхук
    await bot.set_webhook(f"{os.getenv('YOUR_DOMAIN')}/bot")

if __name__ == "__main__":
    from aiogram.dispatcher.webhook import get_new_configured_app
    app = get_new_configured_app(dispatcher=dp, path='/bot')
    app.router.add_post('/webhook', stripe_webhook)
    app.on_startup.append(on_startup)
    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
