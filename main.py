import os
import stripe
import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import psycopg2
from aiohttp import web

# Логирование (поможет видеть ошибки в Railway)
logging.basicConfig(level=logging.INFO)

# === ENV ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
DATABASE_URL = os.getenv("DATABASE_URL")
YOUR_DOMAIN = os.getenv("YOUR_DOMAIN")
PRICE_1M = os.getenv("PRICE_1M")
PRICE_6M = os.getenv("PRICE_6M")
PRICE_12M = os.getenv("PRICE_12M")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))

stripe.api_key = STRIPE_SECRET_KEY
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# Глобальная переменная для БД
conn = None

def init_db():
    global conn
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        telegram_id BIGINT PRIMARY KEY,
        paid BOOLEAN DEFAULT FALSE,
        subscription_id TEXT,
        expiry_date TIMESTAMP
    )
    """)
    conn.commit()
    cur.close()

# === HANDLERS ===
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    # Используем await для отправки
    await message.answer("""Добро пожаловать в обновлённую версию онлайн-клуба\n\nЭто пространство про осознанную работу с телом: без перегрузок, но с результатом.
Здесь вы найдёте систему тренировок и практик, которую можно встроить в свою жизнь: в своём ритме, в удобное время и с пониманием, что вы делаете.
Я рядом, в чате и на живых встречах.\n\nЧувствуйте себя комфортно и относитесь бережно к себе, к своему телу и друг другу.""")
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("💳 1 месяц", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев", callback_data="sub_12")
    )

    await message.answer("Выбери формат участия 👇", reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data.startswith("sub_"))
async def process_sub(callback_query: types.CallbackQuery):
    # 1. ОБЯЗАТЕЛЬНО отвечаем на callback, чтобы убрать «загрузку»
    await callback_query.answer()
    
    user_id = callback_query.from_user.id
    data = callback_query.data
    
    # Добавим логирование, чтобы видеть в консоли Railway, что кнопка нажата
    logging.info(f"Пользователь {user_id} нажал кнопку: {data}")

    try:
        price = PRICE_1M if data == "sub_1" else (PRICE_6M if data == "sub_6" else PRICE_12M)

        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{'price': price, 'quantity': 1}],
            mode='subscription',
            success_url=YOUR_DOMAIN,
            cancel_url=YOUR_DOMAIN,
            metadata={"user_id": str(user_id)}
        )
        await bot.send_message(user_id, f"Оплата здесь 👇\n{session.url}")
        
    except Exception as e:
        # Если ошибка, мы её увидим в логах и бот пришлет уведомление
        logging.error(f"Ошибка Stripe: {e}")
        await bot.send_message(user_id, "Произошла ошибка при создании платежа. Попробуй позже.")

# === WEBHOOK STUFF ===
async def stripe_webhook(request):
    payload = await request.text()
    sig = request.headers.get("stripe-signature")
    try:
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
    except:
        return web.Response(status=400)

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        user_id = int(session["metadata"]["user_id"])
        sub_id = session["subscription"]
        
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO users (telegram_id, paid, subscription_id, expiry_date)
        VALUES (%s, TRUE, %s, NOW() + INTERVAL '30 days')
        ON CONFLICT (telegram_id)
        DO UPDATE SET paid=TRUE, subscription_id=%s, expiry_date=NOW() + INTERVAL '30 days'
        """, (user_id, sub_id, sub_id))
        conn.commit()
        cur.close()
    
    return web.Response(status=200)

# === STARTUP ===
async def on_startup(app):
    logging.info("Starting bot...")
    init_db()
    # Если ты используешь вебхуки для ТГ, тут надо делать set_webhook
    # Для polling просто игнорируем, бот запустится через start_polling ниже

if __name__ == "__main__":
    # Запуск сервера
    app = web.Application()
    app.router.add_post('/webhook', stripe_webhook)
    app.on_startup.append(on_startup)
    
    # Запуск бота и сервера
    # Важно: это правильный способ запустить обоих вместе
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)
