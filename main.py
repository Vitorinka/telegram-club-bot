import os
import stripe
import asyncio
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor

import psycopg2
from aiohttp import web

# === ENV ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
DATABASE_URL = os.getenv("DATABASE_URL")
YOUR_DOMAIN = os.getenv("YOUR_DOMAIN")

PRICE_1M = os.getenv("PRICE_1M")
PRICE_6M = os.getenv("PRICE_6M")
PRICE_12M = os.getenv("PRICE_12M")

CHANNEL_ID = int(os.getenv("CHANNEL_ID"))  # пример: -100XXXXXXXX

stripe.api_key = STRIPE_SECRET_KEY

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# === DB ===
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

# === START ===
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.answer_photo(
        photo="https://via.placeholder.com/600x400",
        caption="Добро пожаловать в закрытый клуб ✨"
    )

    await asyncio.sleep(2)
    await message.answer("Ты получишь поддержку и окружение")
    await asyncio.sleep(1)
    await message.answer("Пошаговые разборы и рост")
    await asyncio.sleep(1)
    await message.answer("И доступ к закрытому комьюнити")

    await asyncio.sleep(2)

    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("💳 1 месяц", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев", callback_data="sub_12")
    )

    await message.answer_photo(
        photo="https://via.placeholder.com/600x400",
        caption="Выбери формат участия 👇",
        reply_markup=keyboard
    )

# === CREATE CHECKOUT ===
async def create_checkout(user_id, price_id):
    session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        line_items=[{
            'price': price_id,
            'quantity': 1,
        }],
        mode='subscription',
        success_url=YOUR_DOMAIN,
        cancel_url=YOUR_DOMAIN,
        metadata={"user_id": str(user_id)}
    )
    return session.url

# === BUTTONS ===
@dp.callback_query_handler(lambda c: c.data.startswith("sub_"))
async def process_sub(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id

    if callback_query.data == "sub_1":
        price = PRICE_1M
    elif callback_query.data == "sub_6":
        price = PRICE_6M
    else:
        price = PRICE_12M

    url = await create_checkout(user_id, price)

    await bot.send_message(user_id, f"Оплата здесь 👇\n{url}")

    # дожим через 20 минут
    await asyncio.sleep(1200)
    await bot.send_message(
        user_id,
        "Если не получилось оплатить — напиши мне, помогу 💬"
    )

# === ACCESS ===
@dp.message_handler(commands=['access'])
async def access(message: types.Message):
    user_id = message.from_user.id

    cur.execute("SELECT paid, expiry_date FROM users WHERE telegram_id=%s", (user_id,))
    res = cur.fetchone()

    if res and res[0] and res[1] and res[1] > datetime.utcnow():
        invite = await bot.create_chat_invite_link(
            chat_id=CHANNEL_ID,
            member_limit=1
        )
        await message.answer(f"Твой доступ 👇\n{invite.invite_link}")
    else:
        await message.answer("Подписка не активна ❌")

# === WEBHOOK ===
async def stripe_webhook(request):
    payload = await request.text()
    sig = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
    except Exception:
        return web.Response(status=400)

    # оплата прошла
    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        user_id = int(session["metadata"]["user_id"])
        sub_id = session["subscription"]

        cur.execute("""
        INSERT INTO users (telegram_id, paid, subscription_id, expiry_date)
        VALUES (%s, TRUE, %s, NOW() + INTERVAL '30 days')
        ON CONFLICT (telegram_id)
        DO UPDATE SET paid=TRUE, subscription_id=%s, expiry_date=NOW() + INTERVAL '30 days'
        """, (user_id, sub_id, sub_id))
        conn.commit()

        await bot.send_message(user_id, "Оплата прошла ✅ Напиши /access")

    # продление
    if event["type"] == "invoice.paid":
        sub_id = event["data"]["object"]["subscription"]

        cur.execute("""
        UPDATE users SET expiry_date = NOW() + INTERVAL '30 days'
        WHERE subscription_id=%s
        """, (sub_id,))
        conn.commit()

    # отмена
    if event["type"] == "customer.subscription.deleted":
        sub_id = event["data"]["object"]["id"]

        cur.execute("UPDATE users SET paid=FALSE WHERE subscription_id=%s", (sub_id,))
        conn.commit()

    return web.Response(status=200)

# === AUTO CLEAN ===
async def cleaner():
    while True:
        cur.execute("""
        SELECT telegram_id FROM users
        WHERE expiry_date < NOW() AND paid=TRUE
        """)
        users = cur.fetchall()

        for u in users:
            uid = u[0]
            try:
                await bot.ban_chat_member(CHANNEL_ID, uid)
                await bot.unban_chat_member(CHANNEL_ID, uid)
            except:
                pass

            cur.execute("UPDATE users SET paid=FALSE WHERE telegram_id=%s", (uid,))
            conn.commit()

        await asyncio.sleep(3600)

# === SERVER ===
app = web.Application()
app.router.add_post('/webhook', stripe_webhook)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(cleaner())
    loop.create_task(dp.start_polling())
    web.run_app(app, port=8000)
