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
    # 1. Сначала фото + текст
    # Замени URL на свою ссылку или file_id
    await message.answer_photo(
        photo="AgACAgIAAxkBAAMPaee4TD_FGuIQ4LProdOdL5XV5EkAAiYRaxulqkBL5YKQtOj0fV4BAAMCAAN5AAM7BA", 
        caption="""Добро пожаловать в обновлённую версию онлайн-клуба
Это пространство про осознанную работу с телом: без перегрузок, но с результатом.
Здесь вы найдёте систему тренировок и практик, которую можно встроить в свою жизнь: в своём ритме, в удобное время и с пониманием, что вы делаете.
Я рядом, в чате и на живых встречах.
Чувствуйте себя комфортно и относитесь бережно к себе, к своему телу и друг другу."""
    )
    
    await asyncio.sleep(1) # Пауза для естественности
    
    # 2. Большой текст №2
    await message.answer("""Основные правила, по которым мы будем взаимодействовать:
1.Клуб закрытый и включает:
- неограниченный доступ ко всем материалам
- тренировки в записи
- рецепты
- общение и обратную связь

Также остаются живые тренировки по расписанию.

2. На живые тренировки обязательна предварительная запись.

3. Чтобы записаться, нужно отметить себя в голосовании, которое я буду создавать накануне занятия.

4. Если на тренировку записывается менее 3 человек, занятие не проводится

5. Записей живых тренировок не будет.

6. Заморозка абонемента не предусмотрена, так как у вас всегда есть доступ ко всем тренировкам в записи и вы можете заниматься в удобное время.""")
    
    await asyncio.sleep(1)
    
    # 3. Большой текст №3
    await message.answer("""Что входит в абонемент клуба:
- большая база тренировок разной направленности, которая будет постоянно пополняться:
антисутулость, сила и гибкость, работа с мышцами тазового дна, ягодицы, руки, ноги, кор, балансы

- тренировки, направленные не только на тело, но и на улучшение нейропластичности, координации и общего качества движений

— короткие зарядки 10-15 минут для ежедневной практики

- мини-уроки: дыхание, работа со стопами, расслабление

— медитации и техники восстановления

- живые тренировки со мной
это не просто тренировки, а возможность поработать со мной лично: разобрать технику, задать вопросы, скорректировать движения и глубже понять своё тело

- постоянная обратная связь: вы можете задавать любые вопросы в чате, я всегда на связи""")
    
    await asyncio.sleep(1)
    
    # 4. Финальное фото + призыв + кнопки
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("💳 1 месяц", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев", callback_data="sub_12")
    )
    
    await message.answer_photo(
        photo="AgACAgIAAxkBAAMSaee9wO7psIiqhOR3M52AQ_aRwPgAAjgRaxulqkBLRv00tJs-NW8BAAMCAAN5AAM7BA",
        caption="""Готова начать? 
Выбирай формат участия ниже и присоединяйся к нам 👇""",
        reply_markup=keyboard
    )

@dp.callback_query_handler() # Убрали фильтр lambda
async def all_callbacks(callback_query: types.CallbackQuery):
    # Логируем абсолютно всё, что пришло
    logging.info(f"!!! ПРИШЛО СОБЫТИЕ: callback_data = '{callback_query.data}'")
    
    # Отвечаем пользователю, чтобы увидеть реакцию в боте
    await callback_query.answer(f"Я получил: {callback_query.data}")

    # Теперь проверяем, то ли это, что нам нужно
    if callback_query.data.startswith("sub_"):
        await process_sub_logic(callback_query) # Вызываем логику Stripe
    else:
        logging.info("Нажата неизвестная кнопка")

async def process_sub_logic(callback_query: types.CallbackQuery):
    # Сюда перенеси код, который раньше был в process_sub
    user_id = callback_query.from_user.id
    data = callback_query.data
    # ... (весь твой код создания Stripe сессии) ...
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

async def on_startup(app):
    logging.info("Starting bot...")
    init_db()  # Инициализация БД
    webhook_url = f"{YOUR_DOMAIN}/bot"
    # Настраиваем вебхук и сразу отбрасываем старые команды
    await bot.set_webhook(webhook_url, drop_pending_updates=True)
    logging.info(f"Telegram webhook set to {webhook_url}")
        
async def on_shutdown(app):
    logging.info("Shutting down...") # Это чтобы мы видели, что он начал выключаться
    await bot.close()                # <--- ЭТА СТРОЧКА "ВЫКЛЮЧАЕТ СВЕТ"
    await bot.delete_webhook()       # А эта "запирает дверь"
        
if __name__ == "__main__":
    from aiogram.dispatcher.webhook import get_new_configured_app
    
    # Создаем приложение, которое слушает /bot и /webhook
    app = get_new_configured_app(dispatcher=dp, path='/bot')
    app.router.add_post('/webhook', stripe_webhook)
    
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    
    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
