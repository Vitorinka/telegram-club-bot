import os
import logging
import asyncio
import stripe
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import psycopg2 

# Логирование
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Инициализация
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = os.getenv("-1003983497950") # ID твоей группы (например, -100123456789)
stripe.api_key = os.getenv("STRIPE_API_KEY")
WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

PRICES = {
    "sub_1": "price_1TOGfWLHHLEfZoWaJT8ED6TN",  # Твой ID для 1 месяца
    "sub_6": "price_1TOGg7LHHLEfZoWaXDDBtD1f",  # Твой ID для 6 месяцев
    "sub_12": "price_1TOGg7LHHLEfZoWaXDDBtD1f"  # Твой ID для 12 месяцев
}

# --- ФУНКЦИЯ СОЗДАНИЯ ССЫЛКИ ---
async def generate_invite_link():
    try:
        invite = await bot.create_chat_invite_link(
            chat_id=-1003983497950,
            member_limit=1, # Ссылка на 1 человека
            expire_date=None
        )
        return invite.invite_link
    except Exception as e:
        logging.error(f"Ошибка создания ссылки: {e}")
        return None

# ВЕБХУК ДЛЯ АВТОМАТИКИ (авто-выдача ссылки)
async def stripe_webhook(request):
    payload = await request.read()
    sig_header = request.headers.get('Stripe-Signature')
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
    except Exception as e:
        return web.Response(status=400)

    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        user_id = session.get('client_reference_id') # Тот самый ID, который мы передали
        
        # Отправляем ссылку сразу
        link = await generate_invite_link()
        if link:
            await bot.send_message(user_id, f"✅ Оплата прошла успешно! Вот ваша персональная ссылка в клуб: {link}")
            
    return web.Response(status=200)
        
# Подключение к БД
def init_db():
    conn = None
    try:
        # Убедись, что DATABASE_URL в Railway точно есть
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            logging.error("DATABASE_URL отсутствует!")
            return

        # Подключаемся с таймаутом (connect_timeout=5 секунд)
        conn = psycopg2.connect(db_url, sslmode='require', connect_timeout=5)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id BIGINT PRIMARY KEY,
            paid BOOLEAN DEFAULT FALSE,
            expiry_date TIMESTAMP
        )
        """)
        conn.commit()
        cur.close()
        logging.info("База данных успешно инициализирована")
    except Exception as e:
        # Теперь бот не сломается, если база недоступна, а просто напишет ошибку
        logging.error(f"Не удалось подключиться к БД: {e}")
    finally:
        if conn is not None:
            conn.close()

# Хендлер /start (Твой оригинальный код)
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    logging.info(f"Получена команда /start от {message.from_user.id}")
    
    # 1. Первое фото
    await message.answer_photo(
        photo="AgACAgIAAxkBAAMPaee4TD_FGuIQ4LProdOdL5XV5EkAAiYRaxulqkBL5YKQtOj0fV4BAAMCAAN5AAM7BA", 
        caption="Добро пожаловать в обновлённую версию онлайн-клуба! Это пространство про осознанную работу с телом..."
    )
    await asyncio.sleep(1)
    
    # 2. Текст 2
    await message.answer("Основные правила нашего клуба:\n1. Клуб закрытый...\n2. Запись обязательна...")
    await asyncio.sleep(1)
    
    # 3. Текст 3
    await message.answer("Что входит в абонемент:\n- База тренировок\n- Живые тренировки\n- Обратная связь")
    await asyncio.sleep(1)
    
    # 4. Финал с кнопками
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("💳 1 месяц", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев", callback_data="sub_12")
    )
    await message.answer_photo(
        photo="AgACAgIAAxkBAAMSaee9wO7psIiqhOR3M52AQ_aRwPgAAjgRaxulqkBLRv00tJs-NW8BAAMCAAN5AAM7BA",
        caption="Готова начать? Выбирай формат участия:",
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('sub_'))
async def process_payment(callback_query: types.CallbackQuery):
    price_id = PRICES.get(callback_query.data)
    
    session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        line_items=[{'price': price_id, 'quantity': 1}],
        mode='subscription',
        success_url='https://t.me/Natalia_SoulFit_bot',
        cancel_url='https://t.me/Natalia_SoulFit_bot',
    )
    
    await bot.send_message(
        callback_query.from_user.id, 
        f"Оплати подписку здесь: {session.url}\n\n"
        "После оплаты напиши мне /getlink, и я пришлю тебе приглашение в закрытый клуб!"
    )

# --- НОВАЯ КОМАНДА ДЛЯ ПОЛУЧЕНИЯ ССЫЛКИ ---
@dp.message_handler(commands=['getlink'])
async def get_link(message: types.Message):
    # Тут можно добавить проверку: платил человек или нет?
    # Но пока просто выдаем ссылку
    link = await generate_invite_link()
    if link:
        await message.answer(f"Вот твоя персональная ссылка на вступление: {link}")
    else:
        await message.answer("Не удалось создать ссылку. Проверь права бота в группе.")

# ХЕНДЛЕР ОПЛАТЫ (теперь с кнопкой)
@dp.callback_query_handler(lambda c: c.data.startswith('sub_'))
async def process_payment(callback_query: types.CallbackQuery):
    price_id = PRICES.get(callback_query.data)
    
    session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        line_items=[{'price': price_id, 'quantity': 1}],
        mode='subscription',
        success_url='https://t.me/Natalia_SoulFit_bot',
        cancel_url='https://t.me/Natalia_SoulFit_bot',
        client_reference_id=str(callback_query.from_user.id) # ПЕРЕДАЕМ ID ПОЛЬЗОВАТЕЛЯ
    )
    
    # Кнопка вместо текста
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("💳 Оплатить", url=session.url))
    
    await bot.send_message(
        callback_query.from_user.id, 
        "Нажмите кнопку ниже для оплаты:",
        reply_markup=keyboard
    )

# Техническая часть (Вебхук + Инициализация)
async def on_startup(app):
    logging.info("--- ЗАПУСК БОТА ---")
    init_db() 
    logging.info("--- БД ИНИЦИАЛИЗИРОВАНА ---")
    
    domain = os.getenv("YOUR_DOMAIN")
    logging.info(f"--- ПЫТАЮСЬ УСТАНОВИТЬ ВЕБХУК НА: {domain}/bot ---")
    
    if domain:
        try:
            await bot.set_webhook(f"{domain}/bot", drop_pending_updates=True)
            logging.info("--- ВЕБХУК УСПЕШНО УСТАНОВЛЕН! ---")
        except Exception as e:
            logging.error(f"--- ОШИБКА УСТАНОВКИ ВЕБХУКА: {e} ---")
    else:
        logging.error("--- ПЕРЕМЕННАЯ YOUR_DOMAIN НЕ ЗАДАНА! ---")
        
async def on_shutdown(app):
    logging.info("--- ОСТАНОВКА БОТА ---")
    await bot.delete_webhook()
    await bot.close() # ЭТО ЗАКРОЕТ СОЕДИНЕНИЕ И УБЕРЕТ ОШИБКУ
    logging.info("--- БОТ УСПЕШНО ЗАКРЫТ ---")

# --- ХЕНДЛЕР ДЛЯ ПРОВЕРКИ ЗДОРОВЬЯ (HEALTH CHECK) ---
async def health_check(request):
    return web.Response(text="Bot is running", status=200)

if __name__ == "__main__":
    from aiogram.dispatcher.webhook import get_new_configured_app
    app = get_new_configured_app(dispatcher=dp, path='/bot')
    app.router.add_get('/', health_check)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
