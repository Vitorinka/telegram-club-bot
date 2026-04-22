import os
import logging
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web

import psycopg2 # Убедись, что этот импорт есть наверху

# Подключение к БД
def init_db():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
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
    conn.close()

# Логирование
logging.basicConfig(level=logging.INFO)

# Инициализация
BOT_TOKEN = os.getenv("BOT_TOKEN")
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    logging.info(f"Бот получил команду /start от пользователя {message.from_user.id}") # <-- Добавь эту строчку
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

# Техническая часть (Вебхук)
async def on_startup(app):
    init_db() # Добавь эту строчку
    await bot.set_webhook(f"{os.getenv('YOUR_DOMAIN')}/bot", drop_pending_updates=True)

async def on_shutdown(app):
    await bot.delete_webhook()

if __name__ == "__main__":
    from aiogram.dispatcher.webhook import get_new_configured_app
    app = get_new_configured_app(dispatcher=dp, path='/bot')
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
