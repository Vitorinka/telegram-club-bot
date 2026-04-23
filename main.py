import os
import logging
import asyncio
import stripe
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import psycopg2 
from datetime import datetime
from aiogram.utils.exceptions import BotBlocked
from apscheduler.schedulers.asyncio import AsyncIOScheduler

def init_db():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            username TEXT,
            subscription_end TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Таблица создана или уже существует.")

# Вызовите init_db() при старте бота

# Логирование
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Инициализация
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = "-1003983497950" # ID твоей группы (например, -100123456789)
stripe.api_key = os.getenv("STRIPE_API_KEY")
WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# 2. Ваш обработчик вебхука (вставьте сюда)
async def stripe_webhook_handler(request):
    payload = await request.read()
    # ВАЖНО: Stripe подписывает запросы. Для продакшена обязательно проверьте сигнатуру!
    
    event = json.loads(payload)

    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        tg_id = session['metadata']['telegram_id']
        
        # ЛОГИКА: Обновляем базу данных здесь
        print(f"Пользователь {tg_id} оплатил! Добавляем в базу.")

    return web.Response(status=200)

# 3. Настройка веб-сервера aiohttp
async def on_startup(app):
    # Эта функция выполнится при старте, можно добавить логику запуска бота
    pass

app = web.Application()
app.router.add_post('/webhook', stripe_webhook_handler)

# 4. Основной блок запуска
if __name__ == '__main__':
    # Запускаем и бота (polling), и веб-сервер
    # В Railway лучше использовать PORT из переменных окружения
    port = int(os.environ.get("PORT", 8080))

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
        
# ВЕБХУК ДЛЯ АВТОМАТИКИ
import asyncio # Добавьте в импорты

async def stripe_webhook(request):
    payload = await request.read()
    sig_header = request.headers.get('Stripe-Signature')
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, os.getenv("STRIPE_WEBHOOK_SECRET"))
    except Exception as e:
        logging.error(f"Ошибка вебхука: {e}")
        return web.Response(status=400)

    # 1. Первая покупка
    if event.type == 'checkout.session.completed':
        session = event.data.object
        user_id = getattr(session, 'client_reference_id', None)
        
        if user_id:
            await asyncio.to_thread(save_user_to_db, int(user_id))

        link = await generate_invite_link()
        if link and user_id:
            await bot.send_message(user_id, f"✅ Оплата прошла успешно! Ваша ссылка: {link}")

    # 2. ОБРАБОТКА АВТО-ПРОДЛЕНИЯ
    elif event.type == 'invoice.payment_succeeded':
        invoice = event.data.object
        
        # Проверяем, что это оплата за подписку
        if getattr(invoice, 'billing_reason', None) == 'subscription_cycle':
            sub_id = getattr(invoice, 'subscription', None)
            if sub_id:
                # Получаем подписку
                subscription = stripe.Subscription.retrieve(sub_id)
                # Достаем метаданные через getattr
                metadata = getattr(subscription, 'metadata', {})
                telegram_id = getattr(metadata, 'telegram_id', None)
                
                if telegram_id:
                    # Безопасно достаем период (lines -> data -> period -> end)
                    lines = getattr(invoice, 'lines', None)
                    data = getattr(lines, 'data', [])
                    if data:
                        period = getattr(data[0], 'period', {})
                        end_timestamp = getattr(period, 'end', None)
                        
                        if end_timestamp:
                            date_str = datetime.fromtimestamp(end_timestamp).strftime('%d.%m.%Y')
                            
                            # Выполняем запись в БД через поток
                            await asyncio.to_thread(update_db_sub, int(telegram_id), end_timestamp)
                            
                            await bot.send_message(
                                chat_id=int(telegram_id),
                                text=f"Вижу, что оплата прошла!\nДоступ продлён на месяц ❤️\nСледующая оплата спишется {date_str}"
                            )

    # 3. ОБРАБОТКА ОТМЕНЫ
    elif event.type == 'customer.subscription.deleted':
        subscription = event.data.object
        metadata = getattr(subscription, 'metadata', {})
        telegram_id = getattr(metadata, 'telegram_id', None)
        
        if telegram_id:
            try:
                await bot.ban_chat_member(chat_id=GROUP_ID, user_id=int(telegram_id))
                await bot.unban_chat_member(chat_id=GROUP_ID, user_id=int(telegram_id))
                await bot.send_message(
                    chat_id=int(telegram_id),
                    text="Твоя подписка закончилась, доступ в клуб закрыт. Будем ждать снова! ❤️"
                )
            except Exception as e:
                logging.error(f"Не удалось исключить пользователя {telegram_id}: {e}")
                
    return web.Response(status=200)

# Добавьте еще одну функцию для обновления БД, чтобы не смешивать с первичной вставкой
def update_db_sub(telegram_id, timestamp):
    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO users (telegram_id, paid, expiry_date)
        VALUES (%s, TRUE, to_timestamp(%s))
        ON CONFLICT (telegram_id) 
        DO UPDATE SET paid = TRUE, expiry_date = to_timestamp(%s);
    """, (telegram_id, timestamp, timestamp))
    conn.commit()
    cur.close()
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

# Забираем цены из переменных окружения (из твоего скриншота)
PRICES = {
    "sub_1": os.getenv("PRICE_1M"),
    "sub_6": os.getenv("PRICE_6M"),
    "sub_12": os.getenv("PRICE_12M")
}

# ХЕНДЛЕР ОПЛАТЫ (теперь с кнопкой)
@dp.callback_query_handler(lambda c: c.data.startswith('sub_'))
async def process_payment(callback_query: types.CallbackQuery):
    price_id = PRICES.get(callback_query.data)
    
    # 1. Создаем сессию
    session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        line_items=[{'price': price_id, 'quantity': 1}],
        mode='subscription',
        success_url='https://t.me/Natalia_SoulFit_bot',
        cancel_url='https://t.me/Natalia_SoulFit_bot',
        client_reference_id=str(callback_query.from_user.id),
        subscription_data={"metadata": {"telegram_id": str(callback_query.from_user.id)}}
    )
    
    # 2. Создаем кнопку
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("💳 Оплатить", url=session.url))
    
    # 3. Делаем всё одним запросом к Telegram
    await callback_query.message.edit_caption(
        caption="Отлично! Переходите по ссылке для оплаты:",
        reply_markup=keyboard
    )
    
    # 4. Убираем "часики" с кнопки
    await callback_query.answer()

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

# Команда для админа: /testlink
@dp.message_handler(commands=['testlink'])
async def test_invite_link(message: types.Message):
    # Добавь проверку, что это именно ты (твой ID)
    if message.from_user.id == 309993986: # Укажи свой ID (из логов выше)
        link = await generate_invite_link()
        if link:
            await message.answer(f"✅ Тестовая ссылка: {link}")
        else:
            await message.answer("❌ Ошибка: проверь права бота в группе.")
    else:
        await message.answer("У вас нет доступа к этой команде.")

# --- РАССЫЛКА НА ВСЕХ ПОЛЬЗОВАТЕЛЕЙ ---
@dp.message_handler(commands=['broadcast_private'])
async def broadcast_private(message: types.Message):
    # Проверка, что команду пишешь только ты (твой ID 309993986)
    if message.from_user.id != 309993986: 
        return

    text_to_send = message.get_args()
    if not text_to_send:
        await message.answer("Используй: /broadcast_private <текст>")
        return

    # Подключаемся к БД
    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
    cur = conn.cursor()
    cur.execute("SELECT telegram_id FROM users;")
    users = cur.fetchall()
    cur.close()
    conn.close()

    count = 0
    blocked = 0
    
    await message.answer(f"Начинаю рассылку на {len(users)} пользователей...")

    for user in users:
        user_id = user[0]
        try:
            await bot.send_message(chat_id=user_id, text=text_to_send)
            count += 1
            await asyncio.sleep(0.05) 
        except BotBlocked:
            # Если пользователь заблокировал бота
            blocked += 1
        except Exception:
            # Если произошла любая другая ошибка (например, пользователь удалил чат), 
            # просто идем дальше, не останавливая рассылку
            pass
        except Exception as e:
            logging.error(f"Ошибка при отправке {user_id}: {e}")

    await message.answer(f"✅ Рассылка завершена!\nУспешно: {count}\nЗаблокировали бота: {blocked}")

async def send_renewal_reminders():
    logging.info("Запуск проверки подписок для напоминаний...")
    conn = None
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
        cur = conn.cursor()
        
        # Ищем пользователей, у которых expiry_date наступает через 2 дня
        query = """
        SELECT telegram_id 
        FROM users 
        WHERE expiry_date::date = (CURRENT_DATE + INTERVAL '2 days')::date;
        """
        cur.execute(query)
        users_to_remind = cur.fetchall()
        
        for user in users_to_remind:
            user_id = user[0]
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text="Привет! 👋 Напоминаю, что твоя подписка продлевается через 2 дня. Позаботься о том, чтобы на карте были средства!"
                )
            except Exception as e:
                logging.error(f"Не удалось отправить напоминание {user_id}: {e}")
        
        cur.close()
        logging.info(f"Напоминания отправлены {len(users_to_remind)} пользователям.")
    except Exception as e:
        logging.error(f"Ошибка в планировщике: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Соединение с БД закрыто.")

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
    # Инициализация планировщика
    scheduler = AsyncIOScheduler()
    # Запускаем проверку каждый день в 10:00 утра
    scheduler.add_job(send_renewal_reminders, 'cron', hour=10, minute=0)
    scheduler.start()
        
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
    app.router.add_post('/webhook', stripe_webhook)
    app.router.add_get('/', health_check)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
