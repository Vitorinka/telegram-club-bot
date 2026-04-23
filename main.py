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

# Логирование
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Инициализация
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = "-1003983497950" # ID твоей группы (например, -100123456789)
stripe.api_key = os.getenv("STRIPE_API_KEY")
WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

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
        event = stripe.Webhook.construct_event(payload, sig_header, os.getenv("STRIPE_WEBHOOK_SECRET"))
    except Exception as e:
        logging.error(f"Ошибка вебхука: {e}")
        return web.Response(status=400)

    # 1. ОБРАБОТКА ПЕРВОЙ ПОКУПКИ
    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        user_id = session.get('client_reference_id')
        link = await generate_invite_link()
        if link:
            await bot.send_message(user_id, f"✅ Оплата прошла успешно! Вот ваша ссылка для вступления: {link}")

    # 2. ОБРАБОТКА АВТО-ПРОДЛЕНИЯ
    elif event['type'] == 'invoice.payment_succeeded':
        invoice = event['data']['object']
        
        # Проверяем, что это именно продление, а не первая оплата
        if invoice.get('billing_reason') == 'subscription_cycle':
            subscription_id = invoice.get('subscription')
            
            # Получаем объект подписки, чтобы достать metadata
            subscription = stripe.Subscription.retrieve(subscription_id)
            telegram_id = subscription.get('metadata', {}).get('telegram_id')
            
            if telegram_id:
                # Получаем дату следующего списания
                next_payment_timestamp = invoice.get('lines', {}).get('data', [{}])[0].get('period', {}).get('end')
                date_str = datetime.fromtimestamp(next_payment_timestamp).strftime('%d.%m.%Y')

                # Обновляем дату в базе данных
                conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
                cur = conn.cursor()
                # Используем UPSERT (обновление или вставка)
                cur.execute("""
                    INSERT INTO users (telegram_id, paid, expiry_date)
                    VALUES (%s, TRUE, to_timestamp(%s))
                    ON CONFLICT (telegram_id)
                    DO UPDATE SET paid = TRUE, expiry_date = to_timestamp(%s);
                """, (int(telegram_id), next_payment_timestamp, next_payment_timestamp))
                conn.commit()
                cur.close()
                conn.close()
                
                # Отправляем сообщение
                await bot.send_message(
                    chat_id=int(telegram_id),
                    text=f"Вижу, что оплата прошла!\nДоступ продлён на месяц ❤️\nСледующая оплата спишется {date_str}"
                )
                
    # 3. ОБРАБОТКА ОТМЕНЫ/ОКОНЧАНИЯ ПОДПИСКИ
    elif event['type'] == 'customer.subscription.deleted':
        subscription = event['data']['object']
        telegram_id = subscription.get('metadata', {}).get('telegram_id')
        
        if telegram_id:
            try:
                # 1. Исключаем пользователя из группы (бан)
                # ban_chat_member автоматически выкидывает человека
                await bot.ban_chat_member(chat_id=GROUP_ID, user_id=int(telegram_id))
                
                # 2. Сразу "разбаниваем", чтобы если он оплатит снова, 
                # он мог вступить по новой ссылке (иначе он останется в ЧС)
                await bot.unban_chat_member(chat_id=GROUP_ID, user_id=int(telegram_id))
                
                # 3. Отправляем уведомление
                await bot.send_message(
                    chat_id=int(telegram_id),
                    text="Твоя подписка закончилась, доступ в клуб закрыт. Будем ждать снова! ❤️"
                )
            except Exception as e:
                logging.error(f"Не удалось исключить пользователя {telegram_id}: {e}")
                
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
    
    # Создаем сессию Stripe
    session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        line_items=[{'price': price_id, 'quantity': 1}],
        mode='subscription',
        success_url='https://t.me/Natalia_SoulFit_bot',
        cancel_url='https://t.me/Natalia_SoulFit_bot',
        client_reference_id=str(callback_query.from_user.id),
    subscription_data={
        "metadata": {
            "telegram_id": str(callback_query.from_user.id)
        }
    }
)
    
    # Создаем кнопку, которая сразу ведет на оплату
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("💳 Оплатить", url=session.url))
    
    # Отправляем сообщение только с кнопкой
    await bot.send_message(
        callback_query.from_user.id, 
        "Оплата (Stripe):", 
        reply_markup=keyboard
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
