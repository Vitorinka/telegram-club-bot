import os
import logging
import asyncio
import stripe
import psycopg2
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.exceptions import BotBlocked
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info(f"DEBUG: Подключаюсь к БД: {os.getenv('DATABASE_URL')}")
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = os.getenv("GROUP_ID") 
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
stripe.api_key = os.getenv("STRIPE_API_KEY")

# Вставьте сюда ссылки на ваши фото или ID файлов из Telegram
PHOTO_URL_INTRO = "AgACAgIAAxkBAAMPaee4TD_FGuIQ4LProdOdL5XV5EkAAiYRaxulqkBL5YKQtOj0fV4BAAMCAAN5AAM7BA" 
PHOTO_URL_RULES = "AgACAgIAAxkBAAMSaee9wO7psIiqhOR3M52AQ_aRwPgAAjgRaxulqkBLRv00tJs-NW8BAAMCAAN5AAM7BA"

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler()

# --- СОСТОЯНИЯ (FSM) ---
class RegistrationStates(StatesGroup):
    intro = State()
    description = State()  # Вот это пропущенное состояние
    rules = State()
    choice = State()       # И это состояние тебе тоже понадобится для этапа выбора тарифа
    
# --- ФУНКЦИИ БАЗЫ ---
def get_db_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')

# --- БАЗА ДАННЫХ ---
def init_db():
    conn = None
    try:
        # Подключаемся
        conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
        cur = conn.cursor()  # <--- ВОТ ЭТА СТРОКА БЫЛА УДАЛЕНА
        
        # 1. Создаем таблицу, если её нет
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                paid BOOLEAN DEFAULT FALSE,
                expiry_date TIMESTAMP,
                stripe_subscription_id TEXT
            );
        """)
        
        # 2. Добавляем колонку, если её нет
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT;")
        
        conn.commit()
        cur.close()
        logging.info("--- БД ИНИЦИАЛИЗИРОВАНА И ПРОВЕРЕНА ---")
        
    except Exception as e:
        logging.error(f"ОШИБКА ИНИЦИАЛИЗАЦИИ БД: {e}")
        
    finally:
        # Всегда закрываем соединение, если оно было создано
        if conn is not None:
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
    logging.info("Запуск проверки подписок...")
    
    # Клавиатура
    kb = InlineKeyboardMarkup(row_width=1).add(
        InlineKeyboardButton("💳 1 месяц", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев", callback_data="sub_12")
    )
    
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT telegram_id, expiry_date FROM users WHERE paid = TRUE")
    users = cur.fetchall()
    
    now = datetime.utcnow()
    
    for telegram_id, expiry in users:
        # Разница во времени
        time_left = expiry - now
        
        # 1. Если срок истек (time_left < 0)
        if time_left < timedelta(0):
            try:
                await bot.ban_chat_member(chat_id=int(GROUP_ID), user_id=telegram_id)
                cur.execute("UPDATE users SET paid = FALSE WHERE telegram_id = %s", (telegram_id,))
                await bot.send_message(telegram_id, "⚠️ Ваша подписка истекла. Доступ закрыт. Выберите тариф для продления:", reply_markup=kb)
                logging.info(f"Пользователь {telegram_id} исключен.")
            except Exception as e:
                logging.error(f"Ошибка при бане {telegram_id}: {e}")
        
        # 2. Если до конца осталось 48 часов или меньше
        elif time_left < timedelta(days=2):
            try:
                # Отправляем напоминание
                await bot.send_message(telegram_id, "⏳ Ваша подписка заканчивается менее чем через 48 часов! Продлите доступ, чтобы не потерять прогресс:", reply_markup=kb)
            except BotBlocked:
                logging.info(f"Пользователь {telegram_id} заблокировал бота.")
    
    conn.commit()
    cur.close()
    conn.close()

# --- АВТОМАТИЗАЦИЯ (ПРОВЕРКА ПО УТРАМ) ---
async def check_subscriptions():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT telegram_id, expiry_date FROM users WHERE paid = TRUE")
    users = cur.fetchall()
    
    now = datetime.utcnow()
    for user_id, expiry in users:
        # Если срок истек
        if expiry < now:
            try:
                await bot.ban_chat_member(chat_id=int(GROUP_ID), user_id=user_id)
                cur.execute("UPDATE users SET paid = FALSE WHERE telegram_id = %s", (user_id,))
                await bot.send_message(user_id, "Ваша подписка истекла. Доступ закрыт. Продлите подписку, написав /start")
            except Exception as e:
                logging.error(f"Ошибка бана {user_id}: {e}")
        # Если срок истекает завтра (напоминание)
        elif expiry - timedelta(days=1) < now < expiry:
            await bot.send_message(user_id, "Ваша подписка заканчивается завтра! Успейте продлить.")
    
    conn.commit()
    cur.close()
    conn.close()

# --- ХЕНДЛЕРЫ ---
@dp.message_handler(commands=['broadcast'])
async def broadcast(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return
    text = message.text.replace('/broadcast ', '')
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT telegram_id FROM users")
    users = cur.fetchall()
    
    success_count = 0
    blocked_count = 0
    
    for user in users:
        try: 
            await bot.send_message(user[0], text)
            success_count += 1
        except BotBlocked:
            blocked_count += 1
        except Exception as e:
            logging.error(f"Ошибка отправки пользователю {user[0]}: {e}")
            
    cur.close()
    conn.close()
    await message.answer(f"Рассылка завершена. Успешно: {success_count}, заблокировали бота: {blocked_count}.")

# --- ХЕНДЛЕРЫ ---
# --- 1. ПРИВЕТСТВИЕ (ФОТО) ---
@dp.message_handler(commands=['start'], state='*')
async def start(message: types.Message, state: FSMContext):
    await state.finish()
    await RegistrationStates.intro.set()
    text = """Приветствую! Добро пожаловать в закрытый клуб Натальи Ребковец.

Это пространство для тех, кто перерос погоню за быстрыми результатами и выбирает осознанный путь. Мы здесь не просто качаем мышцы — мы выстраиваем глубокий контакт с телом, работаем с нервной системой и возвращаем себе естественную легкость движений.

Здесь нет хаоса. Здесь есть система. Моя задача — не заставлять вас тренироваться, а помочь вам научиться понимать свое тело, чувствовать его и получать удовольствие от каждого движения.

Добро пожаловать в сообщество, где здоровое и сильное тело — это не случайность, а результат регулярной и бережной практики."""
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_desc"))
    await bot.send_photo(message.chat.id, PHOTO_URL_INTRO, caption=text, reply_markup=kb)

# --- 2. ОПИСАНИЕ (ТЕКСТ) ---
@dp.callback_query_handler(text="to_desc", state=RegistrationStates.intro)
async def show_description(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.description.set()
    text = "ℹ️ **О клубе:**\nЗдесь мы делаем то-то и то-то..."
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_rules"))
    # Отправляем новым сообщением, ничего не удаляем
    await bot.send_message(callback.message.chat.id, text, reply_markup=kb)

# --- 3. ПРАВИЛА (ТЕКСТ) ---
@dp.callback_query_handler(text="to_rules", state=RegistrationStates.description)
async def show_rules(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.rules.set()
    text = "📜 **Правила клуба:**\n1. Не спамить.\n2. Уважать других."
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_choice"))
    await bot.send_message(callback.message.chat.id, text, reply_markup=kb)

# --- 4. ВЫБОР ТАРИФА (ФОТО) ---
@dp.callback_query_handler(text="to_choice", state=RegistrationStates.rules)
async def show_choice(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.choice.set()
    text = "💎 **Выберите свой формат участия:**"
    kb = InlineKeyboardMarkup(row_width=1).add(
        InlineKeyboardButton("💎 Пробная неделя", callback_data="sub_trial"),
        InlineKeyboardButton("💳 1 месяц", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев", callback_data="sub_12")
    )
    # Отправляем фото
    await bot.send_photo(callback.message.chat.id, PHOTO_URL_RULES, caption=text, reply_markup=kb)

# --- 5. ВЫБОР ТАРИФА И ОПЛАТА (РЕДАКТИРОВАНИЕ) ---
@dp.callback_query_handler(lambda c: c.data.startswith('sub_'), state=RegistrationStates.choice)
async def process_payment(callback_query: types.CallbackQuery, state: FSMContext):
    sub_type = callback_query.data
    
    price_map = {"sub_trial": "PRICE_TRIAL", "sub_1": "PRICE_1M", "sub_6": "PRICE_6M", "sub_12": "PRICE_12M"}
    price_id = os.getenv(price_map.get(sub_type))
    mode = 'payment' if sub_type == "sub_trial" else 'subscription'
    
    session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        line_items=[{'price': price_id, 'quantity': 1}],
        mode=mode,
        success_url='https://t.me/Natalia_SoulFit_bot',
        client_reference_id=str(callback_query.from_user.id)
    )
    
    # Меняем кнопки: "Оплатить" и "Назад"
    kb = InlineKeyboardMarkup(row_width=1).add(
        InlineKeyboardButton("💳 Оплатить", url=session.url),
        InlineKeyboardButton("🔙 Назад к тарифам", callback_data="back_to_tariffs")
    )
    await callback_query.message.edit_caption(caption=f"✅ Вы выбрали тариф. Переходите к оплате:", reply_markup=kb)
    await callback_query.answer()

# --- 6. КНОПКА НАЗАД ---
@dp.callback_query_handler(text="back_to_tariffs", state=RegistrationStates.choice)
async def back_to_tariffs(callback_query: types.CallbackQuery, state: FSMContext):
    # Восстанавливаем кнопки выбора тарифа
    kb = InlineKeyboardMarkup(row_width=1).add(
        InlineKeyboardButton("💎 Пробная неделя", callback_data="sub_trial"),
        InlineKeyboardButton("💳 1 месяц", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев", callback_data="sub_12")
    )
    await callback_query.message.edit_caption(caption="💎 **Выберите свой формат участия:**", reply_markup=kb)
    await callback_query.answer()
    
# --- WEBHOOK STRIPE ---
async def stripe_webhook(request):
    provided_token = request.rel_url.query.get('token')
    if provided_token != os.getenv("WEBHOOK_SECRET"):
        logging.warning("Попытка несанкционированного доступа к вебхуку!")
        return web.Response(status=403) # Forbidden
    # ДОБАВЬ ЭТОТ БЛОК ДЛЯ ОТЛАДКИ:
    headers = dict(request.headers)
    logging.info(f"Получены заголовки: {headers}")
    
    payload = await request.read()
    sig_header = request.headers.get('Stripe-Signature')
    logging.info(f"Заголовок Stripe-Signature: {sig_header}")
    payload = await request.read()
    sig_header = request.headers.get('Stripe-Signature')
    
    # 1. Проверка подписи
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, os.getenv("STRIPE_WEBHOOK_SECRET")
        )
    except Exception as e:
        logging.error(f"Ошибка проверки подписи Stripe: {e}")
        return web.Response(status=400)

    # 2. Обработка успешного платежа
    if event.type == 'checkout.session.completed':
        session = event.data.object
        user_id = session.client_reference_id
        
        # БЕЗОПАСНОЕ ПОЛУЧЕНИЕ ID ПОДПИСКИ
        sub_id = getattr(session, 'subscription', None)
        
        if user_id:
            # --- ЗАЩИТА ОТ СПАМА (ПРОВЕРКА БД) ---
            conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode='require')
            cur = conn.cursor()
            cur.execute("SELECT paid FROM users WHERE telegram_id = %s", (int(user_id),))
            row = cur.fetchone()
            
            # Если юзер уже есть и у него paid = TRUE, ничего не делаем, чтобы не спамить
            if row and row[0] is True:
                cur.close()
                conn.close()
                return web.Response(status=200)
            
            # --- ОБРАБОТКА ОПЛАТЫ ---
            try:
                # Разбан
                await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(user_id), only_if_banned=True)
    
                # ИСПОЛЬЗУЕМ INSERT ON CONFLICT (UPSERT)
                cur.execute("""
                    INSERT INTO users (telegram_id, paid, expiry_date, stripe_subscription_id)
                    VALUES (%s, TRUE, NOW() + INTERVAL '30 days', %s)
                    ON CONFLICT (telegram_id) 
                    DO UPDATE SET 
                        paid = TRUE, 
                        expiry_date = NOW() + INTERVAL '30 days', 
                        stripe_subscription_id = EXCLUDED.stripe_subscription_id;
                """, (int(user_id), sub_id))
    
                conn.commit()
                logging.info(f"Данные успешно записаны в БД для {user_id}")
                
                # Генерация временной ссылки
                link = await generate_invite_link()
                if link:
                    try:
                        await bot.send_message(user_id, f"✅ Оплата прошла успешно! Ваша ссылка в клуб: {link}")
                    except BotBlocked:
                        logging.warning(f"Оплата принята, но пользователь {user_id} заблокировал бота.")
                else:
                    try:
                        await bot.send_message(user_id, "✅ Оплата прошла успешно, но не удалось создать ссылку. Напишите @re_tasha, мы всё исправим!")
                    except BotBlocked:
                        logging.warning(f"Ошибка оплаты (нет ссылки), но пользователь {user_id} заблокировал бота.")
            
            except Exception as e:
                logging.error(f"Ошибка обработки успешной оплаты: {e}")
                await bot.send_message(user_id, "Произошла ошибка при начислении доступа. Пожалуйста, напишите @re_tasha, мы всё проверим.")
            
            cur.close()
            conn.close()

    # 3. Обработка ошибок (сессия истекла или оплата не прошла)
    elif event.type in ['checkout.session.expired', 'checkout.session.async_payment_failed']:
        session = event.data.object
        user_id = session.client_reference_id
        if user_id:
            await bot.send_message(user_id, "❌ Оплата не прошла или сессия истекла. Если деньги списались, пожалуйста, напишите @re_tasha и пришлите чек.")

    return web.Response(status=200)

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
                # 3. Отправляем сообщение (с защитой!)
                try:
                    await bot.send_message(telegram_id, "Ваша подписка истекла. Доступ в клуб ограничен.")
                except BotBlocked:
                    logging.info(f"Пользователь {telegram_id} заблокировал бота, уведомление не отправлено.")
            except Exception as e:
                logging.error(f"Не удалось исключить {telegram_id}: {e}")
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Ошибка проверки подписок: {e}")

# --- ОБНОВЛЕННЫЕ ХЕНДЛЕРЫ ---

@dp.message_handler(commands=['profile'], state='*') # Добавили state='*'
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

# --- ОБРАБОТКА ПОМОЩИ ---
# Обработка команды /help (если пользователь напишет это сам)
@dp.message_handler(commands=['help'], state='*') # Добавили state='*'
async def help_command(message: types.Message):
    await message.answer("По всем вопросам можно связаться с @re_tasha")

@dp.message_handler(commands=['test_expiry'])
async def test_expiry(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        await message.answer("Запускаю проверку подписок...")
        await send_renewal_reminders()
        await message.answer("Проверка завершена.")
    else:
        await message.answer("У вас нет прав для этого.")

# --- ЗАПУСК ---
async def on_startup(app):
    init_db()
    # Планировщик
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_renewal_reminders, 'cron', hour=10)
    scheduler.start()
    # Вебхук
    secret = os.getenv("WEBHOOK_SECRET")
    await bot.set_webhook(f"{os.getenv('YOUR_DOMAIN')}/webhook?token={secret}")

async def on_shutdown(app):
    await bot.close() # Закрываем сессию бота при выключении
    logging.info("Бот остановлен, сессия закрыта.")

if __name__ == "__main__":
    from aiogram.dispatcher.webhook import get_new_configured_app
    app = get_new_configured_app(dispatcher=dp, path='/webhook')    
    app.router.add_post('/stripe-payment', stripe_webhook)    
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown) # <-- Добавь эту строку
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host='0.0.0.0', port=port)
