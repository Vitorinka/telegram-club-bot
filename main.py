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
logging.info("Начинаю подключение к базе данных...")
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
                stripe_subscription_id TEXT,
                reminder_sent BOOLEAN DEFAULT FALSE
            );
        """)
        
        # 2. Добавляем колонку, если её нет
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS payment_failed BOOLEAN DEFAULT FALSE;")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS grace_period_end TIMESTAMP;")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS auto_renew BOOLEAN DEFAULT TRUE;")
        
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

def get_tariffs_keyboard(show_trial=True):
    kb = InlineKeyboardMarkup(row_width=1)
    if show_trial:
        kb.add(InlineKeyboardButton("Пробная неделя", callback_data="sub_trial"))
    kb.add(
        InlineKeyboardButton("💳 1 месяц", callback_data="sub_1"),
        InlineKeyboardButton("💳 6 месяцев", callback_data="sub_6"),
        InlineKeyboardButton("💳 12 месяцев", callback_data="sub_12")
    )
    return kb

# --- АВТОМАТИЗАЦИЯ (ПРОВЕРКА ПО УТРАМ) ---
async def check_subscriptions_and_reminders():
    logging.info("--- Запуск ежедневной проверки подписок ---")
    conn = get_db_conn()
    cur = conn.cursor()
    # Выбираем всех активных
    cur.execute("SELECT telegram_id, expiry_date, payment_failed, grace_period_end, auto_renew, reminder_sent FROM users WHERE paid = TRUE")
    users = cur.fetchall()
    
    now = datetime.utcnow()
    
    for telegram_id, expiry, payment_failed, grace_end, auto_renew, reminder_sent in users:
        time_left = expiry - now
        
        # 1. Сценарий: Срок действия истек
        if time_left < timedelta(0):
            
            # А) Если была ошибка оплаты — даем льготный период (Grace Period)
            if payment_failed:
                if grace_end is None:
                    # Даем 24 часа (до конца дня)
                    deadline = now + timedelta(days=1)
                    cur.execute("UPDATE users SET grace_period_end = %s WHERE telegram_id = %s", (deadline, telegram_id))
                    await bot.send_message(telegram_id, "⚠️ Оплата вашей подписки не прошла. Пожалуйста, пополните карту до конца дня, чтобы доступ не был ограничен.")
                
                # Если время вышло и после grace period
                elif now > grace_end:
                    await ban_user_logic(telegram_id, cur) # Функция-хелпер (см. ниже)
            
            # Б) Если всё было ок, но срок вышел — просто баним
            else:
                await ban_user_logic(telegram_id, cur)
        
        # 2. Сценарий: Срок заканчивается (напоминание)
        elif timedelta(0) < time_left < timedelta(days=2):
            # Шлем напоминание ТОЛЬКО если нет автопродления и еще не напоминали
            if not auto_renew and not reminder_sent:
                try:
                    await bot.send_message(telegram_id, "⏳ Ваша подписка заканчивается через 48 часов. Продлите доступ вручную.", reply_markup=get_tariffs_keyboard(show_trial=False))
                    cur.execute("UPDATE users SET reminder_sent = TRUE WHERE telegram_id = %s", (telegram_id,))
                except BotBlocked:
                    pass
    
    conn.commit()
    cur.close()
    conn.close()

# Хелпер для бана (чтобы не дублировать код)
async def ban_user_logic(telegram_id, cur):
    try:
        await bot.ban_chat_member(chat_id=int(GROUP_ID), user_id=telegram_id)
        cur.execute("UPDATE users SET paid = FALSE, payment_failed = FALSE, grace_period_end = NULL, reminder_sent = FALSE WHERE telegram_id = %s", (telegram_id,))
        await bot.send_message(telegram_id, "⚠️ Ваша подписка истекла. Доступ закрыт.")
    except Exception as e:
        logging.error(f"Ошибка при бане {telegram_id}: {e}")

async def notify_admins(text: str):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"⚠️ **Уведомление от бота:**\n{text}")
        except Exception as e:
            logging.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")

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
    text = """Что внутри клуба

Став участником, вы получаете доступ к пространству, которое будет поддерживать вас каждый день:

Библиотека тренировок — огромная база, которая постоянно пополняется. От работы с осанкой, стопами и тазовым дном до развития силы, мобильности и гибкости.

Короткие зарядки — 10-15 минут, когда нужно взбодриться или, наоборот, расслабиться в плотном графике.

Мини-уроки — емкие практические знания о дыхании и паттернах движения в повседневной жизни.

Медитации — бережные практики для восстановления нервной системы, снятия стресса и возвращения спокойствия.

Живые эфиры — наши встречи 2-4 раза в месяц, где мы разбираем технику, отвечаем на вопросы и работаем с вашими запросами в реальном времени.

Фитнес-аптечка — готовые решения, если болит поясница, затекла шея, появились отеки или накопилась усталость.

Постоянная поддержка — наш закрытый чат, где я лично отвечаю на ваши вопросы, даю рекомендации и сопровождаю на пути к здоровью."""
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_rules"))
    # Отправляем новым сообщением, ничего не удаляем
    await bot.send_message(callback.message.chat.id, text, reply_markup=kb)
    await callback.answer()

# --- 3. ПРАВИЛА (ТЕКСТ) ---
@dp.callback_query_handler(text="to_rules", state=RegistrationStates.description)
async def show_rules(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.rules.set()
    text = """Кому подходит и ответы на вопросы

Этот клуб для каждого, кто хочет жить без боли и ограничений. Неважно, мужчина вы или женщина, какая у вас сейчас физическая форма или где вы находитесь — наш формат легко встраивается в любой ритм жизни.

Если вас беспокоят зажимы, отеки, сутулость или просто не хватает энергии — здесь вы найдете все необходимые инструменты.

Коротко о главном:

Я новичок? Отлично, все тренировки легко адаптировать под ваш уровень подготовки.

Есть ограничения или боли? Клуб помогает восстанавливаться, но если у вас острый период, мы всегда начинаем с консультации врача.

Мало времени? Мы создали систему, которая помогает вам жить, а не требует жертв и часов свободного времени.

Живу далеко? Клуб полностью онлайн, доступ есть из любой точки мира."""
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("➡️ Продолжить", callback_data="to_choice"))
    await bot.send_message(callback.message.chat.id, text, reply_markup=kb)
    await callback.answer()

# --- 4. ВЫБОР ТАРИФА (ФОТО) ---
@dp.callback_query_handler(text="to_choice", state=RegistrationStates.rules)
async def show_choice(callback: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.choice.set()
    text = """Выберите вариант, который откликается вам сейчас:

Пробная неделя за 15 евро — лучший способ познакомиться с форматом и почувствовать, подходит ли вам такой подход.

Ежемесячная подписка за 50 евро — идеальный ритм для постоянной практики. Автопродление можно отключить в любой момент.

Полгода в клубе за 240 евро — выбор тех, кто настроен на качественные изменения и системный результат.

Годовой абонемент за 410 евро — самое выгодное предложение для долгосрочной заботы о себе.

Нажмите на кнопку ниже, чтобы оформить подписку и присоединиться к нам. Буду рада видеть вас в числе участников!"""
    kb = get_tariffs_keyboard(show_trial=True)
    
    # Отправляем фото
    await bot.send_photo(callback.message.chat.id, PHOTO_URL_RULES, caption=text, reply_markup=kb)
    await callback.answer()

# --- 5. ВЫБОР ТАРИФА И ОПЛАТА ---
@dp.callback_query_handler(lambda c: c.data.startswith('sub_'), state='*')
async def process_payment(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer("Загрузка...")
    sub_type = callback_query.data
    
    price_map = {"sub_trial": "PRICE_TRIAL", "sub_1": "PRICE_1M", "sub_6": "PRICE_6M", "sub_12": "PRICE_12M"}
    price_id = os.getenv(price_map.get(sub_type))
    
    if not price_id:
        await callback_query.answer("Ошибка конфигурации тарифа. Напишите администратору.")
        return

    mode = 'payment' if sub_type == "sub_trial" else 'subscription'
    
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{'price': price_id, 'quantity': 1}],
            mode=mode,
            success_url='https://t.me/Natalia_SoulFit_bot',
            client_reference_id=str(callback_query.from_user.id)
        )
        
        kb = InlineKeyboardMarkup(row_width=1).add(
            InlineKeyboardButton("💳 Оплатить", url=session.url),
            InlineKeyboardButton("🔙 Назад к тарифам", callback_data="back_to_tariffs")
        )
        
        await state.finish() 
        
        # БЕЗОПАСНАЯ ЗАМЕНА
        try:
            await callback_query.message.edit_caption(
                caption=f"✅ Вы выбрали тариф. Переходите к оплате:", 
                reply_markup=kb
            )
        except Exception:
            await callback_query.message.edit_text(
                text=f"✅ Вы выбрали тариф. Переходите к оплате:", 
                reply_markup=kb
            )

    except Exception as e:
        error_text = f"Критическая ошибка создания сессии для {callback_query.from_user.id}: {e}"
        logging.error(error_text)
        await notify_admins(error_text)
        
        # Новый вариант с модальным окном
        await callback_query.answer(
            "⚠️ Произошла техническая ошибка при переходе к оплате.\n\n"
            "Пожалуйста, напишите администратору @re_tasha, мы уже получили уведомление о проблеме и свяжемся с вами!",
            show_alert=True
        )

# --- 6. КНОПКА НАЗАД (ИСПРАВЛЕННАЯ) ---
@dp.callback_query_handler(text="back_to_tariffs", state='*')
async def back_to_tariffs(callback_query: types.CallbackQuery, state: FSMContext):
    await RegistrationStates.choice.set()
    
    # 1. Проверяем, платил ли пользователь раньше
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT paid FROM users WHERE telegram_id = %s", (callback_query.from_user.id,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    
    # 2. Собираем клавиатуру (триал будет только у того, кто еще не paid)
    is_client = user and user[0] is True
    kb = get_tariffs_keyboard(show_trial=not is_client)
    
    text = "Выберите свой формат участия:"

    # 3. Редактируем сообщение (безопасно)
    try:
        await callback_query.message.edit_caption(caption=text, reply_markup=kb)
    except Exception:
        await callback_query.message.edit_text(text=text, reply_markup=kb)
        
    await callback_query.answer()

# --- ОТМЕНА ПОДПИСКИ (ИСПРАВЛЕНО) ---
@dp.callback_query_handler(text="cancel_subscription", state='*')
async def cancel_subscription(callback: types.CallbackQuery):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT stripe_subscription_id FROM users WHERE telegram_id = %s", (callback.from_user.id,))
    result = cur.fetchone()
    
    # --- ИСПРАВЛЕННЫЙ БЛОК ОТМЕНЫ ---
    if result and result[0]:
        sub_id = result[0]
        try:
            # Ставим флаг "отменить в конце периода", а не удаляем сразу
            stripe.Subscription.modify(sub_id, cancel_at_period_end=True)
        
            # Обновляем только флаг auto_renew, но НЕ ТРОГАЕМ paid
            cur.execute("UPDATE users SET auto_renew = FALSE WHERE telegram_id = %s", (callback.from_user.id,))
            conn.commit()
        
            await callback.message.edit_text("✅ Автопродление отключено. Ваш доступ сохранится до конца оплаченного периода.")
        except Exception as e:
            await callback.answer("Ошибка при отмене. Напишите администратору.")
            logging.error(f"Ошибка Stripe: {e}")
        else:
            await callback.answer("Не удалось найти подписку.")
    
    cur.close()
    conn.close()
    
# --- WEBHOOK STRIPE ---
async def stripe_webhook(request):
    payload = await request.read()
    sig_header = request.headers.get('Stripe-Signature')
    
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, os.getenv("STRIPE_WEBHOOK_SECRET")
        )
    except Exception as e:
        logging.error(f"Ошибка проверки подписи Stripe: {e}")
        return web.Response(status=400)

    # 1. ПОКУПКА / ПОДПИСКА
    if event.type == 'checkout.session.completed':
        session = event.data.object
        user_id = session.client_reference_id
        sub_id = getattr(session, 'subscription', None) # Здесь будет None для триала
        
        if not user_id: return web.Response(status=200)

        conn = get_db_conn()
        cur = conn.cursor()
        try:
            # Проверка, был ли пользователь уже в БД
            cur.execute("SELECT paid FROM users WHERE telegram_id = %s", (int(user_id),))
            row = cur.fetchone()
            is_existing_user = (row and row[0] is True)

            # --- ИСПРАВЛЕННАЯ ЛОГИКА ОПРЕДЕЛЕНИЯ ДАТЫ ---
            if sub_id:
                # Если это полноценная подписка
                sub = stripe.Subscription.retrieve(sub_id)
                expiry_date = datetime.fromtimestamp(sub.current_period_end)
            else:
                # Если это триал (оплата прошла, но subscription ID нет)
                # Добавляем 7 дней
                expiry_date = datetime.utcnow() + timedelta(days=7)
            # ---------------------------------------------

            cur.execute("""
                INSERT INTO users (telegram_id, paid, expiry_date, stripe_subscription_id, auto_renew)
                VALUES (%s, TRUE, %s, %s, %s)
                ON CONFLICT (telegram_id) DO UPDATE SET 
                    paid = TRUE, 
                    expiry_date = EXCLUDED.expiry_date,
                    stripe_subscription_id = EXCLUDED.stripe_subscription_id,
                    auto_renew = EXCLUDED.auto_renew;
            """, (int(user_id), expiry_date, sub_id, (sub_id is not None))) # auto_renew True только если есть sub_id
            conn.commit()

            await bot.unban_chat_member(chat_id=int(GROUP_ID), user_id=int(user_id), only_if_banned=True)
            
            # Разная логика сообщений
            if is_existing_user:
                message_text = f"✅ Ваша подписка успешно продлена до {expiry_date.strftime('%d.%m.%Y')}. Спасибо, что остаетесь с нами! ❤️"
            else:
                link = await generate_invite_link()
                message_text = f"✅ Оплата прошла успешно! Доступ открыт до {expiry_date.strftime('%d.%m.%Y')}. Ваша ссылка: {link}\n\nДобро пожаловать в наше пространство! ❤️"
            
            try:
                await bot.send_message(user_id, message_text)
            except BotBlocked:
                logging.warning(f"Оплата принята, но {user_id} заблокировал бота.")

        except Exception as e:
            error_text = f"Ошибка в обработке платежа для {user_id}: {e}"
            logging.error(error_text)
            await notify_admins(error_text)
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    # 2. УСПЕШНОЕ АВТОПРОДЛЕНИЕ / ОПЛАТА ИНВОЙСА
    elif event.type == 'invoice.payment_succeeded':
        # Превращаем объект в чистый Python-словарь (это уберет всю «магию» Stripe)
        invoice_obj = event.data.object
        invoice_dict = invoice_obj.to_dict()
        
        # Получаем sub_id из словаря
        sub_id = invoice_dict.get('subscription')
        
        if not sub_id:
            return web.Response(status=200)

        conn = get_db_conn()
        cur = conn.cursor()
        try:
            # Получаем подписку
            subscription = stripe.Subscription.retrieve(sub_id)
            sub_dict = subscription.to_dict()
            
            # Безопасно достаем дату (используем dict.get)
            end_timestamp = sub_dict.get('current_period_end')
            
            if not end_timestamp:
                logging.error(f"DEBUG: Поле current_period_end не найдено! Полный объект подписки: {sub_dict}")
                return web.Response(status=200)

            new_expiry_date = datetime.fromtimestamp(end_timestamp)
            
            # Обновление БД
            cur.execute("UPDATE users SET expiry_date = %s, paid = TRUE WHERE stripe_subscription_id = %s", (new_expiry_date, sub_id))
            conn.commit()
            logging.info(f"Успешно продлили подписку {sub_id} до {new_expiry_date}")
            
            # Уведомление
            cur.execute("SELECT telegram_id FROM users WHERE stripe_subscription_id = %s", (sub_id,))
            user = cur.fetchone()
            if user:
                try:
                    await bot.send_message(user[0], f"✅ Ваша подписка успешно продлена! Новый доступ открыт до {new_expiry_date.strftime('%d.%m.%Y')}. ❤️")
                except Exception as e:
                    logging.warning(f"Не удалось отправить сообщение: {e}")
                
        except Exception as e:
            # Выводим в лог тип ошибки и само сообщение, чтобы понять, на чем именно падает
            logging.error(f"КРИТИЧЕСКАЯ ОШИБКА: {type(e).__name__}: {str(e)}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
        return web.Response(status=200)

    # 3. ИЗМЕНЕНИЕ СТАТУСА (Отмена)
    elif event.type == 'customer.subscription.updated':
        sub = event.data.object
        auto_renew = not sub.cancel_at_period_end
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("UPDATE users SET auto_renew = %s WHERE stripe_subscription_id = %s", (auto_renew, sub.id))
        conn.commit()
        cur.close()
        conn.close()
                
    # 4. ОШИБКА ОПЛАТЫ
    elif event.type == 'invoice.payment_failed':
        sub_id = event.data.object.subscription
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("UPDATE users SET payment_failed = TRUE WHERE stripe_subscription_id = %s", (sub_id,))
        conn.commit()
        cur.close()
        conn.close()

    # 5. УДАЛЕНИЕ ПОДПИСКИ
    elif event.type == 'customer.subscription.deleted':
        sub = event.data.object
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("UPDATE users SET paid = FALSE, auto_renew = FALSE, stripe_subscription_id = NULL WHERE stripe_subscription_id = %s", (sub.id,))
        conn.commit()
        cur.close()
        conn.close()

    # 6. ОШИБКИ СЕССИИ (Пользователь не оплатил)
    elif event.type in ['checkout.session.expired', 'checkout.session.async_payment_failed']:
        session = event.data.object
        user_id = session.client_reference_id
        if user_id:
            try:
                await bot.send_message(user_id, "❌ Оплата не прошла или время сессии истекло. Если деньги списались, напишите администратору @re_tasha.")
            except:
                pass

    return web.Response(status=200)

# --- ОБНОВЛЕННЫЕ ХЕНДЛЕРЫ ---

@dp.message_handler(commands=['profile'], state='*')
async def profile(message: types.Message):
    conn = get_db_conn()
    cur = conn.cursor()
    # Снова запрашиваем stripe_subscription_id, чтобы проверить его наличие
    cur.execute("SELECT paid, expiry_date, stripe_subscription_id FROM users WHERE telegram_id = %s", (message.from_user.id,))
    user = cur.fetchone()
    cur.close()
    conn.close()

    if not user or not user[0]:
        await message.answer("У вас пока нет активной подписки. Нажмите /start, чтобы оформить её.")
    else:
        expiry_date = user[1]
        # Форматируем дату в красивый вид (день.месяц.год)
        date_text = expiry_date.strftime("%d.%m.%Y")

        text = f"✅ Ваша подписка активна.\n📅 Действует до: {date_text}\n\nХотите продлить доступ?"
        
        keyboard = InlineKeyboardMarkup(row_width=1)
        # Кнопка продления есть у всех
        keyboard.add(InlineKeyboardButton("💳 Продлить доступ", callback_data="show_renew_options"))
        
        # «Умная» кнопка отмены: появится только если есть ID подписки в базе
        if user[2]: 
            keyboard.add(InlineKeyboardButton("❌ Отменить автопродление", callback_data="cancel_subscription"))
        
        await message.answer(text, reply_markup=keyboard)

@dp.callback_query_handler(text="show_renew_options", state='*')
async def show_renew_options(callback: types.CallbackQuery):
    # Сразу передаем False, так как это меню продления
    kb = get_tariffs_keyboard(show_trial=False)
    
    await callback.message.edit_text(
        "Выберите тариф для продления доступа:", 
        reply_markup=kb
    )
    await callback.answer()

@dp.message_handler(commands=['give_access'], state='*')
async def give_access_command(message: types.Message):
    # 1. Проверка прав
    if message.from_user.id not in ADMIN_IDS:
        return

    # 2. Парсинг аргументов
    args = message.get_args().split()
    if len(args) < 1:
        await message.reply("⚠️ Использование: `/give_access <user_id> <дней>`", parse_mode="Markdown")
        return
    
    target_user_id = args[0]
    days = int(args[1]) if len(args) > 1 else 30

    # 3. Обновляем БД
    conn = None # Инициализируем переменную для finally
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        
        interval_query = f"{days} days"
        
        cur.execute(f"""
            INSERT INTO users (telegram_id, paid, expiry_date)
            VALUES (%s, TRUE, NOW() + INTERVAL '{interval_query}')
            ON CONFLICT (telegram_id) DO UPDATE 
            SET paid = TRUE, 
                expiry_date = CASE 
                    WHEN users.expiry_date > NOW() THEN users.expiry_date + INTERVAL '{interval_query}'
                    ELSE NOW() + INTERVAL '{interval_query}'
                END;
        """, (int(target_user_id),))
        
        conn.commit()
        cur.close()
        
        # 4. Генерируем ссылку
        link = await generate_invite_link()
        
        # 5. Отправляем пользователю
        try:
            if link:
                await bot.send_message(target_user_id, f"✅ Администратор предоставил вам доступ к клубу на {days} дней!\nВаша ссылка: {link}")
                await message.answer(f"✅ Доступ пользователю {target_user_id} предоставлен.")
            else:
                await message.answer("❌ Доступ в БД обновлен, но не удалось создать ссылку.")
        except BotBlocked:
            await message.answer("⚠️ Доступ в БД обновлен, но пользователь заблокировал бота.")
        except Exception: 
            await message.answer(f"⚠️ Доступ в БД обновлен, но не удалось отправить сообщение пользователю {target_user_id}.")

    except Exception as e:
        logging.error(f"Критическая ошибка в give_access: {e}")
        await message.answer(f"❌ Ошибка при работе с базой данных: {e}")
        if conn:
            conn.rollback()
            
    finally:
        if conn:
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
        # Исправлено имя функции здесь:
        await check_subscriptions_and_reminders() 
        await message.answer("Проверка завершена.")
    else:
        await message.answer("У вас нет прав для этого.")

# --- ЗАПУСК ---
async def on_startup(app):
    init_db()
    await bot.delete_webhook() 
    
    # Затем устанавливаем актуальный
    secret = os.getenv("WEBHOOK_SECRET")
    await bot.set_webhook(f"{os.getenv('YOUR_DOMAIN')}/webhook?token={secret}")
    # Используем глобальный scheduler
    scheduler.add_job(check_subscriptions_and_reminders, 'cron', hour=10)    
    scheduler.start()

async def on_shutdown(app):
    # Правильный способ закрытия сессии в Aiogram 2.x
    await bot.session.close() 
    logging.info("Бот остановлен, сессия закрыта.")
    
if __name__ == "__main__":
    from aiogram.dispatcher.webhook import get_new_configured_app
    app = get_new_configured_app(dispatcher=dp, path='/webhook')    
    app.router.add_post('/stripe-payment', stripe_webhook)    
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown) 
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host='0.0.0.0', port=port)
