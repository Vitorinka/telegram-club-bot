import hashlib
from datetime import datetime


SUPPORTED_OVERVIEW_PERIODS = frozenset({7, 30, 90, 365})
DEFAULT_ACTIVITY_LIMIT = 12
MAX_ACTIVITY_LIMIT = 20


class AdminOverviewQueryError(ValueError):
    pass


def parse_overview_query(days, limit):
    try:
        parsed_days = int(days)
        parsed_limit = int(limit)
    except (TypeError, ValueError) as error:
        raise AdminOverviewQueryError("invalid_overview_query") from error
    if parsed_days not in SUPPORTED_OVERVIEW_PERIODS:
        raise AdminOverviewQueryError("invalid_overview_period")
    if parsed_limit < 1 or parsed_limit > MAX_ACTIVITY_LIMIT:
        raise AdminOverviewQueryError("invalid_overview_limit")
    return parsed_days, parsed_limit


def _iso(value):
    return value.isoformat() if isinstance(value, datetime) else str(value)


def _event_identity(source, source_id):
    digest = hashlib.sha256(f"{source}:{source_id}".encode("utf-8")).hexdigest()
    return digest[:20]


def _event_copy(event_type, payment_kind=None):
    copies = {
        "user_registered": ("Новый пользователь", "Профиль создан в Mini App", "user", "normal"),
        "payment_failed": ("Платёж не прошёл", "Требуется проверка оплаты", "payment", "danger"),
        "gift_paid": ("Оплачен подарочный сертификат", "Подарок готов к активации", "gift", "normal"),
        "gift_redeemed": ("Подарочный доступ активирован", "Доступ участника обновлён", "gift", "normal"),
        "class_paid": ("Участник записался на занятие", "Оплата занятия подтверждена", "class", "normal"),
        "class_cancelled": ("Запись на занятие отменена", "Бронирование отменено", "class", "warning"),
        "access_closed": ("Доступ завершён", "Доступ закрыт по сроку", "access", "warning"),
        "member_left": ("Участник вышел из клуба", "Событие Telegram подтверждено", "user", "warning"),
        "manual_access": ("Доступ участника изменён", "Изменение подтверждено администратором", "access", "normal"),
    }
    if event_type == "payment_succeeded":
        label = "Подписка продлена" if payment_kind == "recurring" else "Оплата подписки подтверждена"
        return label, "Доступ оплачен", "payment", "normal"
    return copies[event_type]


def load_admin_overview_supplementary(get_conn, days=7, limit=DEFAULT_ACTIVITY_LIMIT):
    days, limit = parse_overview_query(days, limit)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SET LOCAL statement_timeout = 5000")
        cur.execute(
            """
            WITH bounds AS (
                SELECT timezone('UTC', NOW())::date - (%s::integer - 1) AS start_day,
                       timezone('UTC', NOW())::date + 1 AS end_day
            ), requested_days AS (
                SELECT generate_series(
                    bounds.start_day,
                    bounds.end_day - 1,
                    interval '1 day'
                )::date AS day
                FROM bounds
            ), registrations AS (
                SELECT users.registered_at::date AS day, COUNT(*) AS value
                FROM users CROSS JOIN bounds
                WHERE users.registered_at >= bounds.start_day::timestamp
                  AND users.registered_at < bounds.end_day::timestamp
                GROUP BY users.registered_at::date
            )
            SELECT requested_days.day, COALESCE(registrations.value, 0)
            FROM requested_days
            LEFT JOIN registrations ON registrations.day = requested_days.day
            ORDER BY requested_days.day
            """,
            (days,),
        )
        series = [
            {"date": day.isoformat(), "value": int(value)}
            for day, value in cur.fetchall()
        ]
        cur.execute(
            """
            WITH activity AS (
                SELECT 'user'::text AS source, users.telegram_id::text AS source_id,
                       'user_registered'::text AS event_type, NULL::text AS event_detail,
                       users.telegram_id, users.first_name, users.username,
                       users.registered_at AS occurred_at
                FROM users
                WHERE users.registered_at >= NOW() - INTERVAL '365 days'

                UNION ALL

                SELECT 'payment', payment_events.id::text,
                       CASE WHEN payment_events.payment_status = 'succeeded'
                            THEN 'payment_succeeded' ELSE 'payment_failed' END,
                       payment_events.payment_kind,
                       payment_events.telegram_id, users.first_name, users.username,
                       payment_events.created_at
                FROM payment_events
                LEFT JOIN users ON users.telegram_id = payment_events.telegram_id
                WHERE payment_events.payment_status IN ('succeeded', 'failed')
                  AND payment_events.payment_kind IN ('initial_subscription', 'recurring', 'trial')
                  AND payment_events.created_at >= NOW() - INTERVAL '365 days'

                UNION ALL

                SELECT 'gift', gift_access_events.id::text, gift_access_events.event_type,
                       NULL::text, gift_access_events.telegram_id, users.first_name,
                       users.username, gift_access_events.created_at
                FROM gift_access_events
                LEFT JOIN users ON users.telegram_id = gift_access_events.telegram_id
                WHERE gift_access_events.event_type IN ('gift_paid', 'gift_redeemed')
                  AND gift_access_events.created_at >= NOW() - INTERVAL '365 days'

                UNION ALL

                SELECT 'class', class_bookings.booking_id::text, 'class_paid',
                       bookable_classes.title, class_bookings.telegram_id,
                       users.first_name, users.username, class_bookings.paid_at
                FROM class_bookings
                JOIN bookable_classes ON bookable_classes.class_id = class_bookings.class_id
                LEFT JOIN users ON users.telegram_id = class_bookings.telegram_id
                WHERE class_bookings.paid_at >= NOW() - INTERVAL '365 days'

                UNION ALL

                SELECT 'class-cancelled', class_bookings.booking_id::text, 'class_cancelled',
                       bookable_classes.title, class_bookings.telegram_id,
                       users.first_name, users.username, class_bookings.cancelled_at
                FROM class_bookings
                JOIN bookable_classes ON bookable_classes.class_id = class_bookings.class_id
                LEFT JOIN users ON users.telegram_id = class_bookings.telegram_id
                WHERE class_bookings.cancelled_at >= NOW() - INTERVAL '365 days'

                UNION ALL

                SELECT 'access', access_events.id::text,
                       CASE
                         WHEN access_events.event_type = 'auto_access_closed_expired' THEN 'access_closed'
                         WHEN access_events.event_type IN ('group_member_left', 'group_member_left_service') THEN 'member_left'
                         ELSE 'manual_access'
                       END,
                       NULL::text, access_events.telegram_id, users.first_name,
                       users.username, access_events.created_at
                FROM access_events
                LEFT JOIN users ON users.telegram_id = access_events.telegram_id
                WHERE access_events.event_type IN (
                    'auto_access_closed_expired', 'group_member_left',
                    'group_member_left_service', 'manual_give_access', 'manual_set_expiry'
                )
                  AND access_events.created_at >= NOW() - INTERVAL '365 days'
            )
            SELECT source, source_id, event_type, event_detail, telegram_id,
                   first_name, username, occurred_at
            FROM activity
            WHERE occurred_at IS NOT NULL
            ORDER BY occurred_at DESC, source DESC, source_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        events = []
        for source, source_id, event_type, detail, telegram_id, first_name, username, occurred_at in cur.fetchall():
            title, secondary, icon, tone = _event_copy(event_type, detail)
            display_name = first_name or (f"@{username}" if username else None)
            if display_name:
                title = f"{display_name} · {title.lower()}"
            if detail and event_type in {"class_paid", "class_cancelled"}:
                secondary = detail
            events.append({
                "id": _event_identity(source, source_id),
                "title": title,
                "secondary": secondary,
                "icon": icon,
                "tone": tone,
                "occurred_at": _iso(occurred_at),
            })
        conn.rollback()
        return {
            "period_days": days,
            "metric": "new_registrations",
            "series": series,
            "events": events,
            "events_limit": limit,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
