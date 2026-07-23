import csv
import os
from datetime import datetime, timedelta, timezone
from io import StringIO
from zoneinfo import ZoneInfo


MOSCOW_TZ = ZoneInfo("Europe/Moscow")

TARIFF_LABELS = {
    "sub_trial": "Пробная неделя",
    "sub_1": "1 месяц",
    "sub_6": "6 месяцев",
    "sub_12": "12 месяцев",
    "unknown": "Тариф не определён",
}

PAYMENT_KIND_LABELS = {
    "trial": "trial",
    "initial_subscription": "первая покупка",
    "recurring": "автопродление",
    "adjustment": "корректировка",
    "out_of_band": "ручная оплата",
    "unknown": "неизвестно",
}


def _as_moscow(value=None):
    if value is None:
        return datetime.now(MOSCOW_TZ)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).astimezone(MOSCOW_TZ)
    return value.astimezone(MOSCOW_TZ)


def _week_start(value):
    value = _as_moscow(value)
    return value.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=value.weekday())


def get_current_week_bounds(now=None):
    start = _week_start(now)
    end = _as_moscow(now)
    return start, end


def get_last_completed_week_bounds(now=None):
    current_start = _week_start(now)
    return current_start - timedelta(days=7), current_start


def get_previous_comparison_week_bounds(now=None):
    report_start, _ = get_last_completed_week_bounds(now)
    return report_start - timedelta(days=7), report_start


def to_utc_naive(value):
    return _as_moscow(value).astimezone(timezone.utc).replace(tzinfo=None)


def report_key(period_start):
    return _as_moscow(period_start).date().isoformat()


def format_period_title(period_start, period_end):
    start = _as_moscow(period_start)
    end = _as_moscow(period_end)
    if end > start and end.time() == datetime.min.time():
        end = end - timedelta(days=1)
    if start.date() == end.date():
        months = {
            1: "января",
            2: "февраля",
            3: "марта",
            4: "апреля",
            5: "мая",
            6: "июня",
            7: "июля",
            8: "августа",
            9: "сентября",
            10: "октября",
            11: "ноября",
            12: "декабря",
        }
        return f"{start.day} {months[start.month]} {start.year}"
    months = {
        1: "января",
        2: "февраля",
        3: "марта",
        4: "апреля",
        5: "мая",
        6: "июня",
        7: "июля",
        8: "августа",
        9: "сентября",
        10: "октября",
        11: "ноября",
        12: "декабря",
    }
    if start.month == end.month:
        return f"{start.day}–{end.day} {months[end.month]} {end.year}"
    return f"{start.day} {months[start.month]} – {end.day} {months[end.month]} {end.year}"


def format_change(current, previous):
    current = int(current or 0)
    previous = int(previous or 0)
    diff = current - previous
    if previous > 0:
        percent = round(diff * 100 / previous)
        arrow = "↑" if diff > 0 else "↓" if diff < 0 else "→"
        return f"{arrow} {abs(percent)}% ({diff:+d}) к прошлой неделе"
    if current > 0:
        return f"рост с 0 до {current}"
    return "без изменений"


def format_minor_amount(amount, currency):
    amount = int(amount or 0)
    currency = (currency or "").upper()
    value = amount / 100
    formatted = f"{value:,.2f}".replace(",", " ").replace(".", ",")
    symbols = {"EUR": "€", "USD": "$", "RUB": "₽"}
    symbol = symbols.get(currency)
    return f"{formatted} {symbol}" if symbol else f"{formatted} {currency}".strip()


def format_major_amount(amount, currency):
    amount = int(amount or 0)
    value = amount / 100
    return f"{value:.2f}"


def format_money_change(current_minor, previous_minor, currency):
    current_minor = int(current_minor or 0)
    previous_minor = int(previous_minor or 0)
    diff = current_minor - previous_minor
    if previous_minor > 0:
        percent = round(diff * 100 / previous_minor)
        arrow = "↑" if diff > 0 else "↓" if diff < 0 else "→"
        return f"{arrow} {abs(percent)}% ({format_minor_amount(diff, currency) if diff < 0 else '+' + format_minor_amount(diff, currency)}) к прошлой неделе"
    if current_minor > 0:
        return f"рост с 0 до {format_minor_amount(current_minor, currency)}"
    return "без изменений"


def tariff_code_from_price_id(price_id, env=None):
    env = env or os.environ
    for env_name, code in (
        ("PRICE_TRIAL", "sub_trial"),
        ("PRICE_1M", "sub_1"),
        ("PRICE_6M", "sub_6"),
        ("PRICE_12M", "sub_12"),
    ):
        expected = env.get(env_name)
        if expected and price_id == expected:
            return code
    return "unknown"


def stripe_value(obj, *path):
    current = obj
    for key in path:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(key)
        else:
            current = getattr(current, key, None)
    return current


def price_id_from_invoice_lines(invoice):
    lines = stripe_value(invoice, "lines", "data") or []
    for line in lines:
        price_id = stripe_value(line, "price", "id")
        price_id = price_id or stripe_value(line, "pricing", "price_details", "price")
        if price_id:
            return price_id
    return None


def tariff_code_from_invoice(invoice, env=None):
    return tariff_code_from_price_id(price_id_from_invoice_lines(invoice), env=env)


def format_buyer_name(payment):
    username = payment.get("username")
    if username:
        return f"@{username.lstrip('@')}"
    full_name = " ".join(
        part for part in (payment.get("first_name"), payment.get("last_name")) if part
    ).strip()
    if full_name:
        return full_name
    return f"telegram_id: {payment.get('telegram_id')}"


def sanitize_csv_cell(value):
    if value is None:
        return ""
    value = str(value)
    if value.startswith(("=", "+", "-", "@", "\t", "\r")):
        return "'" + value
    return value


def build_weekly_report_text(
    period_start,
    period_end,
    metrics,
    comparison=None,
    buyers=None,
    history_note=None,
):
    comparison = comparison or {}
    buyers = buyers or []
    revenue = metrics.get("revenue_by_currency", {})
    revenue_previous = comparison.get("revenue_by_currency", {})
    revenue_lines = []
    currencies = sorted(set(revenue) | set(revenue_previous))
    if currencies:
        for currency in currencies:
            amount = revenue.get(currency, 0)
            revenue_lines.append(
                f"{format_minor_amount(amount, currency)} — {format_money_change(amount, revenue_previous.get(currency, 0), currency)}"
            )
    else:
        revenue_lines.append(f"{format_minor_amount(0, 'EUR')} — без изменений")

    tariff_counts = metrics.get("tariff_counts", {})
    tariff_lines = [
        f"• {TARIFF_LABELS.get(code, TARIFF_LABELS['unknown'])} — {tariff_counts.get(code, 0)}"
        for code in ("sub_1", "sub_6", "sub_12", "sub_trial", "unknown")
        if tariff_counts.get(code, 0) or code != "unknown"
    ]

    visible_buyers = buyers[:10]
    buyer_lines = []
    for payment in visible_buyers:
        paid_at = _as_moscow(payment.get("paid_at")).strftime("%d.%m, %H:%M")
        buyer_lines.append(
            "• "
            f"{paid_at}, {format_buyer_name(payment)} — "
            f"{TARIFF_LABELS.get(payment.get('tariff_code'), TARIFF_LABELS['unknown'])} — "
            f"{PAYMENT_KIND_LABELS.get(payment.get('payment_kind'), 'неизвестно')} — "
            f"{format_minor_amount(payment.get('amount_paid'), payment.get('currency'))}"
        )
    if not buyer_lines:
        buyer_lines.append("Покупок за период нет.")
    if len(buyers) > 10:
        buyer_lines.append(f"Ещё покупок: {len(buyers) - 10}. Полный список доступен в CSV.")

    lines = [
        "📊 Итоги недели",
        format_period_title(period_start, period_end),
        "",
    ]
    if history_note:
        lines.extend([history_note, ""])
    lines.extend([
        "👥 Аудитория",
        f"Новые регистрации: {metrics.get('new_registrations', 0)} — {format_change(metrics.get('new_registrations', 0), comparison.get('new_registrations', 0))}",
        f"Получили бесплатный урок: {metrics.get('free_lessons', 0)}",
        f"Вошли в клуб: {metrics.get('group_joins', 0)}",
        f"Вышли из клуба: {metrics.get('group_leaves', 0)}",
        f"Активных участников сейчас: {metrics.get('active_paid_now', 0)}",
        f"Всего пользователей: {metrics.get('total_users_now', 0)}",
        f"Заблокировали бота: {metrics.get('blocked_bot_now', 0)}",
        "",
        "💳 Оплаты",
        f"Первые покупки: {metrics.get('initial_purchases', 0)} — {format_change(metrics.get('initial_purchases', 0), comparison.get('initial_purchases', 0))}",
        f"Автопродления: {metrics.get('recurring_payments', 0)}",
        f"Пробные недели: {metrics.get('trial_payments', 0)}",
        f"Остальные корректировки: {metrics.get('adjustment_payments', 0)}",
        f"Неуспешные платежи: {metrics.get('failed_payments', 0)}",
        f"Восстановлены после ошибки: {metrics.get('recovered_after_failure', 0)}",
        f"Уникальных покупателей: {metrics.get('unique_payers', 0)}",
        f"Успешных оплат всего: {metrics.get('successful_payments', 0)} — {format_change(metrics.get('successful_payments', 0), comparison.get('successful_payments', 0))}",
        f"Выручка: {'; '.join(revenue_lines)}",
        "",
        "По тарифам:",
        *tariff_lines,
        "",
        "🛍 Кто купил",
        *buyer_lines,
        "",
        "⚠️ Подписки",
        f"Отключили автопродление: {metrics.get('auto_renew_disabled', 0)}",
        f"Доступ закрыт: {metrics.get('access_closed', 0)}",
        f"В grace period сейчас: {metrics.get('grace_period_now', 0)}",
        f"Ошибки платежей сейчас: {metrics.get('payment_failed_now', 0)}",
        f"Непривязанные Stripe-события: {metrics.get('unlinked_stripe_events', 0)}",
        f"paid=True с истекшим доступом: {metrics.get('expired_paid_now', 0)}",
    ])

    text = "\n".join(lines)
    if len(text) <= 4096:
        return text

    while len(buyer_lines) > 1 and len(text) > 4096:
        buyer_lines.pop(-2 if buyer_lines[-1].startswith("Ещё покупок") else -1)
        lines = lines[: lines.index("🛍 Кто купил") + 1] + buyer_lines + lines[lines.index("⚠️ Подписки") - 1 :]
        text = "\n".join(lines)
    return text[:4093] + "..." if len(text) > 4096 else text


def build_payments_csv(payments):
    output = StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "paid_at_moscow",
            "telegram_id",
            "username",
            "full_name",
            "tariff",
            "payment_kind",
            "amount",
            "currency",
            "billing_reason",
            "recovered_after_failure",
        ],
    )
    writer.writeheader()
    for payment in payments:
        full_name = " ".join(
            part for part in (payment.get("first_name"), payment.get("last_name")) if part
        ).strip()
        writer.writerow({
            "paid_at_moscow": _as_moscow(payment.get("paid_at")).strftime("%Y-%m-%d %H:%M:%S"),
            "telegram_id": payment.get("telegram_id") or "",
            "username": sanitize_csv_cell(payment.get("username") or ""),
            "full_name": sanitize_csv_cell(full_name),
            "tariff": TARIFF_LABELS.get(payment.get("tariff_code"), TARIFF_LABELS["unknown"]),
            "payment_kind": PAYMENT_KIND_LABELS.get(payment.get("payment_kind"), "неизвестно"),
            "amount": format_major_amount(payment.get("amount_paid"), payment.get("currency")),
            "currency": (payment.get("currency") or "").upper(),
            "billing_reason": payment.get("billing_reason") or "",
            "recovered_after_failure": "yes" if payment.get("recovered_after_failure") else "no",
        })
    return output.getvalue().encode("utf-8-sig")


def parse_admin_ids(value):
    if not value:
        return []
    return [
        int(part)
        for part in str(value).split(",")
        if part.strip()
    ]


def should_create_manual_link_payment_event(amount_paid):
    return int(amount_paid or 0) > 0


def classify_manual_link_payment_kind(billing_reason, invoice_action=None):
    if invoice_action == "process_out_of_band":
        return "out_of_band"
    if invoice_action == "process_payment":
        if billing_reason == "subscription_create":
            return "initial_subscription"
        if billing_reason == "subscription_cycle":
            return "recurring"
        return "adjustment"
    return "unknown"


def claim_weekly_report_run_record(cur, key, period_start_utc, period_end_utc, now_utc, lease_minutes=30):
    stale_before = now_utc - timedelta(minutes=lease_minutes)
    cur.execute(
        """
        INSERT INTO weekly_report_runs (
            report_key,
            period_start,
            period_end,
            status,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, 'processing', %s, %s)
        ON CONFLICT (report_key) DO UPDATE SET
            status = 'processing',
            period_start = EXCLUDED.period_start,
            period_end = EXCLUDED.period_end,
            updated_at = EXCLUDED.updated_at,
            error_text = NULL
        WHERE weekly_report_runs.status = 'failed'
           OR (
                weekly_report_runs.status = 'processing'
                AND COALESCE(weekly_report_runs.updated_at, weekly_report_runs.created_at) < %s
           )
        RETURNING status, sent_admin_ids
        """,
        (key, period_start_utc, period_end_utc, now_utc, now_utc, stale_before),
    )
    row = cur.fetchone()
    if row:
        return {
            "status": "claimed",
            "sent_admin_ids": parse_admin_ids(row[1] if len(row) > 1 else None),
        }

    cur.execute("SELECT status, sent_admin_ids FROM weekly_report_runs WHERE report_key = %s", (key,))
    row = cur.fetchone()
    if row and row[0] == "completed":
        return {"status": "duplicate_completed", "sent_admin_ids": parse_admin_ids(row[1])}
    if row and row[0] == "processing":
        return {"status": "already_processing", "sent_admin_ids": parse_admin_ids(row[1])}
    return {"status": "already_processing", "sent_admin_ids": []}
