import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo


WARNING_3D = "renewal_warning_3d"
WARNING_1D = "renewal_warning_1d"
WARNING_TYPES = (WARNING_3D, WARNING_1D)


class RenewalNotificationError(ValueError):
    pass


def value(obj, *path):
    current = obj
    for key in path:
        if current is None:
            return None
        current = current.get(key) if isinstance(current, dict) else getattr(current, key, None)
    return current


def subscription_terms(tariff_code):
    terms = {
        "sub_1": {"label": "1 месяц", "amount_minor": 5000, "period": "1 месяц"},
        "sub_6": {"label": "6 месяцев", "amount_minor": 24000, "period": "6 месяцев"},
        "sub_12": {"label": "12 месяцев", "amount_minor": 41000, "period": "12 месяцев"},
    }
    if tariff_code not in terms:
        raise RenewalNotificationError("unsupported_recurring_tariff")
    return terms[tariff_code]


def format_eur(amount_minor):
    amount = Decimal(int(amount_minor)) / 100
    return f"{amount:.2f}".replace(".00", "").replace(".", ",") + " €"


def recurring_checkout_disclosure(tariff_code):
    terms = subscription_terms(tariff_code)
    amount = format_eur(terms["amount_minor"])
    return (
        f"Ваш абонемент — {terms['label']}, {amount}.\n\n"
        "Подписка продлевается автоматически. По окончании оплаченного периода "
        f"Stripe спишет {amount} за следующий период ({terms['period']}), если автопродление не отключено.\n\n"
        "Вы можете отключить автопродление в разделе «Моя подписка». "
        "Доступ сохранится до конца оплаченного периода.\n\n"
        "Нажимая «Подтверждаю и перехожу к оплате», вы соглашаетесь с условиями регулярных платежей."
    )


def validate_price_for_terms(price, tariff_code):
    terms = subscription_terms(tariff_code)
    if value(price, "active") is False:
        raise RenewalNotificationError("stripe_price_inactive")
    if str(value(price, "currency") or "").lower() != "eur":
        raise RenewalNotificationError("stripe_price_currency_mismatch")
    if int(value(price, "unit_amount") or -1) != terms["amount_minor"]:
        raise RenewalNotificationError("stripe_price_amount_mismatch")
    recurring = value(price, "recurring")
    interval = value(recurring, "interval")
    interval_count = int(value(recurring, "interval_count") or 1)
    expected_months = {"sub_1": 1, "sub_6": 6, "sub_12": 12}[tariff_code]
    if interval != "month" or interval_count != expected_months:
        raise RenewalNotificationError("stripe_price_interval_mismatch")
    return True


def upcoming_snapshot(subscription, invoice, now=None, timezone_name="Europe/Moscow"):
    status = value(subscription, "status")
    if status not in ("active", "trialing"):
        raise RenewalNotificationError("subscription_not_renewable")
    if bool(value(subscription, "cancel_at_period_end")):
        raise RenewalNotificationError("auto_renew_disabled")
    subscription_id = value(subscription, "id")
    invoice_subscription = value(invoice, "subscription")
    if not subscription_id or (invoice_subscription and invoice_subscription != subscription_id):
        raise RenewalNotificationError("invoice_subscription_mismatch")
    amount_due = value(invoice, "amount_due")
    currency = str(value(invoice, "currency") or "").lower()
    period_end = (
        value(invoice, "next_payment_attempt")
        or value(invoice, "period_start")
        or value(subscription, "current_period_end")
    )
    if amount_due is None or int(amount_due) <= 0:
        raise RenewalNotificationError("upcoming_amount_unconfirmed")
    if not currency or not period_end:
        raise RenewalNotificationError("upcoming_invoice_incomplete")
    billing_at = datetime.fromtimestamp(int(period_end), tz=timezone.utc)
    now = now or datetime.now(timezone.utc)
    tz = ZoneInfo(timezone_name)
    days_until = (billing_at.astimezone(tz).date() - now.astimezone(tz).date()).days
    warning_type = WARNING_3D if days_until == 3 else WARNING_1D if days_until == 1 else None
    fingerprint = hashlib.sha256(
        f"{subscription_id}:{int(period_end)}:{int(amount_due)}:{currency}".encode()
    ).hexdigest()
    return {
        "stripe_subscription_id": subscription_id,
        "billing_reference": fingerprint,
        "billing_at": billing_at,
        "amount_minor": int(amount_due),
        "currency": currency,
        "days_until": days_until,
        "warning_type": warning_type,
    }


def delivery_key(snapshot, warning_type):
    if warning_type not in WARNING_TYPES:
        raise RenewalNotificationError("invalid_warning_type")
    return f"renewal-warning:{snapshot['billing_reference']}:{warning_type}"


def warning_text(snapshot, warning_type):
    date_text = snapshot["billing_at"].strftime("%d.%m.%Y")
    currency = snapshot["currency"].lower()
    amount = format_eur(snapshot["amount_minor"]) if currency == "eur" else (
        f"{Decimal(snapshot['amount_minor']) / 100:.2f} {currency.upper()}"
    )
    if warning_type == WARNING_3D:
        return (
            "🤍 Ваша подписка скоро продлится.\n\n"
            f"{date_text} запланировано автоматическое списание {amount} за следующий период участия в клубе.\n\n"
            "Если хотите продолжить занятия, ничего делать не нужно.\n\n"
            "Если решили сделать паузу, вы можете отключить автопродление до даты списания "
            "в разделе «Моя подписка»."
        )
    if warning_type == WARNING_1D:
        return (
            f"🤍 Напоминаем: завтра, {date_text}, запланировано продление вашей подписки.\n\n"
            f"Стоимость следующего периода — {amount}.\n\n"
            "Если хотите продолжить занятия, ничего делать не нужно.\n\n"
            "Автопродление можно отключить в разделе «Моя подписка» до следующего списания."
        )
    raise RenewalNotificationError("invalid_warning_type")
