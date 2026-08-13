STRIPE_PURPOSE_LABELS = {
    "checkout_expired": "Напоминание об истёкшей ссылке на оплату",
    "checkout_async_payment_failed": "Сообщение о неуспешной оплате",
    "invoice_payment_failed": "Сообщение об ошибке регулярного платежа",
    "payment_failed": "Сообщение об ошибке оплаты",
    "payment_success": "Подтверждение успешной оплаты",
    "renewal_success": "Подтверждение продления подписки",
    "rejoin_invite": "Ссылка для повторного входа после оплаты",
}

DELIVERY_TYPE_LABELS = {
    "access_restore_invite": "Ссылка для восстановления доступа",
    "stripe_rejoin_invite": "Ссылка для повторного входа после оплаты",
    "stripe_rejoin_check": "Сообщение о восстановлении входа после оплаты",
    "gift_certificate_buyer": "Подарочный сертификат покупателю",
    "gift_certificate_recipient": "Подарочный сертификат получателю",
    "gift_redeemed_recipient": "Сообщение получателю об активации подарка",
    "gift_refunded_buyer": "Сообщение покупателю о возврате подарка",
    "gift_refunded_recipient": "Сообщение получателю о возврате подарка",
}


def stripe_delivery_purpose(delivery_key):
    parts = str(delivery_key or "").split(":", 2)
    return parts[2] if len(parts) == 3 and parts[0] == "stripe" else None


def human_delivery_label(delivery_type, delivery_key=None, payload=None):
    if delivery_type == "stripe_user_message":
        purpose = (payload or {}).get("purpose") or stripe_delivery_purpose(delivery_key)
        return STRIPE_PURPOSE_LABELS.get(purpose, "Важное сообщение о подписке или оплате")
    return DELIVERY_TYPE_LABELS.get(delivery_type, "Важное сообщение пользователю")


def human_failure_reason(reason, blocked=False, retryable=False):
    if blocked or reason == "telegram_forbidden_user_delivery":
        return (
            "Пользователь заблокировал бота или запретил ему отправлять сообщения.",
            "Повторная отправка невозможна, пока пользователь сам не откроет бот снова.",
        )
    if reason == "telegram_bad_request_terminal":
        return (
            "Telegram сообщил, что чат с пользователем недоступен или сообщение нельзя доставить.",
            "Автоматическая повторная отправка не выполняется. Проверьте ситуацию вручную.",
        )
    if reason in {"telegram_retry_after", "telegram_network_error", "telegram_bad_request_retryable"}:
        return (
            "Временная ошибка Telegram. Бот попробует отправить сообщение повторно.",
            "Повторная отправка будет выполнена автоматически.",
        )
    return (
        "Сообщение не удалось доставить из-за технической ошибки.",
        "Повторная отправка будет выполнена автоматически." if retryable else "Автоматическая повторная отправка не выполняется.",
    )


def render_critical_delivery_alert(
    *, delivery_type, delivery_key, telegram_id, reason, blocked, retryable, safe_user_ref, payload=None
):
    failure_reason, guidance = human_failure_reason(reason, blocked=blocked, retryable=retryable)
    title = "⚠️ Важное сообщение пока не доставлено" if retryable else "⚠️ Не удалось доставить важное сообщение пользователю"
    retry_text = "будет выполнена автоматически" if retryable else "не выполняется"
    user_ref = safe_user_ref(str(telegram_id)) if telegram_id is not None else "недоступен"
    return (
        f"{title}\n\n"
        f"Пользователь: {user_ref}\n"
        f"Сообщение: {human_delivery_label(delivery_type, delivery_key, payload)}\n"
        f"Причина: {failure_reason}\n"
        f"Повторная отправка: {retry_text}.\n\n"
        f"{guidance}\n"
        "Ошибка относится только к доставке сообщения; автоматический откат операции не выполнялся.\n"
        "Детали: /bot_health"
    )
