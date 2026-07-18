from datetime import datetime


def stripe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def is_zero_amount_invoice(amount_paid):
    return stripe_int(amount_paid, default=0) == 0


def is_zero_subscription_update_invoice(amount_paid, billing_reason):
    return is_zero_amount_invoice(amount_paid) and billing_reason == "subscription_update"


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


def stripe_object_id(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return stripe_value(value, "id")


def paid_invoice_payment_records(invoice):
    payments_data = stripe_value(invoice, "payments", "data") or []
    return [
        payment_record
        for payment_record in payments_data
        if stripe_value(payment_record, "status") == "paid"
    ]


def paid_invoice_payment_intent_total(invoice):
    total = 0
    for payment_record in paid_invoice_payment_records(invoice):
        payment = stripe_value(payment_record, "payment") or {}
        payment_type = stripe_value(payment, "type")
        payment_intent_id = stripe_object_id(stripe_value(payment, "payment_intent"))
        charge_id = stripe_object_id(stripe_value(payment, "charge"))
        if payment_type in ("payment_intent", "charge") and (payment_intent_id or charge_id):
            total += stripe_int(stripe_value(payment_record, "amount_paid"), default=0)
    return total


def has_paid_out_of_band_payment_record(invoice):
    for payment_record in paid_invoice_payment_records(invoice):
        payment_type = stripe_value(payment_record, "payment", "type")
        payment_record_id = stripe_object_id(stripe_value(payment_record, "payment", "payment_record"))
        if payment_type == "payment_record" and payment_record_id:
            return True
    return False


def is_paid_out_of_band_invoice(invoice, amount_paid, amount_due):
    if stripe_value(invoice, "paid_out_of_band") is True:
        return True

    amount_paid = stripe_int(amount_paid, default=0)
    if amount_paid <= 0:
        return False

    return has_paid_out_of_band_payment_record(invoice)


def has_future_trial(status, trial_end, now=None):
    if status != "trialing" or not trial_end:
        return False

    now = now or datetime.utcnow()
    trial_end_dt = datetime.utcfromtimestamp(stripe_int(trial_end))
    return trial_end_dt > now


def successful_invoice_action(
    amount_paid,
    billing_reason,
    subscription_status,
    trial_end,
    now=None,
    invoice=None,
    amount_due=None,
):
    if is_zero_amount_invoice(amount_paid):
        if has_future_trial(subscription_status, trial_end, now=now):
            return "sync_trial"
        return "ignore_zero"

    if invoice is not None and is_paid_out_of_band_invoice(invoice, amount_paid, amount_due):
        return "process_out_of_band"

    return "process_payment"


def should_ignore_payment_failed_for_active_trial(subscription_status, trial_end, now=None):
    return has_future_trial(subscription_status, trial_end, now=now)
