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


def has_future_trial(status, trial_end, now=None):
    if status != "trialing" or not trial_end:
        return False

    now = now or datetime.utcnow()
    trial_end_dt = datetime.utcfromtimestamp(stripe_int(trial_end))
    return trial_end_dt > now


def successful_invoice_action(amount_paid, billing_reason, subscription_status, trial_end, now=None):
    if is_zero_amount_invoice(amount_paid):
        if has_future_trial(subscription_status, trial_end, now=now):
            return "sync_trial"
        return "ignore_zero"

    return "process_payment"


def should_ignore_payment_failed_for_active_trial(subscription_status, trial_end, now=None):
    return has_future_trial(subscription_status, trial_end, now=now)
