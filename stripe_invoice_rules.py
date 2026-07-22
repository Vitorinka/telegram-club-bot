from datetime import datetime, timezone
from urllib.parse import urlsplit


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
    trial_end_dt = datetime.fromtimestamp(stripe_int(trial_end), timezone.utc).replace(tzinfo=None)
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


def checkout_completion_action(mode, subscription_id=None):
    """Subscription Checkout only links Stripe IDs; invoice confirms access."""
    if mode == "subscription":
        return "link_only"
    return "activate_access"


def invoice_payment_kind(billing_reason, invoice_action):
    """Classify paid invoices so first subscription payments are not renewals."""
    if invoice_action == "process_out_of_band":
        return "out_of_band"
    if billing_reason == "subscription_create":
        return "initial_subscription"
    if billing_reason == "subscription_cycle":
        return "recurring"
    return "subscription_adjustment"


def should_skip_invoice_notice_for_current_expiry(payment_kind, old_expiry, new_expiry):
    """Expiry equality is not webhook idempotency for real paid invoices."""
    if payment_kind in ("initial_subscription", "recurring"):
        return False
    return bool(old_expiry and old_expiry >= new_expiry)


def merge_expiry_without_regression(existing_expiry, stripe_expiry):
    """Use Stripe period only when it is present and does not shorten access."""
    if stripe_expiry is None:
        return existing_expiry
    if existing_expiry is None:
        return stripe_expiry
    return max(existing_expiry, stripe_expiry)


def subscription_update_period(status, current_period_end, trial_end):
    """Pick a safe period for subscription.updated without inventing dates."""
    if current_period_end:
        return current_period_end, "current_period_end"
    if status == "trialing" and trial_end:
        return trial_end, "trial_end"
    return None, None


def should_send_rejoin_invite(previous_expiry, now, telegram_member_status=None, restricted_has_access=True):
    """Telegram membership wins over local expiry when deciding rejoin links."""
    if telegram_member_status in ("member", "administrator", "creator"):
        return False
    if telegram_member_status == "restricted" and restricted_has_access:
        return False
    if telegram_member_status in ("left", "kicked"):
        return True
    if telegram_member_status is None:
        return False
    access_was_inactive = previous_expiry is None or previous_expiry < now
    return access_was_inactive


def claim_stripe_event(cur, event_id, lease_seconds=600):
    """Atomically claim a Stripe event before side effects."""
    cur.execute(
        """
        INSERT INTO stripe_events (event_id, processed, processed_at)
        VALUES (%s, FALSE, NOW())
        ON CONFLICT (event_id) DO UPDATE SET
            processed = FALSE,
            processed_at = NOW()
        WHERE stripe_events.processed IS NOT TRUE
          AND stripe_events.processed_at < NOW() - (%s * INTERVAL '1 second')
        RETURNING event_id
        """,
        (event_id, lease_seconds),
    )
    if cur.fetchone():
        return "claimed"

    cur.execute("SELECT processed FROM stripe_events WHERE event_id = %s", (event_id,))
    row = cur.fetchone()
    if row and row[0]:
        return "duplicate_processed"
    return "duplicate_processing"


def mark_stripe_event_processed(cur, event_id):
    cur.execute(
        """
        INSERT INTO stripe_events (event_id, processed, processed_at)
        VALUES (%s, TRUE, NOW())
        ON CONFLICT (event_id) DO UPDATE SET
            processed = TRUE,
            processed_at = NOW()
        """,
        (event_id,),
    )


def release_stripe_event_claim(cur, event_id):
    cur.execute(
        "DELETE FROM stripe_events WHERE event_id = %s AND processed IS NOT TRUE",
        (event_id,),
    )


def redact_email(email):
    if not email:
        return None
    email = str(email)
    if "@" not in email:
        return "***"
    local, domain = email.split("@", 1)
    visible = local[:1] if local else ""
    return f"{visible}***@{domain}"


def redact_identifier(value, visible_tail=6):
    if value is None:
        return None
    value = str(value)
    if len(value) <= visible_tail:
        return "***"
    prefix = value.split("_", 1)[0] if "_" in value else "id"
    return f"{prefix}_***{value[-visible_tail:]}"


def redact_url(url):
    if not url:
        return None
    try:
        parsed = urlsplit(str(url))
        if not parsed.scheme or not parsed.netloc:
            return "***"
        return f"{parsed.scheme}://{parsed.netloc}/***"
    except Exception:
        return "***"
