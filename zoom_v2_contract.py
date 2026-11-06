"""Pure Zoom v2 state-contract rules.

This module deliberately has no database, Telegram, or Stripe SDK imports.  It
defines the agreed v2 vocabulary and validates decisions made by later service
and persistence layers without changing the existing booking implementation.
"""

from datetime import timedelta


BOOKING_MODEL_V2 = "authorization_v2"
LEGACY_BOOKING_MODEL = "legacy_capture_now"

CLASS_DURATION_MINUTES = 60
CLASS_PUBLISH_WINDOW = timedelta(days=7)
CANCELLATION_WINDOW = timedelta(hours=24)
ACCESS_REMINDER_WINDOW = timedelta(hours=48)
AUTHORIZATION_SAFETY_MARGIN = timedelta(hours=2)
GUEST_PRICE_AMOUNT = 1000
GUEST_PRICE_CURRENCY = "eur"

CLASS_STATES = frozenset({
    "draft", "open", "in_progress", "settlement_pending",
    "settlement_review", "completed", "cancelled",
})
BOOKING_STATES = frozenset({
    "access_required", "booked_subscription", "authorization_pending",
    "booked_guest", "cancelled_early", "cancelled_late",
    "organizer_cancelled", "attended", "no_show",
})
PAYMENT_STATES = frozenset({
    "none", "authorization_creating", "requires_action", "authorized",
    "cancel_pending", "cancelled", "capture_pending", "captured", "expired",
    "retryable_failed", "manual_review",
})

# Existing 0034 rows use these states.  They remain valid inputs, but v2 code
# must not silently reinterpret or mutate them before migration 0036 exists.
LEGACY_CLASS_STATES = frozenset({"draft", "open", "confirmed", "cancelled", "completed"})
LEGACY_BOOKING_STATES = frozenset({"pending", "checkout_open", "paid", "cancelled", "refunded"})


class ZoomV2ContractError(ValueError):
    def __init__(self, category):
        super().__init__(category)
        self.category = category


def validate_class_state(value):
    if value not in CLASS_STATES:
        raise ZoomV2ContractError("invalid_class_state")
    return value


def validate_booking_state(value):
    if value not in BOOKING_STATES:
        raise ZoomV2ContractError("invalid_booking_state")
    return value


def validate_payment_state(value):
    if value not in PAYMENT_STATES:
        raise ZoomV2ContractError("invalid_payment_state")
    return value


def _require_aware(value, category):
    if value is None or value.tzinfo is None or value.utcoffset() is None:
        raise ZoomV2ContractError(category)
    return value


def validate_class_contract(*, published_at, starts_at, duration_minutes, capacity):
    """Validate product invariants for a published v2 class."""
    published_at = _require_aware(published_at, "invalid_published_at")
    starts_at = _require_aware(starts_at, "invalid_starts_at")
    if starts_at <= published_at or starts_at - published_at > CLASS_PUBLISH_WINDOW:
        raise ZoomV2ContractError("invalid_publish_window")
    if duration_minutes != CLASS_DURATION_MINUTES:
        raise ZoomV2ContractError("invalid_duration")
    if capacity is not None:
        raise ZoomV2ContractError("capacity_must_be_unlimited")
    return True


def classify_booking_access(*, has_active_access, access_expires_at, class_starts_at):
    """Classify an initial booking without creating a payment."""
    class_starts_at = _require_aware(class_starts_at, "invalid_class_starts_at")
    if not has_active_access or access_expires_at is None:
        return "access_required"
    access_expires_at = _require_aware(access_expires_at, "invalid_access_expiry")
    return "booked_subscription" if access_expires_at >= class_starts_at else "access_required"


def reconcile_subscription_booking(
    current_state, *, has_active_access, access_expires_at, class_starts_at
):
    """Re-check subscription-derived access without overriding guest payment."""
    if current_state in LEGACY_BOOKING_STATES:
        return current_state
    if current_state == "booked_guest":
        return current_state
    if current_state not in ("access_required", "booked_subscription"):
        raise ZoomV2ContractError("booking_not_access_reconcilable")
    return classify_booking_access(
        has_active_access=has_active_access,
        access_expires_at=access_expires_at,
        class_starts_at=class_starts_at,
    )


def request_guest_authorization(
    current_state, *, explicit_consent, amount=GUEST_PRICE_AMOUNT,
    currency=GUEST_PRICE_CURRENCY
):
    """Record an explicit guest choice; this never calls Stripe itself."""
    if current_state == "authorization_pending":
        return current_state
    if current_state != "access_required":
        raise ZoomV2ContractError("guest_payment_not_available")
    if explicit_consent is not True:
        raise ZoomV2ContractError("explicit_payment_consent_required")
    if amount != GUEST_PRICE_AMOUNT or str(currency).lower() != GUEST_PRICE_CURRENCY:
        raise ZoomV2ContractError("invalid_guest_price")
    return "authorization_pending"


def confirm_guest_authorization(
    current_state, *, stripe_status, authorization_expires_at,
    class_starts_at, duration_minutes=CLASS_DURATION_MINUTES
):
    """Accept only a manual-capture authorization covering the class window."""
    if current_state == "booked_guest" and stripe_status == "requires_capture":
        return current_state
    if current_state != "authorization_pending":
        raise ZoomV2ContractError("authorization_not_pending")
    if stripe_status != "requires_capture":
        raise ZoomV2ContractError("authorization_not_capturable")
    authorization_expires_at = _require_aware(
        authorization_expires_at, "invalid_authorization_expiry"
    )
    class_starts_at = _require_aware(class_starts_at, "invalid_class_starts_at")
    class_end = class_starts_at + timedelta(minutes=duration_minutes)
    if authorization_expires_at < class_end + AUTHORIZATION_SAFETY_MARGIN:
        raise ZoomV2ContractError("authorization_expires_too_soon")
    return "booked_guest"


def cancellation_state(*, requested_at, class_starts_at):
    requested_at = _require_aware(requested_at, "invalid_requested_at")
    class_starts_at = _require_aware(class_starts_at, "invalid_class_starts_at")
    if requested_at >= class_starts_at:
        raise ZoomV2ContractError("class_already_started")
    cutoff = class_starts_at - CANCELLATION_WINDOW
    return "cancelled_early" if requested_at <= cutoff else "cancelled_late"


def organizer_cancellation_state(current_state):
    """Cancel a booking without authorizing capture; repeated calls are safe."""
    if current_state == "organizer_cancelled":
        return current_state
    validate_booking_state(current_state)
    if current_state in ("attended", "no_show"):
        raise ZoomV2ContractError("booking_already_settled")
    return "organizer_cancelled"


def payment_action_for_booking(booking_state):
    """Describe the later payment worker action without performing it."""
    validate_booking_state(booking_state)
    if booking_state in ("organizer_cancelled", "cancelled_early"):
        return "cancel_authorization"
    if booking_state in ("attended", "no_show", "cancelled_late"):
        return "capture_authorization"
    return "none"


def should_send_access_reminder(*, booking_state, now, class_starts_at):
    now = _require_aware(now, "invalid_now")
    class_starts_at = _require_aware(class_starts_at, "invalid_class_starts_at")
    remaining = class_starts_at - now
    return booking_state == "access_required" and timedelta(0) < remaining <= ACCESS_REMINDER_WINDOW


def can_access_zoom(
    *, class_state, booking_state, now, class_starts_at,
    has_active_access=False, access_expires_at=None,
    payment_state="none", authorization_expires_at=None,
    legacy_paid=False
):
    """Final access fence, intended to run immediately before URL disclosure."""
    now = _require_aware(now, "invalid_now")
    class_starts_at = _require_aware(class_starts_at, "invalid_class_starts_at")
    if booking_state in LEGACY_BOOKING_STATES:
        return class_state == "confirmed" and booking_state == "paid" and legacy_paid
    if class_state not in ("open", "in_progress"):
        return False
    if booking_state == "booked_subscription":
        if not has_active_access or access_expires_at is None:
            return False
        access_expires_at = _require_aware(access_expires_at, "invalid_access_expiry")
        return access_expires_at >= class_starts_at and access_expires_at >= now
    if booking_state == "booked_guest":
        if payment_state != "authorized" or authorization_expires_at is None:
            return False
        authorization_expires_at = _require_aware(
            authorization_expires_at, "invalid_authorization_expiry"
        )
        return authorization_expires_at >= now
    return False


def validate_legacy_booking(*, class_state, booking_state):
    """Compatibility fence: accept known 0034 values without converting them."""
    if class_state not in LEGACY_CLASS_STATES or booking_state not in LEGACY_BOOKING_STATES:
        raise ZoomV2ContractError("invalid_legacy_booking")
    return {"class_state": class_state, "booking_state": booking_state}
