import asyncio
import logging
import time
from datetime import datetime, timedelta

from access_mismatch_observability import TRUSTED_SUBSCRIPTION_PAYMENT_KINDS


EXPIRY_DRIFT_TOLERANCE = timedelta(minutes=10)
ACTIVE_STATUSES = frozenset({"active", "trialing"})
TERMINAL_STATUSES = frozenset({"canceled", "incomplete_expired"})
PROBLEM_STATUSES = frozenset({"past_due", "unpaid", "incomplete", "paused"})
KNOWN_STATUSES = ACTIVE_STATUSES | TERMINAL_STATUSES | PROBLEM_STATUSES
ACCESS_OVERRIDE_EVENTS = (
    "manual_set_expiry", "manual_restore_access", "manual_give_access",
    "manual_link_stripe_user", "gift_access_applied", "subscription_refund_access_revoked",
)


def stripe_field(value, name, default=None):
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def stripe_object_id(value):
    if isinstance(value, str):
        return value
    return stripe_field(value, "id")


def load_reconcile_candidates(cur, limit=100):
    bounded_limit = max(1, min(int(limit), 100))
    cur.execute(
        """
        SELECT
            u.telegram_id, u.paid, u.expiry_date,
            COALESCE(u.stripe_customer_id, sl.stripe_customer_id, history.stripe_customer_id),
            COALESCE(u.stripe_subscription_id, sl.stripe_subscription_id, history.stripe_subscription_id),
            sl.stripe_customer_id, sl.stripe_subscription_id,
            proof.stripe_event_id, proof.period_end, proof.created_at,
            EXISTS (
                SELECT 1 FROM access_events ae
                WHERE ae.telegram_id = u.telegram_id
                  AND ae.event_type = ANY(%s)
                  AND (proof.created_at IS NULL OR ae.created_at > proof.created_at)
            ) AS has_access_override
        FROM users u
        LEFT JOIN LATERAL (
            SELECT stripe_customer_id, stripe_subscription_id
            FROM stripe_links
            WHERE telegram_id = u.telegram_id
              AND is_active IS TRUE
              AND stripe_subscription_id IS NOT NULL
            ORDER BY updated_at DESC NULLS LAST, id DESC
            LIMIT 1
        ) sl ON TRUE
        LEFT JOIN LATERAL (
            SELECT stripe_customer_id, stripe_subscription_id
            FROM payment_events
            WHERE telegram_id = u.telegram_id
              AND payment_status = 'succeeded'
              AND stripe_customer_id IS NOT NULL
              AND stripe_subscription_id IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        ) history ON TRUE
        LEFT JOIN LATERAL (
            SELECT stripe_event_id, period_end, created_at
            FROM payment_events pe
            WHERE pe.telegram_id = u.telegram_id
              AND pe.stripe_subscription_id = COALESCE(u.stripe_subscription_id, sl.stripe_subscription_id, history.stripe_subscription_id)
              AND pe.stripe_customer_id = COALESCE(u.stripe_customer_id, sl.stripe_customer_id, history.stripe_customer_id)
              AND pe.event_type = 'invoice.payment_succeeded'
              AND pe.payment_status = 'succeeded'
              AND pe.payment_kind = ANY(%s)
              AND pe.amount_paid > 0
              AND pe.period_end IS NOT NULL
              AND pe.period_end > NOW()
              AND pe.stripe_event_id IS NOT NULL
              AND pe.invoice_id IS NOT NULL
            ORDER BY pe.period_end DESC, pe.id DESC
            LIMIT 1
        ) proof ON TRUE
        WHERE u.stripe_customer_id IS NOT NULL
           OR u.stripe_subscription_id IS NOT NULL
           OR sl.stripe_subscription_id IS NOT NULL
           OR history.stripe_subscription_id IS NOT NULL
        ORDER BY u.telegram_id
        LIMIT %s
        """,
        (list(ACCESS_OVERRIDE_EVENTS), list(TRUSTED_SUBSCRIPTION_PAYMENT_KINDS), bounded_limit),
    )
    keys = (
        "telegram_id", "paid", "expiry_date", "user_customer_id", "user_subscription_id",
        "link_customer_id", "link_subscription_id", "payment_event_id", "payment_period_end",
        "payment_created_at", "has_access_override",
    )
    return [dict(zip(keys, row)) for row in cur.fetchall()]


def classify_candidate(candidate, subscription, now=None):
    now = now or datetime.utcnow()
    local_active = bool(candidate["paid"] and candidate["expiry_date"] and candidate["expiry_date"] > now)
    subscription_id = candidate["user_subscription_id"] or candidate["link_subscription_id"]
    customer_id = candidate["user_customer_id"] or candidate["link_customer_id"]
    if (
        candidate["user_subscription_id"] and candidate["link_subscription_id"]
        and candidate["user_subscription_id"] != candidate["link_subscription_id"]
    ) or (
        candidate["user_customer_id"] and candidate["link_customer_id"]
        and candidate["user_customer_id"] != candidate["link_customer_id"]
    ):
        return "STRIPE_SUBSCRIPTION_IDENTITY_MISMATCH"
    if subscription is None:
        return "LOCAL_ACTIVE_WITH_MISSING_STRIPE_SUBSCRIPTION" if local_active else "CLEAN_MATCH"

    status = str(stripe_field(subscription, "status") or "unknown")
    stripe_customer = stripe_object_id(stripe_field(subscription, "customer"))
    if stripe_customer and customer_id and stripe_customer != customer_id:
        return "STRIPE_SUBSCRIPTION_IDENTITY_MISMATCH"
    live_id = stripe_field(subscription, "id")
    if live_id and subscription_id and live_id != subscription_id:
        return "STRIPE_SUBSCRIPTION_IDENTITY_MISMATCH"
    if status not in KNOWN_STATUSES:
        return "UNKNOWN_EXTERNAL_STATUS"
    if status in ACTIVE_STATUSES and not candidate["paid"]:
        return "STRIPE_ACTIVE_LOCAL_UNPAID"
    if status in TERMINAL_STATUSES and local_active:
        return "STRIPE_TERMINAL_LOCAL_PAID"
    if local_active and not candidate["payment_event_id"] and not candidate["has_access_override"]:
        return "PAYMENT_EVIDENCE_MISMATCH"

    period_end = stripe_field(subscription, "current_period_end")
    if period_end and candidate["expiry_date"] and candidate["payment_event_id"] and not candidate["has_access_override"]:
        live_expiry = datetime.utcfromtimestamp(int(period_end))
        if abs(live_expiry - candidate["expiry_date"]) > EXPIRY_DRIFT_TOLERANCE:
            return "EXPIRY_DRIFT"
    if status in ACTIVE_STATUSES and local_active:
        return "STRIPE_ACTIVE_LOCAL_PAID"
    return "CLEAN_MATCH"


def _is_not_found(error):
    return type(error).__name__ in {"InvalidRequestError", "NotFoundError"} and getattr(error, "http_status", None) == 404


def _is_rate_limit(error):
    return "RateLimit" in type(error).__name__ or getattr(error, "http_status", None) == 429


def _is_auth_error(error):
    return "Authentication" in type(error).__name__ or getattr(error, "http_status", None) in {401, 403}


async def reconcile_candidates(candidates, retrieve_subscription):
    started = time.monotonic()
    results = []
    calls = 0
    partial = False
    for candidate in candidates:
        subscription_id = candidate["user_subscription_id"] or candidate["link_subscription_id"]
        if not subscription_id:
            results.append((candidate, classify_candidate(candidate, None), None))
            continue
        try:
            calls += 1
            subscription = await asyncio.to_thread(retrieve_subscription, subscription_id)
            results.append((candidate, classify_candidate(candidate, subscription), subscription))
        except Exception as error:
            if _is_auth_error(error):
                logging.error("STRIPE_RECONCILE_AUTH_FAILED: error_class=%s", type(error).__name__)
                return {"aborted": True, "partial": True, "results": [], "calls": calls, "duration": time.monotonic() - started}
            if _is_not_found(error):
                results.append((candidate, classify_candidate(candidate, None), None))
                continue
            logging.warning("STRIPE_RECONCILE_LOOKUP_FAILED: error_class=%s", type(error).__name__)
            results.append((candidate, "STRIPE_API_UNAVAILABLE", None))
            if _is_rate_limit(error):
                partial = True
                break
    return {"aborted": False, "partial": partial, "results": results, "calls": calls, "duration": time.monotonic() - started}


def render_reconcile_audit(audit, safe_user_ref, message_limit=3900):
    if audit["aborted"]:
        return ["💳 Stripe reconciliation audit\n\nStripe API unavailable/configuration error. Audit aborted safely.\nread-only: yes"]
    actionable_names = {
        "STRIPE_ACTIVE_LOCAL_UNPAID", "STRIPE_TERMINAL_LOCAL_PAID",
        "LOCAL_ACTIVE_WITH_MISSING_STRIPE_SUBSCRIPTION", "STRIPE_SUBSCRIPTION_IDENTITY_MISMATCH",
        "EXPIRY_DRIFT", "PAYMENT_EVIDENCE_MISMATCH",
    }
    clean_names = {"CLEAN_MATCH", "STRIPE_ACTIVE_LOCAL_PAID"}
    results = audit["results"]
    counts = {
        "clean": sum(issue in clean_names for _, issue, _ in results),
        "actionable": sum(issue in actionable_names for _, issue, _ in results),
        "api": sum(issue == "STRIPE_API_UNAVAILABLE" for _, issue, _ in results),
        "unknown": sum(issue == "UNKNOWN_EXTERNAL_STATUS" for _, issue, _ in results),
    }
    lines = [
        "💳 Stripe reconciliation audit", "", "Summary:", f"- users checked: {len(results)}",
        f"- clean: {counts['clean']}", f"- actionable mismatches: {counts['actionable']}",
        f"- API unavailable/error: {counts['api']}", f"- unknown external statuses: {counts['unknown']}",
        f"- partial: {'yes' if audit['partial'] else 'no'}", f"- Stripe API calls: {audit['calls']}",
        f"- duration: {audit['duration']:.2f}s", "- read-only: yes",
    ]
    for candidate, issue, subscription in results:
        if issue in clean_names:
            continue
        expiry = candidate["expiry_date"]
        status = str(stripe_field(subscription, "status") or "unavailable")
        lines.extend([
            "", f"User ref: {safe_user_ref(str(candidate['telegram_id']))}", f"Issue: {issue}",
            f"Local paid: {bool(candidate['paid'])}",
            f"Local expiry: {expiry.strftime('%Y-%m-%d %H:%M') if expiry else 'none'}",
            f"Stripe status: {status}",
            f"Trusted payment proof: {'yes' if candidate['payment_event_id'] else 'none'}",
            "Action: review only — no automatic change performed",
        ])
    text = "\n".join(lines)
    return [text[i:i + message_limit] for i in range(0, len(text), message_limit)] or [text]
