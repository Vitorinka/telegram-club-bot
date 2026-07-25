import hashlib
import logging
import os
from datetime import datetime

import stripe

from stripe_invoice_rules import redact_identifier


STRIPE_WEBHOOK_SECRET_PREFIX = "wh" + "sec_"
RAILWAY_DIAGNOSTIC_ENV_KEYS = (
    "RAILWAY_ENVIRONMENT",
    "RAILWAY_ENVIRONMENT_NAME",
    "RAILWAY_SERVICE_NAME",
    "RAILWAY_PROJECT_NAME",
    "RAILWAY_DEPLOYMENT_ID",
    "RAILWAY_REPLICA_ID",
)


def stripe_signature_error_class():
    return getattr(getattr(stripe, "error", None), "SignatureVerificationError", Exception)


def secret_fingerprint(secret):
    if not secret:
        return None
    return hashlib.sha256(str(secret)[:12].encode("utf-8")).hexdigest()[:12]


def webhook_secret_diagnostics(secret):
    raw = secret or ""
    stripped = raw.strip()
    return {
        "secret_length": len(raw),
        "secret_starts_whsec": raw.startswith(STRIPE_WEBHOOK_SECRET_PREFIX),
        "secret_strip_differs": raw != stripped,
        "secret_stripped_length": len(stripped),
        "secret_stripped_starts_whsec": stripped.startswith(STRIPE_WEBHOOK_SECRET_PREFIX),
        "secret_prefix12_sha256": secret_fingerprint(raw),
    }


def stripe_signature_timestamp(sig_header):
    if not sig_header:
        return None
    for part in str(sig_header).split(","):
        key, sep, value = part.partition("=")
        if sep and key == "t":
            return value or None
    return None


def railway_diagnostics(env=None):
    env = env or os.environ
    return {key: env.get(key) for key in RAILWAY_DIAGNOSTIC_ENV_KEYS if env.get(key)}


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


def stripe_event_created_at(created):
    if created in (None, ""):
        return None
    try:
        return datetime.utcfromtimestamp(int(created))
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def normalize_stripe_event(event):
    event_object = stripe_value(event, "data", "object")
    return {
        "event_id": stripe_value(event, "id"),
        "event_type": stripe_value(event, "type"),
        "event_created_at": stripe_event_created_at(stripe_value(event, "created")),
        "event_object": event_object,
        "object_id": stripe_value(event_object, "id"),
    }


def require_normalized_stripe_event(normalized_event):
    event_id = normalized_event.get("event_id")
    event_type = normalized_event.get("event_type")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("Stripe event id missing")
    if not isinstance(event_type, str) or not event_type.strip():
        raise ValueError("Stripe event type missing")

    normalized_event = dict(normalized_event)
    normalized_event["event_id"] = event_id.strip()
    normalized_event["event_type"] = event_type.strip()
    return normalized_event


def safe_log_identifier(value):
    return redact_identifier(value) or "нет"


async def claim_normalized_stripe_event(
    claim_event_processing,
    release_event_processing,
    event_id,
    *,
    event_created_at=None,
    event_type=None,
    object_id=None,
):
    try:
        return await claim_event_processing(
            event_id,
            event_created_at=event_created_at,
            event_type=event_type,
            object_id=object_id,
        )
    except Exception:
        try:
            await release_event_processing(event_id)
        except Exception as release_error:
            logging.exception(
                "Stripe webhook event release after claim failure also failed: event_id=%s, error=%s",
                safe_log_identifier(event_id),
                release_error,
            )
        raise


def stripe_webhook_diagnostics(request, payload, sig_header, webhook_secret, env=None):
    headers = getattr(request, "headers", {}) or {}
    return {
        "path": getattr(request, "path", None),
        "host": getattr(request, "host", None),
        "content_type": headers.get("Content-Type") if hasattr(headers, "get") else None,
        "payload_bytes": len(payload or b""),
        "signature_present": bool(sig_header),
        "signature_timestamp": stripe_signature_timestamp(sig_header),
        "webhook_secret_configured": bool(webhook_secret),
        **webhook_secret_diagnostics(webhook_secret),
        **railway_diagnostics(env),
    }


def construct_verified_stripe_event(payload, sig_header, webhook_secret):
    if not webhook_secret:
        raise ValueError("STRIPE_WEBHOOK_SECRET is not configured")
    if not sig_header:
        raise LookupError("Stripe-Signature header is missing")
    return stripe.Webhook.construct_event(payload, sig_header, webhook_secret.strip())
