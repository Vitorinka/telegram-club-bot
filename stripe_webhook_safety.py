import hashlib
import os

import stripe


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
