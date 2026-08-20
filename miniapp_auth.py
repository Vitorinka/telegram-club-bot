import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from functools import wraps
from urllib.parse import parse_qsl


MINIAPP_AUTH_MAX_AGE_SECONDS = 300
MINIAPP_INIT_DATA_MAX_BYTES = 16384


@dataclass(frozen=True)
class MiniAppIdentity:
    telegram_id: int
    username: str | None = None
    first_name: str | None = None


class MiniAppAuthError(Exception):
    def __init__(self, category, status=401):
        super().__init__(category)
        self.category = str(category)
        self.status = int(status)


def miniapp_auth_error_reference(category):
    digest = hashlib.sha256(f"miniapp-auth:{category}".encode("utf-8")).hexdigest()[:12]
    return f"miniapp_auth:{digest}"


def parse_miniapp_authorization(value):
    if not isinstance(value, str):
        raise MiniAppAuthError("authorization_missing")
    scheme, separator, raw_init_data = value.partition(" ")
    if not separator or scheme.lower() != "tma" or not raw_init_data:
        raise MiniAppAuthError("authorization_scheme_invalid")
    return raw_init_data


def validate_telegram_init_data(
    raw_init_data,
    bot_token,
    admin_ids,
    *,
    now=None,
    max_age_seconds=MINIAPP_AUTH_MAX_AGE_SECONDS,
):
    if not isinstance(raw_init_data, str) or not raw_init_data:
        raise MiniAppAuthError("init_data_missing")
    if len(raw_init_data.encode("utf-8")) > MINIAPP_INIT_DATA_MAX_BYTES:
        raise MiniAppAuthError("init_data_too_large")
    if not isinstance(bot_token, str) or not bot_token:
        raise MiniAppAuthError("server_auth_unavailable")

    try:
        pairs = parse_qsl(raw_init_data, keep_blank_values=True, strict_parsing=True)
    except (TypeError, ValueError):
        raise MiniAppAuthError("init_data_malformed") from None
    keys = [key for key, _value in pairs]
    if len(keys) != len(set(keys)):
        raise MiniAppAuthError("init_data_duplicate_field")
    fields = dict(pairs)

    received_hash = fields.pop("hash", None)
    if (
        not isinstance(received_hash, str)
        or len(received_hash) != 64
        or any(char not in "0123456789abcdefABCDEF" for char in received_hash)
    ):
        raise MiniAppAuthError("hash_missing_or_invalid")

    data_check_string = "\n".join(
        f"{key}={value}" for key, value in sorted(fields.items())
    )
    secret_key = hmac.new(
        b"WebAppData",
        bot_token.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    expected_hash = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected_hash, received_hash.lower()):
        raise MiniAppAuthError("hash_mismatch")

    try:
        auth_date = int(fields["auth_date"])
    except (KeyError, TypeError, ValueError):
        raise MiniAppAuthError("auth_date_invalid") from None
    current_time = int(time.time() if now is None else now)
    if auth_date > current_time:
        raise MiniAppAuthError("auth_date_future")
    if current_time - auth_date > int(max_age_seconds):
        raise MiniAppAuthError("auth_date_stale")

    try:
        user = json.loads(fields["user"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        raise MiniAppAuthError("user_invalid") from None
    if not isinstance(user, dict):
        raise MiniAppAuthError("user_invalid")
    telegram_id = user.get("id")
    if isinstance(telegram_id, bool) or not isinstance(telegram_id, int):
        raise MiniAppAuthError("user_id_invalid")
    if telegram_id not in {int(admin_id) for admin_id in admin_ids or ()}:
        raise MiniAppAuthError("admin_forbidden", status=403)

    username = user.get("username")
    first_name = user.get("first_name")
    return MiniAppIdentity(
        telegram_id=telegram_id,
        username=username if isinstance(username, str) else None,
        first_name=first_name if isinstance(first_name, str) else None,
    )


def require_admin_miniapp(bot_token, admin_ids, on_error=None, error_response=None):
    def decorator(handler):
        @wraps(handler)
        async def wrapped(request, *args, **kwargs):
            try:
                raw_init_data = parse_miniapp_authorization(
                    request.headers.get("Authorization")
                )
                request["miniapp_identity"] = validate_telegram_init_data(
                    raw_init_data,
                    bot_token,
                    admin_ids,
                )
            except MiniAppAuthError as error:
                if on_error is not None:
                    on_error(error.category, miniapp_auth_error_reference(error.category))
                if error_response is None:
                    raise
                return error_response(error.status)
            return await handler(request, *args, **kwargs)

        return wrapped

    return decorator
