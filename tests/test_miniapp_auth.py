import hashlib
import hmac
import json
import unittest
from unittest import mock
from urllib.parse import urlencode

import miniapp_auth


BOT_TOKEN = "123456:miniapp-test-token"
NOW = 1_787_250_000


def signed_init_data(user=None, auth_date=NOW, token=BOT_TOKEN, extra=None):
    fields = {
        "auth_date": str(auth_date),
        "query_id": "test-query",
    }
    if user is not None:
        fields["user"] = json.dumps(user, separators=(",", ":"), ensure_ascii=False)
    fields.update(extra or {})
    data_check_string = "\n".join(
        f"{key}={value}" for key, value in sorted(fields.items())
    )
    secret_key = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    fields["hash"] = hmac.new(
        secret_key, data_check_string.encode(), hashlib.sha256
    ).hexdigest()
    return urlencode(fields)


class MiniAppAuthTests(unittest.TestCase):
    def validate(self, raw, admin_ids=(42,), **kwargs):
        return miniapp_auth.validate_telegram_init_data(
            raw, BOT_TOKEN, admin_ids, now=NOW, **kwargs
        )

    def assert_auth_error(self, category, raw, admin_ids=(42,)):
        with self.assertRaises(miniapp_auth.MiniAppAuthError) as ctx:
            self.validate(raw, admin_ids=admin_ids)
        self.assertEqual(ctx.exception.category, category)
        return ctx.exception

    def test_valid_init_data_for_admin_is_accepted(self):
        identity = self.validate(signed_init_data({"id": 42, "username": "admin"}))
        self.assertEqual(identity.telegram_id, 42)
        self.assertEqual(identity.username, "admin")

    def test_valid_non_admin_is_forbidden(self):
        error = self.assert_auth_error(
            "admin_forbidden", signed_init_data({"id": 99}), admin_ids=(42,)
        )
        self.assertEqual(error.status, 403)

    def test_bad_hash_is_unauthorized(self):
        raw = signed_init_data({"id": 42})
        self.assert_auth_error("hash_mismatch", raw.replace("query_id=test-query", "query_id=other"))

    def test_missing_hash_is_unauthorized(self):
        self.assert_auth_error("hash_missing_or_invalid", "auth_date=1787250000&user=%7B%22id%22%3A42%7D")

    def test_malformed_auth_date_is_unauthorized(self):
        self.assert_auth_error(
            "auth_date_invalid", signed_init_data({"id": 42}, auth_date="not-a-time")
        )

    def test_stale_auth_date_is_unauthorized(self):
        self.assert_auth_error(
            "auth_date_stale", signed_init_data({"id": 42}, auth_date=NOW - 301)
        )

    def test_future_auth_date_is_unauthorized(self):
        self.assert_auth_error(
            "auth_date_future", signed_init_data({"id": 42}, auth_date=NOW + 1)
        )

    def test_malformed_user_json_is_unauthorized(self):
        self.assert_auth_error(
            "user_invalid", signed_init_data(None, extra={"user": "not-json"})
        )

    def test_missing_user_is_unauthorized(self):
        self.assert_auth_error("user_invalid", signed_init_data(None))

    def test_user_id_must_be_integer(self):
        self.assert_auth_error("user_id_invalid", signed_init_data({"id": "42"}))

    def test_hash_comparison_uses_constant_time_compare(self):
        raw = signed_init_data({"id": 42})
        with mock.patch.object(
            miniapp_auth.hmac,
            "compare_digest",
            wraps=hmac.compare_digest,
        ) as compare:
            self.validate(raw)
        compare.assert_called_once()

    def test_authorization_contract_rejects_missing_and_wrong_scheme(self):
        for value in (None, "", "Bearer value", "tma", "tma "):
            with self.subTest(value=value), self.assertRaises(miniapp_auth.MiniAppAuthError):
                miniapp_auth.parse_miniapp_authorization(value)

    def test_authorization_contract_returns_raw_init_data(self):
        raw = signed_init_data({"id": 42})
        self.assertEqual(miniapp_auth.parse_miniapp_authorization(f"tma {raw}"), raw)


if __name__ == "__main__":
    unittest.main()
