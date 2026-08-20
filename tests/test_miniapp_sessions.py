import hashlib
import unittest
from unittest import mock

import miniapp_sessions


class MiniAppSessionTests(unittest.TestCase):
    def test_bearer_contract(self):
        self.assertEqual(
            miniapp_sessions.parse_bearer_authorization("Bearer opaque-token"),
            "opaque-token",
        )
        for invalid in (None, "", "tma data", "Bearer", "Bearer ", "Bearer a b"):
            with self.subTest(invalid=invalid), self.assertRaises(
                miniapp_sessions.MiniAppSessionError
            ):
                miniapp_sessions.parse_bearer_authorization(invalid)

    def test_token_hash_is_one_way_sha256(self):
        token = "opaque-session-token"
        digest = miniapp_sessions.hash_session_token(token)
        self.assertEqual(digest, hashlib.sha256(token.encode()).hexdigest())
        self.assertNotEqual(token, digest)

    def test_token_generator_requests_256_bits_of_entropy(self):
        connection = mock.Mock()
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = [mock.Mock()]
        with mock.patch.object(
            miniapp_sessions.secrets,
            "token_urlsafe",
            return_value="generated-opaque-token",
        ) as generator:
            token, _session = miniapp_sessions.create_miniapp_admin_session(
                lambda: connection, 42
            )
        self.assertEqual(token, "generated-opaque-token")
        generator.assert_called_once_with(32)
        insert_call = next(
            call for call in cursor.execute.call_args_list
            if "INSERT INTO miniapp_admin_sessions" in call.args[0]
        )
        insert_params = insert_call.args[1]
        self.assertNotIn(token, insert_params)
        self.assertIn(miniapp_sessions.hash_session_token(token), insert_params)


if __name__ == "__main__":
    unittest.main()
