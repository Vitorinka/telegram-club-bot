import unittest
from datetime import datetime

from admin_notification_acknowledgements import (
    AdminNotificationAcknowledgementError,
    acknowledge_admin_notification,
    list_admin_notification_acknowledgements,
    validate_notification_key,
)


class FakeCursor:
    def __init__(self, database):
        self.database = database
        self.rows = []
        self.fetchone_row = None
        self.queries = []

    def execute(self, query, params=()):
        self.queries.append((query, params))
        if "INSERT INTO admin_notification_acknowledgements" in query:
            admin_id, key = params
            self.database.setdefault((admin_id, key), datetime(2026, 9, 19, 12, 0))
            self.fetchone_row = (self.database[(admin_id, key)],)
        elif "SELECT notification_key" in query:
            admin_id = params[0]
            self.rows = [(key,) for owner, key in sorted(self.database) if owner == admin_id]

    def fetchone(self): return self.fetchone_row
    def fetchall(self): return list(self.rows)
    def close(self): pass


class FakeConnection:
    def __init__(self, database):
        self.cursor_object = FakeCursor(database)
        self.commits = 0
        self.rollbacks = 0
    def cursor(self): return self.cursor_object
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1
    def close(self): pass


class AdminNotificationAcknowledgementTests(unittest.TestCase):
    def setUp(self):
        self.database = {}
        self.connections = []

    def get_connection(self):
        connection = FakeConnection(self.database)
        self.connections.append(connection)
        return connection

    def test_mark_is_idempotent_and_scoped_per_admin(self):
        first = acknowledge_admin_notification(self.get_connection, 10, "failed:operation-1")
        duplicate = acknowledge_admin_notification(self.get_connection, 10, "failed:operation-1")
        acknowledge_admin_notification(self.get_connection, 20, "failed:operation-1")
        self.assertEqual(first, duplicate)
        self.assertEqual(len(self.database), 2)
        self.assertEqual(
            list_admin_notification_acknowledgements(self.get_connection, 10),
            {"notification_keys": ["failed:operation-1"]},
        )
        self.assertEqual(
            list_admin_notification_acknowledgements(self.get_connection, 20),
            {"notification_keys": ["failed:operation-1"]},
        )

    def test_notification_key_validation_is_bounded_and_structured(self):
        for invalid in (None, "", "no-prefix", "failed:", " failed:one ", "failed:" + "x" * 200):
            with self.subTest(invalid=invalid), self.assertRaises(AdminNotificationAcknowledgementError):
                validate_notification_key(invalid)
        self.assertEqual(validate_notification_key("delivery:delivery_key-1"), "delivery:delivery_key-1")


if __name__ == "__main__":
    unittest.main()
