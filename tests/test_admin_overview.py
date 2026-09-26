import unittest
from datetime import date, datetime

from admin_overview import (
    AdminOverviewQueryError,
    load_admin_overview_supplementary,
    parse_overview_query,
)


class _Cursor:
    def __init__(self):
        self.queries = []
        self._rows = []

    def execute(self, query, params=None):
        self.queries.append((query, params))
        if "WITH bounds AS" in query:
            self._rows = [(date(2026, 9, 25), 2), (date(2026, 9, 26), 0)]
        elif "WITH activity AS" in query:
            self._rows = [
                ("payment", "14", "payment_succeeded", "recurring", 7,
                 "Анна", "anna", datetime(2026, 9, 26, 9, 30)),
                ("class", "booking-1", "class_paid", "Медитация", 8,
                 None, "maria", datetime(2026, 9, 26, 8, 15)),
            ]

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


class _Connection:
    def __init__(self):
        self.cursor_instance = _Cursor()
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class AdminOverviewTests(unittest.TestCase):
    def test_query_is_strictly_bounded(self):
        for days in (7, 30, 90, 365):
            self.assertEqual(parse_overview_query(str(days), "12"), (days, 12))
        for days in ("6", "366", "bad"):
            with self.assertRaises(AdminOverviewQueryError):
                parse_overview_query(days, "12")
        with self.assertRaises(AdminOverviewQueryError):
            parse_overview_query("7", "21")

    def test_loader_is_read_only_and_projects_authoritative_records(self):
        connection = _Connection()
        result = load_admin_overview_supplementary(lambda: connection, 7, 12)
        self.assertEqual(result["metric"], "new_registrations")
        self.assertEqual(result["series"], [
            {"date": "2026-09-25", "value": 2},
            {"date": "2026-09-26", "value": 0},
        ])
        self.assertEqual(result["events"][0]["title"], "Анна · подписка продлена")
        self.assertEqual(result["events"][1]["secondary"], "Медитация")
        self.assertNotIn("telegram_id", result["events"][0])
        self.assertTrue(connection.rolled_back)
        self.assertTrue(connection.closed)
        sql = "\n".join(query for query, _params in connection.cursor_instance.queries)
        self.assertIn("SET TRANSACTION READ ONLY", sql)
        self.assertIn("SET LOCAL statement_timeout = 5000", sql)
        self.assertIn("LIMIT %s", sql)

    def test_activity_identity_is_stable_and_does_not_expose_source_id(self):
        first = load_admin_overview_supplementary(lambda: _Connection(), 7, 12)
        second = load_admin_overview_supplementary(lambda: _Connection(), 7, 12)
        self.assertEqual(first["events"][0]["id"], second["events"][0]["id"])
        self.assertNotEqual(first["events"][0]["id"], "14")
        self.assertEqual(len(first["events"][0]["id"]), 20)


if __name__ == "__main__":
    unittest.main()
