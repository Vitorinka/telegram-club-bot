import unittest

from admin_dashboard import collect_admin_dashboard


class DashboardCursor:
    def __init__(self):
        self.queries = []
        self.rows = iter([
            (10, 6, 2, 2, 1, 1, 4, 1, 3, 2),
            (1, 2, 3),
            (4, 1, 2, 1, 8),
            (17, "0017_miniapp_admin_sessions"),
            (1, 2, 1),
        ])

    def execute(self, query, params=None):
        self.queries.append((query, params))

    def fetchone(self):
        return next(self.rows)

    def close(self):
        pass


class DashboardConnection:
    def __init__(self):
        self.cursor_obj = DashboardCursor()
        self.rollbacks = 0
        self.closed = 0

    def cursor(self):
        return self.cursor_obj

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed += 1


class AdminDashboardTests(unittest.TestCase):
    def test_dashboard_is_read_only_bounded_and_aggregate_only(self):
        conn = DashboardConnection()
        dashboard = collect_admin_dashboard(
            lambda: conn,
            lambda: {"pool_used": 2, "pool_available": 3, "private": "excluded"},
            9,
        )

        sql = "\n".join(query for query, _params in conn.cursor_obj.queries)
        self.assertIn("SET TRANSACTION READ ONLY", sql)
        self.assertIn("SET LOCAL statement_timeout", sql)
        for mutation in ("INSERT ", "UPDATE ", "DELETE ", "ALTER ", "SELECT *"):
            self.assertNotIn(mutation, sql.upper())
        self.assertEqual(conn.rollbacks, 1)
        self.assertEqual(conn.closed, 1)
        self.assertEqual(dashboard["users"]["active_access"], 6)
        self.assertEqual(dashboard["billing"]["expired_grace"], 1)
        self.assertEqual(dashboard["access"]["retryable_removals"], 2)
        self.assertEqual(dashboard["deliveries"]["permanently_failed"], 1)
        self.assertEqual(dashboard["system"]["migrations"]["count"], 17)
        self.assertEqual(dashboard["system"]["scheduler"]["known_jobs"], 9)
        self.assertNotIn("private", dashboard["system"]["db_pool"])


if __name__ == "__main__":
    unittest.main()
