import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class PerformancePhase3Tests(unittest.TestCase):
    def test_db_telemetry_separates_pool_wait_from_sql(self):
        source = (ROOT / "main.py").read_text(encoding="utf-8")
        marker = source[source.index('"MINIAPP_API_PERF method='):]
        for field in (
            "auth_pool_wait_ms=%.1f", "auth_sql_ms=%.1f",
            "pool_wait_ms=%.1f", "sql_ms=%.1f", "db_total_ms=%.1f",
            "query_count=%s",
        ):
            self.assertIn(field, marker)
        cursor = source[source.index("class TrackedDbCursor"):source.index("def get_db_pool")]
        acquire = source[source.index("def get_db_conn"):source.index("async def run_sync_db")]
        self.assertIn('metrics["sql_ms"]', cursor)
        self.assertNotIn('metrics["pool_wait_ms"]', cursor)
        self.assertIn('metrics["pool_wait_ms"]', acquire)

    def test_initial_admin_render_is_staged_and_heavy_screens_are_on_demand(self):
        source = (ROOT / "miniapp" / "app.js").read_text(encoding="utf-8")
        dashboard = source[source.index("const loadDashboard ="):source.index("const addBadges =")]
        self.assertEqual(dashboard.count('api("/api/admin/dashboard")'), 1)
        for path in (
            "/api/admin/content/cms", "/api/admin/users", "/api/admin/schedule",
            "/api/admin/system", "/api/admin/deliveries",
        ):
            self.assertNotIn(path, dashboard)
        bootstrap = source[source.index('fetch("/api/admin/session"'):]
        dashboard_call = bootstrap.index("return loadDashboard().then")
        deferred_attention = bootstrap.index("window.setTimeout", dashboard_call)
        self.assertGreater(deferred_attention, dashboard_call)
        self.assertIn("loadAdminNotificationAcknowledgements()", bootstrap[deferred_attention:])
        self.assertIn(".then(refreshAttentionCount)", bootstrap[deferred_attention:])

    def test_dashboard_aggregates_use_one_business_query(self):
        source = (ROOT / "admin_dashboard.py").read_text(encoding="utf-8")
        function = source[source.index("def collect_admin_dashboard"):]
        self.assertEqual(function.count("cur.execute("), 3)
        self.assertIn("cur.execute(DASHBOARD_METRICS_SQL)", function)


if __name__ == "__main__":
    unittest.main()
