import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HTML = (ROOT / "miniapp" / "index.html").read_text(encoding="utf-8")
JS = (ROOT / "miniapp" / "app.js").read_text(encoding="utf-8")
CSS = (ROOT / "miniapp" / "styles.css").read_text(encoding="utf-8")


class MobileAdminOverviewTests(unittest.TestCase):
    def test_mobile_header_uses_confirmed_identity_and_canonical_badge(self):
        self.assertIn('id="mobile-dashboard-greeting"', HTML)
        self.assertIn('id="mobile-overview-notifications"', HTML)
        self.assertIn('"mobile-overview-attention"', JS)
        self.assertIn('document.getElementById("mobile-dashboard-greeting").textContent', JS)
        self.assertNotIn("Добрый день, Виктория", HTML + JS)
        self.assertIn("#admin-hero { display:none; }", CSS)

    def test_mobile_kpis_use_dashboard_fields_without_fake_revenue(self):
        for metric in (
            "users.active_access", "users.total", "users.trial",
            "billing.auto_renew",
        ):
            self.assertIn(f'data-metric="{metric}"', HTML)
        mobile = HTML[HTML.index('class="mobile-admin-overview"'):HTML.index('class="admin-dashboard-greeting"')]
        self.assertNotIn("Доход", mobile)
        self.assertNotIn("€", mobile)

    def test_first_paint_remains_dashboard_only_and_secondary_work_is_deferred(self):
        dashboard = JS[JS.index("const loadDashboard ="):JS.index("const userPrimaryStatus =")]
        self.assertEqual(dashboard.count('api("/api/admin/dashboard")'), 1)
        self.assertNotIn("/api/admin/classes", dashboard)
        bootstrap = JS[JS.index('fetch("/api/admin/session"'):]
        deferred = bootstrap.index("window.setTimeout")
        self.assertIn('api("/api/admin/classes?limit=20")', bootstrap[deferred:])
        self.assertGreater(bootstrap.index('api("/api/admin/classes?limit=20")'), deferred)

    def test_mobile_navigation_is_one_row_with_five_canonical_destinations(self):
        self.assertIn("grid-template-columns:repeat(5,minmax(0,1fr))", CSS)
        for destination in ("overview", "users", "content", "subscriptions", "analytics"):
            self.assertIn(f'[data-nav="{destination}"]', CSS)
        self.assertIn('#bottom-nav > #nav-notifications', CSS)
        self.assertIn('#bottom-nav > .admin-more-nav', CSS)

    def test_mobile_old_placeholder_panels_are_hidden_and_quick_actions_are_real(self):
        self.assertIn("#dashboard > .admin-dashboard-grid { display:none; }", CSS)
        self.assertIn('id="mobile-create-class"', HTML)
        self.assertIn('id="mobile-create-content"', HTML)
        self.assertIn('document.getElementById("content-create-open").click()', JS)
        self.assertIn("loadSchedule(false).then", JS)

    def test_no_fake_analytics_or_activity_feed_is_added(self):
        mobile = HTML[HTML.index('class="mobile-admin-overview"'):HTML.index('class="admin-dashboard-greeting"')]
        self.assertNotIn("canvas", mobile)
        self.assertNotIn("Последние события", mobile)
        self.assertNotIn("Динамика за 7 дней", mobile)

    def test_responsive_hooks_cover_reference_widths_and_safe_area(self):
        self.assertIn("@media (max-width: 360px)", CSS)
        self.assertIn("@media (max-width: 1023px)", CSS)
        self.assertIn("env(safe-area-inset-bottom)", CSS)
        self.assertIn("grid-template-columns:minmax(0,1fr) 44px", CSS)
        self.assertIn("overflow-x:auto", CSS)


if __name__ == "__main__":
    unittest.main()
