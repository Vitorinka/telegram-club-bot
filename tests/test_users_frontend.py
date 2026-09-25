import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_JS = (ROOT / "miniapp" / "app.js").read_text(encoding="utf-8")
INDEX_HTML = (ROOT / "miniapp" / "index.html").read_text(encoding="utf-8")
STYLES_CSS = (ROOT / "miniapp" / "styles.css").read_text(encoding="utf-8")


class UsersFrontendTests(unittest.TestCase):
    def test_users_list_matches_compact_reference_structure(self):
        for marker in (
            'class="users-page-header"', 'class="users-quick-filters"',
            'class="users-search-box"', 'class="users-filter-strip"',
            'data-users-status="active"', 'data-users-status="trial"',
        ):
            self.assertIn(marker, INDEX_HTML)
        self.assertIn("grid-template-rows:auto auto", STYLES_CSS)
        self.assertIn("min-height:74px", STYLES_CSS)
        self.assertIn(".user-card + .user-card", STYLES_CSS)
        self.assertNotIn("online", APP_JS.lower())

    def test_canonical_profile_uses_only_authoritative_metrics_and_events(self):
        self.assertIn('text("small","Текущий доступ")', APP_JS)
        self.assertIn('text("small","Статус")', APP_JS)
        self.assertIn("user.access_history.slice(0,4)", APP_JS)
        for fake_metric in (
            "completion rate", "engagement score", "Просмотров контента",
            "Сообщений", "Дней в клубе",
        ):
            self.assertNotIn(fake_metric, APP_JS)
        self.assertIn("Авторитетных событий доступа пока нет.", APP_JS)

    def test_profile_is_lazy_and_users_search_is_cancelled(self):
        load_users = APP_JS[APP_JS.index("const loadUsers ="):APP_JS.index("const detailCard =")]
        load_profile = APP_JS[APP_JS.index("function loadUserDetails"):APP_JS.index("const subscriptionStateLabels")]
        self.assertIn("new AbortController()", load_users)
        self.assertIn("usersRequestController.abort()", load_users)
        self.assertEqual(load_profile.count("api(`"), 1)
        self.assertNotRegex(load_users, r"loadUserDetails\(")

    def test_mobile_hooks_preserve_full_screen_profile_and_horizontal_controls(self):
        self.assertIn("#user-details { position: fixed", STYLES_CSS)
        self.assertIn(".users-quick-filters { display:flex", STYLES_CSS)
        self.assertIn("overflow-x:auto", STYLES_CSS)
        self.assertIn("@media (max-width: 360px)", STYLES_CSS)
        self.assertIn("@media (min-width: 1024px)", STYLES_CSS)
        self.assertNotIn("body.member-preview-mode) .users-", STYLES_CSS)

    def test_unsupported_message_action_is_explicitly_disabled(self):
        self.assertRegex(
            INDEX_HTML,
            re.compile(r'<button type="button" disabled title="[^"]+">Написать сообщение</button>'),
        )
        self.assertIn('id="user-more-actions"', INDEX_HTML)


if __name__ == "__main__":
    unittest.main()
