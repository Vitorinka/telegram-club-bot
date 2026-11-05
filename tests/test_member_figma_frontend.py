import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class MemberFigmaFrontendTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (ROOT / "miniapp" / "index.html").read_text(encoding="utf-8")
        cls.css = (ROOT / "miniapp" / "styles.css").read_text(encoding="utf-8")
        cls.js = (ROOT / "miniapp" / "app.js").read_text(encoding="utf-8")

    def test_member_core_keeps_existing_real_api_and_dom_contracts(self):
        for element_id in (
            "member-home", "member-library", "member-lesson", "member-training-list",
            "member-library-search", "member-library-chips", "member-bottom-nav",
        ):
            self.assertIn(f'id="{element_id}"', self.html)
        self.assertIn('"/api/member/home"', self.js)
        self.assertIn('"/api/member/content?content_type=lesson&limit=50"', self.js)
        self.assertIn('`/api/member/content/${encodeURIComponent(contentId)}`', self.js)

    def test_lesson_video_request_is_deferred_until_explicit_start(self):
        start = self.js.index('const startVideo = text("button", "Начать тренировку →"')
        listener = self.js.index('startVideo.addEventListener("click"', start)
        media_request = self.js.index('/media/${encodeURIComponent(item.video_media_id)}/url', listener)
        self.assertLess(start, listener)
        self.assertLess(listener, media_request)
        self.assertIn('{once: true}', self.js[listener:media_request + 3000])

    def test_figma_member_tokens_and_responsive_breakpoints_are_scoped(self):
        self.assertIn("/* Figma member core: nodes 136:1077, 155:881, 155:914 and 155:930. */", self.css)
        self.assertIn("body.member-preview-mode", self.css)
        self.assertIn("--member-teal-dark: #004d52", self.css)
        self.assertIn("--member-teal: #007d7a", self.css)
        self.assertIn("@media (min-width: 760px)", self.css)
        self.assertIn("@media (max-width: 360px)", self.css)

    def test_local_preview_is_explicit_and_loopback_only(self):
        self.assertIn('["127.0.0.1", "localhost"].includes(window.location.hostname)', self.js)
        self.assertIn('get("preview") === "member"', self.js)
        self.assertIn('Локальный preview · тестовые данные', self.js)
        self.assertIn('if (localMemberPreview)', self.js)

    def test_ui_v3_uses_approved_home_and_desktop_layout_contract(self):
        self.assertIn("approved Figma nodes 234:2081, 234:1999, 235:246 and 235:182", self.css)
        for element_id in (
            "member-layout-toggle", "member-side-nav", "member-resume-section",
            "member-direction-grid", "member-next-zoom-title", "member-hero-photo",
        ):
            self.assertIn(f'id="{element_id}"', self.html)
        self.assertIn("member-desktop-compact", self.js)
        self.assertIn("member-desktop-expanded", self.js)
        self.assertNotIn("requestFullscreen", self.js[self.js.index("const applyMemberDesktopLayout"):self.js.index("const api =")])

    def test_ui_v3_navigation_has_vector_icons_and_preserves_member_routes(self):
        for icon in ("home", "library", "calendar", "profile"):
            self.assertIn(f'data-member-icon="{icon}"', self.html)
        self.assertIn('id="member-open-meditations"', self.html)
        self.assertIn('id="member-open-recipes"', self.html)
        self.assertIn('id="member-open-zoom"', self.html)
        self.assertIn("loadMemberMeditations()", self.js)
        self.assertIn("loadMemberRecipes()", self.js)
        self.assertIn("loadMemberSchedule()", self.js)

    def test_ui_v3_static_assets_are_local_and_nonempty(self):
        for name in ("hero.jpg", "meditation.jpg", "recipe.jpg"):
            asset = ROOT / "miniapp" / "assets" / "member-v3" / name
            self.assertTrue(asset.is_file())
            self.assertGreater(asset.stat().st_size, 10_000)
            self.assertIn(f"/miniapp/assets/member-v3/{name}", self.css + self.js)

    def test_preview_progress_is_not_used_as_production_member_data(self):
        self.assertIn('"60%"', self.js)
        self.assertIn("if (localMemberPreview && regularLessons[0])", self.js)
        self.assertIn('document.getElementById("member-resume-section").hidden = true', self.js)


if __name__ == "__main__":
    unittest.main()
