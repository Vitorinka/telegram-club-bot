import unittest

from admin_content import (
    AdminContentQueryError,
    get_admin_content,
    list_admin_content,
)


class AdminContentTests(unittest.TestCase):
    def test_catalog_contains_only_real_safe_metadata(self):
        result = list_admin_content(free_lesson_configured=True)

        self.assertTrue(result["read_only"])
        self.assertEqual(len(result["items"]), 6)
        self.assertEqual(result["summary"]["total"], 6)
        self.assertEqual(
            {item["content_id"] for item in result["items"]},
            {
                "onboarding-welcome", "onboarding-club-overview",
                "onboarding-faq", "club-rules", "free-lesson",
                "free-lesson-followup",
            },
        )
        serialized = repr(result).casefold()
        for forbidden in (
            "file_id", "bot_token", "private_path", "stripe", "payload",
        ):
            self.assertNotIn(forbidden, serialized)
        for item in result["items"]:
            self.assertIsNone(item["published"])
            self.assertIsNone(item["active"])
            self.assertIsNone(item["created_at"])
            self.assertIsNone(item["updated_at"])

    def test_filter_search_and_details_are_read_only(self):
        filtered = list_admin_content(
            free_lesson_configured=True,
            category="free_materials",
            query="БЕСПЛАТНЫЙ",
        )
        self.assertEqual(
            [item["content_id"] for item in filtered["items"]],
            ["free-lesson"],
        )
        details = get_admin_content(
            "free-lesson-followup", free_lesson_configured=True
        )
        self.assertEqual(details["media_type"], "text")
        self.assertFalse(details["has_media"])
        self.assertTrue(details["read_only"])
        self.assertIsNone(get_admin_content("unknown", free_lesson_configured=True))

    def test_missing_free_lesson_media_is_visible_without_exposing_value(self):
        details = get_admin_content(
            "free-lesson", free_lesson_configured=False
        )
        self.assertFalse(details["has_media"])
        self.assertEqual(details["availability"], "missing_media")

    def test_invalid_filters_fail_closed(self):
        with self.assertRaisesRegex(AdminContentQueryError, "invalid_category"):
            list_admin_content(
                free_lesson_configured=True, category="recipes"
            )
        with self.assertRaisesRegex(AdminContentQueryError, "query_too_long"):
            list_admin_content(
                free_lesson_configured=True, query="x" * 81
            )


if __name__ == "__main__":
    unittest.main()
