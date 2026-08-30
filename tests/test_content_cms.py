import unittest

from content_cms import (
    ContentCmsError,
    validate_create_payload,
    validate_update_payload,
)


class ContentCmsValidationTests(unittest.TestCase):
    def test_create_is_lesson_only_and_trims_values(self):
        values = validate_create_payload({
            "content_type": "lesson",
            "title": "  Первый   урок  ",
            "category": "main_workout",
            "description": "  Описание  ",
            "duration_seconds": 900,
        })
        self.assertEqual(values["title"], "Первый урок")
        self.assertEqual(values["description"], "Описание")

        with self.assertRaisesRegex(ContentCmsError, "invalid_content_type"):
            validate_create_payload({"content_type": "recipe", "title": "X"})
        with self.assertRaisesRegex(ContentCmsError, "invalid_content_payload"):
            validate_create_payload({
                "content_type": "lesson", "title": "X", "status": "published",
            })

    def test_server_validation_is_bounded(self):
        invalid = (
            ({"content_type": "lesson", "title": ""}, "invalid_title"),
            ({"content_type": "lesson", "title": "x" * 121}, "invalid_title"),
            ({"content_type": "lesson", "title": "X", "description": "x" * 5001}, "invalid_description"),
            ({"content_type": "lesson", "title": "X", "category": "Bad value"}, "invalid_category"),
            ({"content_type": "lesson", "title": "X", "duration_seconds": 0}, "invalid_duration"),
            ({"content_type": "lesson", "title": "X", "duration_seconds": 86401}, "invalid_duration"),
        )
        for payload, category in invalid:
            with self.subTest(category=category), self.assertRaisesRegex(
                ContentCmsError, category
            ):
                validate_create_payload(payload)

    def test_update_requires_version_and_rejects_unknown_fields(self):
        version, values = validate_update_payload({
            "expected_version": 2, "title": " Новое название ", "sort_order": 3,
        })
        self.assertEqual(version, 2)
        self.assertEqual(values, {"title": "Новое название", "sort_order": 3})
        for payload in (
            {"title": "X"},
            {"expected_version": 1, "status": "published"},
            {"expected_version": 1},
        ):
            with self.assertRaises(ContentCmsError):
                validate_update_payload(payload)


if __name__ == "__main__":
    unittest.main()
