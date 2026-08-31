import unittest

from content_cms import ContentCmsError, validate_create_payload
from content_media import media_allowed_for_content
from nutrition_cms import MAX_NUTRITION_BODY_LENGTH, normalize_nutrition_body


class NutritionCmsTests(unittest.TestCase):
    def test_type_duration_and_body_validation(self):
        values = validate_create_payload({
            "content_type": "nutrition_material", "title": "Водный баланс",
            "category": "habits", "description": "Кратко", "duration_seconds": None,
        })
        self.assertEqual(values["content_type"], "nutrition_material")
        with self.assertRaisesRegex(ContentCmsError, "invalid_nutrition_material"):
            validate_create_payload({
                "content_type": "nutrition_material", "title": "X",
                "duration_seconds": 60,
            })
        self.assertEqual(normalize_nutrition_body(" Первый абзац  \r\n\r\nВторой\t \r\n"), "Первый абзац\n\nВторой")
        with self.assertRaisesRegex(ContentCmsError, "invalid_nutrition_body"):
            normalize_nutrition_body("x" * (MAX_NUTRITION_BODY_LENGTH + 1))

    def test_media_is_cover_only(self):
        self.assertTrue(media_allowed_for_content("nutrition_material", "cover"))
        self.assertFalse(media_allowed_for_content("nutrition_material", "video"))


if __name__ == "__main__":
    unittest.main()
