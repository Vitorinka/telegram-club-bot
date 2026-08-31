import unittest

from content_cms import ContentCmsError, validate_create_payload
from content_media import media_allowed_for_content
from recipe_cms import MAX_INGREDIENTS, MAX_STEPS, validate_recipe_payload


class RecipeCmsValidationTests(unittest.TestCase):
    def test_recipe_draft_payload_uses_existing_content_validation(self):
        result = validate_create_payload({
            "content_type": "recipe", "title": "Овощной суп",
            "category": "soups", "description": "Простой рецепт",
            "duration_seconds": 1800,
        })
        self.assertEqual(result["content_type"], "recipe")
        with self.assertRaisesRegex(ContentCmsError, "invalid_content_payload"):
            validate_create_payload({
                "content_type": "recipe", "title": "X", "status": "published",
            })

    def test_structured_recipe_validation_and_unknown_fields(self):
        version, ingredients, steps = validate_recipe_payload({
            "expected_version": 2,
            "ingredients": [
                {"name": " Томаты ", "amount": " 2 шт. ", "sort_order": 0},
                {"name": "Соль", "amount": None, "sort_order": 1},
            ],
            "steps": [{"step_number": 1, "instruction": " Нарезать овощи. "}],
        })
        self.assertEqual(version, 2)
        self.assertEqual(ingredients[0]["name"], "Томаты")
        self.assertEqual(ingredients[0]["amount"], "2 шт.")
        self.assertEqual(steps[0]["instruction"], "Нарезать овощи.")
        with self.assertRaisesRegex(ContentCmsError, "invalid_recipe_payload"):
            validate_recipe_payload({
                "expected_version": 1, "ingredients": [], "steps": [],
                "calories": 100,
            })

    def test_bounds_duplicates_and_text_validation(self):
        invalid = (
            {"expected_version": 1, "ingredients": [{}] * (MAX_INGREDIENTS + 1), "steps": []},
            {"expected_version": 1, "ingredients": [], "steps": [{}] * (MAX_STEPS + 1)},
            {"expected_version": 1, "ingredients": [{"name": "x" * 201, "amount": None, "sort_order": 0}], "steps": []},
            {"expected_version": 1, "ingredients": [{"name": "X", "amount": "x" * 101, "sort_order": 0}], "steps": []},
            {"expected_version": 1, "ingredients": [], "steps": [{"step_number": 1, "instruction": "x" * 2001}]},
            {"expected_version": 1, "ingredients": [{"name": "A", "amount": None, "sort_order": 0}, {"name": "B", "amount": None, "sort_order": 0}], "steps": []},
        )
        for payload in invalid:
            with self.assertRaises(ContentCmsError):
                validate_recipe_payload(payload)

    def test_recipe_media_is_cover_only(self):
        self.assertTrue(media_allowed_for_content("recipe", "cover"))
        self.assertFalse(media_allowed_for_content("recipe", "video"))
        self.assertTrue(media_allowed_for_content("lesson", "video"))
        self.assertTrue(media_allowed_for_content("meditation", "video"))


if __name__ == "__main__":
    unittest.main()
