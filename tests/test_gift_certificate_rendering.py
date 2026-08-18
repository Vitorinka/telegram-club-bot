import tempfile
import unittest
from pathlib import Path

from PIL import Image, ImageChops

from gift_certificate import (
    CERTIFICATE_FONT_PATH,
    CERTIFICATE_NAME_BOX,
    CERTIFICATE_NAME_MAX_LENGTH,
    CERTIFICATE_TEMPLATE_PATHS,
    CertificateNameError,
    normalize_certificate_name,
    render_gift_certificate,
    validate_certificate_name_fits,
)


class GiftCertificateRenderingTests(unittest.TestCase):
    def test_canonical_assets_and_cyrillic_font_exist(self):
        self.assertEqual(set(CERTIFICATE_TEMPLATE_PATHS), {"gift_1m", "gift_6m", "gift_12m"})
        for path in CERTIFICATE_TEMPLATE_PATHS.values():
            with Image.open(path) as image:
                self.assertEqual(image.size, (1536, 1024))
        self.assertTrue(CERTIFICATE_FONT_PATH.is_file())
        self.assertEqual(validate_certificate_name_fits("Анастасия Иванова"), "Анастасия Иванова")
        self.assertEqual(validate_certificate_name_fits("Anne-Marie Smith"), "Anne-Marie Smith")

    def test_name_normalization_and_rejection(self):
        self.assertEqual(normalize_certificate_name("  Анна   Мария  "), "Анна Мария")
        for invalid in (None, "--", "Анна\nМария", "Анна_Мария", "А" * (CERTIFICATE_NAME_MAX_LENGTH + 1)):
            with self.assertRaises(CertificateNameError):
                normalize_certificate_name(invalid)
        with self.assertRaises(CertificateNameError):
            validate_certificate_name_fits("W" * CERTIFICATE_NAME_MAX_LENGTH)

    def test_font_license_is_bundled(self):
        license_text = (CERTIFICATE_FONT_PATH.parent / "OFL-EBGaramond.txt").read_text(encoding="utf-8")
        self.assertIn("SIL OPEN FONT LICENSE Version 1.1", license_text)

    def test_personalized_name_is_rendered_only_inside_measured_box(self):
        for tariff_code, template_path in CERTIFICATE_TEMPLATE_PATHS.items():
            with tempfile.NamedTemporaryFile(suffix=".png") as output:
                render_gift_certificate(tariff_code, "Анастасия Иванова", output.name)
                with Image.open(template_path) as template, Image.open(output.name) as rendered:
                    changed = ImageChops.difference(template.convert("RGB"), rendered.convert("RGB")).getbbox()
                    self.assertIsNotNone(changed)
                    left, top, right, bottom = CERTIFICATE_NAME_BOX
                    self.assertGreaterEqual(changed[0], left)
                    self.assertGreaterEqual(changed[1], top)
                    self.assertLessEqual(changed[2], right)
                    self.assertLessEqual(changed[3], bottom)

    def test_without_name_keeps_clean_template_pixels(self):
        template_path = CERTIFICATE_TEMPLATE_PATHS["gift_1m"]
        with tempfile.NamedTemporaryFile(suffix=".png") as output:
            render_gift_certificate("gift_1m", None, output.name)
            with Image.open(template_path) as template, Image.open(output.name) as rendered:
                difference = ImageChops.difference(template.convert("RGB"), rendered.convert("RGB"))
                self.assertIsNone(difference.getbbox())

    def test_assets_are_project_relative_not_temporary_paths(self):
        project_root = Path(__file__).resolve().parents[1]
        for path in CERTIFICATE_TEMPLATE_PATHS.values():
            self.assertTrue(path.is_relative_to(project_root))


if __name__ == "__main__":
    unittest.main()
