import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from schedule_uploads import (
    SCHEDULE_UPLOAD_MAX_BYTES,
    ScheduleUploadError,
    schedule_upload_content_type,
    validate_schedule_upload_month,
)


class ScheduleUploadValidationTests(unittest.TestCase):
    def test_magic_bytes_allow_only_supported_raster_formats(self):
        self.assertEqual(schedule_upload_content_type(b"\x89PNG\r\n\x1a\nrest"), "image/png")
        self.assertEqual(schedule_upload_content_type(b"\xff\xd8\xffrest"), "image/jpeg")
        self.assertEqual(schedule_upload_content_type(b"RIFF1234WEBPrest"), "image/webp")
        self.assertIsNone(schedule_upload_content_type(b"<svg></svg>"))

    def test_month_range_uses_moscow_current_month(self):
        now = datetime(2026, 8, 22, tzinfo=ZoneInfo("Europe/Moscow"))
        self.assertEqual(validate_schedule_upload_month("2025-08", now), "2025-08")
        self.assertEqual(validate_schedule_upload_month("2028-08", now), "2028-08")
        for value in ("2025-07", "2028-09", "2026-13", "2026-8", "bad"):
            with self.subTest(value=value), self.assertRaises(ScheduleUploadError):
                validate_schedule_upload_month(value, now)

    def test_upload_limit_is_exactly_ten_mebibytes(self):
        self.assertEqual(SCHEDULE_UPLOAD_MAX_BYTES, 10 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
