import unittest

from content_media import (
    AUDIO_MAX_BYTES,
    COVER_MAX_BYTES,
    VIDEO_MAX_BYTES,
    ContentMediaError,
    detect_media_mime,
    validate_media_bytes,
)


class ContentMediaValidationTests(unittest.TestCase):
    def test_cover_magic_allows_only_bounded_raster_formats(self):
        samples = {
            b"\x89PNG\r\n\x1a\nrest": "image/png",
            b"\xff\xd8\xffrest": "image/jpeg",
            b"RIFF1234WEBPrest": "image/webp",
        }
        for data, expected in samples.items():
            with self.subTest(expected=expected):
                self.assertEqual(validate_media_bytes("cover", data), expected)
        for data in (b"<svg></svg>", b"GIF89a", b"not-an-image"):
            with self.subTest(data=data), self.assertRaisesRegex(
                ContentMediaError, "unsupported_content_media"
            ):
                validate_media_bytes("cover", data)

    def test_video_requires_supported_mp4_file_type_brand(self):
        for brand in (b"isom", b"iso2", b"mp41", b"mp42", b"avc1", b"M4V ", b"dash"):
            with self.subTest(brand=brand):
                data = b"\x00\x00\x00\x18ftyp" + brand + b"payload"
                self.assertEqual(detect_media_mime("video", data), "video/mp4")
        for data in (
            b"video/mp4",
            b"\x00\x00\x00\x18ftypBAD!payload",
            b"\x1aE\xdf\xa3webm",
        ):
            with self.subTest(data=data), self.assertRaisesRegex(
                ContentMediaError, "unsupported_content_media"
            ):
                validate_media_bytes("video", data)

    def test_media_type_and_size_limits_fail_closed(self):
        self.assertEqual(COVER_MAX_BYTES, 10 * 1024 * 1024)
        self.assertEqual(VIDEO_MAX_BYTES, 20 * 1024 * 1024)
        self.assertEqual(AUDIO_MAX_BYTES, 20 * 1024 * 1024)
        self.assertEqual(validate_media_bytes("audio", b"\xff\xfb\x90\x64payload"), "audio/mpeg")
        self.assertEqual(validate_media_bytes("audio", b"ID3\x04\x00\x00\x00\x00\x00\x00\xff\xfb\x90\x64payload"), "audio/mpeg")
        for invalid in (b"data", b"ID3\x04\x00\x00\x80\x00\x00\x00payload", b"\xff\xfb\x00\x64payload"):
            with self.assertRaisesRegex(ContentMediaError, "unsupported_content_media"):
                validate_media_bytes("audio", invalid)
        with self.assertRaisesRegex(ContentMediaError, "content_media_too_large"):
            validate_media_bytes("cover", b"\x89PNG\r\n\x1a\n" + b"x" * COVER_MAX_BYTES)
        with self.assertRaisesRegex(ContentMediaError, "content_media_too_large"):
            validate_media_bytes(
                "video", b"\x00\x00\x00\x18ftypisom" + b"x" * VIDEO_MAX_BYTES
            )
        with self.assertRaisesRegex(ContentMediaError, "content_media_too_large"):
            validate_media_bytes("audio", b"\xff\xfb\x90\x64" + b"x" * AUDIO_MAX_BYTES)


if __name__ == "__main__":
    unittest.main()
