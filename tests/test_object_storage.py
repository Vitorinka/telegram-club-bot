import unittest
from urllib.parse import urlsplit

from object_storage import (
    MAX_OBJECT_BYTES,
    ObjectStorageConfig,
    ObjectStorageError,
    R2ObjectStorage,
    generate_safe_object_key,
    validate_object_upload,
)


class FakeS3Client:
    def __init__(self):
        self.calls = []

    def generate_presigned_url(self, operation, Params, ExpiresIn):
        self.calls.append((operation, Params, ExpiresIn))
        return f"https://example.invalid/{Params['Key']}?signature=opaque"

    def head_object(self, **kwargs):
        self.calls.append(("head", kwargs))
        return {"ContentLength": 12, "ContentType": "video/mp4"}


class ObjectStorageTests(unittest.TestCase):
    ACCOUNT_ID = "0123456789abcdef0123456789abcdef"

    def configured(self):
        return ObjectStorageConfig.from_env({
            "R2_ENABLED": "true", "R2_ACCOUNT_ID": self.ACCOUNT_ID,
            "R2_ACCESS_KEY_ID": "access", "R2_SECRET_ACCESS_KEY": "secret",
            "R2_BUCKET_NAME": "private-media",
        })

    def test_r2_disabled_is_not_configured(self):
        config = ObjectStorageConfig.from_env({})
        self.assertFalse(config.enabled)
        self.assertFalse(config.is_configured())
        self.assertEqual(config.missing_fields(), ())

    def test_incomplete_enabled_config_is_rejected_without_exposing_values(self):
        config = ObjectStorageConfig.from_env({"R2_ENABLED": "true", "R2_ACCOUNT_ID": self.ACCOUNT_ID})
        self.assertFalse(config.is_configured())
        self.assertIn("R2_SECRET_ACCESS_KEY", config.missing_fields())
        with self.assertRaisesRegex(ObjectStorageError, "object_storage_configuration_invalid"):
            R2ObjectStorage(config, FakeS3Client()).create_download_url("content/x/file.mp4")

    def test_safe_key_and_strict_mime_extension_validation(self):
        content_id = "f808165c-23d6-46d6-9d82-108e5d0990f8"
        key = generate_safe_object_key(content_id, "mp4")
        self.assertTrue(key.startswith(f"content/{content_id}/"))
        self.assertNotIn("lesson.mp4", key)
        self.assertEqual(
            validate_object_upload("lesson.mp4", "video/mp4", MAX_OBJECT_BYTES, "video"),
            ("lesson.mp4", "mp4", MAX_OBJECT_BYTES),
        )
        with self.assertRaisesRegex(ObjectStorageError, "unsupported_content_media"):
            validate_object_upload("lesson.webm", "video/mp4", 10, "video")
        with self.assertRaisesRegex(ObjectStorageError, "content_media_too_large"):
            validate_object_upload("lesson.mp4", "video/mp4", MAX_OBJECT_BYTES + 1, "video")

    def test_presigned_urls_keep_credentials_server_side(self):
        client = FakeS3Client(); storage = R2ObjectStorage(self.configured(), client)
        result = storage.create_upload_url("content/id/file.mp4", "video/mp4", "upload-id")
        self.assertEqual(result["required_headers"]["Content-Type"], "video/mp4")
        self.assertEqual(result["required_headers"]["x-amz-meta-upload-id"], "upload-id")
        self.assertNotIn("access", repr(result))
        self.assertNotIn("secret", repr(result))
        self.assertIn("signature=", storage.create_download_url("content/id/file.mp4"))

    def test_endpoint_is_exactly_bound_to_cloudflare_account(self):
        expected = f"https://{self.ACCOUNT_ID}.r2.cloudflarestorage.com"
        base = {
            "R2_ENABLED": "true", "R2_ACCOUNT_ID": self.ACCOUNT_ID,
            "R2_ACCESS_KEY_ID": "access", "R2_SECRET_ACCESS_KEY": "secret",
            "R2_BUCKET_NAME": "private-media",
        }
        self.assertEqual(ObjectStorageConfig.from_env(base).endpoint, expected)
        self.assertEqual(ObjectStorageConfig.from_env({**base, "R2_ENDPOINT": expected}).endpoint, expected)
        for endpoint in (
            "https://arbitrary.example",
            "https://ffffffffffffffffffffffffffffffff.r2.cloudflarestorage.com",
            f"https://user:pass@{self.ACCOUNT_ID}.r2.cloudflarestorage.com",
            f"https://{self.ACCOUNT_ID}.r2.cloudflarestorage.com/path",
            f"https://{self.ACCOUNT_ID}.r2.cloudflarestorage.com?query=1",
            f"https://{self.ACCOUNT_ID}.r2.cloudflarestorage.com#fragment",
        ):
            with self.subTest(endpoint=endpoint):
                config = ObjectStorageConfig.from_env({**base, "R2_ENDPOINT": endpoint})
                self.assertIsNone(config.endpoint)
                self.assertFalse(config.is_configured())

    def test_real_botocore_presigning_origin_matches_csp_origin_and_is_bounded(self):
        storage = R2ObjectStorage(self.configured())
        upload = storage.create_upload_url(
            "content/00000000-0000-0000-0000-000000000001/file.mp4",
            "video/mp4", "upload-id",
        )
        download = storage.create_download_url(
            "content/00000000-0000-0000-0000-000000000001/file.mp4"
        )
        expected_origin = self.configured().public_origin()
        for url in (upload["url"], download):
            parsed = urlsplit(url)
            self.assertEqual(f"{parsed.scheme}://{parsed.netloc}", expected_origin)
            self.assertNotIn("secret", url)
        client_config = storage._get_client().meta.config
        self.assertEqual(client_config.connect_timeout, 5)
        self.assertEqual(client_config.read_timeout, 10)
        self.assertEqual(client_config.retries["total_max_attempts"], 3)
        self.assertEqual(client_config.s3["addressing_style"], "path")


if __name__ == "__main__":
    unittest.main()
