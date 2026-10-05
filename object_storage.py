import os
import re
import uuid
from dataclasses import dataclass
from urllib.parse import urlsplit


DEFAULT_PRESIGNED_URL_TTL_SECONDS = 900
MAX_PRESIGNED_URL_TTL_SECONDS = 3600
MAX_OBJECT_BYTES = 2 * 1024 * 1024 * 1024

MIME_EXTENSIONS = {
    "video/mp4": {"mp4"},
    "video/webm": {"webm"},
    "audio/mpeg": {"mp3"},
    "audio/mp4": {"m4a", "mp4"},
    "audio/wav": {"wav"},
    "audio/ogg": {"ogg", "oga"},
    "image/jpeg": {"jpg", "jpeg"},
    "image/png": {"png"},
    "image/webp": {"webp"},
}


class ObjectStorageError(ValueError):
    def __init__(self, category, status=400):
        super().__init__(category)
        self.category = category
        self.status = int(status)


def _enabled(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _safe_endpoint(value, account_id):
    normalized_account_id = str(account_id or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{32}", normalized_account_id):
        return None
    expected_host = f"{normalized_account_id}.r2.cloudflarestorage.com"
    candidate = str(value or "").strip()
    if not candidate:
        candidate = f"https://{expected_host}"
    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return None
    if (
        parsed.scheme != "https" or parsed.hostname != expected_host
        or parsed.port is not None or parsed.username or parsed.password
        or parsed.query or parsed.fragment or parsed.path not in {"", "/"}
    ):
        return None
    return candidate.rstrip("/")


@dataclass(frozen=True)
class ObjectStorageConfig:
    enabled: bool
    account_id: str | None
    access_key_id: str | None
    secret_access_key: str | None
    bucket_name: str | None
    endpoint: str | None
    ttl_seconds: int = DEFAULT_PRESIGNED_URL_TTL_SECONDS

    @classmethod
    def from_env(cls, environ=None):
        env = environ if environ is not None else os.environ
        account_id = str(env.get("R2_ACCOUNT_ID") or "").strip() or None
        try:
            ttl = int(env.get("R2_PRESIGNED_URL_TTL_SECONDS", DEFAULT_PRESIGNED_URL_TTL_SECONDS))
        except (TypeError, ValueError):
            ttl = DEFAULT_PRESIGNED_URL_TTL_SECONDS
        ttl = min(MAX_PRESIGNED_URL_TTL_SECONDS, max(60, ttl))
        return cls(
            enabled=_enabled(env.get("R2_ENABLED")),
            account_id=account_id.lower() if account_id else None,
            access_key_id=str(env.get("R2_ACCESS_KEY_ID") or "").strip() or None,
            secret_access_key=str(env.get("R2_SECRET_ACCESS_KEY") or "").strip() or None,
            bucket_name=str(env.get("R2_BUCKET_NAME") or "").strip() or None,
            endpoint=_safe_endpoint(env.get("R2_ENDPOINT"), account_id),
            ttl_seconds=ttl,
        )

    def missing_fields(self):
        if not self.enabled:
            return ()
        values = {
            "R2_ACCOUNT_ID": self.account_id,
            "R2_ACCESS_KEY_ID": self.access_key_id,
            "R2_SECRET_ACCESS_KEY": self.secret_access_key,
            "R2_BUCKET_NAME": self.bucket_name,
            "R2_ENDPOINT": self.endpoint,
        }
        return tuple(name for name, value in values.items() if not value)

    def is_configured(self):
        return self.enabled and not self.missing_fields()

    def public_origin(self):
        if not self.endpoint:
            return None
        parsed = urlsplit(self.endpoint)
        return f"{parsed.scheme}://{parsed.netloc}"


def validate_object_upload(filename, content_type, size_bytes, media_type):
    if not isinstance(filename, str) or not filename.strip() or len(filename) > 255:
        raise ObjectStorageError("invalid_media_filename")
    basename = filename.replace("\\", "/").rsplit("/", 1)[-1].strip()
    if basename in {"", ".", ".."} or any(ord(char) < 32 for char in basename):
        raise ObjectStorageError("invalid_media_filename")
    extension = basename.rsplit(".", 1)[-1].lower() if "." in basename else ""
    if content_type not in MIME_EXTENSIONS or extension not in MIME_EXTENSIONS[content_type]:
        raise ObjectStorageError("unsupported_content_media", 415)
    allowed_by_type = {
        "cover": content_type.startswith("image/"),
        "video": content_type.startswith("video/"),
        "audio": content_type.startswith("audio/"),
    }
    if media_type not in allowed_by_type or not allowed_by_type[media_type]:
        raise ObjectStorageError("invalid_content_media_type")
    try:
        size_bytes = int(size_bytes)
    except (TypeError, ValueError):
        raise ObjectStorageError("invalid_media_size") from None
    max_bytes = 10 * 1024 * 1024 if media_type == "cover" else MAX_OBJECT_BYTES
    if size_bytes < 1 or size_bytes > max_bytes:
        raise ObjectStorageError("content_media_too_large", 413)
    return basename, extension, size_bytes


def generate_safe_object_key(content_id, extension):
    content_id = str(uuid.UUID(str(content_id)))
    if not re.fullmatch(r"[a-z0-9]{2,5}", str(extension or "").lower()):
        raise ObjectStorageError("invalid_media_filename")
    return f"content/{content_id}/{uuid.uuid4()}.{extension.lower()}"


class R2ObjectStorage:
    def __init__(self, config, client=None):
        self.config = config
        self._client = client

    def is_configured(self):
        return self.config.is_configured()

    def _require_configured(self):
        if not self.config.enabled:
            raise ObjectStorageError("object_storage_disabled", 503)
        if not self.config.is_configured():
            raise ObjectStorageError("object_storage_configuration_invalid", 503)

    def _get_client(self):
        self._require_configured()
        if self._client is None:
            import boto3
            from botocore.config import Config
            self._client = boto3.client(
                "s3",
                endpoint_url=self.config.endpoint,
                aws_access_key_id=self.config.access_key_id,
                aws_secret_access_key=self.config.secret_access_key,
                region_name="auto",
                config=Config(
                    signature_version="s3v4",
                    connect_timeout=5,
                    read_timeout=10,
                    retries={"mode": "standard", "total_max_attempts": 3},
                    s3={"addressing_style": "path"},
                ),
            )
        return self._client

    def create_upload_url(self, object_key, content_type, upload_id):
        client = self._get_client()
        url = client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self.config.bucket_name,
                "Key": object_key,
                "ContentType": content_type,
                "Metadata": {"upload-id": str(upload_id)},
            },
            ExpiresIn=self.config.ttl_seconds,
        )
        return {
            "url": url,
            "expires_in": self.config.ttl_seconds,
            "required_headers": {
                "Content-Type": content_type,
                "x-amz-meta-upload-id": str(upload_id),
            },
        }

    def create_download_url(self, object_key):
        client = self._get_client()
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.config.bucket_name, "Key": object_key},
            ExpiresIn=self.config.ttl_seconds,
        )

    def head_object(self, object_key):
        client = self._get_client()
        return client.head_object(Bucket=self.config.bucket_name, Key=object_key)

    def delete_object(self, object_key):
        client = self._get_client()
        return client.delete_object(Bucket=self.config.bucket_name, Key=object_key)
