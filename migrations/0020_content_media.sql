CREATE TABLE IF NOT EXISTS content_media (
    media_id UUID PRIMARY KEY,
    content_id UUID NOT NULL REFERENCES content_items(content_id),
    media_type TEXT NOT NULL,
    storage_kind TEXT NOT NULL,
    server_reference TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    sha256 CHAR(64) NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1,
    created_by_telegram_id BIGINT NOT NULL,
    replaces_media_id UUID REFERENCES content_media(media_id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    CONSTRAINT content_media_type_check CHECK (media_type IN ('cover', 'video')),
    CONSTRAINT content_media_storage_check CHECK (storage_kind = 'telegram_file_id'),
    CONSTRAINT content_media_reference_check CHECK (length(server_reference) BETWEEN 1 AND 1024),
    CONSTRAINT content_media_mime_check CHECK (
        (media_type = 'cover' AND mime_type IN ('image/jpeg', 'image/png', 'image/webp'))
        OR (media_type = 'video' AND mime_type = 'video/mp4')
    ),
    CONSTRAINT content_media_size_check CHECK (
        (media_type = 'cover' AND size_bytes BETWEEN 1 AND 10485760)
        OR (media_type = 'video' AND size_bytes BETWEEN 1 AND 20971520)
    ),
    CONSTRAINT content_media_sha256_check CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT content_media_sort_order_check CHECK (sort_order BETWEEN -100000 AND 100000),
    CONSTRAINT content_media_version_check CHECK (version >= 1)
);

CREATE UNIQUE INDEX IF NOT EXISTS content_media_one_active_type_idx
ON content_media (content_id, media_type) WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS content_media_content_history_idx
ON content_media (content_id, media_type, version DESC);

CREATE TABLE IF NOT EXISTS content_media_uploads (
    upload_id UUID PRIMARY KEY,
    admin_telegram_id BIGINT NOT NULL,
    content_id UUID NOT NULL REFERENCES content_items(content_id),
    expected_content_version INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    media_bytes BYTEA NOT NULL,
    mime_type TEXT NOT NULL,
    byte_size BIGINT NOT NULL,
    sha256 CHAR(64) NOT NULL,
    expected_existing_media_id UUID REFERENCES content_media(media_id),
    status TEXT NOT NULL DEFAULT 'pending',
    action_id UUID UNIQUE REFERENCES admin_action_requests(action_id),
    telegram_file_id TEXT,
    upload_started_at TIMESTAMP,
    telegram_uploaded_at TIMESTAMP,
    applied_media_id UUID REFERENCES content_media(media_id),
    applied_at TIMESTAMP,
    consumed_at TIMESTAMP,
    failure_category TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    CONSTRAINT content_media_uploads_version_check CHECK (expected_content_version >= 1),
    CONSTRAINT content_media_uploads_type_check CHECK (media_type IN ('cover', 'video')),
    CONSTRAINT content_media_uploads_mime_check CHECK (
        (media_type = 'cover' AND mime_type IN ('image/jpeg', 'image/png', 'image/webp'))
        OR (media_type = 'video' AND mime_type = 'video/mp4')
    ),
    CONSTRAINT content_media_uploads_size_check CHECK (
        (
            status IN ('pending', 'confirmed', 'uploading', 'uploaded')
            AND (
                (media_type = 'cover' AND byte_size BETWEEN 1 AND 10485760)
                OR (media_type = 'video' AND byte_size BETWEEN 1 AND 20971520)
            )
            AND octet_length(media_bytes) = byte_size
        ) OR (
            status IN ('applied', 'cancelled', 'failed', 'expired')
            AND byte_size = 0 AND octet_length(media_bytes) = 0
        )
    ),
    CONSTRAINT content_media_uploads_sha256_check CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT content_media_uploads_reference_check CHECK (
        telegram_file_id IS NULL OR length(telegram_file_id) BETWEEN 1 AND 1024
    ),
    CONSTRAINT content_media_uploads_status_check CHECK (
        status IN ('pending', 'confirmed', 'uploading', 'uploaded', 'applied', 'cancelled', 'failed', 'expired')
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS content_media_uploads_action_idx
ON content_media_uploads (action_id) WHERE action_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS content_media_uploads_owner_created_idx
ON content_media_uploads (admin_telegram_id, created_at DESC);

CREATE INDEX IF NOT EXISTS content_media_uploads_expiry_idx
ON content_media_uploads (expires_at) WHERE status IN ('pending', 'confirmed');
