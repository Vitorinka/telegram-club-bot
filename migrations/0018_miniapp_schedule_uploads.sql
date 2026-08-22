CREATE TABLE IF NOT EXISTS miniapp_schedule_uploads (
    upload_id UUID PRIMARY KEY,
    admin_telegram_id BIGINT NOT NULL,
    schedule_month VARCHAR(7) NOT NULL,
    image_bytes BYTEA NOT NULL,
    content_type TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    sha256 CHAR(64) NOT NULL,
    expected_schedule_exists BOOLEAN NOT NULL,
    expected_schedule_updated_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'pending',
    action_id UUID UNIQUE,
    telegram_file_id TEXT,
    upload_started_at TIMESTAMP,
    telegram_uploaded_at TIMESTAMP,
    applied_at TIMESTAMP,
    consumed_at TIMESTAMP,
    failure_category TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    CONSTRAINT miniapp_schedule_uploads_month_check
        CHECK (schedule_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    CONSTRAINT miniapp_schedule_uploads_content_type_check
        CHECK (content_type IN ('image/jpeg', 'image/png', 'image/webp')),
    CONSTRAINT miniapp_schedule_uploads_size_check
        CHECK (
            (
                status IN ('pending', 'confirmed', 'uploading', 'uploaded')
                AND byte_size > 0
                AND byte_size <= 10485760
                AND octet_length(image_bytes) = byte_size
            )
            OR
            (
                status IN ('applied', 'cancelled', 'failed', 'expired')
                AND byte_size = 0
                AND octet_length(image_bytes) = 0
            )
        ),
    CONSTRAINT miniapp_schedule_uploads_sha256_check
        CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT miniapp_schedule_uploads_status_check
        CHECK (status IN (
            'pending', 'confirmed', 'uploading', 'uploaded',
            'applied', 'cancelled', 'failed', 'expired'
        )),
    CONSTRAINT miniapp_schedule_uploads_expected_version_check
        CHECK (
            (expected_schedule_exists IS TRUE AND expected_schedule_updated_at IS NOT NULL)
            OR
            (expected_schedule_exists IS FALSE AND expected_schedule_updated_at IS NULL)
        )
);

ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS upload_id UUID;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS admin_telegram_id BIGINT;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS schedule_month VARCHAR(7);
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS image_bytes BYTEA;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS content_type TEXT;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS byte_size INTEGER;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS sha256 CHAR(64);
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS expected_schedule_exists BOOLEAN;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS expected_schedule_updated_at TIMESTAMP;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending';
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS action_id UUID;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS telegram_file_id TEXT;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS upload_started_at TIMESTAMP;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS telegram_uploaded_at TIMESTAMP;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS applied_at TIMESTAMP;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS consumed_at TIMESTAMP;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS failure_category TEXT;
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
ALTER TABLE miniapp_schedule_uploads ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP;

CREATE UNIQUE INDEX IF NOT EXISTS miniapp_schedule_uploads_action_idx
ON miniapp_schedule_uploads (action_id)
WHERE action_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS miniapp_schedule_uploads_owner_created_idx
ON miniapp_schedule_uploads (admin_telegram_id, created_at DESC);

CREATE INDEX IF NOT EXISTS miniapp_schedule_uploads_expiry_idx
ON miniapp_schedule_uploads (expires_at)
WHERE status IN ('pending', 'confirmed');
