ALTER TABLE content_media
    ADD COLUMN IF NOT EXISTS original_filename TEXT,
    ADD COLUMN IF NOT EXISTS object_etag TEXT;

ALTER TABLE content_media_uploads
    ADD COLUMN IF NOT EXISTS storage_kind TEXT NOT NULL DEFAULT 'telegram_file_id',
    ADD COLUMN IF NOT EXISTS object_key TEXT,
    ADD COLUMN IF NOT EXISTS original_filename TEXT,
    ADD COLUMN IF NOT EXISTS object_etag TEXT;

ALTER TABLE content_media ALTER COLUMN sha256 DROP NOT NULL;
ALTER TABLE content_media_uploads ALTER COLUMN sha256 DROP NOT NULL;

ALTER TABLE content_media DROP CONSTRAINT IF EXISTS content_media_storage_check;
ALTER TABLE content_media DROP CONSTRAINT IF EXISTS content_media_mime_check;
ALTER TABLE content_media DROP CONSTRAINT IF EXISTS content_media_size_check;
ALTER TABLE content_media DROP CONSTRAINT IF EXISTS content_media_sha256_check;
ALTER TABLE content_media ADD CONSTRAINT content_media_storage_check
    CHECK (storage_kind IN ('telegram_file_id', 'r2')) NOT VALID;
ALTER TABLE content_media ADD CONSTRAINT content_media_mime_check CHECK (
    (storage_kind='telegram_file_id' AND (
        (media_type='cover' AND mime_type IN ('image/jpeg','image/png','image/webp')) OR
        (media_type='video' AND mime_type='video/mp4') OR
        (media_type='audio' AND mime_type='audio/mpeg')
    )) OR
    (storage_kind='r2' AND (
        (media_type='cover' AND mime_type IN ('image/jpeg','image/png','image/webp')) OR
        (media_type='video' AND mime_type IN ('video/mp4','video/webm')) OR
        (media_type='audio' AND mime_type IN ('audio/mpeg','audio/mp4','audio/wav','audio/ogg'))
    ))
) NOT VALID;
ALTER TABLE content_media ADD CONSTRAINT content_media_size_check CHECK (
    (media_type='cover' AND size_bytes BETWEEN 1 AND 10485760) OR
    (storage_kind='telegram_file_id' AND media_type IN ('video','audio') AND size_bytes BETWEEN 1 AND 20971520) OR
    (storage_kind='r2' AND media_type IN ('video','audio') AND size_bytes BETWEEN 1 AND 2147483648)
) NOT VALID;
ALTER TABLE content_media ADD CONSTRAINT content_media_sha256_check CHECK (
    (storage_kind='telegram_file_id' AND sha256 IS NOT NULL AND sha256 ~ '^[0-9a-f]{64}$') OR
    (storage_kind='r2' AND sha256 IS NULL)
) NOT VALID;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='content_media'::regclass AND conname='content_media_original_filename_check') THEN
        ALTER TABLE content_media ADD CONSTRAINT content_media_original_filename_check CHECK (
            (storage_kind='telegram_file_id' AND (original_filename IS NULL OR length(original_filename) BETWEEN 1 AND 255)) OR
            (storage_kind='r2' AND original_filename IS NOT NULL AND length(original_filename) BETWEEN 1 AND 255)
        ) NOT VALID;
    END IF;
END $$;

ALTER TABLE content_media_uploads DROP CONSTRAINT IF EXISTS content_media_uploads_mime_check;
ALTER TABLE content_media_uploads DROP CONSTRAINT IF EXISTS content_media_uploads_size_check;
ALTER TABLE content_media_uploads DROP CONSTRAINT IF EXISTS content_media_uploads_sha256_check;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='content_media_uploads'::regclass AND conname='content_media_uploads_storage_check') THEN
        ALTER TABLE content_media_uploads ADD CONSTRAINT content_media_uploads_storage_check
            CHECK (storage_kind IN ('telegram_file_id', 'r2')) NOT VALID;
    END IF;
END $$;
ALTER TABLE content_media_uploads ADD CONSTRAINT content_media_uploads_mime_check CHECK (
    (storage_kind='telegram_file_id' AND (
        (media_type='cover' AND mime_type IN ('image/jpeg','image/png','image/webp')) OR
        (media_type='video' AND mime_type='video/mp4') OR
        (media_type='audio' AND mime_type='audio/mpeg')
    )) OR
    (storage_kind='r2' AND (
        (media_type='cover' AND mime_type IN ('image/jpeg','image/png','image/webp')) OR
        (media_type='video' AND mime_type IN ('video/mp4','video/webm')) OR
        (media_type='audio' AND mime_type IN ('audio/mpeg','audio/mp4','audio/wav','audio/ogg'))
    ))
) NOT VALID;
ALTER TABLE content_media_uploads ADD CONSTRAINT content_media_uploads_size_check CHECK (
    (storage_kind='telegram_file_id' AND (
        (status IN ('pending','confirmed','uploading','uploaded') AND
         ((media_type='cover' AND byte_size BETWEEN 1 AND 10485760) OR
          (media_type IN ('video','audio') AND byte_size BETWEEN 1 AND 20971520)) AND
         octet_length(media_bytes)=byte_size) OR
        (status IN ('applied','cancelled','failed','expired') AND byte_size=0 AND octet_length(media_bytes)=0)
    )) OR
    (storage_kind='r2' AND octet_length(media_bytes)=0 AND
        ((media_type='cover' AND byte_size BETWEEN 1 AND 10485760) OR
         (media_type IN ('video','audio') AND byte_size BETWEEN 1 AND 2147483648)))
) NOT VALID;
ALTER TABLE content_media_uploads ADD CONSTRAINT content_media_uploads_sha256_check CHECK (
    (storage_kind='telegram_file_id' AND sha256 IS NOT NULL AND sha256 ~ '^[0-9a-f]{64}$') OR
    (storage_kind='r2' AND sha256 IS NULL)
) NOT VALID;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='content_media_uploads'::regclass AND conname='content_media_uploads_object_key_check') THEN
        ALTER TABLE content_media_uploads ADD CONSTRAINT content_media_uploads_object_key_check CHECK (
            (storage_kind='telegram_file_id' AND object_key IS NULL) OR
            (storage_kind='r2' AND object_key IS NOT NULL AND length(object_key) BETWEEN 1 AND 1024)
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='content_media_uploads'::regclass AND conname='content_media_uploads_original_filename_check') THEN
        ALTER TABLE content_media_uploads ADD CONSTRAINT content_media_uploads_original_filename_check CHECK (
            (storage_kind='telegram_file_id' AND (original_filename IS NULL OR length(original_filename) BETWEEN 1 AND 255)) OR
            (storage_kind='r2' AND original_filename IS NOT NULL AND length(original_filename) BETWEEN 1 AND 255)
        ) NOT VALID;
    END IF;
END $$;

ALTER TABLE content_media VALIDATE CONSTRAINT content_media_storage_check;
ALTER TABLE content_media VALIDATE CONSTRAINT content_media_mime_check;
ALTER TABLE content_media VALIDATE CONSTRAINT content_media_size_check;
ALTER TABLE content_media VALIDATE CONSTRAINT content_media_sha256_check;
ALTER TABLE content_media VALIDATE CONSTRAINT content_media_original_filename_check;
ALTER TABLE content_media_uploads VALIDATE CONSTRAINT content_media_uploads_storage_check;
ALTER TABLE content_media_uploads VALIDATE CONSTRAINT content_media_uploads_mime_check;
ALTER TABLE content_media_uploads VALIDATE CONSTRAINT content_media_uploads_size_check;
ALTER TABLE content_media_uploads VALIDATE CONSTRAINT content_media_uploads_sha256_check;
ALTER TABLE content_media_uploads VALIDATE CONSTRAINT content_media_uploads_object_key_check;
ALTER TABLE content_media_uploads VALIDATE CONSTRAINT content_media_uploads_original_filename_check;
