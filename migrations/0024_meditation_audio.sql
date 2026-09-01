DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='content_media'::regclass AND conname='content_media_type_check' AND pg_get_constraintdef(oid) LIKE '%audio%') THEN
        ALTER TABLE content_media DROP CONSTRAINT IF EXISTS content_media_type_check;
        ALTER TABLE content_media ADD CONSTRAINT content_media_type_check CHECK (media_type IN ('cover','video','audio')) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='content_media'::regclass AND conname='content_media_mime_check' AND pg_get_constraintdef(oid) LIKE '%audio/mpeg%') THEN
        ALTER TABLE content_media DROP CONSTRAINT IF EXISTS content_media_mime_check;
        ALTER TABLE content_media ADD CONSTRAINT content_media_mime_check CHECK (
            (media_type='cover' AND mime_type IN ('image/jpeg','image/png','image/webp')) OR
            (media_type='video' AND mime_type='video/mp4') OR
            (media_type='audio' AND mime_type='audio/mpeg')) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='content_media'::regclass AND conname='content_media_size_check' AND pg_get_constraintdef(oid) LIKE '%audio%') THEN
        ALTER TABLE content_media DROP CONSTRAINT IF EXISTS content_media_size_check;
        ALTER TABLE content_media ADD CONSTRAINT content_media_size_check CHECK (
            (media_type='cover' AND size_bytes BETWEEN 1 AND 10485760) OR
            (media_type IN ('video','audio') AND size_bytes BETWEEN 1 AND 20971520)) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='content_media_uploads'::regclass AND conname='content_media_uploads_type_check' AND pg_get_constraintdef(oid) LIKE '%audio%') THEN
        ALTER TABLE content_media_uploads DROP CONSTRAINT IF EXISTS content_media_uploads_type_check;
        ALTER TABLE content_media_uploads ADD CONSTRAINT content_media_uploads_type_check CHECK (media_type IN ('cover','video','audio')) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='content_media_uploads'::regclass AND conname='content_media_uploads_mime_check' AND pg_get_constraintdef(oid) LIKE '%audio/mpeg%') THEN
        ALTER TABLE content_media_uploads DROP CONSTRAINT IF EXISTS content_media_uploads_mime_check;
        ALTER TABLE content_media_uploads ADD CONSTRAINT content_media_uploads_mime_check CHECK (
            (media_type='cover' AND mime_type IN ('image/jpeg','image/png','image/webp')) OR
            (media_type='video' AND mime_type='video/mp4') OR
            (media_type='audio' AND mime_type='audio/mpeg')) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='content_media_uploads'::regclass AND conname='content_media_uploads_size_check' AND pg_get_constraintdef(oid) LIKE '%audio%') THEN
        ALTER TABLE content_media_uploads DROP CONSTRAINT IF EXISTS content_media_uploads_size_check;
        ALTER TABLE content_media_uploads ADD CONSTRAINT content_media_uploads_size_check CHECK (
            (status IN ('pending','confirmed','uploading','uploaded') AND
             ((media_type='cover' AND byte_size BETWEEN 1 AND 10485760) OR
              (media_type IN ('video','audio') AND byte_size BETWEEN 1 AND 20971520)) AND
             octet_length(media_bytes)=byte_size) OR
            (status IN ('applied','cancelled','failed','expired') AND byte_size=0 AND octet_length(media_bytes)=0)) NOT VALID;
    END IF;
END
$$;

ALTER TABLE content_media VALIDATE CONSTRAINT content_media_type_check;
ALTER TABLE content_media VALIDATE CONSTRAINT content_media_mime_check;
ALTER TABLE content_media VALIDATE CONSTRAINT content_media_size_check;
ALTER TABLE content_media_uploads VALIDATE CONSTRAINT content_media_uploads_type_check;
ALTER TABLE content_media_uploads VALIDATE CONSTRAINT content_media_uploads_mime_check;
ALTER TABLE content_media_uploads VALIDATE CONSTRAINT content_media_uploads_size_check;
