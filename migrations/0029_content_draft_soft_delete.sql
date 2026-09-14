ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS deleted_by_telegram_id BIGINT;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='content_items_soft_delete_shape_check') THEN
        ALTER TABLE content_items ADD CONSTRAINT content_items_soft_delete_shape_check
            CHECK (
                (deleted_at IS NULL AND deleted_by_telegram_id IS NULL)
                OR
                (deleted_at IS NOT NULL AND deleted_by_telegram_id IS NOT NULL AND status='draft')
            ) NOT VALID;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS content_items_active_status_updated_idx
ON content_items (status, updated_at DESC, content_id DESC)
WHERE deleted_at IS NULL;
