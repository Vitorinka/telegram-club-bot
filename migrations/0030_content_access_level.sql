ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS access_level TEXT NOT NULL DEFAULT 'paid';

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='content_items_access_level_check') THEN
        ALTER TABLE content_items ADD CONSTRAINT content_items_access_level_check
            CHECK (access_level IN ('paid','free')) NOT VALID;
    END IF;
END $$;

ALTER TABLE content_item_versions
    ADD COLUMN IF NOT EXISTS access_level TEXT NOT NULL DEFAULT 'paid';

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='content_item_versions_access_level_check') THEN
        ALTER TABLE content_item_versions ADD CONSTRAINT content_item_versions_access_level_check
            CHECK (access_level IN ('paid','free')) NOT VALID;
    END IF;
END $$;

DROP INDEX IF EXISTS content_items_one_draft_revision_idx;
CREATE UNIQUE INDEX content_items_one_draft_revision_idx
ON content_items (logical_content_id)
WHERE status='draft' AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS content_items_member_access_order_idx
ON content_items (content_type, access_level, sort_order, updated_at DESC, content_id)
WHERE status='published' AND deleted_at IS NULL;
