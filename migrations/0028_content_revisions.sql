ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS logical_content_id UUID,
    ADD COLUMN IF NOT EXISTS revision_of UUID,
    ADD COLUMN IF NOT EXISTS revision_number INTEGER NOT NULL DEFAULT 1;

UPDATE content_items
SET logical_content_id = content_id
WHERE logical_content_id IS NULL;

ALTER TABLE content_items
    ALTER COLUMN logical_content_id SET NOT NULL;

CREATE OR REPLACE FUNCTION initialize_content_logical_identity()
RETURNS trigger AS $$
BEGIN
    IF NEW.logical_content_id IS NULL THEN
        NEW.logical_content_id := NEW.content_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS initialize_content_logical_identity_trigger ON content_items;
CREATE TRIGGER initialize_content_logical_identity_trigger
BEFORE INSERT ON content_items
FOR EACH ROW EXECUTE FUNCTION initialize_content_logical_identity();

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='content_items_logical_content_fk') THEN
        ALTER TABLE content_items ADD CONSTRAINT content_items_logical_content_fk
            FOREIGN KEY (logical_content_id) REFERENCES content_items(content_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='content_items_revision_of_fk') THEN
        ALTER TABLE content_items ADD CONSTRAINT content_items_revision_of_fk
            FOREIGN KEY (revision_of) REFERENCES content_items(content_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='content_items_revision_number_check') THEN
        ALTER TABLE content_items ADD CONSTRAINT content_items_revision_number_check
            CHECK (revision_number >= 1);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='content_items_revision_shape_check') THEN
        ALTER TABLE content_items ADD CONSTRAINT content_items_revision_shape_check
            CHECK (
                (revision_of IS NULL AND logical_content_id = content_id AND revision_number = 1)
                OR
                (revision_of IS NOT NULL AND logical_content_id <> content_id AND revision_number > 1)
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS content_items_logical_history_idx
ON content_items (logical_content_id, revision_number DESC, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS content_items_one_draft_revision_idx
ON content_items (logical_content_id) WHERE status='draft';

CREATE UNIQUE INDEX IF NOT EXISTS content_items_one_published_revision_idx
ON content_items (logical_content_id) WHERE status='published';
