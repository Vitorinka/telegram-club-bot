DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='content_items'::regclass
          AND conname='content_items_type_check'
          AND pg_get_constraintdef(oid) LIKE '%nutrition_material%'
    ) THEN
        ALTER TABLE content_items DROP CONSTRAINT IF EXISTS content_items_type_check;
        ALTER TABLE content_items ADD CONSTRAINT content_items_type_check
            CHECK (content_type IN ('lesson','meditation','recipe','nutrition_material')) NOT VALID;
    END IF;
END
$$;

ALTER TABLE content_items VALIDATE CONSTRAINT content_items_type_check;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='content_items'::regclass
          AND conname='content_items_nutrition_duration_check'
    ) THEN
        ALTER TABLE content_items ADD CONSTRAINT content_items_nutrition_duration_check
            CHECK (content_type <> 'nutrition_material' OR duration_seconds IS NULL) NOT VALID;
    END IF;
END
$$;

ALTER TABLE content_items VALIDATE CONSTRAINT content_items_nutrition_duration_check;

CREATE TABLE IF NOT EXISTS nutrition_material_bodies (
    content_id UUID PRIMARY KEY REFERENCES content_items(content_id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT nutrition_material_bodies_length_check
        CHECK (length(btrim(body)) BETWEEN 1 AND 30000)
);
