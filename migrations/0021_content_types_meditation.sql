DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'content_items'::regclass
          AND conname = 'content_items_type_check'
          AND pg_get_constraintdef(oid) LIKE '%meditation%'
    ) THEN
        ALTER TABLE content_items
        DROP CONSTRAINT IF EXISTS content_items_type_check;
        ALTER TABLE content_items
        ADD CONSTRAINT content_items_type_check
        CHECK (content_type IN ('lesson', 'meditation')) NOT VALID;
    END IF;
END
$$;

ALTER TABLE content_items
VALIDATE CONSTRAINT content_items_type_check;
