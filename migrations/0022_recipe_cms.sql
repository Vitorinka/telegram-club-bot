DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='content_items'::regclass
          AND conname='content_items_type_check'
          AND pg_get_constraintdef(oid) LIKE '%recipe%'
    ) THEN
        ALTER TABLE content_items DROP CONSTRAINT IF EXISTS content_items_type_check;
        ALTER TABLE content_items ADD CONSTRAINT content_items_type_check
            CHECK (content_type IN ('lesson', 'meditation', 'recipe')) NOT VALID;
    END IF;
END
$$;

ALTER TABLE content_items VALIDATE CONSTRAINT content_items_type_check;

CREATE TABLE IF NOT EXISTS recipe_ingredients (
    ingredient_id UUID PRIMARY KEY,
    content_id UUID NOT NULL REFERENCES content_items(content_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    amount TEXT,
    sort_order INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT recipe_ingredients_name_check CHECK (length(btrim(name)) BETWEEN 1 AND 200),
    CONSTRAINT recipe_ingredients_amount_check CHECK (amount IS NULL OR length(amount) <= 100),
    CONSTRAINT recipe_ingredients_sort_check CHECK (sort_order BETWEEN 0 AND 100000),
    CONSTRAINT recipe_ingredients_order_unique UNIQUE (content_id, sort_order)
);

CREATE TABLE IF NOT EXISTS recipe_steps (
    step_id UUID PRIMARY KEY,
    content_id UUID NOT NULL REFERENCES content_items(content_id) ON DELETE CASCADE,
    step_number INTEGER NOT NULL,
    instruction TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT recipe_steps_number_check CHECK (step_number BETWEEN 1 AND 50),
    CONSTRAINT recipe_steps_instruction_check CHECK (length(btrim(instruction)) BETWEEN 1 AND 2000),
    CONSTRAINT recipe_steps_number_unique UNIQUE (content_id, step_number)
);

CREATE INDEX IF NOT EXISTS recipe_ingredients_content_order_idx
ON recipe_ingredients (content_id, sort_order, ingredient_id);

CREATE INDEX IF NOT EXISTS recipe_steps_content_number_idx
ON recipe_steps (content_id, step_number, step_id);
