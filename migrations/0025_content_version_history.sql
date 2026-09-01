CREATE TABLE IF NOT EXISTS content_item_versions (
    version_id UUID PRIMARY KEY,
    content_id UUID NOT NULL REFERENCES content_items(content_id),
    content_version INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    content_type TEXT NOT NULL,
    status TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    category TEXT,
    duration_seconds INTEGER,
    sort_order INTEGER NOT NULL,
    published_at TIMESTAMP,
    archived_at TIMESTAMP,
    action_id UUID NOT NULL UNIQUE REFERENCES admin_action_requests(action_id),
    created_by_telegram_id BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT content_item_versions_content_version_unique UNIQUE (content_id, content_version),
    CONSTRAINT content_item_versions_version_check CHECK (content_version >= 1),
    CONSTRAINT content_item_versions_event_check CHECK (event_type IN ('publish','archive')),
    CONSTRAINT content_item_versions_type_check CHECK (content_type IN ('lesson','meditation','recipe','nutrition_material')),
    CONSTRAINT content_item_versions_status_check CHECK (status IN ('published','archived')),
    CONSTRAINT content_item_versions_event_status_check CHECK (
        (event_type='publish' AND status='published' AND published_at IS NOT NULL AND archived_at IS NULL)
        OR (event_type='archive' AND status='archived' AND published_at IS NOT NULL AND archived_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS content_item_versions_content_created_idx
ON content_item_versions (content_id, content_version DESC, created_at DESC);

CREATE TABLE IF NOT EXISTS content_item_version_media (
    version_id UUID NOT NULL REFERENCES content_item_versions(version_id),
    media_id UUID NOT NULL REFERENCES content_media(media_id),
    media_type TEXT NOT NULL,
    media_version INTEGER NOT NULL,
    sort_order INTEGER NOT NULL,
    mime_type TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    sha256 CHAR(64) NOT NULL,
    PRIMARY KEY (version_id, media_type),
    CONSTRAINT content_item_version_media_type_check CHECK (media_type IN ('cover','video','audio')),
    CONSTRAINT content_item_version_media_version_check CHECK (media_version >= 1),
    CONSTRAINT content_item_version_media_size_check CHECK (size_bytes > 0),
    CONSTRAINT content_item_version_media_sha_check CHECK (sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS content_item_version_recipe_ingredients (
    version_id UUID NOT NULL REFERENCES content_item_versions(version_id),
    position INTEGER NOT NULL,
    name TEXT NOT NULL,
    amount TEXT,
    sort_order INTEGER NOT NULL,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE IF NOT EXISTS content_item_version_recipe_steps (
    version_id UUID NOT NULL REFERENCES content_item_versions(version_id),
    position INTEGER NOT NULL,
    step_number INTEGER NOT NULL,
    instruction TEXT NOT NULL,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE IF NOT EXISTS content_item_version_nutrition (
    version_id UUID PRIMARY KEY REFERENCES content_item_versions(version_id),
    body TEXT NOT NULL
);

CREATE OR REPLACE FUNCTION prevent_content_version_history_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'content version history is append-only';
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'content_item_versions', 'content_item_version_media',
        'content_item_version_recipe_ingredients',
        'content_item_version_recipe_steps', 'content_item_version_nutrition'
    ] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid=table_name::regclass
              AND tgname='prevent_content_version_history_mutation_trigger'
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER prevent_content_version_history_mutation_trigger '
                'BEFORE UPDATE OR DELETE ON %I FOR EACH ROW '
                'EXECUTE FUNCTION prevent_content_version_history_mutation()', table_name
            );
        END IF;
    END LOOP;
END
$$;
