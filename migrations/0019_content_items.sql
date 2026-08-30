CREATE TABLE IF NOT EXISTS content_items (
    content_id UUID PRIMARY KEY,
    content_type TEXT NOT NULL,
    category TEXT,
    title TEXT NOT NULL,
    description TEXT,
    duration_seconds INTEGER,
    sort_order INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'draft',
    version INTEGER NOT NULL DEFAULT 1,
    created_by_telegram_id BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP,
    archived_at TIMESTAMP,
    CONSTRAINT content_items_type_check
        CHECK (content_type IN ('lesson')),
    CONSTRAINT content_items_category_check
        CHECK (category IS NULL OR category ~ '^[a-z][a-z0-9_]{0,47}$'),
    CONSTRAINT content_items_title_check
        CHECK (length(btrim(title)) BETWEEN 1 AND 120),
    CONSTRAINT content_items_description_check
        CHECK (description IS NULL OR length(description) <= 5000),
    CONSTRAINT content_items_duration_check
        CHECK (duration_seconds IS NULL OR duration_seconds BETWEEN 1 AND 86400),
    CONSTRAINT content_items_sort_order_check
        CHECK (sort_order BETWEEN -100000 AND 100000),
    CONSTRAINT content_items_status_check
        CHECK (status IN ('draft', 'published', 'archived')),
    CONSTRAINT content_items_version_check
        CHECK (version >= 1),
    CONSTRAINT content_items_lifecycle_timestamps_check
        CHECK (
            (status = 'draft' AND published_at IS NULL AND archived_at IS NULL)
            OR (status = 'published' AND published_at IS NOT NULL AND archived_at IS NULL)
            OR (status = 'archived' AND archived_at IS NOT NULL)
        )
);

CREATE INDEX IF NOT EXISTS content_items_status_updated_idx
ON content_items (status, updated_at DESC, content_id DESC);

CREATE INDEX IF NOT EXISTS content_items_category_sort_idx
ON content_items (category, sort_order, created_at, content_id);
