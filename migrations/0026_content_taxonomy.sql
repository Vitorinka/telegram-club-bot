CREATE TABLE IF NOT EXISTS content_categories (
    category_id UUID PRIMARY KEY,
    content_type TEXT NOT NULL,
    slug TEXT NOT NULL,
    title TEXT NOT NULL,
    group_slug TEXT,
    sort_order INTEGER NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT content_categories_type_check CHECK (content_type IN ('lesson','meditation','recipe','nutrition_material')),
    CONSTRAINT content_categories_slug_check CHECK (slug ~ '^[a-z][a-z0-9_]{0,47}$'),
    CONSTRAINT content_categories_title_check CHECK (length(btrim(title)) BETWEEN 1 AND 80),
    CONSTRAINT content_categories_group_check CHECK (group_slug IS NULL OR group_slug ~ '^[a-z][a-z0-9_]{0,47}$'),
    CONSTRAINT content_categories_sort_check CHECK (sort_order BETWEEN -100000 AND 100000),
    UNIQUE (content_type, slug)
);

CREATE TABLE IF NOT EXISTS content_item_categories (
    content_id UUID NOT NULL REFERENCES content_items(content_id),
    category_id UUID NOT NULL REFERENCES content_categories(category_id),
    sort_order INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (content_id, category_id),
    CONSTRAINT content_item_categories_sort_check CHECK (sort_order IS NULL OR sort_order BETWEEN -100000 AND 100000)
);

CREATE INDEX IF NOT EXISTS content_item_categories_category_content_idx
ON content_item_categories (category_id, content_id);

CREATE TABLE IF NOT EXISTS content_item_version_categories (
    version_id UUID NOT NULL REFERENCES content_item_versions(version_id),
    position INTEGER NOT NULL,
    category_id UUID NOT NULL REFERENCES content_categories(category_id),
    content_type TEXT NOT NULL,
    slug TEXT NOT NULL,
    title TEXT NOT NULL,
    group_slug TEXT,
    sort_order INTEGER NOT NULL,
    PRIMARY KEY (version_id, position),
    UNIQUE (version_id, category_id),
    CONSTRAINT content_item_version_categories_type_check CHECK (content_type IN ('lesson','meditation','recipe','nutrition_material')),
    CONSTRAINT content_item_version_categories_slug_check CHECK (slug ~ '^[a-z][a-z0-9_]{0,47}$'),
    CONSTRAINT content_item_version_categories_title_check CHECK (length(btrim(title)) BETWEEN 1 AND 80),
    CONSTRAINT content_item_version_categories_group_check CHECK (group_slug IS NULL OR group_slug ~ '^[a-z][a-z0-9_]{0,47}$'),
    CONSTRAINT content_item_version_categories_sort_check CHECK (sort_order BETWEEN -100000 AND 100000)
);

INSERT INTO content_categories (category_id,content_type,slug,title,group_slug,sort_order)
VALUES
('10000000-0000-4000-8000-000000000001','lesson','strength','Силовые','workout',10),
('10000000-0000-4000-8000-000000000002','lesson','flexibility','Гибкость','workout',20),
('10000000-0000-4000-8000-000000000003','lesson','glutes','Ягодицы','workout',30),
('10000000-0000-4000-8000-000000000004','lesson','posture','Осанка','workout',40),
('10000000-0000-4000-8000-000000000005','lesson','pelvic_floor','Тазовое дно','workout',50),
('10000000-0000-4000-8000-000000000006','lesson','mobility','Мобилити','workout',60),
('10000000-0000-4000-8000-000000000007','lesson','feet','Стопы','workout',70),
('10000000-0000-4000-8000-000000000008','lesson','recovery','Восстановление','workout',80),
('10000000-0000-4000-8000-000000000009','lesson','first_aid_neck','Шея','first_aid',110),
('10000000-0000-4000-8000-000000000010','lesson','first_aid_back','Спина','first_aid',120),
('10000000-0000-4000-8000-000000000011','lesson','first_aid_lower_back','Поясница','first_aid',130),
('10000000-0000-4000-8000-000000000012','lesson','first_aid_legs','Ноги','first_aid',140),
('20000000-0000-4000-8000-000000000001','recipe','breakfast','Завтраки','recipe',10),
('20000000-0000-4000-8000-000000000002','recipe','salads','Салаты','recipe',20),
('20000000-0000-4000-8000-000000000003','recipe','main_courses','Основные блюда','recipe',30),
('20000000-0000-4000-8000-000000000004','recipe','desserts','Десерты','recipe',40)
ON CONFLICT (content_type,slug) DO NOTHING;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid='content_item_version_categories'::regclass AND tgname='prevent_content_version_history_mutation_trigger') THEN
    CREATE TRIGGER prevent_content_version_history_mutation_trigger BEFORE UPDATE OR DELETE ON content_item_version_categories
    FOR EACH ROW EXECUTE FUNCTION prevent_content_version_history_mutation();
  END IF;
END $$;
