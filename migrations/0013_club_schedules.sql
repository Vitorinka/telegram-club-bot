CREATE TABLE IF NOT EXISTS club_schedules (
    schedule_month VARCHAR(7) PRIMARY KEY,
    telegram_file_id TEXT NOT NULL,
    uploaded_by_telegram_id BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT club_schedules_month_format_check
        CHECK (schedule_month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$')
);
