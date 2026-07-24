BEGIN;

CREATE TABLE IF NOT EXISTS access_removal_jobs (
    job_key TEXT PRIMARY KEY,
    telegram_id BIGINT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    last_error TEXT,
    attempt_count INTEGER DEFAULT 0,
    lease_until TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS access_removal_jobs_status_idx
ON access_removal_jobs (status, updated_at);

CREATE INDEX IF NOT EXISTS access_removal_jobs_telegram_id_idx
ON access_removal_jobs (telegram_id);

CREATE TABLE IF NOT EXISTS notification_outbox (
    id BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT UNIQUE NOT NULL,
    recipient_id BIGINT,
    recipient_type TEXT NOT NULL DEFAULT 'telegram_user',
    notification_type TEXT NOT NULL,
    stripe_event_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER DEFAULT 0,
    lease_until TIMESTAMP,
    last_error TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    sent_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS notification_outbox_status_idx
ON notification_outbox (status, updated_at);

ALTER TABLE stripe_events ADD COLUMN IF NOT EXISTS attempt_count INTEGER DEFAULT 0;
ALTER TABLE stripe_events ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE stripe_events ADD COLUMN IF NOT EXISTS dead_letter BOOLEAN DEFAULT FALSE;
ALTER TABLE stripe_events ADD COLUMN IF NOT EXISTS dead_letter_at TIMESTAMP;

CREATE TABLE IF NOT EXISTS stripe_manual_review_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    object_id TEXT,
    stripe_customer_id TEXT,
    stripe_subscription_id TEXT,
    telegram_id BIGINT,
    reason TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

COMMIT;

