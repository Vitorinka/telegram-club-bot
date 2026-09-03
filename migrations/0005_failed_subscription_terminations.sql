CREATE TABLE IF NOT EXISTS failed_subscription_terminations (
    operation_id TEXT PRIMARY KEY,
    telegram_id BIGINT NOT NULL,
    stripe_subscription_id TEXT NOT NULL,
    failed_invoice_id TEXT,
    reason TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    owner_id TEXT,
    claim_generation BIGINT NOT NULL DEFAULT 0,
    lease_until TIMESTAMP,
    access_expiry TIMESTAMP,
    stripe_cancelled_at TIMESTAMP,
    collection_stopped_at TIMESTAMP,
    telegram_banned_at TIMESTAMP,
    telegram_removed_at TIMESTAMP,
    db_finalized_at TIMESTAMP,
    completed_at TIMESTAMP,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error_category TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS operation_id TEXT;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS telegram_id BIGINT;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS failed_invoice_id TEXT;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS reason TEXT;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending';
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS owner_id TEXT;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS claim_generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS lease_until TIMESTAMP;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS access_expiry TIMESTAMP;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS stripe_cancelled_at TIMESTAMP;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS collection_stopped_at TIMESTAMP;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS telegram_banned_at TIMESTAMP;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS telegram_removed_at TIMESTAMP;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS db_finalized_at TIMESTAMP;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS last_error_category TEXT;
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE failed_subscription_terminations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS failed_subscription_terminations_due_idx
ON failed_subscription_terminations (status, lease_until, updated_at);

CREATE UNIQUE INDEX IF NOT EXISTS failed_subscription_terminations_subscription_uidx
ON failed_subscription_terminations (stripe_subscription_id);

CREATE INDEX IF NOT EXISTS failed_subscription_terminations_user_idx
ON failed_subscription_terminations (telegram_id, created_at DESC);
