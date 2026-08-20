ALTER TABLE subscription_removal_events
    ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT;

ALTER TABLE subscription_removal_events
    ADD COLUMN IF NOT EXISTS access_expiry TIMESTAMP;

ALTER TABLE subscription_removal_events
    ADD COLUMN IF NOT EXISTS stripe_canceled_at TIMESTAMP;

ALTER TABLE subscription_removal_events
    ADD COLUMN IF NOT EXISTS telegram_banned_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS subscription_removal_events_retry_idx
ON subscription_removal_events (lease_until, updated_at, telegram_id)
WHERE status IN ('pending', 'processing', 'stripe_canceled', 'telegram_failed', 'telegram_removed');
