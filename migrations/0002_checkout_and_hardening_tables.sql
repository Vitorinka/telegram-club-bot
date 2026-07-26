CREATE TABLE IF NOT EXISTS checkout_sessions (
    id BIGSERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL,
    tariff_code TEXT NOT NULL,
    mode TEXT NOT NULL,
    stripe_session_id TEXT UNIQUE,
    stripe_customer_id TEXT,
    stripe_subscription_id TEXT,
    idempotency_key TEXT UNIQUE NOT NULL,
    checkout_url TEXT,
    status TEXT NOT NULL,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    last_error TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS checkout_sessions_one_open_tariff
ON checkout_sessions (telegram_id, tariff_code)
WHERE status IN ('creating', 'creation_unknown', 'open');

CREATE TABLE IF NOT EXISTS checkout_retry_events (
    id BIGSERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL,
    tariff_code TEXT NOT NULL,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    attempt_at TIMESTAMP DEFAULT NOW(),
    last_admin_alert_at TIMESTAMP,
    resolved_at TIMESTAMP,
    resolved_source TEXT
);

CREATE INDEX IF NOT EXISTS checkout_retry_events_user_attempt_idx
ON checkout_retry_events (telegram_id, attempt_at DESC);

CREATE TABLE IF NOT EXISTS trial_redemptions (
    telegram_id BIGINT PRIMARY KEY,
    stripe_event_id TEXT UNIQUE,
    checkout_session_id TEXT,
    redeemed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admin_action_requests (
    action_id UUID PRIMARY KEY,
    admin_id BIGINT NOT NULL,
    action_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scheduled_job_runs (
    job_key TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    schedule_slot TEXT NOT NULL,
    status TEXT NOT NULL,
    owner_id TEXT,
    lease_until TIMESTAMP,
    started_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    error_text TEXT
);

CREATE TABLE IF NOT EXISTS message_delivery_events (
    delivery_key TEXT PRIMARY KEY,
    telegram_id BIGINT NOT NULL,
    delivery_type TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER DEFAULT 0,
    claimed_at TIMESTAMP,
    lease_until TIMESTAMP,
    sent_at TIMESTAMP,
    next_attempt_at TIMESTAMP,
    payload_json TEXT,
    invite_link TEXT,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS subscription_removal_events (
    telegram_id BIGINT PRIMARY KEY,
    status TEXT NOT NULL,
    reason TEXT,
    owner_id TEXT,
    claimed_at TIMESTAMP,
    lease_until TIMESTAMP,
    telegram_removed_at TIMESTAMP,
    db_finalized_at TIMESTAMP,
    admin_notified_at TIMESTAMP,
    attempt_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_invite_links (
    invite_link TEXT PRIMARY KEY,
    source TEXT,
    telegram_id BIGINT,
    status TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP,
    revoked_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS admin_alerts (
    id BIGSERIAL PRIMARY KEY,
    alert_key TEXT,
    severity TEXT,
    text TEXT,
    status TEXT,
    delivered_admin_ids TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
