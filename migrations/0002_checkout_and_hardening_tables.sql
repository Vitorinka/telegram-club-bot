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

ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS id BIGSERIAL;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS telegram_id BIGINT;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS tariff_code TEXT;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS mode TEXT;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS stripe_session_id TEXT;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS checkout_url TEXT;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP;
ALTER TABLE checkout_sessions ADD COLUMN IF NOT EXISTS last_error TEXT;

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

ALTER TABLE checkout_retry_events ADD COLUMN IF NOT EXISTS id BIGSERIAL;
ALTER TABLE checkout_retry_events ADD COLUMN IF NOT EXISTS telegram_id BIGINT;
ALTER TABLE checkout_retry_events ADD COLUMN IF NOT EXISTS tariff_code TEXT;
ALTER TABLE checkout_retry_events ADD COLUMN IF NOT EXISTS username TEXT;
ALTER TABLE checkout_retry_events ADD COLUMN IF NOT EXISTS first_name TEXT;
ALTER TABLE checkout_retry_events ADD COLUMN IF NOT EXISTS last_name TEXT;
ALTER TABLE checkout_retry_events ADD COLUMN IF NOT EXISTS attempt_at TIMESTAMP DEFAULT NOW();
ALTER TABLE checkout_retry_events ADD COLUMN IF NOT EXISTS last_admin_alert_at TIMESTAMP;
ALTER TABLE checkout_retry_events ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP;
ALTER TABLE checkout_retry_events ADD COLUMN IF NOT EXISTS resolved_source TEXT;

CREATE INDEX IF NOT EXISTS checkout_retry_events_user_attempt_idx
ON checkout_retry_events (telegram_id, attempt_at DESC);

CREATE TABLE IF NOT EXISTS trial_redemptions (
    telegram_id BIGINT PRIMARY KEY,
    stripe_event_id TEXT UNIQUE,
    checkout_session_id TEXT,
    redeemed_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE trial_redemptions ADD COLUMN IF NOT EXISTS stripe_event_id TEXT;
ALTER TABLE trial_redemptions ADD COLUMN IF NOT EXISTS checkout_session_id TEXT;
ALTER TABLE trial_redemptions ADD COLUMN IF NOT EXISTS redeemed_at TIMESTAMP DEFAULT NOW();

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

ALTER TABLE admin_action_requests ADD COLUMN IF NOT EXISTS admin_id BIGINT;
ALTER TABLE admin_action_requests ADD COLUMN IF NOT EXISTS action_type TEXT;
ALTER TABLE admin_action_requests ADD COLUMN IF NOT EXISTS payload_json TEXT;
ALTER TABLE admin_action_requests ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE admin_action_requests ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
ALTER TABLE admin_action_requests ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP;
ALTER TABLE admin_action_requests ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP;

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

ALTER TABLE scheduled_job_runs ADD COLUMN IF NOT EXISTS job_name TEXT;
ALTER TABLE scheduled_job_runs ADD COLUMN IF NOT EXISTS schedule_slot TEXT;
ALTER TABLE scheduled_job_runs ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE scheduled_job_runs ADD COLUMN IF NOT EXISTS owner_id TEXT;
ALTER TABLE scheduled_job_runs ADD COLUMN IF NOT EXISTS lease_until TIMESTAMP;
ALTER TABLE scheduled_job_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMP DEFAULT NOW();
ALTER TABLE scheduled_job_runs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
ALTER TABLE scheduled_job_runs ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP;
ALTER TABLE scheduled_job_runs ADD COLUMN IF NOT EXISTS error_text TEXT;

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

ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS attempt_count INTEGER DEFAULT 0;
ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS telegram_id BIGINT;
ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS delivery_type TEXT;
ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP;
ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS lease_until TIMESTAMP;
ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS sent_at TIMESTAMP;
ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP;
ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS payload_json TEXT;
ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS invite_link TEXT;
ALTER TABLE message_delivery_events ADD COLUMN IF NOT EXISTS last_error TEXT;

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

ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS reason TEXT;
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS owner_id TEXT;
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP;
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS lease_until TIMESTAMP;
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS telegram_removed_at TIMESTAMP;
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS db_finalized_at TIMESTAMP;
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS admin_notified_at TIMESTAMP;
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS attempt_count INTEGER DEFAULT 0;
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

CREATE TABLE IF NOT EXISTS bot_invite_links (
    invite_link TEXT PRIMARY KEY,
    source TEXT,
    telegram_id BIGINT,
    status TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP,
    revoked_at TIMESTAMP
);

ALTER TABLE bot_invite_links ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE bot_invite_links ADD COLUMN IF NOT EXISTS telegram_id BIGINT;
ALTER TABLE bot_invite_links ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE bot_invite_links ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
ALTER TABLE bot_invite_links ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP;
ALTER TABLE bot_invite_links ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMP;

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

ALTER TABLE admin_alerts ADD COLUMN IF NOT EXISTS id BIGSERIAL;
ALTER TABLE admin_alerts ADD COLUMN IF NOT EXISTS alert_key TEXT;
ALTER TABLE admin_alerts ADD COLUMN IF NOT EXISTS severity TEXT;
ALTER TABLE admin_alerts ADD COLUMN IF NOT EXISTS text TEXT;
ALTER TABLE admin_alerts ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE admin_alerts ADD COLUMN IF NOT EXISTS delivered_admin_ids TEXT;
ALTER TABLE admin_alerts ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
ALTER TABLE admin_alerts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
