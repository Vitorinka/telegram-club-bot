CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE NOT NULL,
    paid BOOLEAN DEFAULT FALSE,
    expiry_date TIMESTAMP,
    stripe_subscription_id TEXT,
    stripe_customer_id TEXT,
    reminder_sent BOOLEAN DEFAULT FALSE,
    payment_failed BOOLEAN DEFAULT FALSE,
    payment_failed_at TIMESTAMP,
    last_payment_succeeded_at TIMESTAMP,
    grace_period_end TIMESTAMP,
    auto_renew BOOLEAN DEFAULT TRUE,
    trial_used BOOLEAN DEFAULT FALSE,
    first_payment_done BOOLEAN DEFAULT FALSE,
    registered_at TIMESTAMP DEFAULT NOW(),
    blocked_bot BOOLEAN DEFAULT FALSE,
    video_sent BOOLEAN DEFAULT FALSE,
    video_sent_at TIMESTAMP,
    feedback_sent BOOLEAN DEFAULT FALSE,
    feedback_sent_at TIMESTAMP,
    feedback_received BOOLEAN DEFAULT FALSE,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    profile_updated_at TIMESTAMP,
    last_successful_invoice_created_at TIMESTAMP,
    last_subscription_state_event_created_at TIMESTAMP,
    last_payment_failure_event_created_at TIMESTAMP,
    manual_sync_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stripe_events (
    event_id TEXT PRIMARY KEY,
    processed BOOLEAN DEFAULT TRUE,
    processed_at TIMESTAMP DEFAULT NOW(),
    event_created_at TIMESTAMP,
    event_type TEXT,
    object_id TEXT
);

CREATE TABLE IF NOT EXISTS access_events (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT,
    old_expiry TIMESTAMP,
    new_expiry TIMESTAMP,
    stripe_event_id TEXT,
    stripe_subscription_id TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stripe_links (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT NOT NULL,
    stripe_customer_id TEXT,
    stripe_subscription_id TEXT,
    customer_email TEXT,
    status TEXT,
    current_period_end TIMESTAMP,
    is_active BOOLEAN DEFAULT FALSE,
    source TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (telegram_id, stripe_customer_id, stripe_subscription_id)
);

CREATE TABLE IF NOT EXISTS unlinked_stripe_events (
    id SERIAL PRIMARY KEY,
    event_id TEXT UNIQUE,
    event_type TEXT,
    invoice_id TEXT,
    stripe_customer_id TEXT,
    stripe_subscription_id TEXT,
    customer_email TEXT,
    amount_paid BIGINT,
    currency TEXT,
    billing_reason TEXT,
    period_end TIMESTAMP,
    raw_summary TEXT,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_by BIGINT,
    resolved_telegram_id BIGINT,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payment_events (
    id BIGSERIAL PRIMARY KEY,
    stripe_event_id TEXT UNIQUE NOT NULL,
    event_type TEXT NOT NULL,
    telegram_id BIGINT,
    invoice_id TEXT,
    checkout_session_id TEXT,
    stripe_customer_id TEXT,
    stripe_subscription_id TEXT,
    payment_status TEXT NOT NULL,
    payment_kind TEXT,
    billing_reason TEXT,
    tariff_code TEXT,
    amount_paid BIGINT DEFAULT 0,
    amount_due BIGINT DEFAULT 0,
    currency TEXT,
    period_start TIMESTAMP,
    period_end TIMESTAMP,
    recovered_after_failure BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS payment_events_created_at_idx ON payment_events (created_at);
CREATE INDEX IF NOT EXISTS payment_events_telegram_id_idx ON payment_events (telegram_id);
CREATE INDEX IF NOT EXISTS payment_events_status_kind_idx ON payment_events (payment_status, payment_kind);

CREATE TABLE IF NOT EXISTS weekly_report_runs (
    report_key TEXT PRIMARY KEY,
    period_start TIMESTAMP NOT NULL,
    period_end TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    sent_admin_ids TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    error_text TEXT
);

CREATE TABLE IF NOT EXISTS system_settings (
    key TEXT PRIMARY KEY,
    value_text TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO system_settings (key, value_text)
VALUES ('payment_history_started_at', NOW()::TEXT)
ON CONFLICT (key) DO NOTHING;
