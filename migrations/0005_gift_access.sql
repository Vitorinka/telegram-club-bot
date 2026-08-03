CREATE TABLE IF NOT EXISTS gift_access_grants (
    id UUID PRIMARY KEY,
    public_reference TEXT NOT NULL UNIQUE,
    purchaser_telegram_id BIGINT NOT NULL,
    recipient_telegram_id BIGINT,
    recipient_name TEXT,
    sender_name TEXT,
    gift_message TEXT,
    tariff_code TEXT NOT NULL,
    duration_days INTEGER NOT NULL,
    status TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    token_version INTEGER NOT NULL DEFAULT 1,
    stripe_session_id TEXT UNIQUE,
    stripe_payment_intent_id TEXT UNIQUE,
    amount_total INTEGER,
    currency TEXT,
    paid_at TIMESTAMP,
    reserved_at TIMESTAMP,
    redeemed_at TIMESTAMP,
    applied_at TIMESTAMP,
    applied_expiry TIMESTAMP,
    refunded_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    last_error TEXT,
    CHECK (duration_days IN (30, 180, 365)),
    CHECK (status IN (
        'checkout_pending',
        'checkout_open',
        'payment_pending',
        'paid_unclaimed',
        'reserved',
        'redeemed',
        'cancelled',
        'refunded',
        'review_required'
    )),
    CHECK (tariff_code IN ('gift_1m', 'gift_6m', 'gift_12m'))
);

ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS id UUID;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS public_reference TEXT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS purchaser_telegram_id BIGINT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS recipient_telegram_id BIGINT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS recipient_name TEXT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS sender_name TEXT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS gift_message TEXT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS tariff_code TEXT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS duration_days INTEGER;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS token_hash TEXT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS token_version INTEGER DEFAULT 1;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS stripe_session_id TEXT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS stripe_payment_intent_id TEXT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS amount_total INTEGER;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS currency TEXT;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS paid_at TIMESTAMP;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS reserved_at TIMESTAMP;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS redeemed_at TIMESTAMP;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS applied_at TIMESTAMP;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS applied_expiry TIMESTAMP;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS refunded_at TIMESTAMP;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMP;
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
ALTER TABLE gift_access_grants ADD COLUMN IF NOT EXISTS last_error TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS gift_access_grants_checkout_open_idx
ON gift_access_grants (purchaser_telegram_id, tariff_code)
WHERE status IN ('checkout_pending', 'checkout_open', 'payment_pending');

CREATE INDEX IF NOT EXISTS gift_access_grants_purchaser_idx
ON gift_access_grants (purchaser_telegram_id, created_at DESC);

CREATE INDEX IF NOT EXISTS gift_access_grants_recipient_idx
ON gift_access_grants (recipient_telegram_id, created_at DESC);

CREATE INDEX IF NOT EXISTS gift_access_grants_status_idx
ON gift_access_grants (status, updated_at);

CREATE TABLE IF NOT EXISTS gift_certificate_templates (
    tariff_code TEXT PRIMARY KEY,
    file_id TEXT NOT NULL,
    uploaded_by BIGINT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CHECK (tariff_code IN ('gift_1m', 'gift_6m', 'gift_12m'))
);

ALTER TABLE gift_certificate_templates ADD COLUMN IF NOT EXISTS tariff_code TEXT;
ALTER TABLE gift_certificate_templates ADD COLUMN IF NOT EXISTS file_id TEXT;
ALTER TABLE gift_certificate_templates ADD COLUMN IF NOT EXISTS uploaded_by BIGINT;
ALTER TABLE gift_certificate_templates ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE;
ALTER TABLE gift_certificate_templates ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
ALTER TABLE gift_certificate_templates ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

CREATE TABLE IF NOT EXISTS gift_access_events (
    id BIGSERIAL PRIMARY KEY,
    gift_id UUID NOT NULL,
    public_reference TEXT NOT NULL,
    telegram_id BIGINT,
    event_type TEXT NOT NULL,
    source TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE gift_access_events ADD COLUMN IF NOT EXISTS id BIGSERIAL;
ALTER TABLE gift_access_events ADD COLUMN IF NOT EXISTS gift_id UUID;
ALTER TABLE gift_access_events ADD COLUMN IF NOT EXISTS public_reference TEXT;
ALTER TABLE gift_access_events ADD COLUMN IF NOT EXISTS telegram_id BIGINT;
ALTER TABLE gift_access_events ADD COLUMN IF NOT EXISTS event_type TEXT;
ALTER TABLE gift_access_events ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE gift_access_events ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE gift_access_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();

CREATE INDEX IF NOT EXISTS gift_access_events_gift_idx
ON gift_access_events (gift_id, created_at DESC);

CREATE INDEX IF NOT EXISTS gift_access_events_public_reference_idx
ON gift_access_events (public_reference, created_at DESC);
