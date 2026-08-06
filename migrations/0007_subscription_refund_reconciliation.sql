CREATE TABLE IF NOT EXISTS subscription_refund_reconciliations (
    id BIGSERIAL PRIMARY KEY,
    reconciliation_key TEXT NOT NULL,
    refund_id TEXT,
    stripe_event_id TEXT,
    charge_id TEXT,
    payment_intent_id TEXT,
    invoice_id TEXT,
    customer_id TEXT,
    subscription_id TEXT,
    telegram_id BIGINT,
    original_payment_event_id BIGINT,
    amount_refunded INTEGER,
    original_amount INTEGER,
    currency TEXT,
    refund_status TEXT,
    is_full_refund BOOLEAN DEFAULT FALSE,
    reconciliation_result TEXT NOT NULL,
    review_reason TEXT,
    access_revoked_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS reconciliation_key TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS refund_id TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS stripe_event_id TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS charge_id TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS payment_intent_id TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS invoice_id TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS customer_id TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS subscription_id TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS telegram_id BIGINT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS original_payment_event_id BIGINT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS amount_refunded INTEGER;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS original_amount INTEGER;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS currency TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS refund_status TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS is_full_refund BOOLEAN DEFAULT FALSE;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS reconciliation_result TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS review_reason TEXT;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS access_revoked_at TIMESTAMP;
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
ALTER TABLE subscription_refund_reconciliations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

UPDATE subscription_refund_reconciliations
SET reconciliation_key = COALESCE(
    reconciliation_key,
    CASE
        WHEN refund_id IS NOT NULL THEN 'refund:' || refund_id
        WHEN charge_id IS NOT NULL THEN 'charge:' || charge_id
        ELSE 'event:' || stripe_event_id
    END
)
WHERE reconciliation_key IS NULL;

ALTER TABLE subscription_refund_reconciliations ALTER COLUMN reconciliation_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS subscription_refund_reconciliations_unique_key
ON subscription_refund_reconciliations(reconciliation_key);

CREATE UNIQUE INDEX IF NOT EXISTS subscription_refund_reconciliations_unique_refund_id
ON subscription_refund_reconciliations(refund_id)
WHERE refund_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS srr_unique_refund_payment_revoke
ON subscription_refund_reconciliations(original_payment_event_id)
WHERE original_payment_event_id IS NOT NULL
  AND reconciliation_result = 'access_revoked';

CREATE TABLE IF NOT EXISTS subscription_refund_events (
    id BIGSERIAL PRIMARY KEY,
    reconciliation_id BIGINT REFERENCES subscription_refund_reconciliations(id) ON DELETE CASCADE,
    stripe_event_id TEXT NOT NULL,
    event_type TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE subscription_refund_events ADD COLUMN IF NOT EXISTS reconciliation_id BIGINT;
ALTER TABLE subscription_refund_events ADD COLUMN IF NOT EXISTS stripe_event_id TEXT;
ALTER TABLE subscription_refund_events ADD COLUMN IF NOT EXISTS event_type TEXT;
ALTER TABLE subscription_refund_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();

CREATE UNIQUE INDEX IF NOT EXISTS subscription_refund_events_unique_stripe_event_id
ON subscription_refund_events(stripe_event_id);

ALTER TABLE subscription_removal_events ADD COLUMN IF NOT EXISTS revoke_started_at TIMESTAMP;
