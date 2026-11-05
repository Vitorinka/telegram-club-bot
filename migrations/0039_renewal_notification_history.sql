ALTER TABLE message_delivery_events
    ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT,
    ADD COLUMN IF NOT EXISTS billing_reference TEXT,
    ADD COLUMN IF NOT EXISTS notification_kind TEXT,
    ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS amount_minor BIGINT,
    ADD COLUMN IF NOT EXISTS currency TEXT,
    ADD COLUMN IF NOT EXISTS telegram_message_id BIGINT;

ALTER TABLE message_delivery_events
    ADD CONSTRAINT message_delivery_events_renewal_kind_check
    CHECK (
        notification_kind IS NULL
        OR notification_kind IN ('renewal_warning_3d', 'renewal_warning_1d')
    ) NOT VALID;

ALTER TABLE message_delivery_events
    ADD CONSTRAINT message_delivery_events_renewal_amount_check
    CHECK (amount_minor IS NULL OR amount_minor >= 0) NOT VALID;

CREATE INDEX IF NOT EXISTS message_delivery_events_renewal_history_idx
ON message_delivery_events (telegram_id, stripe_subscription_id, scheduled_for DESC)
WHERE notification_kind IS NOT NULL;
