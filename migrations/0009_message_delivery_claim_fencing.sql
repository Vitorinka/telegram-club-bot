ALTER TABLE message_delivery_events
ADD COLUMN IF NOT EXISTS claim_generation BIGINT NOT NULL DEFAULT 0;
