ALTER TABLE message_delivery_events
ADD CONSTRAINT message_delivery_events_status_check
CHECK (status IN ('pending', 'processing', 'failed', 'sent', 'cancelled', 'permanently_failed'))
NOT VALID;

ALTER TABLE message_delivery_events
ADD CONSTRAINT message_delivery_events_claim_generation_nonnegative_check
CHECK (claim_generation >= 0)
NOT VALID;

ALTER TABLE message_delivery_events
ADD CONSTRAINT message_delivery_events_attempt_count_nonnegative_check
CHECK (attempt_count IS NULL OR attempt_count >= 0)
NOT VALID;

ALTER TABLE message_delivery_events
ADD CONSTRAINT message_delivery_events_processing_lease_check
CHECK (status <> 'processing' OR lease_until IS NOT NULL)
NOT VALID;

ALTER TABLE message_delivery_events
ADD CONSTRAINT message_delivery_events_sent_timestamp_check
CHECK (status <> 'sent' OR sent_at IS NOT NULL)
NOT VALID;

ALTER TABLE stripe_events
ADD CONSTRAINT stripe_events_claim_generation_nonnegative_check
CHECK (claim_generation >= 0)
NOT VALID;

ALTER TABLE stripe_events
ADD CONSTRAINT stripe_events_processed_timestamp_check
CHECK (processed IS NOT TRUE OR processed_at IS NOT NULL)
NOT VALID;

ALTER TABLE payment_events
ADD CONSTRAINT payment_events_payment_status_check
CHECK (payment_status IN ('succeeded', 'failed'))
NOT VALID;
