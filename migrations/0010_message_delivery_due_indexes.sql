CREATE INDEX IF NOT EXISTS message_delivery_events_pending_failed_due_idx
ON message_delivery_events
(next_attempt_at ASC NULLS FIRST, delivery_key)
WHERE status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS message_delivery_events_processing_lease_idx
ON message_delivery_events
(lease_until, delivery_key)
WHERE status = 'processing';
