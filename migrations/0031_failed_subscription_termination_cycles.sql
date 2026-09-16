ALTER TABLE failed_subscription_terminations
ADD COLUMN IF NOT EXISTS failure_cycle_started_at TIMESTAMP;

UPDATE failed_subscription_terminations
SET failure_cycle_started_at = created_at
WHERE failure_cycle_started_at IS NULL;

ALTER TABLE failed_subscription_terminations
ALTER COLUMN failure_cycle_started_at SET NOT NULL;

ALTER TABLE failed_subscription_terminations
ALTER COLUMN failure_cycle_started_at SET DEFAULT NOW();

DROP INDEX IF EXISTS failed_subscription_terminations_subscription_uidx;

CREATE UNIQUE INDEX IF NOT EXISTS failed_subscription_terminations_subscription_cycle_uidx
ON failed_subscription_terminations (stripe_subscription_id, failure_cycle_started_at);
