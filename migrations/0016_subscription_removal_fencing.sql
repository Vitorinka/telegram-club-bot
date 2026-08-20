ALTER TABLE subscription_removal_events
    ADD COLUMN IF NOT EXISTS claim_generation BIGINT NOT NULL DEFAULT 0;

ALTER TABLE scheduled_job_runs
    ADD COLUMN IF NOT EXISTS claim_generation BIGINT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'subscription_removal_events_status_check'
          AND conrelid = 'subscription_removal_events'::regclass
    ) THEN
        ALTER TABLE subscription_removal_events
            ADD CONSTRAINT subscription_removal_events_status_check
            CHECK (status IN (
                'pending', 'processing', 'stripe_canceled', 'telegram_failed',
                'telegram_removed', 'db_finalized', 'cancelled', 'not_due', 'superseded'
            )) NOT VALID;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'subscription_removal_events_claim_generation_nonnegative_check'
          AND conrelid = 'subscription_removal_events'::regclass
    ) THEN
        ALTER TABLE subscription_removal_events
            ADD CONSTRAINT subscription_removal_events_claim_generation_nonnegative_check
            CHECK (claim_generation >= 0) NOT VALID;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'scheduled_job_runs_claim_generation_nonnegative_check'
          AND conrelid = 'scheduled_job_runs'::regclass
    ) THEN
        ALTER TABLE scheduled_job_runs
            ADD CONSTRAINT scheduled_job_runs_claim_generation_nonnegative_check
            CHECK (claim_generation >= 0) NOT VALID;
    END IF;
END
$$;
