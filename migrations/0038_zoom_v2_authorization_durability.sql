ALTER TABLE class_bookings
    ADD COLUMN IF NOT EXISTS consent_terms_hash CHAR(64),
    ADD COLUMN IF NOT EXISTS consent_terms_code TEXT,
    ADD COLUMN IF NOT EXISTS consent_class_ends_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS consent_cancellation_cutoff_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS stripe_capture_before TIMESTAMP,
    ADD COLUMN IF NOT EXISTS capture_before_verified_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS capture_before_source TEXT,
    ADD COLUMN IF NOT EXISTS authorization_reconciliation_status TEXT,
    ADD COLUMN IF NOT EXISTS authorization_last_reconciled_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS authorization_review_reason TEXT;

ALTER TABLE class_payment_operations
    ADD COLUMN IF NOT EXISTS stripe_request_id TEXT,
    ADD COLUMN IF NOT EXISTS request_started_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS response_recorded_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS reconciliation_status TEXT,
    ADD COLUMN IF NOT EXISTS stripe_last_checked_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS manual_review_reason TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS class_payment_operations_stripe_request_uidx
ON class_payment_operations (stripe_request_id)
WHERE stripe_request_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS class_payment_operations_reconciliation_idx
ON class_payment_operations (reconciliation_status, stripe_last_checked_at, operation_id)
WHERE reconciliation_status IN ('request_uncertain','pending','retryable_failed','manual_review');

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='class_bookings'::regclass AND conname='class_bookings_v2_consent_snapshot_check') THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_v2_consent_snapshot_check CHECK (
            consent_terms_hash IS NULL OR (
                consent_terms_hash ~ '^[0-9a-f]{64}$'
                AND consent_terms_code IS NOT NULL AND length(btrim(consent_terms_code)) BETWEEN 1 AND 100
                AND consent_version IS NOT NULL AND consented_at IS NOT NULL
                AND consent_class_ends_at IS NOT NULL
                AND consent_cancellation_cutoff_at IS NOT NULL
                AND consent_cancellation_cutoff_at < consent_class_ends_at
            )
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='class_bookings'::regclass AND conname='class_bookings_v2_capture_before_check') THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_v2_capture_before_check CHECK (
            stripe_capture_before IS NULL OR (
                capture_before_verified_at IS NOT NULL
                AND capture_before_source IN ('stripe_charge_card_capture_before')
                AND consent_class_ends_at IS NOT NULL
                AND stripe_capture_before >= consent_class_ends_at + INTERVAL '24 hours'
            )
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='class_bookings'::regclass AND conname='class_bookings_v2_reconciliation_check') THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_v2_reconciliation_check CHECK (
            authorization_reconciliation_status IS NULL OR authorization_reconciliation_status IN (
                'pending','verified','insufficient_window','missing_capture_before',
                'retryable_failed','manual_review','cancel_pending','cancelled'
            )
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='class_bookings'::regclass AND conname='class_bookings_v2_guest_capture_fence') THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_v2_guest_capture_fence CHECK (
            booking_model <> 'authorization_v2' OR access_status <> 'booked_guest' OR (
                payment_state='authorized'
                AND authorization_reconciliation_status='verified'
                AND stripe_capture_before IS NOT NULL
                AND capture_before_verified_at IS NOT NULL
                AND stripe_capture_before >= consent_class_ends_at + INTERVAL '24 hours'
            )
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='class_payment_operations'::regclass AND conname='class_payment_operations_reconciliation_check') THEN
        ALTER TABLE class_payment_operations ADD CONSTRAINT class_payment_operations_reconciliation_check CHECK (
            reconciliation_status IS NULL OR reconciliation_status IN (
                'pending','request_uncertain','verified','retryable_failed','manual_review','cancelled'
            )
        );
    END IF;
END $$;
