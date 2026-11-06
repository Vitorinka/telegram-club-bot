ALTER TABLE bookable_classes
    ADD COLUMN IF NOT EXISTS booking_model TEXT NOT NULL DEFAULT 'legacy_capture_now',
    ADD COLUMN IF NOT EXISTS lifecycle_status TEXT,
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS settlement_due_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS unlimited_capacity BOOLEAN NOT NULL DEFAULT FALSE;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bookable_classes'::regclass
          AND conname = 'bookable_classes_booking_model_check'
    ) THEN
        ALTER TABLE bookable_classes ADD CONSTRAINT bookable_classes_booking_model_check
            CHECK (booking_model IN ('legacy_capture_now', 'authorization_v2'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'bookable_classes'::regclass
          AND conname = 'bookable_classes_v2_shape_check'
    ) THEN
        ALTER TABLE bookable_classes ADD CONSTRAINT bookable_classes_v2_shape_check CHECK (
            (booking_model = 'legacy_capture_now'
             AND lifecycle_status IS NULL
             AND unlimited_capacity = FALSE)
            OR
            (booking_model = 'authorization_v2'
             AND lifecycle_status IN (
                 'draft', 'open', 'in_progress', 'settlement_pending',
                 'settlement_review', 'completed', 'cancelled'
             )
             AND duration_minutes = 60
             AND price_amount = 1000
             AND currency = 'eur'
             AND unlimited_capacity = TRUE)
        );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS bookable_classes_v2_lifecycle_idx
ON bookable_classes (lifecycle_status, starts_at, class_id)
WHERE booking_model = 'authorization_v2';

ALTER TABLE class_bookings
    ADD COLUMN IF NOT EXISTS booking_model TEXT NOT NULL DEFAULT 'legacy_capture_now',
    ADD COLUMN IF NOT EXISTS booking_type TEXT,
    ADD COLUMN IF NOT EXISTS access_status TEXT,
    ADD COLUMN IF NOT EXISTS payment_state TEXT,
    ADD COLUMN IF NOT EXISTS payment_generation INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stripe_authorized_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS authorization_expires_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS consent_version TEXT,
    ADD COLUMN IF NOT EXISTS consented_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS cancellation_requested_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS cancellation_reason TEXT,
    ADD COLUMN IF NOT EXISTS attendance_status TEXT,
    ADD COLUMN IF NOT EXISTS attendance_recorded_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS attendance_recorded_by BIGINT,
    ADD COLUMN IF NOT EXISTS settlement_status TEXT,
    ADD COLUMN IF NOT EXISTS settlement_due_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS settled_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_access_check_at TIMESTAMP;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'class_bookings'::regclass
          AND conname = 'class_bookings_booking_model_check'
    ) THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_booking_model_check
            CHECK (booking_model IN ('legacy_capture_now', 'authorization_v2'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'class_bookings'::regclass
          AND conname = 'class_bookings_v2_shape_check'
    ) THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_v2_shape_check CHECK (
            (booking_model = 'legacy_capture_now'
             AND booking_type IS NULL
             AND access_status IS NULL
             AND payment_state IS NULL
             AND settlement_status IS NULL)
            OR
            (booking_model = 'authorization_v2'
             AND booking_type IN ('subscription', 'guest')
             AND access_status IN (
                 'access_required', 'booked_subscription', 'authorization_pending',
                 'booked_guest', 'cancelled_early', 'cancelled_late',
                 'organizer_cancelled', 'attended', 'no_show'
             )
             AND payment_state IN (
                 'none', 'authorization_creating', 'requires_action', 'authorized',
                 'cancel_pending', 'cancelled', 'capture_pending', 'captured',
                 'expired', 'retryable_failed', 'manual_review'
             )
             AND settlement_status IN (
                 'none', 'pending', 'processing', 'cancelled', 'captured',
                 'retryable_failed', 'manual_review'
             ))
        );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'class_bookings'::regclass
          AND conname = 'class_bookings_payment_generation_check'
    ) THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_payment_generation_check
            CHECK (payment_generation >= 0);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'class_bookings'::regclass
          AND conname = 'class_bookings_authorization_window_check'
    ) THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_authorization_window_check
            CHECK (authorization_expires_at IS NULL OR stripe_authorized_at IS NULL
                   OR authorization_expires_at > stripe_authorized_at);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'class_bookings'::regclass
          AND conname = 'class_bookings_consent_check'
    ) THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_consent_check CHECK (
            (consent_version IS NULL AND consented_at IS NULL)
            OR
            (consent_version IS NOT NULL
             AND length(btrim(consent_version)) BETWEEN 1 AND 100
             AND consented_at IS NOT NULL)
        );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'class_bookings'::regclass
          AND conname = 'class_bookings_cancellation_reason_check'
    ) THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_cancellation_reason_check
            CHECK (cancellation_reason IS NULL OR length(cancellation_reason) BETWEEN 1 AND 500);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'class_bookings'::regclass
          AND conname = 'class_bookings_v2_guest_consent_check'
    ) THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_v2_guest_consent_check CHECK (
            booking_model <> 'authorization_v2'
            OR booking_type <> 'guest'
            OR (payment_state = 'none' AND access_status = 'access_required')
            OR (consent_version IS NOT NULL AND consented_at IS NOT NULL)
        );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'class_bookings'::regclass
          AND conname = 'class_bookings_attendance_check'
    ) THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_attendance_check CHECK (
            attendance_status IS NULL
            OR attendance_status IN ('attended', 'no_show', 'excused')
        );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'class_bookings'::regclass
          AND conname = 'class_bookings_booking_intent_unique'
    ) THEN
        ALTER TABLE class_bookings ADD CONSTRAINT class_bookings_booking_intent_unique
            UNIQUE (booking_id, stripe_payment_intent_id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS class_bookings_v2_access_idx
ON class_bookings (access_status, updated_at, booking_id)
WHERE booking_model = 'authorization_v2'
  AND access_status IN ('access_required', 'booked_subscription', 'authorization_pending');

CREATE INDEX IF NOT EXISTS class_bookings_v2_authorization_expiry_idx
ON class_bookings (authorization_expires_at, booking_id)
WHERE booking_model = 'authorization_v2'
  AND payment_state = 'authorized';

CREATE INDEX IF NOT EXISTS class_bookings_v2_settlement_idx
ON class_bookings (settlement_status, settlement_due_at, booking_id)
WHERE booking_model = 'authorization_v2'
  AND settlement_status IN ('pending', 'processing', 'retryable_failed');

CREATE TABLE IF NOT EXISTS class_payment_operations (
    operation_id UUID PRIMARY KEY,
    booking_id UUID NOT NULL REFERENCES class_bookings(booking_id),
    operation_type TEXT NOT NULL,
    payment_generation INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'pending',
    stripe_payment_intent_id TEXT,
    stripe_event_id TEXT UNIQUE,
    amount INTEGER NOT NULL DEFAULT 1000,
    currency CHAR(3) NOT NULL DEFAULT 'eur',
    owner_id TEXT,
    lease_until TIMESTAMP,
    claim_generation INTEGER NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP,
    last_error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    CONSTRAINT class_payment_operations_type_check
        CHECK (operation_type IN ('authorize', 'cancel', 'capture')),
    CONSTRAINT class_payment_operations_status_check CHECK (
        status IN (
            'pending', 'processing', 'requires_action', 'retryable_failed',
            'completed', 'manual_review', 'superseded'
        )
    ),
    CONSTRAINT class_payment_operations_generation_check CHECK (
        payment_generation >= 0 AND claim_generation >= 0 AND attempt_count >= 0
    ),
    CONSTRAINT class_payment_operations_amount_check
        CHECK (amount = 1000 AND currency = 'eur'),
    CONSTRAINT class_payment_operations_idempotency_key_check
        CHECK (length(btrim(idempotency_key)) BETWEEN 1 AND 200),
    CONSTRAINT class_payment_operations_error_check
        CHECK (last_error IS NULL OR length(last_error) <= 2000),
    CONSTRAINT class_payment_operations_cycle_unique
        UNIQUE (booking_id, operation_type, payment_generation)
);

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'class_payment_operations'::regclass
          AND conname = 'class_payment_operations_booking_intent_fk'
    ) THEN
        ALTER TABLE class_payment_operations
            ADD CONSTRAINT class_payment_operations_booking_intent_fk
            FOREIGN KEY (booking_id, stripe_payment_intent_id)
            REFERENCES class_bookings (booking_id, stripe_payment_intent_id);
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS class_payment_operations_intent_type_uidx
ON class_payment_operations (stripe_payment_intent_id, operation_type)
WHERE stripe_payment_intent_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS class_payment_operations_due_idx
ON class_payment_operations (status, next_attempt_at, lease_until, created_at, operation_id)
WHERE status IN ('pending', 'processing', 'retryable_failed');

CREATE INDEX IF NOT EXISTS class_payment_operations_booking_idx
ON class_payment_operations (booking_id, payment_generation, created_at, operation_id);
