CREATE TABLE IF NOT EXISTS bookable_classes (
    class_id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    starts_at TIMESTAMP NOT NULL,
    duration_minutes INTEGER NOT NULL,
    zoom_url TEXT NOT NULL,
    price_amount INTEGER NOT NULL DEFAULT 1000,
    currency CHAR(3) NOT NULL DEFAULT 'eur',
    capacity INTEGER NOT NULL,
    minimum_participants INTEGER NOT NULL DEFAULT 3,
    booking_deadline TIMESTAMP NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    created_by_telegram_id BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    confirmed_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    completed_at TIMESTAMP,
    CONSTRAINT bookable_classes_title_check CHECK (length(btrim(title)) BETWEEN 1 AND 120),
    CONSTRAINT bookable_classes_description_check CHECK (description IS NULL OR length(description) <= 5000),
    CONSTRAINT bookable_classes_duration_check CHECK (duration_minutes BETWEEN 5 AND 480),
    CONSTRAINT bookable_classes_zoom_check CHECK (length(btrim(zoom_url)) BETWEEN 8 AND 2048),
    CONSTRAINT bookable_classes_price_check CHECK (price_amount > 0 AND currency = lower(currency)),
    CONSTRAINT bookable_classes_capacity_check CHECK (capacity BETWEEN 1 AND 500 AND minimum_participants BETWEEN 1 AND capacity),
    CONSTRAINT bookable_classes_deadline_check CHECK (booking_deadline < starts_at),
    CONSTRAINT bookable_classes_status_check CHECK (status IN ('draft','open','confirmed','cancelled','completed'))
);

CREATE INDEX IF NOT EXISTS bookable_classes_status_start_idx
ON bookable_classes (status, starts_at, class_id);

CREATE TABLE IF NOT EXISTS class_bookings (
    booking_id UUID PRIMARY KEY,
    class_id UUID NOT NULL REFERENCES bookable_classes(class_id),
    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id),
    status TEXT NOT NULL DEFAULT 'pending',
    amount INTEGER NOT NULL,
    currency CHAR(3) NOT NULL,
    stripe_checkout_session_id TEXT UNIQUE,
    stripe_payment_intent_id TEXT UNIQUE,
    checkout_url TEXT,
    checkout_expires_at TIMESTAMP,
    checkout_generation INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    paid_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    refunded_at TIMESTAMP,
    CONSTRAINT class_bookings_status_check CHECK (status IN ('pending','checkout_open','paid','refunded','cancelled','manual_review')),
    CONSTRAINT class_bookings_amount_check CHECK (amount > 0 AND currency = lower(currency)),
    CONSTRAINT class_bookings_checkout_generation_check CHECK (checkout_generation >= 0),
    CONSTRAINT class_bookings_identity_unique UNIQUE (class_id, telegram_id)
);

CREATE INDEX IF NOT EXISTS class_bookings_class_status_idx
ON class_bookings (class_id, status, created_at, booking_id);

CREATE TABLE IF NOT EXISTS class_refund_operations (
    operation_id UUID PRIMARY KEY,
    booking_id UUID NOT NULL UNIQUE REFERENCES class_bookings(booking_id),
    stripe_payment_intent_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    owner_id TEXT,
    lease_until TIMESTAMP,
    claim_generation INTEGER NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    stripe_refund_id TEXT UNIQUE,
    last_error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    CONSTRAINT class_refund_operations_status_check CHECK (status IN ('pending','processing','retryable_failed','completed','manual_review')),
    CONSTRAINT class_refund_operations_generation_check CHECK (claim_generation >= 0 AND attempt_count >= 0)
);

CREATE INDEX IF NOT EXISTS class_refund_operations_due_idx
ON class_refund_operations (status, lease_until, created_at)
WHERE status IN ('pending','processing','retryable_failed');
