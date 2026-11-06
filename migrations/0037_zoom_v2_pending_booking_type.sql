ALTER TABLE class_bookings
    DROP CONSTRAINT IF EXISTS class_bookings_v2_shape_check;

ALTER TABLE class_bookings
    ADD CONSTRAINT class_bookings_v2_shape_check CHECK (
        (booking_model = 'legacy_capture_now'
         AND booking_type IS NULL
         AND access_status IS NULL
         AND payment_state IS NULL
         AND settlement_status IS NULL)
        OR
        (booking_model = 'authorization_v2'
         AND booking_type IN ('pending', 'subscription', 'guest')
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
    ) NOT VALID;

ALTER TABLE class_bookings
    VALIDATE CONSTRAINT class_bookings_v2_shape_check;
