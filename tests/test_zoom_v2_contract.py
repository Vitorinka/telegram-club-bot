import unittest
from datetime import datetime, timedelta, timezone

import zoom_v2_contract as contract


UTC = timezone.utc


class ZoomV2ContractTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)
        self.starts_at = self.now + timedelta(days=5)

    def test_state_validators_reject_unknown_values(self):
        self.assertEqual(contract.validate_class_state("open"), "open")
        self.assertEqual(contract.validate_booking_state("access_required"), "access_required")
        self.assertEqual(contract.validate_payment_state("authorized"), "authorized")
        for validator, category in (
            (contract.validate_class_state, "invalid_class_state"),
            (contract.validate_booking_state, "invalid_booking_state"),
            (contract.validate_payment_state, "invalid_payment_state"),
        ):
            with self.assertRaisesRegex(contract.ZoomV2ContractError, category):
                validator("invented")

    def test_published_class_is_60_minutes_with_unlimited_capacity_within_seven_days(self):
        self.assertTrue(contract.validate_class_contract(
            published_at=self.now, starts_at=self.starts_at,
            duration_minutes=60, capacity=None,
        ))
        for changes, category in (
            ({"starts_at": self.now + timedelta(days=7, seconds=1)}, "invalid_publish_window"),
            ({"duration_minutes": 45}, "invalid_duration"),
            ({"capacity": 20}, "capacity_must_be_unlimited"),
        ):
            values = dict(
                published_at=self.now, starts_at=self.starts_at,
                duration_minutes=60, capacity=None,
            )
            values.update(changes)
            with self.assertRaisesRegex(contract.ZoomV2ContractError, category):
                contract.validate_class_contract(**values)

    def test_active_subscription_must_cover_class_start(self):
        self.assertEqual(contract.classify_booking_access(
            has_active_access=True,
            access_expires_at=self.starts_at + timedelta(minutes=1),
            class_starts_at=self.starts_at,
        ), "booked_subscription")
        self.assertEqual(contract.classify_booking_access(
            has_active_access=True,
            access_expires_at=self.starts_at - timedelta(seconds=1),
            class_starts_at=self.starts_at,
        ), "access_required")

    def test_guest_and_expired_subscriber_require_access(self):
        self.assertEqual(contract.classify_booking_access(
            has_active_access=False, access_expires_at=None,
            class_starts_at=self.starts_at,
        ), "access_required")
        self.assertEqual(contract.classify_booking_access(
            has_active_access=True, access_expires_at=self.now,
            class_starts_at=self.starts_at,
        ), "access_required")

    def test_renewal_restores_subscription_booking_idempotently(self):
        expiry = self.starts_at + timedelta(days=30)
        first = contract.reconcile_subscription_booking(
            "access_required", has_active_access=True,
            access_expires_at=expiry, class_starts_at=self.starts_at,
        )
        second = contract.reconcile_subscription_booking(
            first, has_active_access=True,
            access_expires_at=expiry, class_starts_at=self.starts_at,
        )
        self.assertEqual((first, second), ("booked_subscription", "booked_subscription"))

    def test_subscription_expiry_returns_booking_to_access_required(self):
        self.assertEqual(contract.reconcile_subscription_booking(
            "booked_subscription", has_active_access=False,
            access_expires_at=self.now, class_starts_at=self.starts_at,
        ), "access_required")

    def test_ten_euro_guest_authorization_requires_explicit_consent(self):
        with self.assertRaisesRegex(
            contract.ZoomV2ContractError, "explicit_payment_consent_required"
        ):
            contract.request_guest_authorization(
                "access_required", explicit_consent=False,
            )
        self.assertEqual(contract.request_guest_authorization(
            "access_required", explicit_consent=True, amount=1000, currency="EUR",
        ), "authorization_pending")

    def test_repeated_authorization_request_is_idempotent(self):
        self.assertEqual(contract.request_guest_authorization(
            "authorization_pending", explicit_consent=True,
        ), "authorization_pending")

    def test_authorization_must_be_manual_capture_and_outlive_class(self):
        valid_expiry = self.starts_at + timedelta(hours=4)
        self.assertEqual(contract.confirm_guest_authorization(
            "authorization_pending", stripe_status="requires_capture",
            authorization_expires_at=valid_expiry,
            class_starts_at=self.starts_at,
        ), "booked_guest")
        with self.assertRaisesRegex(
            contract.ZoomV2ContractError, "authorization_expires_too_soon"
        ):
            contract.confirm_guest_authorization(
                "authorization_pending", stripe_status="requires_capture",
                authorization_expires_at=self.starts_at + timedelta(hours=2),
                class_starts_at=self.starts_at,
            )
        with self.assertRaisesRegex(
            contract.ZoomV2ContractError, "authorization_not_capturable"
        ):
            contract.confirm_guest_authorization(
                "authorization_pending", stripe_status="succeeded",
                authorization_expires_at=valid_expiry,
                class_starts_at=self.starts_at,
            )

    def test_cancellation_cutoff_is_inclusive_at_24_hours(self):
        self.assertEqual(contract.cancellation_state(
            requested_at=self.starts_at - timedelta(hours=24),
            class_starts_at=self.starts_at,
        ), "cancelled_early")
        self.assertEqual(contract.cancellation_state(
            requested_at=self.starts_at - timedelta(hours=23, minutes=59),
            class_starts_at=self.starts_at,
        ), "cancelled_late")

    def test_cancellation_payment_contract_requires_no_automatic_charge(self):
        self.assertEqual(
            contract.payment_action_for_booking("cancelled_early"),
            "cancel_authorization",
        )
        self.assertEqual(
            contract.payment_action_for_booking("cancelled_late"),
            "capture_authorization",
        )
        self.assertEqual(
            contract.payment_action_for_booking("booked_guest"),
            "none",
        )

    def test_organizer_cancellation_releases_authorization_idempotently(self):
        first = contract.organizer_cancellation_state("booked_guest")
        second = contract.organizer_cancellation_state(first)
        self.assertEqual((first, second), ("organizer_cancelled", "organizer_cancelled"))
        self.assertEqual(
            contract.payment_action_for_booking(second), "cancel_authorization"
        )

    def test_access_required_reminder_is_immediate_inside_48_hours(self):
        starts_at = self.now + timedelta(hours=20)
        self.assertTrue(contract.should_send_access_reminder(
            booking_state="access_required", now=self.now, class_starts_at=starts_at,
        ))
        self.assertFalse(contract.should_send_access_reminder(
            booking_state="booked_subscription", now=self.now, class_starts_at=starts_at,
        ))

    def test_final_zoom_fence_rechecks_subscription(self):
        self.assertTrue(contract.can_access_zoom(
            class_state="open", booking_state="booked_subscription",
            now=self.starts_at, class_starts_at=self.starts_at,
            has_active_access=True,
            access_expires_at=self.starts_at + timedelta(minutes=1),
        ))
        self.assertFalse(contract.can_access_zoom(
            class_state="open", booking_state="booked_subscription",
            now=self.starts_at, class_starts_at=self.starts_at,
            has_active_access=False, access_expires_at=self.now,
        ))

    def test_final_zoom_fence_rejects_access_required_and_expired_authorization(self):
        self.assertFalse(contract.can_access_zoom(
            class_state="open", booking_state="access_required",
            now=self.starts_at, class_starts_at=self.starts_at,
        ))
        self.assertFalse(contract.can_access_zoom(
            class_state="open", booking_state="booked_guest",
            now=self.starts_at, class_starts_at=self.starts_at,
            payment_state="authorized",
            authorization_expires_at=self.starts_at - timedelta(seconds=1),
        ))

    def test_final_zoom_fence_accepts_valid_guest_and_rejects_cancelled_class(self):
        values = dict(
            booking_state="booked_guest", now=self.starts_at,
            class_starts_at=self.starts_at, payment_state="authorized",
            authorization_expires_at=self.starts_at + timedelta(hours=2),
        )
        self.assertTrue(contract.can_access_zoom(class_state="open", **values))
        self.assertFalse(contract.can_access_zoom(class_state="cancelled", **values))

    def test_legacy_booking_states_are_preserved_without_conversion(self):
        for booking_state in contract.LEGACY_BOOKING_STATES:
            result = contract.validate_legacy_booking(
                class_state="confirmed", booking_state=booking_state,
            )
            self.assertEqual(result["booking_state"], booking_state)
            self.assertEqual(contract.reconcile_subscription_booking(
                booking_state, has_active_access=True,
                access_expires_at=self.starts_at + timedelta(days=1),
                class_starts_at=self.starts_at,
            ), booking_state)

    def test_legacy_paid_zoom_access_remains_explicitly_compatible(self):
        self.assertTrue(contract.can_access_zoom(
            class_state="confirmed", booking_state="paid", legacy_paid=True,
            now=self.starts_at, class_starts_at=self.starts_at,
        ))
        self.assertFalse(contract.can_access_zoom(
            class_state="confirmed", booking_state="paid", legacy_paid=False,
            now=self.starts_at, class_starts_at=self.starts_at,
        ))


if __name__ == "__main__":
    unittest.main()
