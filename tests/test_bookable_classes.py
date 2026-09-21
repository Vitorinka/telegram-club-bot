import unittest
import uuid
from datetime import datetime, timedelta

from bookable_classes import (
    BookableClassError,
    apply_booking_checkout_completed,
    validate_class_payload,
)


class BookingCursor:
    def __init__(self, row, paid_count=3):
        self.row = row
        self.paid_count = paid_count
        self.queries = []
        self.rowcount = 1

    def execute(self, query, params=()):
        self.queries.append((query, params))

    def fetchone(self):
        if self.queries and "COUNT(*) FROM class_bookings" in self.queries[-1][0]:
            return (self.paid_count,)
        return self.row


class BookableClassTests(unittest.TestCase):
    def payload(self):
        now = datetime.utcnow()
        return {
            "title": "Утренняя практика",
            "description": "Zoom-класс",
            "starts_at": (now + timedelta(days=2)).isoformat(),
            "booking_deadline": (now + timedelta(days=1)).isoformat(),
            "duration_minutes": 60,
            "zoom_url": "https://zoom.us/j/123456",
            "price_amount": 1000,
            "capacity": 12,
            "minimum_participants": 3,
        }

    def test_payload_is_bounded_and_zoom_is_https(self):
        values = validate_class_payload(self.payload())
        self.assertEqual(values["price_amount"], 1000)
        self.assertEqual(values["currency"], "eur")
        unsafe = self.payload(); unsafe["zoom_url"] = "javascript:alert(1)"
        with self.assertRaises(BookableClassError):
            validate_class_payload(unsafe)
        invalid = self.payload(); invalid["minimum_participants"] = 13
        with self.assertRaises(BookableClassError):
            validate_class_payload(invalid)

    def test_aware_datetimes_are_normalized_to_utc(self):
        payload = self.payload()
        payload["starts_at"] = "2026-10-20T18:00:00+03:00"
        payload["booking_deadline"] = "2026-10-20T14:00:00+03:00"
        values = validate_class_payload(payload)
        self.assertEqual(values["starts_at"], datetime(2026, 10, 20, 15, 0))
        self.assertEqual(values["booking_deadline"], datetime(2026, 10, 20, 11, 0))

    def test_paid_booking_is_idempotent_and_confirms_at_minimum(self):
        booking_id = uuid.uuid4(); class_id = uuid.uuid4()
        row = (booking_id, class_id, "checkout_open", 1000, "eur", 3, "open", "cs_exact", 12)
        cursor = BookingCursor(row, paid_count=2)
        result = apply_booking_checkout_completed(cursor, {
            "id": "cs_exact", "metadata": {"payment_kind": "class_booking", "booking_id": str(booking_id)},
            "amount_total": 1000, "currency": "eur", "payment_status": "paid",
            "payment_intent": "pi_exact",
        })
        self.assertEqual(result["status"], "paid")
        sql = "\n".join(query for query, _params in cursor.queries)
        self.assertIn("status='paid'", sql)
        self.assertIn("status='confirmed'", sql)

        already = BookingCursor((booking_id, class_id, "paid", 1000, "eur", 3, "confirmed", "cs_exact", 12))
        result = apply_booking_checkout_completed(already, {
            "id": "cs_exact", "metadata": {"payment_kind": "class_booking", "booking_id": str(booking_id)},
            "amount_total": 1000, "currency": "eur", "payment_status": "paid", "payment_intent": "pi_exact",
        })
        self.assertEqual(result, {"booking_id": str(booking_id), "status": "paid"})

    def test_checkout_identity_amount_and_currency_fail_closed(self):
        booking_id = uuid.uuid4(); class_id = uuid.uuid4()
        row = (booking_id, class_id, "checkout_open", 1000, "eur", 3, "open", "cs_expected", 12)
        for overrides in (
            {"id": "cs_wrong"}, {"amount_total": 999},
            {"currency": "usd"}, {"payment_status": "unpaid"},
        ):
            session = {
                "id": "cs_expected", "metadata": {"payment_kind": "class_booking", "booking_id": str(booking_id)},
                "amount_total": 1000, "currency": "eur", "payment_status": "paid", "payment_intent": "pi_exact",
                **overrides,
            }
            with self.subTest(overrides=overrides), self.assertRaises(BookableClassError):
                apply_booking_checkout_completed(BookingCursor(row), session)

    def test_paid_checkout_over_capacity_is_queued_for_refund(self):
        booking_id = uuid.uuid4(); class_id = uuid.uuid4()
        row = (booking_id, class_id, "checkout_open", 1000, "eur", 3, "confirmed", "cs_exact", 3)
        cursor = BookingCursor(row, paid_count=3)
        result = apply_booking_checkout_completed(cursor, {
            "id": "cs_exact", "metadata": {"payment_kind": "class_booking", "booking_id": str(booking_id)},
            "amount_total": 1000, "currency": "eur", "payment_status": "paid",
            "payment_intent": "pi_capacity",
        })
        self.assertEqual(result["status"], "refund_pending")
        sql = "\n".join(query for query, _params in cursor.queries)
        self.assertIn("status='manual_review'", sql)
        self.assertIn("class_refund_operations", sql)
        self.assertNotIn("status='paid',stripe_payment_intent_id", sql)


if __name__ == "__main__":
    unittest.main()
