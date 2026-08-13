import unittest
import calendar
from datetime import datetime, timedelta

import stripe_reconcile_audit as audit


NOW = datetime(2026, 1, 1, 12, 0, 0)


def candidate(**overrides):
    value = {
        "telegram_id": 777,
        "paid": True,
        "expiry_date": NOW + timedelta(days=30),
        "user_customer_id": "cus_sensitive_123",
        "user_subscription_id": "sub_sensitive_123",
        "link_customer_id": "cus_sensitive_123",
        "link_subscription_id": "sub_sensitive_123",
        "payment_event_id": "evt_sensitive_123",
        "payment_period_end": NOW + timedelta(days=30),
        "payment_created_at": NOW,
        "has_access_override": False,
    }
    value.update(overrides)
    return value


def subscription(status="active", period_end=None, **overrides):
    value = {
        "id": "sub_sensitive_123",
        "customer": "cus_sensitive_123",
        "status": status,
        "current_period_end": calendar.timegm((period_end or (NOW + timedelta(days=30))).utctimetuple()),
    }
    value.update(overrides)
    return value


class StripeReconcileAuditTests(unittest.IsolatedAsyncioTestCase):
    def test_active_paid_matching_expiry_is_clean(self):
        self.assertEqual(audit.classify_candidate(candidate(), subscription(), NOW), "STRIPE_ACTIVE_LOCAL_PAID")

    def test_active_unpaid_is_actionable(self):
        self.assertEqual(audit.classify_candidate(candidate(paid=False), subscription(), NOW), "STRIPE_ACTIVE_LOCAL_UNPAID")

    def test_canceled_local_active_is_actionable(self):
        self.assertEqual(audit.classify_candidate(candidate(), subscription("canceled"), NOW), "STRIPE_TERMINAL_LOCAL_PAID")

    def test_missing_subscription_local_active_is_actionable(self):
        self.assertEqual(audit.classify_candidate(candidate(), None, NOW), "LOCAL_ACTIVE_WITH_MISSING_STRIPE_SUBSCRIPTION")

    def test_customer_ownership_mismatch_is_critical(self):
        self.assertEqual(
            audit.classify_candidate(candidate(), subscription(customer="cus_other"), NOW),
            "STRIPE_SUBSCRIPTION_IDENTITY_MISMATCH",
        )

    def test_conflicting_active_link_identity_is_critical(self):
        self.assertEqual(
            audit.classify_candidate(candidate(link_subscription_id="sub_conflicting"), subscription(), NOW),
            "STRIPE_SUBSCRIPTION_IDENTITY_MISMATCH",
        )

    def test_expiry_drift_above_tolerance(self):
        self.assertEqual(
            audit.classify_candidate(candidate(), subscription(period_end=NOW + timedelta(days=31)), NOW),
            "EXPIRY_DRIFT",
        )

    def test_expiry_within_tolerance_is_clean(self):
        self.assertEqual(
            audit.classify_candidate(
                candidate(expiry_date=NOW + timedelta(days=30, minutes=9)), subscription(), NOW
            ),
            "STRIPE_ACTIVE_LOCAL_PAID",
        )

    def test_unknown_external_status_is_informational(self):
        self.assertEqual(audit.classify_candidate(candidate(), subscription("future_status"), NOW), "UNKNOWN_EXTERNAL_STATUS")

    def test_missing_payment_evidence_is_mismatch(self):
        self.assertEqual(
            audit.classify_candidate(candidate(payment_event_id=None), subscription(), NOW),
            "PAYMENT_EVIDENCE_MISMATCH",
        )

    def test_manual_override_avoids_payment_and_expiry_false_positive(self):
        value = candidate(payment_event_id=None, has_access_override=True, expiry_date=NOW + timedelta(days=90))
        self.assertEqual(audit.classify_candidate(value, subscription(), NOW), "STRIPE_ACTIVE_LOCAL_PAID")

    async def test_timeout_is_unavailable_and_continues(self):
        def timeout(_subscription_id):
            raise TimeoutError("raw sensitive sub_sensitive_123")

        result = await audit.reconcile_candidates([candidate()], timeout)
        self.assertEqual(result["results"][0][1], "STRIPE_API_UNAVAILABLE")
        self.assertFalse(result["aborted"])

    async def test_rate_limit_returns_safe_partial_result(self):
        class RateLimitError(Exception):
            http_status = 429

        def rate_limit(_subscription_id):
            raise RateLimitError("raw sensitive sub_sensitive_123")

        result = await audit.reconcile_candidates([candidate(), candidate(telegram_id=778)], rate_limit)
        self.assertTrue(result["partial"])
        self.assertEqual(result["calls"], 1)
        self.assertEqual(len(result["results"]), 1)

    async def test_auth_failure_aborts_without_fake_mismatches(self):
        class AuthenticationError(Exception):
            http_status = 401

        def auth_failure(_subscription_id):
            raise AuthenticationError("secret details")

        result = await audit.reconcile_candidates([candidate()], auth_failure)
        self.assertTrue(result["aborted"])
        self.assertEqual(result["results"], [])

    async def test_not_found_is_missing_subscription(self):
        class InvalidRequestError(Exception):
            http_status = 404

        def missing(_subscription_id):
            raise InvalidRequestError("raw sub_sensitive_123")

        result = await audit.reconcile_candidates(
            [candidate(expiry_date=datetime.utcnow() + timedelta(days=30))], missing
        )
        self.assertEqual(result["results"][0][1], "LOCAL_ACTIVE_WITH_MISSING_STRIPE_SUBSCRIPTION")

    def test_output_redacts_all_stripe_identifiers_and_error_text(self):
        rendered = "\n".join(audit.render_reconcile_audit(
            {"aborted": False, "partial": False, "calls": 1, "duration": 0.1,
             "results": [(candidate(), "STRIPE_SUBSCRIPTION_IDENTITY_MISMATCH", subscription(customer="cus_other"))]},
            lambda value: "ref:" + value[-2:],
        ))
        for raw in ("cus_sensitive_123", "sub_sensitive_123", "evt_sensitive_123", "cus_other"):
            self.assertNotIn(raw, rendered)
        self.assertIn("ref:77", rendered)

    def test_candidate_query_is_fixed_bounded_and_read_only(self):
        class Cursor:
            def execute(self, query, params):
                self.query = query
                self.params = params

            def fetchall(self):
                return []

        cur = Cursor()
        self.assertEqual(audit.load_reconcile_candidates(cur, limit=999), [])
        upper = cur.query.upper()
        self.assertIn("FROM USERS", upper)
        self.assertIn("STRIPE_LINKS", upper)
        self.assertIn("PAYMENT_EVENTS", upper)
        for write in ("UPDATE ", "DELETE ", "INSERT ", "ALTER ", "CREATE "):
            self.assertNotIn(write, upper)
        self.assertEqual(cur.params[-1], 100)

    async def test_only_subscription_retrieve_is_called_sequentially(self):
        calls = []

        def retrieve(subscription_id):
            calls.append(subscription_id)
            return subscription()

        result = await audit.reconcile_candidates([candidate(), candidate(telegram_id=778)], retrieve)
        self.assertEqual(calls, ["sub_sensitive_123", "sub_sensitive_123"])
        self.assertEqual(result["calls"], 2)


if __name__ == "__main__":
    unittest.main()
