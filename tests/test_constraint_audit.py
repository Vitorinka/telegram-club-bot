import unittest

import constraint_audit as audit


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def execute(self, query):
        normalized = " ".join(str(query).split())
        self.connection.queries.append(normalized)
        if self.connection.fail_fragment and self.connection.fail_fragment in normalized:
            raise RuntimeError("postgresql://user:secret@private/club cus_secret")
        self.rows = self.connection.results.get(normalized, [])

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        pass


class FakeConnection:
    def __init__(self, results=None, fail_fragment=None):
        self.results = results or {}
        self.fail_fragment = fail_fragment
        self.queries = []
        self.rollbacks = 0

    def cursor(self):
        return FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1


def normalized(sql):
    return " ".join(sql.split())


def clean_results():
    results = {}
    for name, sql in audit.GROUPED_QUERIES.items():
        expected = audit.EXPECTED_VALUES.get(name, ())
        results[normalized(sql)] = [(value, 1) for value in sorted(expected, key=lambda value: "" if value is None else value)]
    for name, sql in audit.STRUCTURAL_QUERIES.items():
        results[normalized(sql)] = [tuple(0 for _ in audit.STRUCTURAL_LABELS[name])]
    for sql in audit.OPEN_DOMAIN_QUERIES.values():
        results[normalized(sql)] = [("future_external_value", 2)]
    return results


class ConstraintAuditTests(unittest.TestCase):
    def test_every_closed_group_has_expected_values_and_external_domains_do_not(self):
        self.assertEqual(set(audit.GROUPED_QUERIES), set(audit.EXPECTED_VALUES))
        self.assertTrue(set(audit.OPEN_DOMAIN_QUERIES).isdisjoint(audit.EXPECTED_VALUES))
        self.assertTrue(set(audit.LEGACY_ALLOWED_VALUES).issubset(audit.EXPECTED_VALUES))
        for name, legacy_values in audit.LEGACY_ALLOWED_VALUES.items():
            self.assertTrue(legacy_values.isdisjoint(audit.EXPECTED_VALUES[name]))

    def test_sql_is_fixed_and_select_only(self):
        sql_texts = [
            *audit.GROUPED_QUERIES.values(),
            *audit.STRUCTURAL_QUERIES.values(),
            *audit.OPEN_DOMAIN_QUERIES.values(),
        ]
        forbidden = ("INSERT ", "UPDATE ", "DELETE ", "ALTER ", "CREATE ", "DROP ", "VACUUM", "ANALYZE")
        self.assertEqual(len(audit.GROUPED_QUERIES), 19)
        for sql in sql_texts:
            text = normalized(sql).upper()
            self.assertTrue(text.startswith("SELECT"), text)
            self.assertFalse(any(token in text for token in forbidden), text)

    def test_collection_is_read_only_and_structural_zero_is_clean(self):
        conn = FakeConnection(clean_results())
        data = audit.collect_constraint_audit(conn)
        self.assertEqual(conn.queries[0], "SET TRANSACTION READ ONLY")
        self.assertEqual(audit.unexpected_values(data), {})
        rendered = "\n".join(audit.render_constraint_audit(data))
        self.assertIn("structural violations: 0", rendered)
        self.assertEqual(conn.rollbacks, 1)
        self.assertFalse(any(query.startswith(("INSERT", "UPDATE", "DELETE")) for query in conn.queries))

    def test_unexpected_internal_value_and_null_handling(self):
        results = clean_results()
        results[normalized(audit.GROUPED_QUERIES["message_delivery_events.status"])] = [
            ("pending", 3), ("unexpected_status", 2)
        ]
        results[normalized(audit.GROUPED_QUERIES["payment_events.payment_kind"])] = [(None, 4), ("trial", 1)]
        data = audit.collect_constraint_audit(FakeConnection(results))
        self.assertEqual(audit.unexpected_values(data)["message_delivery_events.status"], [("unexpected_status", 2)])
        self.assertNotIn("payment_events.payment_kind", audit.unexpected_values(data))
        rendered = "\n".join(audit.render_constraint_audit(data))
        self.assertIn("unexpected_status (2)", rendered)
        self.assertIn("NULL: 4", rendered)

    def test_admin_action_and_identity_conflict_expected_sets_and_violations(self):
        results = clean_results()
        valid_data = audit.collect_constraint_audit(FakeConnection(results))
        self.assertNotIn("admin_action_requests.action_type", audit.unexpected_values(valid_data))
        self.assertNotIn("stripe_identity_conflicts.conflict_type", audit.unexpected_values(valid_data))

        results[normalized(audit.GROUPED_QUERIES["admin_action_requests.action_type"])] = [
            ("broadcast", 1), ("unexpected_action", 1),
        ]
        results[normalized(audit.GROUPED_QUERIES["stripe_identity_conflicts.conflict_type"])] = [
            ("users_customer_conflict", 1), ("unexpected_conflict", 1),
        ]
        data = audit.collect_constraint_audit(FakeConnection(results))
        unexpected = audit.unexpected_values(data)
        self.assertEqual(
            unexpected["admin_action_requests.action_type"],
            [("unexpected_action", 1)],
        )
        self.assertEqual(
            unexpected["stripe_identity_conflicts.conflict_type"],
            [("unexpected_conflict", 1)],
        )
        rendered = "\n".join(audit.render_constraint_audit(data))
        self.assertIn("unexpected closed-set values: 2", rendered)
        self.assertIn("unexpected_action (1)", rendered)
        self.assertIn("unexpected_conflict (1)", rendered)
        self.assertNotIn("observed-only candidate", rendered)

    def test_legacy_identity_conflict_is_visible_allowed_and_excluded_from_summary(self):
        legacy = "users_duplicate_subscription"
        self.assertNotIn(legacy, audit.EXPECTED_VALUES["stripe_identity_conflicts.conflict_type"])
        self.assertEqual(
            audit.LEGACY_ALLOWED_VALUES["stripe_identity_conflicts.conflict_type"],
            frozenset((legacy,)),
        )
        results = clean_results()
        results[normalized(audit.GROUPED_QUERIES["stripe_identity_conflicts.conflict_type"])] = [
            ("users_subscription_conflict", 2),
            (legacy, 1),
        ]
        data = audit.collect_constraint_audit(FakeConnection(results))
        self.assertNotIn("stripe_identity_conflicts.conflict_type", audit.unexpected_values(data))
        rendered = "\n".join(audit.render_constraint_audit(data))
        self.assertIn("unexpected closed-set values: 0", rendered)
        self.assertIn("legacy allowed: users_duplicate_subscription", rendered)
        self.assertIn("users_duplicate_subscription: 1", rendered)
        self.assertIn("unexpected: none", rendered)

    def test_unknown_identity_conflict_remains_unexpected_alongside_legacy(self):
        results = clean_results()
        results[normalized(audit.GROUPED_QUERIES["stripe_identity_conflicts.conflict_type"])] = [
            ("users_duplicate_subscription", 1),
            ("unknown_conflict_type", 3),
        ]
        data = audit.collect_constraint_audit(FakeConnection(results))
        self.assertEqual(
            audit.unexpected_values(data)["stripe_identity_conflicts.conflict_type"],
            [("unknown_conflict_type", 3)],
        )
        rendered = "\n".join(audit.render_constraint_audit(data))
        self.assertIn("unexpected closed-set values: 1", rendered)
        self.assertIn("unknown_conflict_type (3)", rendered)
        self.assertIn("users_duplicate_subscription: 1", rendered)

    def test_external_values_are_not_closed_set_violations(self):
        data = audit.collect_constraint_audit(FakeConnection(clean_results()))
        self.assertEqual(audit.unexpected_values(data), {})
        rendered = "\n".join(audit.render_constraint_audit(data))
        self.assertIn("OPEN EXTERNAL DOMAIN — DO NOT CHECK-CONSTRAIN", rendered)
        self.assertIn("future_external_value: 2", rendered)

    def test_seeded_structural_classes_are_reported(self):
        results = clean_results()
        results[normalized(audit.STRUCTURAL_QUERIES["stripe_events"])] = [(1, 1)]
        results[normalized(audit.STRUCTURAL_QUERIES["message_delivery_events"])] = [(1, 1, 1, 1)]
        results[normalized(audit.STRUCTURAL_QUERIES["weekly_report_runs"])] = [(1, 1)]
        results[normalized(audit.STRUCTURAL_QUERIES["payment_events"])] = [(1, 1, 1)]
        rendered = "\n".join(audit.render_constraint_audit(audit.collect_constraint_audit(FakeConnection(results))))
        self.assertIn("structural violations: 11", rendered)
        self.assertIn("processing without lease: 1", rendered)
        self.assertIn("reversed period: 1", rendered)

    def test_output_redacts_identifiers_secrets_urls_and_emails(self):
        results = clean_results()
        sql = normalized(audit.GROUPED_QUERIES["message_delivery_events.status"])
        results[sql] = [("cus_secret", 1), ("https://private.example", 1), ("a@b.example", 1)]
        text = "\n".join(audit.render_constraint_audit(audit.collect_constraint_audit(FakeConnection(results))))
        for raw in ("cus_secret", "https://private.example", "a@b.example"):
            self.assertNotIn(raw, text)
        self.assertIn("<redacted:", text)

    def test_large_output_splits_deterministically_under_limit(self):
        results = clean_results()
        for sql in audit.OPEN_DOMAIN_QUERIES.values():
            results[normalized(sql)] = [(f"external_value_{index}", index) for index in range(100)]
        data = audit.collect_constraint_audit(FakeConnection(results))
        first = audit.render_constraint_audit(data, message_limit=300)
        second = audit.render_constraint_audit(data, message_limit=300)
        self.assertEqual(first, second)
        self.assertGreater(len(first), 3)
        self.assertTrue(all(len(page) <= 300 for page in first))

    def test_optional_metric_failure_degrades_without_raw_error(self):
        conn = FakeConnection(clean_results(), fail_fragment="FROM bot_invite_links")
        with self.assertLogs(level="WARNING") as logs:
            data = audit.collect_constraint_audit(conn)
        rendered = "\n".join(audit.render_constraint_audit(data))
        self.assertIn("observed: unavailable", rendered)
        self.assertNotIn("private", rendered)
        self.assertNotIn("secret", rendered)
        self.assertNotIn("cus_secret", rendered)
        self.assertIn("error_class=RuntimeError", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
