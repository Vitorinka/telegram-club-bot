import unittest

import storage_diagnostics as diagnostics


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def execute(self, query, params=()):
        self.connection.queries.append(" ".join(str(query).split()))
        if self.connection.fail_metric and self.connection.fail_metric in str(query):
            raise RuntimeError("private DB detail")
        self.rows = self.connection.result_for(query, params)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        pass


class FakeConnection:
    def __init__(self, fail_metric=None):
        self.queries = []
        self.rollbacks = 0
        self.fail_metric = fail_metric

    def cursor(self):
        return FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1

    def result_for(self, query, params):
        if query == diagnostics.CATALOG_SQL:
            return [(name, 1, 100, 20, 120, True) for name in diagnostics.APPLICATION_TABLES]
        if "GROUP BY status" in query:
            return [("redeemed", 1)]
        if "MIN(started_at)" in query:
            return [(None, None)]
        if "SELECT MIN(created_at) FROM payment_events" in query:
            return [(1, 2, None, None)]
        if "FILTER" in query:
            return [(1, 2)]
        return [(1,)]


class StorageDiagnosticsTests(unittest.TestCase):
    def test_allowlist_is_fixed_and_complete(self):
        self.assertIsInstance(diagnostics.APPLICATION_TABLES, tuple)
        self.assertEqual(len(diagnostics.APPLICATION_TABLES), 39)
        self.assertIn("recipe_ingredients", diagnostics.APPLICATION_TABLES)
        self.assertIn("recipe_steps", diagnostics.APPLICATION_TABLES)
        self.assertIn("nutrition_material_bodies", diagnostics.APPLICATION_TABLES)
        self.assertIn("schema_migrations", diagnostics.APPLICATION_TABLES)
        self.assertIn("message_delivery_events", diagnostics.APPLICATION_TABLES)

    def test_all_diagnostic_sql_is_read_only(self):
        sql_texts = [diagnostics.CATALOG_SQL, *diagnostics.OPERATIONAL_QUERIES.values(), *diagnostics.RETENTION_QUERIES.values()]
        forbidden = ("DELETE ", "UPDATE ", "INSERT ", "ALTER ", "CREATE ", "DROP ", "VACUUM", "ANALYZE")
        for sql in sql_texts:
            normalized = " ".join(sql.upper().split())
            self.assertTrue(normalized.startswith("SELECT"))
            self.assertFalse(any(token in normalized for token in forbidden), normalized)

    def test_collection_uses_read_only_transaction_and_does_not_mutate(self):
        conn = FakeConnection()
        result = diagnostics.collect_storage_diagnostics(conn)
        self.assertEqual(conn.queries[0], "SET TRANSACTION READ ONLY")
        self.assertEqual(result["tables"][0]["table"], "users")
        self.assertEqual(conn.rollbacks, 1)
        self.assertFalse(any(query.startswith(("DELETE", "UPDATE", "INSERT")) for query in conn.queries))

    def test_optional_metric_failure_degrades_without_raw_error(self):
        conn = FakeConnection(fail_metric="unlinked_stripe_events WHERE resolved")
        result = diagnostics.collect_storage_diagnostics(conn)
        self.assertIsNone(result["operational"]["unlinked_unresolved"])
        pages = diagnostics.render_storage_diagnostics(result)
        self.assertNotIn("private DB detail", "\n".join(pages))

    def test_output_is_aggregate_private_and_must_retain_is_explicit(self):
        result = diagnostics.collect_storage_diagnostics(FakeConnection())
        text = "\n".join(diagnostics.render_storage_diagnostics(result, message_limit=400))
        for secret in ("123456789", "cus_secret", "sub_secret", "https://", "gift-token"):
            self.assertNotIn(secret, text)
        self.assertIn("Outbox: active", text)
        self.assertIn("cleanup NOT SAFE", text)
        self.assertIn("Trial redemptions: cleanup NOT SAFE", text)
        self.assertIn("stripe_events event IDs: MUST RETAIN", text)

    def test_output_pages_stay_below_limit(self):
        pages = diagnostics.render_storage_diagnostics(
            diagnostics.collect_storage_diagnostics(FakeConnection()), message_limit=300
        )
        self.assertGreater(len(pages), 3)
        self.assertTrue(all(len(page) <= 300 for page in pages))

    def test_permanent_alert_keys_are_excluded_from_cleanup_predicate(self):
        sql = diagnostics.RETENTION_QUERIES["ordinary terminal admin alerts (>90d)"]
        self.assertIn("NOT LIKE 'outbox-retry:%%'", sql)
        self.assertIn("NOT LIKE 'outbox-permanent:%%'", sql)

    def test_checkout_cleanup_requires_retained_successful_payment_proof(self):
        sql = diagnostics.RETENTION_QUERIES["existing terminal checkout predicate (>30d)"]
        self.assertIn("pe.telegram_id = cs.telegram_id", sql)
        self.assertIn("pe.payment_status = 'succeeded'", sql)
        self.assertIn("EXISTS", sql)

    def test_expired_active_invite_is_excluded_from_cleanup_predicate(self):
        sql = diagnostics.RETENTION_QUERIES["expired/revoked invite links (>90d)"]
        self.assertIn("status = 'revoked'", sql)
        self.assertIn("status IS DISTINCT FROM 'active'", sql)


if __name__ == "__main__":
    unittest.main()
