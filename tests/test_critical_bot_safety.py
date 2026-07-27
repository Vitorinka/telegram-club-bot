import asyncio
import ast
import inspect
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from admin_security import (
    PRIVATE_ADMIN_MESSAGE,
    admin_action_confirmation_keyboard,
    broadcast_preview,
    cancel_admin_action,
    claim_admin_action,
    complete_admin_action,
    fail_admin_action,
    is_private_admin_message,
)
from checkout_safety import (
    active_or_resumable_subscriptions,
    backup_decision,
    build_pg_dump_command,
    CHECKOUT_AMBIGUOUS_AUTO_RETRY_HOURS,
    has_active_access,
    live_subscription_is_paid,
    manual_link_access_decision,
    parse_moscow_expiry,
    should_apply_negative_event,
    stable_checkout_idempotency_key,
    subscription_status_action,
)
from group_access import (
    group_join_decision,
    invite_link_options,
    load_active_bot_invite_links,
    mark_bot_invite_link_revoked,
    save_bot_invite_link,
)
from scheduled_jobs import (
    claim_message_delivery,
    claim_pending_message_deliveries,
    claim_scheduled_job,
    enqueue_message_delivery,
    process_claimed_delivery,
)
from stripe_invoice_rules import (
    should_apply_subscription_state_update,
    should_live_check_stale_negative_subscription_update,
)


ROOT = Path(__file__).resolve().parents[1]
MAIN_SOURCE = (ROOT / "main.py").read_text()


class Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeCursor:
    def __init__(self, fetches=None):
        self.fetches = list(fetches or [])
        self.queries = []
        self.closed = False

    def execute(self, query, params=()):
        self.queries.append((query, params))

    def fetchone(self):
        if self.fetches:
            return self.fetches.pop(0)
        return None

    def fetchall(self):
        if self.fetches:
            return self.fetches.pop(0)
        return []

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


class AdminActionCursor(FakeCursor):
    def __init__(self, action_id="act_1", owner_id=10):
        super().__init__()
        self.action_id = action_id
        self.owner_id = owner_id
        self.status = "pending"
        self.payload_json = '{"text": "hello"}'
        self.action_type = "broadcast"

    def fetchone(self):
        query, params = self.queries[-1]
        if "UPDATE admin_action_requests" in query and "RETURNING action_type, payload_json" in query:
            action_id, admin_id = params[0], params[1]
            if action_id == self.action_id and admin_id == self.owner_id and self.status == "pending":
                self.status = "processing"
                return (self.action_type, self.payload_json)
            return None
        if "SELECT status FROM admin_action_requests" in query:
            return (self.status,)
        if "RETURNING action_id" in query:
            if self.status == "pending":
                self.status = "cancelled"
                return (self.action_id,)
            return None
        return super().fetchone()

    def execute(self, query, params=()):
        super().execute(query, params)
        if "SET status = 'completed'" in query:
            self.status = "completed"
        if "SET status = 'failed'" in query:
            self.status = "failed"


class DeliveryCursor(FakeCursor):
    def __init__(self, claim_fetch=("free_lesson:1",), status_fetch=None):
        super().__init__()
        self.claim_fetch = claim_fetch
        self.status_fetch = status_fetch
        self.closed = False

    def fetchone(self):
        query, _ = self.queries[-1]
        if "RETURNING delivery_key" in query:
            return self.claim_fetch
        if "SELECT status FROM message_delivery_events" in query:
            return self.status_fetch
        return super().fetchone()

    def close(self):
        self.closed = True


class CriticalBotSafetyTests(unittest.TestCase):
    def on_startup_node(self):
        tree = ast.parse(MAIN_SOURCE)
        for node in ast.walk(tree):
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "on_startup":
                return node
        self.fail("on_startup not found")

    def test_checkout_idempotency_key_reused_for_same_record_seed(self):
        first = stable_checkout_idempotency_key(123, "sub_1", "2026-07-23T10:00:00")
        second = stable_checkout_idempotency_key(123, "sub_1", "2026-07-23T10:00:00")
        other = stable_checkout_idempotency_key(123, "sub_6", "2026-07-23T10:00:00")
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)

    def test_duplicate_subscription_detection(self):
        subscriptions = Obj(data=[
            Obj(id="sub_a", status="active"),
            Obj(id="sub_b", status="trialing"),
            Obj(id="sub_c", status="canceled"),
        ])
        blocking = active_or_resumable_subscriptions(subscriptions)
        self.assertEqual([sub.id for sub in blocking], ["sub_a", "sub_b"])
        self.assertEqual(subscription_status_action("active", count=2), "duplicate_subscriptions")

    def test_trial_and_checkout_migrations_exist(self):
        for needle in (
            "CREATE TABLE IF NOT EXISTS checkout_sessions",
            "checkout_sessions_one_open_tariff",
            "CREATE TABLE IF NOT EXISTS trial_redemptions",
            "CREATE TABLE IF NOT EXISTS stripe_identity_conflicts",
        ):
            self.assertIn(needle, MAIN_SOURCE)

    def test_init_db_uses_versioned_migration_runner_before_legacy_ddl(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("def init_db"):MAIN_SOURCE.index("# Идемпотентность вебхуков")]
        self.assertIn("run_migrations(get_db_conn)", source)
        self.assertLess(source.index("run_migrations(get_db_conn)"), source.index("return"))
        self.assertLess(source.index("return"), source.index("CREATE TABLE IF NOT EXISTS users"))

    def test_process_payment_uses_db_claim_and_stripe_idempotency_key(self):
        self.assertIn("claim_checkout_session_record", MAIN_SOURCE)
        self.assertIn("idempotency_key=checkout_record", MAIN_SOURCE)
        self.assertNotIn("session = stripe.checkout.Session.create(**session_params)", MAIN_SOURCE)

    def test_group_join_decisions(self):
        self.assertEqual(group_join_decision(1, False, False, True), "authorized")
        self.assertEqual(group_join_decision(1, False, False, False), "remove_unauthorized")
        self.assertEqual(group_join_decision(1, False, True, False), "preserve_admin")
        self.assertEqual(group_join_decision(1, True, False, False), "preserve_bot")
        self.assertEqual(group_join_decision(1, False, False, False, db_error=True), "preserve_db_error")

    def test_invite_links_are_one_use_and_24h(self):
        now = datetime(2026, 7, 23, 12, 0)
        options = invite_link_options("test", 123, now=now)
        self.assertEqual(options["member_limit"], 1)
        self.assertEqual(options["expire_date"], now + timedelta(hours=24))
        self.assertIn("test_123", options["name"])

    def test_admin_private_only_decision(self):
        private_message = Obj(from_user=Obj(id=10), chat=Obj(type="private"))
        group_message = Obj(from_user=Obj(id=10), chat=Obj(type="group"))
        self.assertTrue(is_private_admin_message(private_message, [10]))
        self.assertFalse(is_private_admin_message(group_message, [10]))
        self.assertEqual(PRIVATE_ADMIN_MESSAGE, "Используйте эту команду в личном чате с ботом")

    def test_admin_commands_are_decorated_private_only(self):
        for command in (
            "broadcast",
            "give_access",
            "set_expiry",
            "sync_stripe_user",
            "link_stripe_user",
            "weekly_report",
            "weekly_report_current",
            "weekly_report_send",
            "duplicate_subscriptions",
            "revoke_invite_links",
        ):
            marker = f"@router.message(Command('{command}')"
            start = MAIN_SOURCE.index(marker)
            snippet = MAIN_SOURCE[start:start + 220]
            self.assertIn("@admin_private_only(ADMIN_IDS)", snippet, command)

    def test_scheduled_job_claim_has_lease_semantics(self):
        cur = FakeCursor(fetches=[("subscription_check:2026-07-23",)])
        result = claim_scheduled_job(cur, "subscription_check:2026-07-23", "subscription_check", "2026-07-23")
        self.assertEqual(result, "claimed")
        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("lease_until", sql)
        self.assertIn("ON CONFLICT", sql)

    def test_message_delivery_claim_has_sent_and_stale_paths(self):
        cur = FakeCursor(fetches=[None, ("sent",)])
        result = claim_message_delivery(cur, "free_lesson:1", 1, "free_lesson")
        self.assertEqual(result, "already_sent")
        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("message_delivery_events", sql)
        self.assertIn("lease_until < %s", sql)

    def test_message_delivery_outbox_enqueue_is_idempotent(self):
        cur = FakeCursor(fetches=[("stripe:evt_1:payment_success_notice",)])
        created = enqueue_message_delivery(
            cur,
            "stripe:evt_1:payment_success_notice",
            1,
            "stripe_payment_success",
            {"text": "ok"},
        )
        self.assertTrue(created)
        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("ON CONFLICT (delivery_key) DO UPDATE", sql)
        self.assertIn("WHERE message_delivery_events.status <> 'sent'", sql)
        self.assertIn("payload_json", sql)

    def test_message_delivery_outbox_claims_due_rows_with_skip_locked(self):
        cur = FakeCursor(fetches=[[("stripe:evt_1:payment_success_notice", 1, "stripe_payment_success", "{}", 1, None)]])
        rows = claim_pending_message_deliveries(cur, limit=5)
        self.assertEqual(rows[0][0], "stripe:evt_1:payment_success_notice")
        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertIn("status IN ('pending', 'failed')", sql)
        self.assertIn("RETURNING delivery_key, telegram_id, delivery_type, payload_json, attempt_count, invite_link", sql)

    def test_stripe_delivery_worker_uses_existing_outbox_and_saved_invite_links(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def process_pending_message_deliveries"):MAIN_SOURCE.index("async def scheduled_process_message_deliveries")]
        self.assertIn("claim_pending_message_deliveries", source)
        self.assertIn("save_delivery_invite_link", source)
        self.assertIn("mark_delivery_sent", source)
        self.assertIn("mark_delivery_failed", source)
        self.assertIn("BotBlocked", source)
        self.assertIn("blocked_bot = TRUE", source)

    def test_message_delivery_worker_is_scheduled_with_distributed_lock(self):
        self.assertIn("async def scheduled_process_message_deliveries", MAIN_SOURCE)
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def scheduled_process_message_deliveries"):MAIN_SOURCE.index("async def on_startup")]
        self.assertIn("run_scheduled_with_lock", source)
        self.assertIn('"process_message_deliveries"', source)
        startup = MAIN_SOURCE[MAIN_SOURCE.index("async def on_startup"):]
        self.assertIn("scheduled_process_message_deliveries", startup)

    def test_stale_negative_event_decisions(self):
        success = datetime(2026, 7, 23, 12, 0)
        old_failed = datetime(2026, 7, 23, 11, 59)
        new_failed = datetime(2026, 7, 23, 12, 1)
        self.assertFalse(should_apply_negative_event(old_failed, success))
        self.assertTrue(should_apply_negative_event(new_failed, success))
        self.assertTrue(live_subscription_is_paid("active", "paid"))

    def test_moscow_expiry_conversion(self):
        local_dt, utc_dt = parse_moscow_expiry("23.07.2026", "23:59")
        self.assertEqual(local_dt.tzinfo.key, "Europe/Moscow")
        self.assertEqual(utc_dt, datetime(2026, 7, 23, 20, 59))

    def test_grace_only_after_payment_failure(self):
        now = datetime(2026, 7, 23, 12, 0)
        expired = now - timedelta(minutes=5)
        grace = now + timedelta(hours=1)
        self.assertFalse(has_active_access(True, expired, payment_failed=False, grace_period_end=grace, now=now))
        self.assertTrue(has_active_access(True, expired, payment_failed=True, grace_period_end=grace, now=now))

    def test_webhook_required_env_declared(self):
        for name in (
            "BOT_TOKEN",
            "DATABASE_URL",
            "GROUP_ID",
            "ADMIN_IDS",
            "STRIPE_API_KEY",
            "STRIPE_WEBHOOK_SECRET",
            "WEBHOOK_SECRET",
            "YOUR_DOMAIN",
            "PRICE_TRIAL",
            "PRICE_1M",
            "PRICE_6M",
            "PRICE_12M",
        ):
            self.assertIn(name, MAIN_SOURCE)

    def test_db_pool_uses_timeouts_and_threaded_pool(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("class PooledDbConnection"):MAIN_SOURCE.index("def init_db")]
        self.assertIn("ThreadedConnectionPool", source)
        self.assertIn("connect_timeout=DB_CONNECT_TIMEOUT_SECONDS", source)
        self.assertIn("statement_timeout", source)
        self.assertIn("putconn(self._raw_conn)", source)
        self.assertIn("putconn(self._raw_conn, close=True)", source)
        self.assertIn("DB_POOL_CONNECTION_ERRORS", source)

    def test_db_pool_health_and_shutdown_are_exposed(self):
        self.assertIn("def db_pool_health", MAIN_SOURCE)
        self.assertIn("pool_available", MAIN_SOURCE)
        self.assertIn("pool_used", MAIN_SOURCE)
        self.assertIn("connection_errors", MAIN_SOURCE)
        self.assertIn("close_db_pool()", MAIN_SOURCE[MAIN_SOURCE.index("async def on_shutdown"):])
        self.assertIn("app.router.add_get('/health', health)", MAIN_SOURCE)

    def test_backup_config_decision(self):
        self.assertEqual(backup_decision({})["telegram_enabled"], False)
        self.assertFalse(backup_decision({"BACKUP_TELEGRAM_ENABLED": "true"})["allowed"])
        self.assertTrue(
            backup_decision({"BACKUP_TELEGRAM_ENABLED": "true", "BACKUP_ENCRYPTION_KEY": "secret"})["allowed"]
        )

    def test_no_direct_blocking_stripe_calls_in_async_functions(self):
        tree = ast.parse(MAIN_SOURCE)
        parents = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parents[child] = node

        def attr_chain(node):
            parts = []
            while isinstance(node, ast.Attribute):
                parts.append(node.attr)
                node = node.value
            if isinstance(node, ast.Name):
                parts.append(node.id)
            return list(reversed(parts))

        offenders = []
        for fn in [node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)]:
            for call in ast.walk(fn):
                if not isinstance(call, ast.Call):
                    continue
                chain = attr_chain(call.func)
                if not chain or chain[0] != "stripe":
                    continue
                if chain == ["stripe", "Webhook", "construct_event"]:
                    continue
                parent = parents.get(call)
                grandparent = parents.get(parent)
                wrapped = (
                    isinstance(grandparent, ast.Call)
                    and attr_chain(grandparent.func) == ["asyncio", "to_thread"]
                )
                if not wrapped:
                    offenders.append((fn.name, ".".join(chain), call.lineno))
        self.assertEqual(offenders, [])

    def test_dependencies_are_pinned_and_ci_exists(self):
        requirements = (ROOT / "requirements.txt").read_text().splitlines()
        for package in ("aiogram", "aiohttp", "stripe", "psycopg2-binary", "apscheduler"):
            self.assertTrue(any(line.startswith(package + "==") for line in requirements))
        self.assertTrue((ROOT / ".github/workflows/tests.yml").exists())

    def test_on_startup_does_not_delete_existing_webhook(self):
        startup = self.on_startup_node()
        self.assertNotIn("delete_webhook", ast.get_source_segment(MAIN_SOURCE, startup))
        self.assertIn("set_webhook", ast.get_source_segment(MAIN_SOURCE, startup))
        self.assertIn("get_webhook_info", ast.get_source_segment(MAIN_SOURCE, startup))

    def test_on_startup_verifies_webhook_url_after_set(self):
        startup_source = ast.get_source_segment(MAIN_SOURCE, self.on_startup_node())
        self.assertIn("actual_url != webhook_url", startup_source)
        self.assertIn("raise ValueError", startup_source)
        self.assertLess(startup_source.index("set_webhook"), startup_source.index("get_webhook_info"))
        self.assertLess(startup_source.index("get_webhook_info"), startup_source.index("actual_url != webhook_url"))

    def test_on_startup_logs_safe_webhook_path_only(self):
        startup_source = ast.get_source_segment(MAIN_SOURCE, self.on_startup_node())
        self.assertIn("safe_webhook_path", startup_source)
        self.assertIn("path=%s", startup_source)
        self.assertNotIn("safe_webhook_url", startup_source)
        self.assertNotIn("url=%s", startup_source)

    def test_pg_dump_command_keeps_database_url_out_of_argv(self):
        database_url = "postgres://bot_user:very-secret@db.example.com:6543/club_db"
        argv, env = build_pg_dump_command(database_url, {"PATH": "/usr/bin"})
        argv_text = " ".join(argv)
        self.assertEqual(
            argv,
            [
                "pg_dump",
                "--host", "db.example.com",
                "--port", "6543",
                "--username", "bot_user",
                "--dbname", "club_db",
                "--no-owner",
                "--no-privileges",
            ],
        )
        self.assertNotIn("very-secret", argv_text)
        self.assertNotIn(database_url, argv_text)
        self.assertNotIn("postgres://", argv_text)
        self.assertEqual(env["PGPASSWORD"], "very-secret")
        self.assertEqual(env["PGSSLMODE"], "require")
        self.assertEqual(env["PATH"], "/usr/bin")

    def test_pg_dump_command_decodes_percent_encoded_credentials(self):
        database_url = "postgresql://bot%40user:p%40ss%2Fword@db.example.com/safe%20db"
        argv, env = build_pg_dump_command(database_url)
        self.assertIn("bot@user", argv)
        self.assertIn("safe db", argv)
        self.assertEqual(env["PGPASSWORD"], "p@ss/word")
        self.assertNotIn("p%40ss%2Fword", " ".join(argv))

    def test_pg_dump_command_defaults_port_to_5432(self):
        argv, env = build_pg_dump_command("postgres://bot:secret@db.example.com/club")
        self.assertIn("5432", argv)
        self.assertEqual(env["PGSSLMODE"], "require")

    def test_pg_dump_command_rejects_invalid_url_safely(self):
        bad_url = "postgres://user:secret@/club"
        with self.assertRaisesRegex(ValueError, "Invalid PostgreSQL DATABASE_URL") as ctx:
            build_pg_dump_command(bad_url)
        self.assertNotIn("secret", str(ctx.exception))
        self.assertNotIn(bad_url, str(ctx.exception))

    def test_process_payment_trial_redeemed_loaded_before_cursor_close(self):
        start = MAIN_SOURCE.index("async def process_payment")
        end = MAIN_SOURCE.index('@router.callback_query(F.data == "retry_payment"', start)
        source = MAIN_SOURCE[start:end]
        self.assertIn("EXISTS (\n                SELECT 1\n                FROM trial_redemptions", source)
        first_close = source.index("cur.close()")
        trial_branch = source.index('if sub_type == "sub_trial"')
        self.assertLess(first_close, trial_branch)
        self.assertNotIn("cur.execute(\n            \"SELECT 1 FROM trial_redemptions", source[trial_branch:])

    def test_existing_subscription_guard_sql_has_matching_params_and_no_success_timestamps(self):
        marker = "GREATEST(COALESCE(expiry_date, %s), %s)"
        marker_index = MAIN_SOURCE.index(marker)
        source = MAIN_SOURCE[MAIN_SOURCE.rfind('cur.execute("""', 0, marker_index):MAIN_SOURCE.index("))", marker_index)]
        sql = source[source.index('cur.execute("""') + len('cur.execute("""'):source.index('""", (')]
        params = source[source.index('""", (') + len('""", ('):]
        self.assertEqual(sql.count("%s"), len([part for part in params.split(",") if part.strip()]))
        self.assertIn("GREATEST(COALESCE(expiry_date, %s), %s)", sql)
        self.assertNotIn("last_payment_succeeded_at", sql)
        self.assertNotIn("last_successful_invoice_created_at", sql)

    def test_admin_action_confirm_claims_once_and_completes(self):
        cur = AdminActionCursor()
        claim = claim_admin_action(cur, "act_1", 10)
        self.assertEqual(claim["status"], "claimed")
        self.assertEqual(cur.status, "processing")
        duplicate = claim_admin_action(cur, "act_1", 10)
        self.assertEqual(duplicate["status"], "processing")
        complete_admin_action(cur, "act_1")
        self.assertEqual(cur.status, "completed")

    def test_admin_action_wrong_admin_cannot_claim(self):
        cur = AdminActionCursor(owner_id=10)
        claim = claim_admin_action(cur, "act_1", 20)
        self.assertEqual(claim["status"], "pending")
        self.assertEqual(cur.status, "pending")

    def test_admin_action_cancel_pending_only(self):
        cur = AdminActionCursor()
        self.assertTrue(cancel_admin_action(cur, "act_1", 10))
        self.assertEqual(cur.status, "cancelled")
        self.assertFalse(cancel_admin_action(cur, "act_1", 10))

    def test_admin_action_fail_marks_processing_failed(self):
        cur = AdminActionCursor()
        claim_admin_action(cur, "act_1", 10)
        fail_admin_action(cur, "act_1")
        self.assertEqual(cur.status, "failed")

    def test_admin_confirmation_keyboard_and_broadcast_preview(self):
        keyboard = admin_action_confirmation_keyboard("abc")
        self.assertEqual(keyboard["confirm"], "admin_action:confirm:abc")
        self.assertEqual(keyboard["cancel"], "admin_action:cancel:abc")
        preview = broadcast_preview(7, "x" * 350)
        self.assertEqual(preview["recipient_count"], 7)
        self.assertEqual(preview["length"], 350)
        self.assertLessEqual(len(preview["preview"]), 303)

    def test_auto_free_lesson_uses_delivery_claim_helper(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def send_auto_free_lesson"):MAIN_SOURCE.index("async def check_auto_free_lessons")]
        self.assertIn("process_claimed_delivery", source)
        self.assertIn('f"free_lesson:{int(user_id)}"', source)
        self.assertNotIn("cur.execute(\"\"\"\n        UPDATE users\n        SET video_sent", source)

    def test_check_auto_free_lessons_has_no_single_commit_for_all_users(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def check_auto_free_lessons"):MAIN_SOURCE.index("async def send_free_lesson_followup")]
        self.assertIn("for (user_id,) in users:", source)
        self.assertNotIn("\n        conn.commit()\n\n        logging.info", source)

    def test_delivery_claim_sent_does_not_repeat(self):
        cur = DeliveryCursor(claim_fetch=None, status_fetch=("sent",))
        self.assertEqual(claim_message_delivery(cur, "free_lesson:1", 1, "free_lesson"), "already_sent")

    def test_delivery_claim_fresh_processing_does_not_repeat(self):
        cur = DeliveryCursor(claim_fetch=None, status_fetch=("processing",))
        self.assertEqual(claim_message_delivery(cur, "free_lesson:1", 1, "free_lesson"), "already_processing")

    def test_process_claimed_delivery_commits_claim_before_send(self):
        cursors = [DeliveryCursor(claim_fetch=("free_lesson:1",)), FakeCursor()]
        claim_conn = FakeConn(cursors[0])
        conns = [claim_conn, FakeConn(cursors[1])]
        sends = []

        async def send_func():
            sends.append(claim_conn.commits)

        def get_conn():
            return conns.pop(0)

        result = asyncio.run(process_claimed_delivery(get_conn, "free_lesson:1", 1, "free_lesson", send_func))
        self.assertEqual(result, "sent")
        self.assertEqual(sends, [1])

    def test_subscription_update_stale_rules(self):
        newer = datetime(2026, 7, 24, 12, 0)
        older = datetime(2026, 7, 24, 11, 0)
        self.assertFalse(should_apply_subscription_state_update(older, newer))
        self.assertTrue(should_apply_subscription_state_update(newer, older))
        self.assertTrue(should_live_check_stale_negative_subscription_update(older, newer))

    def test_subscription_updated_sql_tracks_last_state_and_live_check(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("elif event_type == 'customer.subscription.updated'"):MAIN_SOURCE.index("# ---------- 5. СЕССИЯ ОПЛАТЫ")]
        self.assertIn("last_subscription_state_event_created_at", source)
        self.assertIn("last_successful_invoice_created_at", source)
        self.assertIn("SUBSCRIPTION_UPDATED_NEGATIVE_STALE_IGNORED", source)
        self.assertIn("stripe.Subscription.retrieve", source)
        self.assertIn("live_subscription_is_paid", source)

    def test_subscription_updated_active_sql_params_match_placeholders(self):
        marker = "last_subscription_state_event_created_at = GREATEST("
        marker_index = MAIN_SOURCE.index(marker, MAIN_SOURCE.index("elif status in (\"active\", \"trialing\")"))
        source = MAIN_SOURCE[MAIN_SOURCE.rfind('cur.execute("""', 0, marker_index):MAIN_SOURCE.index("))", marker_index)]
        sql = source[source.index('cur.execute("""') + len('cur.execute("""'):source.index('""", (')]
        self.assertEqual(sql.count("%s"), 9)
        self.assertIn(
            "customer_id,\n"
            "                            event_created_at,\n"
            "                            event_created_at,\n"
            "                            sub_id,\n",
            source,
        )

    def test_group_admin_live_status_prevents_removal(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def ban_user_logic"):MAIN_SOURCE.index("# 1. Пытаемся удалить пользователя из группы")]
        self.assertIn("bot.get_chat_member", source)
        self.assertIn('telegram_status in ("administrator", "creator")', source)
        self.assertIn("telegram_status_error", source)

    def test_subscription_removal_has_durable_lease_table(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS subscription_removal_events", MAIN_SOURCE)
        self.assertIn("telegram_removed_at TIMESTAMP", MAIN_SOURCE)
        self.assertIn("db_finalized_at TIMESTAMP", MAIN_SOURCE)
        source = MAIN_SOURCE[MAIN_SOURCE.index("def claim_subscription_removal"):MAIN_SOURCE.index("def mark_subscription_removal_status")]
        self.assertIn("ON CONFLICT (telegram_id) DO UPDATE", source)
        self.assertIn("lease_until < %s", source)
        self.assertIn("status IN ('pending', 'telegram_failed')", source)
        self.assertIn("already_processing", source)

    def test_subscription_removal_closes_db_before_stripe_and_telegram(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def ban_user_logic"):MAIN_SOURCE.index("async def check_subscriptions_and_reminders")]
        self.assertLess(source.index("claim_conn.close()"), source.index("fetch_subscription_removal_user"))
        self.assertLess(source.index("fetch_subscription_removal_user"), source.index("refresh_active_stripe_subscription"))
        self.assertLess(source.index("fetch_subscription_removal_user"), source.index("bot.get_chat_member"))
        self.assertLess(source.index("mark_subscription_removal_short(telegram_id, \"removed\")"), source.index("finalize_subscription_removal_in_db"))

    def test_subscription_removal_kick_failure_does_not_close_access(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def ban_user_logic"):MAIN_SOURCE.index("async def check_subscriptions_and_reminders")]
        failure_pos = source.index("mark_subscription_removal_short(telegram_id, \"telegram_failed\", e)")
        return_pos = source.index("if status == \"kick_failed\":")
        finalize_pos = source.index("finalize_subscription_removal_in_db")
        self.assertLess(failure_pos, return_pos)
        self.assertLess(return_pos, finalize_pos)

    def test_subscription_check_releases_batch_cursor_before_side_effects(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def check_subscriptions_and_reminders"):MAIN_SOURCE.index("async def check_free_lesson_followups")]
        self.assertLess(source.index("conn.close()"), source.index("for (telegram_id, expiry"))
        self.assertIn("set_subscription_reminder_sent", source)
        self.assertIn("ban_status = await ban_user_logic(telegram_id)", source)
        self.assertNotIn("ban_user_logic(telegram_id, cur)", source)

    def test_duplicate_subscriptions_uses_live_stripe_list(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def duplicate_subscriptions_command"):MAIN_SOURCE.index("@router.message(Command('revoke_invite_links')")]
        self.assertIn("stripe.Subscription.list", source)
        self.assertIn("asyncio.to_thread", source)
        self.assertIn("asyncio.Semaphore(5)", source)
        self.assertIn("active_or_resumable_subscriptions", source)

    def test_bot_invite_links_migration_and_revoke_command(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS bot_invite_links", MAIN_SOURCE)
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def revoke_invite_links_command"):MAIN_SOURCE.index("@router.message(Command('resolve_checkout')")]
        self.assertIn("load_active_bot_invite_links", source)
        self.assertIn("make_action_request", source)
        executor = MAIN_SOURCE[MAIN_SOURCE.index("async def execute_confirmed_revoke_invite_links"):MAIN_SOURCE.index('@router.callback_query(F.data.startswith("admin_action:confirm:")')]
        self.assertIn("revoke_chat_invite_link", executor)
        self.assertIn("mark_bot_invite_link_revoked", executor)
        self.assertNotIn("create_chat_invite_link", source)

    def test_bot_invite_link_helpers_use_saved_active_links_only(self):
        cur = FakeCursor(fetches=[[("https://t.me/+one",), ("https://t.me/+two",)]])
        self.assertEqual(load_active_bot_invite_links(cur), ["https://t.me/+one", "https://t.me/+two"])
        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("bot_invite_links", sql)
        self.assertIn("status = 'active'", sql)
        save_bot_invite_link(cur, "https://t.me/+one", "test", 1)
        mark_bot_invite_link_revoked(cur, "https://t.me/+one")
        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("ON CONFLICT", sql)
        self.assertIn("revoked_at = NOW()", sql)

    def test_subscription_checkout_stripe_check_failure_is_fail_closed(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def process_payment"):MAIN_SOURCE.index('@router.callback_query(F.data == "retry_payment"')]
        self.assertIn("CHECKOUT_CUSTOMER_SUBSCRIPTIONS_CHECK_FAILED", source)
        self.assertIn("Новый subscription Checkout НЕ создан", source)
        self.assertIn("await state.clear()", source)
        failure_index = source.index("CHECKOUT_CUSTOMER_SUBSCRIPTIONS_CHECK_FAILED")
        create_index = source.index("stripe.checkout.Session.create")
        self.assertLess(failure_index, create_index)

    def test_trial_checkout_not_blocked_by_subscription_fail_closed_guard(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def process_payment"):MAIN_SOURCE.index('@router.callback_query(F.data == "retry_payment"')]
        self.assertIn("mode = 'payment' if sub_type == \"sub_trial\" else 'subscription'", source)
        self.assertIn("if mode == 'subscription' and stripe_customer_id", source)
        self.assertIn("if mode == 'subscription' and stripe_subscription_id", source)

    def test_creation_unknown_is_active_checkout_status(self):
        cur = FakeCursor(fetches=[(1, None, None, "creation_unknown", None, "idem_1", datetime.utcnow())])
        result = active_result = __import__("checkout_safety").claim_checkout_session_record(cur, 1, "sub_1", "subscription")
        self.assertEqual(result["action"], "retry_create")
        self.assertEqual(active_result["record"]["idempotency_key"], "idem_1")
        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("'creation_unknown'", sql)

    def test_checkout_network_error_marks_creation_unknown_not_failed(self):
        cur = FakeCursor()
        __import__("checkout_safety").mark_checkout_failed(cur, 1, TimeoutError("timeout"), status="creation_unknown")
        sql, params = cur.queries[-1]
        self.assertIn("SET status = %s", sql)
        self.assertEqual(params[0], "creation_unknown")

    def test_checkout_invalid_request_can_mark_failed(self):
        cur = FakeCursor()
        __import__("checkout_safety").mark_checkout_failed(cur, 1, ValueError("bad"), status="failed")
        self.assertEqual(cur.queries[-1][1][0], "failed")

    def test_checkout_unique_index_includes_creation_unknown(self):
        self.assertIn("WHERE status IN ('creating', 'creation_unknown', 'open')", MAIN_SOURCE)

    def test_checkout_retry_events_persist_attempts_for_restart_and_replicas(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS checkout_retry_events", MAIN_SOURCE)
        source = MAIN_SOURCE[MAIN_SOURCE.index("def register_checkout_attempt"):MAIN_SOURCE.index("async def notify_admins_about_checkout_retry")]
        self.assertIn("INSERT INTO checkout_retry_events", source)
        self.assertIn("SELECT COUNT(*)", source)
        self.assertIn("FROM checkout_retry_events", source)
        self.assertNotIn("len(retry_state", source)

    def test_checkout_retry_admin_cooldown_is_db_persisted(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def notify_admins_about_checkout_retry"):MAIN_SOURCE.index("def reset_checkout_retry_state_after_success")]
        self.assertIn("FOR UPDATE", source)
        self.assertIn("UPDATE checkout_retry_events", source)
        self.assertIn("last_admin_alert_at < %s", source)
        self.assertIn("RETURNING username, first_name, last_name", source)
        self.assertNotIn('retry_state["last_admin_alert_at"]', source)

    def test_checkout_success_resolves_retry_rows_without_deleting_fresh_audit(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("def reset_checkout_retry_state_after_success"):MAIN_SOURCE.index("async def send_checkout_open_instruction")]
        self.assertIn("resolved_at = COALESCE(resolved_at, NOW())", source)
        self.assertIn("resolved_source = COALESCE(resolved_source, %s)", source)
        self.assertIn("resolved_at < NOW() - INTERVAL '30 days'", source)
        self.assertNotIn("DELETE FROM checkout_retry_events\n            WHERE telegram_id = %s", source)

    def test_checkout_cleanup_preserves_active_creating_and_unknown_rows(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("def reset_checkout_retry_state_after_success"):MAIN_SOURCE.index("async def send_checkout_open_instruction")]
        self.assertIn("DELETE FROM checkout_sessions", source)
        self.assertIn("status IN ('completed', 'expired', 'failed')", source)
        self.assertNotIn("'creating'", source)
        self.assertNotIn("'creation_unknown'", source)
        self.assertNotIn("'open'", source)

    def test_checkout_retry_memory_is_optional_cache_only(self):
        register_source = MAIN_SOURCE[MAIN_SOURCE.index("def register_checkout_attempt"):MAIN_SOURCE.index("async def notify_admins_about_checkout_retry")]
        notify_source = MAIN_SOURCE[MAIN_SOURCE.index("async def notify_admins_about_checkout_retry"):MAIN_SOURCE.index("def reset_checkout_retry_state_after_success")]
        self.assertLess(register_source.index("INSERT INTO checkout_retry_events"), register_source.index("checkout_retry_state[user_id]"))
        self.assertIn("row[0] or checkout_retry_state.get", notify_source)
        self.assertIn("if not row:\n        return", notify_source)

    def test_revoke_invite_links_command_is_confirm_only(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def revoke_invite_links_command"):MAIN_SOURCE.index("@router.message(Command('resolve_checkout')")]
        self.assertIn("make_action_request", source)
        self.assertIn("admin_action_confirmation_keyboard", source)
        self.assertIn("До подтверждения ссылки не отзываются", source)
        self.assertNotIn("revoke_chat_invite_link", source)

    def test_execute_confirmed_admin_action_supports_required_types(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def execute_confirmed_admin_action"):MAIN_SOURCE.index("async def execute_confirmed_broadcast")]
        for action_type in ("broadcast", "give_access", "set_expiry", "link_stripe_user", "revoke_invite_links"):
            self.assertIn(f'action_type == "{action_type}"', source)

    def test_link_stripe_confirm_rechecks_conflicts(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def perform_link_stripe_user"):MAIN_SOURCE.index("async def execute_confirmed_give_access")]
        self.assertIn("Stripe IDs conflict changed before confirmation", source)
        self.assertIn("telegram_id <> %s", source)
        self.assertIn("FROM users", source)
        self.assertIn("FROM stripe_links", source)

    def test_group_join_handler_checks_live_telegram_status_before_removal(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def delete_join_leave_service_messages"):MAIN_SOURCE.index("GROUP_SERVICE_MESSAGE")]
        self.assertIn("bot.get_chat_member", source)
        self.assertIn('telegram_status in ("administrator", "creator")', source)
        self.assertIn("GROUP_JOIN_TELEGRAM_STATUS_ERROR", source)
        self.assertLess(source.index("bot.get_chat_member"), source.index("bot.ban_chat_member"))

    def test_followup_uses_per_user_delivery_claim(self):
        start = MAIN_SOURCE.index("async def send_free_lesson_followup")
        source = MAIN_SOURCE[start:MAIN_SOURCE.index("@router.message(F.content_type.in_([", start)]
        self.assertIn("process_claimed_delivery", source)
        self.assertIn("free_lesson_followup", source)
        self.assertIn("feedback_sent = TRUE", source)

    def test_check_followups_closes_db_before_user_sends(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def check_free_lesson_followups"):MAIN_SOURCE.index("# --- БЭКАП БАЗЫ ДАННЫХ ---")]
        self.assertLess(source.index("cur.close()"), source.index("send_free_lesson_followup"))
        self.assertNotIn("conn.commit()\n\n        logging.info", source)

    def test_duplicate_subscriptions_closes_db_before_stripe_gather(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def duplicate_subscriptions_command"):MAIN_SOURCE.index("@router.message(Command('revoke_invite_links')")]
        self.assertLess(source.index("cur.close()"), source.index("await asyncio.gather"))
        self.assertLess(source.index("await asyncio.gather"), source.rindex("conn = get_db_conn()"))

    def test_grace_block_is_reachable_for_reminder(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("for (telegram_id, expiry, payment_failed"):MAIN_SOURCE.index("# ----- Напоминание за 48 часов -----")]
        self.assertIn("grace_total += 1", source)
        self.assertIn("if not reminder_sent:", source)
        self.assertNotIn("пропущен из-за активного grace_period_end={fmt_report_dt(grace_end)}\"\n                )\n                continue", source)

    def test_give_access_handler_is_confirm_only(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def give_access_command"):MAIN_SOURCE.index("@router.message(Command('set_expiry')")]
        self.assertIn("make_action_request", source)
        self.assertIn("send_admin_action_confirmation", source)
        self.assertNotIn("INSERT INTO users", source)
        self.assertNotIn("UPDATE users", source)
        self.assertNotIn("generate_invite_link", source)
        self.assertNotIn("bot.send_message", source)

    def test_set_expiry_handler_is_confirm_only(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def set_expiry_command"):MAIN_SOURCE.index("@router.message(Command('sync_stripe_user')")]
        self.assertIn("make_action_request", source)
        self.assertIn("send_admin_action_confirmation", source)
        self.assertNotIn("INSERT INTO users", source)
        self.assertNotIn("UPDATE users", source)
        self.assertNotIn("generate_invite_link", source)

    def test_link_stripe_user_handler_is_confirm_only(self):
        link_start = MAIN_SOURCE.index("async def link_stripe_user_command")
        next_handler = MAIN_SOURCE.find("\n@router.message", link_start + 1)
        source = MAIN_SOURCE[link_start:next_handler]
        self.assertIn("make_action_request", source)
        self.assertIn("send_admin_action_confirmation", source)
        self.assertNotIn("backfill_payment_events_for_manual_link", source)
        self.assertNotIn("upsert_stripe_link", source)
        self.assertNotIn("UPDATE users", source)
        self.assertNotIn("UPDATE unlinked_stripe_events", source)

    def test_execute_confirmed_actions_call_perform_functions(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def execute_confirmed_admin_action"):MAIN_SOURCE.index("async def execute_confirmed_broadcast")]
        self.assertIn("execute_confirmed_give_access", MAIN_SOURCE)
        self.assertIn("return await perform_give_access(payload)", MAIN_SOURCE)
        self.assertIn("return await perform_set_expiry(payload)", MAIN_SOURCE)
        self.assertIn("return await perform_link_stripe_user(payload)", MAIN_SOURCE)
        self.assertIn('action_type == "give_access"', source)
        self.assertIn('action_type == "set_expiry"', source)
        self.assertIn('action_type == "link_stripe_user"', source)

    def test_admin_confirm_callback_closes_claim_connection_before_action(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def admin_action_confirm_callback"):MAIN_SOURCE.index('@router.callback_query(F.data.startswith("admin_action:cancel:")')]
        self.assertLess(source.index("conn.close()"), source.index("execute_confirmed_admin_action"))
        self.assertIn("complete_conn = get_db_conn()", source)
        self.assertIn("fail_conn = get_db_conn()", source)

    def test_perform_give_access_preserves_manual_semantics(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def perform_give_access"):MAIN_SOURCE.index("async def perform_set_expiry")]
        self.assertIn("base_expiry = old_expiry if old_expiry and old_expiry > datetime.utcnow() else datetime.utcnow()", source)
        self.assertIn("manual_give_access", source)
        self.assertIn("payment_failed = FALSE", source)
        self.assertNotIn("trial_used", source)
        self.assertNotIn("last_payment_succeeded_at", source)

    def test_perform_set_expiry_upserts_missing_user(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def perform_set_expiry"):MAIN_SOURCE.index("async def perform_link_stripe_user")]
        self.assertIn("INSERT INTO users", source)
        self.assertIn("ON CONFLICT (telegram_id) DO UPDATE", source)
        self.assertIn("manual_set_expiry", source)
        self.assertIn("manual_sync_at = NOW()", source)
        self.assertNotIn("last_payment_succeeded_at", source)

    def test_perform_link_stripe_rechecks_stripe_and_conflicts(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def perform_link_stripe_user"):MAIN_SOURCE.index("async def execute_confirmed_give_access")]
        self.assertIn("stripe.Subscription.retrieve", source)
        self.assertIn("Stripe subscription customer mismatch", source)
        self.assertIn("Stripe IDs conflict changed before confirmation", source)
        self.assertIn("FROM stripe_links", source)
        self.assertIn("prepare_manual_link_payment_events", source)
        self.assertIn("backfill_payment_events_for_manual_link", source)
        self.assertIn("UPDATE unlinked_stripe_events", source)
        self.assertIn("resolved = TRUE", source)
        self.assertIn("upsert_stripe_link", source)

    def test_creation_unknown_old_record_retries_same_key_without_insert(self):
        old = datetime.utcnow() - timedelta(hours=3)
        cur = FakeCursor(fetches=[(7, None, None, "creation_unknown", None, "idem_old", old)])
        result = __import__("checkout_safety").claim_checkout_session_record(cur, 1, "sub_1", "subscription")
        self.assertEqual(result["action"], "retry_create")
        self.assertEqual(result["record"]["id"], 7)
        self.assertEqual(result["record"]["idempotency_key"], "idem_old")
        sql = "\n".join(query for query, _ in cur.queries)
        self.assertNotIn("INSERT INTO checkout_sessions", sql)

    def test_stale_creating_retries_same_key_without_insert(self):
        old = datetime.utcnow() - timedelta(minutes=3)
        cur = FakeCursor(fetches=[(8, None, None, "creating", None, "idem_creating", old)])
        result = __import__("checkout_safety").claim_checkout_session_record(cur, 2, "sub_1", "subscription")
        self.assertEqual(result["action"], "retry_create")
        self.assertEqual(result["record"]["idempotency_key"], "idem_creating")
        sql = "\n".join(query for query, _ in cur.queries)
        self.assertNotIn("INSERT INTO checkout_sessions", sql)

    def test_fresh_creating_still_reports_in_progress(self):
        fresh = datetime.utcnow()
        cur = FakeCursor(fetches=[(9, None, None, "creating", None, "idem_fresh", fresh)])
        result = __import__("checkout_safety").claim_checkout_session_record(cur, 3, "sub_1", "subscription")
        self.assertEqual(result["action"], "creating_in_progress")

    def test_open_expired_terminal_update_allows_new_insert(self):
        old = datetime.utcnow() - timedelta(minutes=5)
        cur = FakeCursor(fetches=[
            (10, "cs_old", "https://checkout", "open", old, "idem_open", old),
            (11, None, None, "creating", None, "idem_new", datetime.utcnow()),
        ])
        result = __import__("checkout_safety").claim_checkout_session_record(cur, 4, "sub_1", "subscription")
        self.assertEqual(result["action"], "create")
        sql = "\n".join(query for query, _ in cur.queries)
        self.assertIn("SET status = CASE WHEN status = 'open' THEN 'expired' ELSE 'failed' END", sql)
        self.assertIn("INSERT INTO checkout_sessions", sql)

    def test_failed_status_not_active_allows_new_key(self):
        cur = FakeCursor(fetches=[None, (12, None, None, "creating", None, "idem_new_failed", datetime.utcnow())])
        result = __import__("checkout_safety").claim_checkout_session_record(cur, 5, "sub_1", "subscription")
        self.assertEqual(result["action"], "create")
        self.assertEqual(result["record"]["id"], 12)

    def test_identity_conflicts_are_upserted_not_duplicated(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("CREATE TABLE IF NOT EXISTS stripe_identity_conflicts"):MAIN_SOURCE.index("if identity_conflicts:")]
        self.assertIn("updated_at TIMESTAMP DEFAULT NOW()", source)
        self.assertIn("stripe_identity_conflicts_active_unique", source)
        self.assertIn("ON CONFLICT (conflict_type, stripe_id, telegram_ids)", source)
        self.assertIn("DO UPDATE SET", source)

    def test_literal_cur_execute_placeholder_counts_match_literal_params(self):
        tree = ast.parse(MAIN_SOURCE)
        mismatches = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute) or node.func.attr != "execute":
                continue
            if len(node.args) < 2:
                continue
            sql_node = node.args[0]
            params_node = node.args[1]
            if not isinstance(sql_node, ast.Constant) or not isinstance(sql_node.value, str):
                continue
            if not isinstance(params_node, (ast.Tuple, ast.List)):
                continue
            placeholder_count = sql_node.value.count("%s")
            param_count = len(params_node.elts)
            if placeholder_count != param_count:
                mismatches.append((node.lineno, placeholder_count, param_count, sql_node.value.strip().splitlines()[0]))
        self.assertEqual(mismatches, [])

    def test_metadata_invoice_update_tracks_success_timestamp_and_initial_flag(self):
        marker = "GREATEST(COALESCE(users.last_successful_invoice_created_at, %s), %s)"
        marker_pos = MAIN_SOURCE.index(marker)
        start = MAIN_SOURCE.rindex('cur.execute("""', 0, marker_pos)
        end = MAIN_SOURCE.index("row = cur.fetchone()", marker_pos)
        block = MAIN_SOURCE[start:end]
        self.assertIn("last_successful_invoice_created_at", block)
        self.assertIn(marker, block)
        self.assertIn("first_payment_done = CASE WHEN %s THEN TRUE ELSE users.first_payment_done END", block)
        cur = FakeCursor()
        query = block[block.index('cur.execute("""') + len('cur.execute("""'):block.index('""", (')]
        params = (1, datetime.utcnow(), datetime.utcnow(), "sub_1", "cus_1", 123, 123, True)
        cur.execute(query, params)
        self.assertEqual(query.count("%s"), len(params))

    def test_metadata_invoice_success_timestamp_none_preserves_existing_value(self):
        marker = "GREATEST(COALESCE(users.last_successful_invoice_created_at, %s), %s)"
        marker_pos = MAIN_SOURCE.index(marker)
        start = MAIN_SOURCE.rindex('cur.execute("""', 0, marker_pos)
        end = MAIN_SOURCE.index("row = cur.fetchone()", marker_pos)
        block = MAIN_SOURCE[start:end]
        self.assertIn("COALESCE(", block)
        self.assertIn("users.last_successful_invoice_created_at", block)
        params_start = block.index('""", (')
        params_block = block[params_start:]
        self.assertEqual(params_block.count("event_created_at"), 2)

    def test_ambiguous_checkout_under_retry_limit_reuses_same_key(self):
        old = datetime.utcnow() - timedelta(hours=19)
        cur = FakeCursor(fetches=[(21, None, None, "creation_unknown", None, "idem_19h", old)])
        result = __import__("checkout_safety").claim_checkout_session_record(cur, 6, "sub_1", "subscription")
        self.assertEqual(result["action"], "retry_create")
        self.assertEqual(result["record"]["idempotency_key"], "idem_19h")
        self.assertNotIn("INSERT INTO checkout_sessions", "\n".join(query for query, _ in cur.queries))

    def test_ambiguous_checkout_over_retry_limit_requires_manual_review(self):
        old = datetime.utcnow() - timedelta(hours=21)
        cur = FakeCursor(fetches=[(22, None, None, "creation_unknown", None, "idem_21h", old)])
        result = __import__("checkout_safety").claim_checkout_session_record(cur, 7, "sub_1", "subscription")
        self.assertEqual(result["action"], "manual_review_required")
        self.assertEqual(result["record"]["idempotency_key"], "idem_21h")
        self.assertNotIn("INSERT INTO checkout_sessions", "\n".join(query for query, _ in cur.queries))

    def test_ambiguous_checkout_days_old_requires_manual_review(self):
        old = datetime.utcnow() - timedelta(days=3)
        cur = FakeCursor(fetches=[(23, None, None, "creating", None, "idem_days", old)])
        result = __import__("checkout_safety").claim_checkout_session_record(cur, 8, "sub_1", "subscription")
        self.assertEqual(result["action"], "manual_review_required")
        self.assertEqual(CHECKOUT_AMBIGUOUS_AUTO_RETRY_HOURS, 20)

    def test_process_payment_manual_review_path_does_not_create_stripe_session(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index('if claim_result["action"] == "manual_review_required"'):MAIN_SOURCE.index('if claim_result["action"] == "reuse_open"')]
        self.assertIn("notify_admins", source)
        self.assertIn("/resolve_checkout <record_id> <failed|expired>", source)
        self.assertNotIn("stripe.checkout.Session.create", source)

    def test_resolve_checkout_command_is_confirm_only(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def resolve_checkout_command"):MAIN_SOURCE.index("@router.message(Command('link_stripe_user')")]
        self.assertIn("make_action_request", source)
        self.assertIn("send_admin_action_confirmation", source)
        self.assertNotIn("UPDATE checkout_sessions", source)

    def test_confirmed_resolve_checkout_updates_only_selected_record(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def execute_confirmed_resolve_checkout"):MAIN_SOURCE.index("async def execute_confirmed_revoke_invite_links")]
        self.assertIn("UPDATE checkout_sessions", source)
        self.assertIn("WHERE id = %s", source)
        self.assertIn("status IN ('creating', 'creation_unknown', 'open')", source)
        self.assertIn("RETURNING id, telegram_id, tariff_code, status", source)

    def test_perform_set_expiry_checks_membership_and_handles_bot_blocked(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def perform_set_expiry"):MAIN_SOURCE.index("async def perform_link_stripe_user")]
        self.assertIn("user_is_current_group_member", source)
        self.assertIn("generate_invite_link", source)
        self.assertIn("BotBlocked", source)
        self.assertIn("mark_user_blocked_bot", source)
        self.assertIn("completed_with_warning", MAIN_SOURCE)

    def test_perform_give_access_post_commit_errors_become_warnings(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def perform_give_access"):MAIN_SOURCE.index("async def perform_set_expiry")]
        self.assertLess(source.index("conn.commit()"), source.index("bot.unban_chat_member"))
        self.assertIn("warnings.append", source)
        self.assertIn("notify_admins", source)
        self.assertIn("mark_user_blocked_bot", source)
        self.assertNotIn("last_payment_succeeded_at", source)
        self.assertNotIn("trial_used", source)

    def test_perform_link_stripe_status_branches_are_safe(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def perform_link_stripe_user"):MAIN_SOURCE.index("async def execute_confirmed_give_access")]
        self.assertIn("manual_link_access_decision", source)
        self.assertIn("paid = CASE WHEN %s THEN TRUE ELSE users.paid END", source)
        self.assertIn("payment_failed = CASE WHEN %s THEN FALSE ELSE users.payment_failed END", source)
        self.assertNotIn("blocked_bot = FALSE", source)

    def test_manual_link_active_status_grants_future_access(self):
        now = datetime(2026, 7, 24, 10, 0)
        period_end = int((now + timedelta(days=30)).timestamp())
        decision = manual_link_access_decision("active", period_end, False, old_expiry=None, now=now)
        self.assertTrue(decision["grant_paid_access"])
        self.assertTrue(decision["auto_renew"])
        self.assertGreater(decision["effective_expiry"], now)

    def test_manual_link_trialing_status_grants_future_access(self):
        now = datetime(2026, 7, 24, 10, 0)
        period_end = int((now + timedelta(days=7)).timestamp())
        decision = manual_link_access_decision("trialing", period_end, True, old_expiry=None, now=now)
        self.assertTrue(decision["grant_paid_access"])
        self.assertFalse(decision["auto_renew"])

    def test_manual_link_past_due_does_not_extend_access(self):
        now = datetime(2026, 7, 24, 10, 0)
        old_expiry = now + timedelta(days=3)
        period_end = int((now + timedelta(days=30)).timestamp())
        decision = manual_link_access_decision("past_due", period_end, False, old_expiry=old_expiry, now=now)
        self.assertFalse(decision["grant_paid_access"])
        self.assertEqual(decision["effective_expiry"], old_expiry)
        self.assertTrue(decision["auto_renew"])

    def test_manual_link_unpaid_does_not_extend_access(self):
        now = datetime(2026, 7, 24, 10, 0)
        period_end = int((now + timedelta(days=30)).timestamp())
        decision = manual_link_access_decision("unpaid", period_end, False, old_expiry=None, now=now)
        self.assertFalse(decision["grant_paid_access"])
        self.assertIsNone(decision["effective_expiry"])

    def test_manual_link_canceled_does_not_grant_access(self):
        now = datetime(2026, 7, 24, 10, 0)
        period_end = int((now + timedelta(days=30)).timestamp())
        decision = manual_link_access_decision("canceled", period_end, False, old_expiry=None, now=now)
        self.assertFalse(decision["grant_paid_access"])
        self.assertFalse(decision["auto_renew"])

    def test_manual_link_past_period_end_does_not_grant_access(self):
        now = datetime(2026, 7, 24, 10, 0)
        old_expiry = now - timedelta(days=1)
        period_end = int((now - timedelta(hours=1)).timestamp())
        decision = manual_link_access_decision("active", period_end, False, old_expiry=old_expiry, now=now)
        self.assertFalse(decision["grant_paid_access"])
        self.assertEqual(decision["effective_expiry"], old_expiry)

    def test_perform_link_stripe_sends_invite_only_after_active_access_and_non_member(self):
        source = MAIN_SOURCE[MAIN_SOURCE.index("async def perform_link_stripe_user"):MAIN_SOURCE.index("async def execute_confirmed_give_access")]
        self.assertIn("if grant_paid_access:", source)
        self.assertIn("is_member = await user_is_current_group_member", source)
        self.assertIn("if not is_member:", source)
        self.assertIn("invite_sent = True", source)

    def test_identity_conflict_queries_sort_telegram_ids(self):
        import checkout_safety
        joined = "\n".join(query for _, query in checkout_safety.stripe_identity_conflict_queries())
        self.assertIn("array_agg(telegram_id ORDER BY telegram_id)", joined)
        self.assertIn("array_agg(DISTINCT telegram_id ORDER BY telegram_id)", joined)


if __name__ == "__main__":
    unittest.main()
