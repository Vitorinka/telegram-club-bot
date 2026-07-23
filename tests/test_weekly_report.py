import ast
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from weekly_report import (
    build_payments_csv,
    build_weekly_report_text,
    claim_weekly_report_run_record,
    classify_manual_link_payment_kind,
    format_change,
    format_buyer_name,
    format_money_change,
    format_minor_amount,
    format_period_title,
    get_current_week_bounds,
    get_last_completed_week_bounds,
    report_key,
    sanitize_csv_cell,
    should_create_manual_link_payment_event,
    tariff_code_from_invoice,
    to_utc_naive,
)


class FakeWeeklyReportCursor:
    def __init__(self, row=None):
        self.row = row
        self.results = []
        self.queries = []

    def execute(self, query, params):
        self.queries.append((query, params))
        normalized = " ".join(query.split()).upper()
        if normalized.startswith("INSERT INTO WEEKLY_REPORT_RUNS"):
            key, period_start, period_end, now, _, stale_before = params
            if self.row is None:
                self.row = {
                    "report_key": key,
                    "period_start": period_start,
                    "period_end": period_end,
                    "status": "processing",
                    "created_at": now,
                    "updated_at": now,
                    "sent_admin_ids": None,
                }
                self.results.append(("processing", None))
            elif self.row["status"] == "failed" or (
                self.row["status"] == "processing"
                and (self.row.get("updated_at") or self.row.get("created_at")) < stale_before
            ):
                self.row.update({
                    "status": "processing",
                    "period_start": period_start,
                    "period_end": period_end,
                    "updated_at": now,
                })
                self.results.append(("processing", self.row.get("sent_admin_ids")))
            else:
                self.results.append(None)
        elif normalized.startswith("SELECT STATUS"):
            if self.row is None:
                self.results.append(None)
            else:
                self.results.append((self.row["status"], self.row.get("sent_admin_ids")))

    def fetchone(self):
        return self.results.pop(0)


class WeeklyReportTest(unittest.TestCase):
    def test_last_completed_week_bounds_use_moscow_monday(self):
        now = datetime(2026, 7, 22, 12, 0, tzinfo=ZoneInfo("Europe/Moscow"))
        start, end = get_last_completed_week_bounds(now)
        self.assertEqual(start.isoformat(), "2026-07-13T00:00:00+03:00")
        self.assertEqual(end.isoformat(), "2026-07-20T00:00:00+03:00")

    def test_monday_boundary_reports_previous_full_week(self):
        now = datetime(2026, 7, 20, 10, 0, tzinfo=ZoneInfo("Europe/Moscow"))
        start, end = get_last_completed_week_bounds(now)
        self.assertEqual(start.isoformat(), "2026-07-13T00:00:00+03:00")
        self.assertEqual(end.isoformat(), "2026-07-20T00:00:00+03:00")

    def test_week_bounds_cross_month_and_year(self):
        now = datetime(2027, 1, 4, 10, 0, tzinfo=ZoneInfo("Europe/Moscow"))
        start, end = get_last_completed_week_bounds(now)
        self.assertEqual(start.isoformat(), "2026-12-28T00:00:00+03:00")
        self.assertEqual(end.isoformat(), "2027-01-04T00:00:00+03:00")

    def test_current_and_previous_comparison_bounds(self):
        now = datetime(2026, 7, 22, 12, 0, tzinfo=ZoneInfo("Europe/Moscow"))
        current_start, current_end = get_current_week_bounds(now)
        previous_start = current_start - timedelta(days=7)
        previous_end = current_start
        self.assertEqual(current_start.isoformat(), "2026-07-20T00:00:00+03:00")
        self.assertEqual(current_end.isoformat(), "2026-07-22T12:00:00+03:00")
        self.assertEqual(previous_start.isoformat(), "2026-07-13T00:00:00+03:00")
        self.assertEqual(previous_end.isoformat(), "2026-07-20T00:00:00+03:00")

    def test_format_period_title_finished_and_current_periods(self):
        tz = ZoneInfo("Europe/Moscow")
        self.assertEqual(
            format_period_title(datetime(2026, 7, 13, tzinfo=tz), datetime(2026, 7, 20, tzinfo=tz)),
            "13–19 июля 2026",
        )
        self.assertEqual(
            format_period_title(datetime(2026, 7, 20, tzinfo=tz), datetime(2026, 7, 22, 12, tzinfo=tz)),
            "20–22 июля 2026",
        )
        self.assertEqual(
            format_period_title(datetime(2026, 12, 28, tzinfo=tz), datetime(2027, 1, 4, tzinfo=tz)),
            "28 декабря – 3 января 2027",
        )
        self.assertEqual(
            format_period_title(datetime(2026, 7, 20, tzinfo=tz), datetime(2026, 7, 20, tzinfo=tz)),
            "20 июля 2026",
        )

    def test_moscow_bounds_convert_to_utc_naive_for_db(self):
        value = datetime(2026, 7, 13, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))
        self.assertEqual(to_utc_naive(value), datetime(2026, 7, 12, 21, 0))

    def test_format_change(self):
        self.assertEqual(format_change(12, 10), "↑ 20% (+2) к прошлой неделе")
        self.assertEqual(format_change(8, 10), "↓ 20% (-2) к прошлой неделе")
        self.assertEqual(format_change(3, 0), "рост с 0 до 3")
        self.assertEqual(format_change(0, 0), "без изменений")

    def test_format_money_change(self):
        self.assertEqual(format_money_change(45000, 30000, "EUR"), "↑ 50% (+150,00 €) к прошлой неделе")
        self.assertEqual(format_money_change(25000, 50000, "EUR"), "↓ 50% (-250,00 €) к прошлой неделе")
        self.assertEqual(format_money_change(5000, 0, "EUR"), "рост с 0 до 50,00 €")
        self.assertEqual(format_money_change(0, 0, "EUR"), "без изменений")

    def test_tariff_code_from_invoice_uses_env_price_ids(self):
        invoice = {"lines": {"data": [{"price": {"id": "configured_monthly_price"}}]}}
        env = {"PRICE_1M": "configured_monthly_price"}
        self.assertEqual(tariff_code_from_invoice(invoice, env=env), "sub_1")
        self.assertEqual(tariff_code_from_invoice(invoice, env={}), "unknown")

    def test_report_key(self):
        start = datetime(2026, 7, 13, tzinfo=ZoneInfo("Europe/Moscow"))
        self.assertEqual(report_key(start), "2026-07-13")

    def test_different_currencies_are_not_summed(self):
        text = build_weekly_report_text(
            datetime(2026, 7, 13, tzinfo=ZoneInfo("Europe/Moscow")),
            datetime(2026, 7, 20, tzinfo=ZoneInfo("Europe/Moscow")),
            {
                "revenue_by_currency": {"EUR": 5000, "USD": 7000},
                "tariff_counts": {},
            },
            comparison={"revenue_by_currency": {"EUR": 2500, "USD": 0}},
            buyers=[],
        )
        self.assertIn("50,00 €", text)
        self.assertIn("70,00 $", text)
        self.assertIn("0,00 € — ↓ 100% (-100,00 €) к прошлой неделе", build_weekly_report_text(
            datetime(2026, 7, 13, tzinfo=ZoneInfo("Europe/Moscow")),
            datetime(2026, 7, 20, tzinfo=ZoneInfo("Europe/Moscow")),
            {"revenue_by_currency": {}, "tariff_counts": {}},
            comparison={"revenue_by_currency": {"EUR": 10000}},
            buyers=[],
        ))

    def test_buyer_list_is_trimmed_and_fallback_name_is_used(self):
        buyers = [
            {
                "paid_at": datetime(2026, 7, 15, 9, i, tzinfo=timezone.utc),
                "telegram_id": 1000 + i,
                "tariff_code": "sub_1",
                "payment_kind": "initial_subscription",
                "amount_paid": 5000,
                "currency": "eur",
            }
            for i in range(11)
        ]
        text = build_weekly_report_text(
            datetime(2026, 7, 13, tzinfo=ZoneInfo("Europe/Moscow")),
            datetime(2026, 7, 20, tzinfo=ZoneInfo("Europe/Moscow")),
            {"revenue_by_currency": {"EUR": 55000}, "tariff_counts": {"sub_1": 11}},
            buyers=buyers,
        )
        self.assertIn("telegram_id: 1000", text)
        self.assertIn("Ещё покупок: 1", text)
        self.assertEqual(format_buyer_name({"telegram_id": 42}), "telegram_id: 42")

    def test_zero_report_values_and_history_notice(self):
        text = build_weekly_report_text(
            datetime(2026, 7, 13, tzinfo=ZoneInfo("Europe/Moscow")),
            datetime(2026, 7, 20, tzinfo=ZoneInfo("Europe/Moscow")),
            {},
            history_note="История платежей собирается с 15.07.2026. Оплаты до этой даты в выручку не включены.",
        )
        self.assertIn("Новые регистрации: 0", text)
        self.assertIn("Покупок за период нет.", text)
        self.assertIn("История платежей собирается", text)

    def test_money_and_csv_do_not_include_stripe_ids_or_email(self):
        payment = {
            "paid_at": datetime(2026, 7, 15, 9, 30, tzinfo=timezone.utc),
            "telegram_id": 123,
            "username": "@client",
            "first_name": "=HYPERLINK(\"https://example.com\")",
            "tariff_code": "sub_1",
            "payment_kind": "initial_subscription",
            "amount_paid": 5000,
            "currency": "eur",
            "billing_reason": "subscription_create",
            "recovered_after_failure": False,
            "invoice_id": "in_should_not_appear",
            "email": "client@example.com",
        }
        csv_text = build_payments_csv([payment]).decode("utf-8-sig")
        self.assertEqual(format_minor_amount(5000, "eur"), "50,00 €")
        self.assertEqual(sanitize_csv_cell("=HYPERLINK(1)"), "'=HYPERLINK(1)")
        report_text = build_weekly_report_text(
            datetime(2026, 7, 13, tzinfo=ZoneInfo("Europe/Moscow")),
            datetime(2026, 7, 20, tzinfo=ZoneInfo("Europe/Moscow")),
            {"revenue_by_currency": {"EUR": 5000}, "tariff_counts": {"sub_1": 1}},
            buyers=[payment],
        )
        self.assertIn("@client", report_text)
        self.assertNotIn("in_should_not_appear", csv_text)
        self.assertNotIn("client@example.com", csv_text)
        self.assertIn("50.00", csv_text)
        self.assertIn("'=HYPERLINK", csv_text)

    def test_main_has_weekly_tables_scheduler_and_event_writes(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        self.assertIn("CREATE TABLE IF NOT EXISTS payment_events", source)
        self.assertIn("CREATE TABLE IF NOT EXISTS weekly_report_runs", source)
        self.assertIn("payment_history_started_at", source)
        self.assertIn("timezone=MOSCOW_TZ", source)
        self.assertIn("misfire_grace_time=3600", source)
        self.assertIn("insert_payment_event(", source)
        self.assertIn("subscription_auto_renew_disabled", source)
        self.assertIn("group_member_joined", source)
        self.assertIn("weekly_csv:", source)
        self.assertIn("ALTER TABLE weekly_report_runs ADD COLUMN IF NOT EXISTS updated_at", source)

    def test_source_payment_event_requirements(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        self.assertIn("ON CONFLICT (stripe_event_id) DO NOTHING", source)
        self.assertIn("payment_kind=payment_kind", source)
        self.assertIn('payment_kind="trial" if is_trial and not has_subscription else "unknown"', source)
        link_only_start = source.index('if checkout_action == "link_only":')
        link_only_end = source.index("days_to_add = 0", link_only_start)
        self.assertNotIn("insert_payment_event(", source[link_only_start:link_only_end])
        self.assertIn('"failed"', source[source.index("elif event['type'] == 'invoice.payment_failed'"):])
        self.assertIn("backfill_payment_events_for_manual_link", source)
        self.assertIn("created_at=event[\"created_at\"]", source)
        self.assertIn("resolved = TRUE", source)
        self.assertIn("prepare_manual_link_payment_events", source)
        self.assertIn("await asyncio.to_thread(stripe.Invoice.retrieve", source)
        self.assertIn("if not event[\"create_payment_event\"]", source)

    def test_source_weekly_claim_and_admin_gates(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        weekly_py = Path(__file__).resolve().parents[1] / "weekly_report.py"
        weekly_source = weekly_py.read_text()
        self.assertIn("ON CONFLICT (report_key) DO UPDATE", weekly_source)
        self.assertIn("weekly_report_runs.status = 'failed'", weekly_source)
        for command in ("weekly_report_command", "weekly_report_current_command", "weekly_report_send_command"):
            start = source.index(f"async def {command}")
            block = source[start:source.index("\n\n", start)]
            self.assertIn("if message.from_user.id not in ADMIN_IDS", block)
        self.assertIn("day_of_week='mon'", source)
        self.assertIn("hour=10", source)
        self.assertIn("with_actions=False", source[source.index("async def weekly_report_current_command"):source.index("async def weekly_report_send_command")])
        self.assertNotIn("force=True", source)

    def test_weekly_report_send_signature_and_calls_do_not_use_force(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        tree = ast.parse(main_py.read_text())
        definitions = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "send_weekly_admin_report"
        ]
        self.assertEqual(len(definitions), 1)
        arg_names = [arg.arg for arg in definitions[0].args.args]
        self.assertNotIn("force", arg_names)

        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "send_weekly_admin_report"
        ]
        self.assertGreaterEqual(len(calls), 1)
        for call in calls:
            self.assertFalse(any(keyword.arg == "force" for keyword in call.keywords))

    def test_weekly_report_claim_state_machine(self):
        now = datetime(2026, 7, 20, 10, 0)
        start = datetime(2026, 7, 13, 21, 0)
        end = datetime(2026, 7, 20, 21, 0)
        fresh = {"status": "processing", "created_at": now - timedelta(minutes=10), "updated_at": now - timedelta(minutes=10), "sent_admin_ids": "1"}
        stale = {"status": "processing", "created_at": now - timedelta(hours=1), "updated_at": now - timedelta(hours=1), "sent_admin_ids": "1"}
        completed = {"status": "completed", "created_at": now, "updated_at": now, "sent_admin_ids": "1,2"}
        failed = {"status": "failed", "created_at": now, "updated_at": now, "sent_admin_ids": ""}

        self.assertEqual(claim_weekly_report_run_record(FakeWeeklyReportCursor(), "2026-07-13", start, end, now)["status"], "claimed")
        self.assertEqual(claim_weekly_report_run_record(FakeWeeklyReportCursor(fresh), "2026-07-13", start, end, now)["status"], "already_processing")
        self.assertEqual(claim_weekly_report_run_record(FakeWeeklyReportCursor(stale), "2026-07-13", start, end, now)["status"], "claimed")
        self.assertEqual(claim_weekly_report_run_record(FakeWeeklyReportCursor(failed), "2026-07-13", start, end, now)["status"], "claimed")
        claimed_completed = claim_weekly_report_run_record(FakeWeeklyReportCursor(completed), "2026-07-13", start, end, now)
        self.assertEqual(claimed_completed["status"], "duplicate_completed")
        self.assertEqual(claimed_completed["sent_admin_ids"], [1, 2])

    def test_group_service_handler_processes_all_new_members(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        self.assertIn("for service_user in service_event_users:", source)
        self.assertNotIn("message.new_chat_members[0] if message.new_chat_members else None", source)

    def test_manual_link_zero_invoice_is_resolved_without_payment_event(self):
        self.assertFalse(should_create_manual_link_payment_event(0))
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        backfill_start = source.index("def backfill_payment_events_for_manual_link")
        backfill_end = source.index("# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---", backfill_start)
        backfill_block = source[backfill_start:backfill_end]
        self.assertIn("if not event[\"create_payment_event\"]:", backfill_block)
        self.assertIn("continue", backfill_block)
        link_start = source.index("async def link_stripe_user_command")
        link_block = source[link_start:source.index("@dp.message_handler(commands=['unban_user']", link_start)]
        self.assertIn("UPDATE unlinked_stripe_events", link_block)
        self.assertIn("resolved = TRUE", link_block)

    def test_manual_link_out_of_band_classification(self):
        self.assertEqual(
            classify_manual_link_payment_kind("subscription_cycle", "process_out_of_band"),
            "out_of_band",
        )
        self.assertEqual(
            classify_manual_link_payment_kind("subscription_cycle", None),
            "unknown",
        )

    def test_weekly_current_report_has_no_future_callbacks(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        current_start = source.index("async def weekly_report_current_command")
        current_block = source[current_start:source.index("async def weekly_report_send_command", current_start)]
        self.assertIn("get_current_week_bounds()", current_block)
        self.assertIn("with_actions=False", current_block)
        self.assertNotIn("_weekly_report_keyboard", current_block)

    def test_finished_week_csv_filename_uses_last_calendar_day(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text()
        csv_start = source.index("async def send_weekly_csv")
        csv_block = source[csv_start:source.index("async def payment_needs_rejoin_invite", csv_start)]
        self.assertIn("csv_end = period_end - timedelta(days=1)", csv_block)
        self.assertIn("period_end > period_start", csv_block)


if __name__ == "__main__":
    unittest.main()
