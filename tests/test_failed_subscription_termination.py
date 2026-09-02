import inspect
import os
import unittest
from pathlib import Path


os.environ.setdefault("BOT_TOKEN", "123456:TEST")
os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")
os.environ.setdefault("GROUP_ID", "-100123")
os.environ.setdefault("ADMIN_IDS", "1,2")
os.environ.setdefault("STRIPE_API_KEY", "sk_test_dummy")
os.environ.setdefault("STRIPE_WEBHOOK_SECRET", "test_webhook_secret")
os.environ.setdefault("WEBHOOK_SECRET", "telegram_test")
os.environ.setdefault("YOUR_DOMAIN", "https://club.example")
os.environ.setdefault("PRICE_TRIAL", "price_trial")
os.environ.setdefault("PRICE_1M", "price_1m")
os.environ.setdefault("PRICE_6M", "price_6m")
os.environ.setdefault("PRICE_12M", "price_12m")

import main


class FailedSubscriptionTerminationTests(unittest.TestCase):
    def test_failed_message_keyboard_has_distinct_actions(self):
        markup = main.get_failed_subscription_actions_keyboard()
        buttons = [button for row in markup.inline_keyboard for button in row]
        self.assertEqual(
            [(button.text, button.callback_data) for button in buttons],
            [
                ("💳 Оплатить / сменить карту", "failed_subscription_payment_action"),
                ("❌ Отписаться и закрыть доступ", "cancel_failed_subscription"),
            ],
        )
        self.assertNotEqual(buttons[1].callback_data, "cancel_subscription")

    def test_normal_cancel_still_uses_end_of_period(self):
        source = inspect.getsource(main.cancel_subscription)
        self.assertIn("cancel_at_period_end=True", source)
        self.assertNotIn("terminate_failed_subscription", source)

    def test_failed_grace_is_fixed_and_hourly_candidate_is_narrow(self):
        source = Path(main.__file__).read_text(encoding="utf-8")
        failed = source[source.index("elif event_type == 'invoice.payment_failed'"):source.index("# ---------- 4. ПОЛЬЗОВАТЕЛЬ")]
        self.assertIn("COALESCE(payment_failed_at, NOW()) + (%s * INTERVAL '1 hour')", failed)
        self.assertNotIn("NOW() + (%s * INTERVAL '1 hour')\n                        )", failed)
        hourly = inspect.getsource(main.process_expired_failed_subscription_grace)
        self.assertIn("payment_failed = TRUE", hourly)
        self.assertIn("grace_period_end <= NOW()", hourly)
        self.assertIn("terminate_failed_subscription", hourly)

    def test_failed_cycle_delivery_key_dedupes_different_events_in_same_grace(self):
        from datetime import datetime
        deadline = datetime(2026, 9, 3, 10, 0)
        self.assertEqual(
            main.failed_subscription_cycle_delivery_key("sub_same", deadline),
            main.failed_subscription_cycle_delivery_key("sub_same", deadline),
        )
        self.assertNotIn("sub_same", main.failed_subscription_cycle_delivery_key("sub_same", deadline))

    def test_immediate_cancel_relies_on_sdk_collection_guarantee_without_void(self):
        source = inspect.getsource(main.terminate_failed_subscription)
        self.assertIn("subscription.cancel", source)
        self.assertNotIn("void_invoice", source)

    def test_late_webhook_fences_are_subscription_specific(self):
        source = Path(main.__file__).read_text(encoding="utf-8")
        self.assertIn("failed_subscription_terminations fst", source)
        self.assertIn("fst.stripe_subscription_id = users.stripe_subscription_id", source)
        self.assertIn("fst.status <> 'superseded'", source)


if __name__ == "__main__":
    unittest.main()
