import unittest
import os
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from delivery_failure_admin_ux import human_delivery_label, render_critical_delivery_alert


os.environ.update({
    "BOT_TOKEN": "123456:TEST_TOKEN", "DATABASE_URL": "postgresql://user:pass@localhost/test",
    "GROUP_ID": "-100123", "ADMIN_IDS": "1,2", "STRIPE_API_KEY": "sk_test_dummy",
    "STRIPE_WEBHOOK_SECRET": "test_webhook_secret", "WEBHOOK_SECRET": "telegram_secret",
    "YOUR_DOMAIN": "https://club.example", "PRICE_TRIAL": "price_trial", "PRICE_1M": "price_1m",
    "PRICE_6M": "price_6m", "PRICE_12M": "price_12m",
    "GIFT_TOKEN_SECRET": "unit-test-gift-token-secret-32chars",
    "GIFT_PRICE_1M": "price_gift_1m", "GIFT_PRICE_6M": "price_gift_6m", "GIFT_PRICE_12M": "price_gift_12m",
})
import main


class DeliveryFailureAdminUxTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        await main.bot.session.close()

    def safe_ref(self, value):
        return "id_***" + value[-3:]

    def test_blocked_stripe_message_is_human_readable_and_private(self):
        raw_user = 777123456
        raw_key = "stripe:evt_secret_full:invoice_payment_failed"
        text = render_critical_delivery_alert(
            delivery_type="stripe_user_message", delivery_key=raw_key,
            telegram_id=raw_user, reason="telegram_forbidden_user_delivery",
            blocked=True, retryable=False, safe_user_ref=self.safe_ref,
            payload={"text": "secret sub_full cus_full in_full pi_full"},
        )
        self.assertIn("Сообщение об ошибке регулярного платежа", text)
        self.assertIn("Пользователь заблокировал бота", text)
        self.assertIn("Повторная отправка: не выполняется", text)
        self.assertIn("id_***456", text)
        for secret in (
            str(raw_user), raw_key, "evt_secret_full", "sub_full", "cus_full", "in_full", "pi_full",
            "TelegramForbiddenError", "delivery_hash",
        ):
            self.assertNotIn(secret, text)

    def test_unknown_stripe_subtype_uses_safe_fallback(self):
        self.assertEqual(
            human_delivery_label("stripe_user_message", "stripe:evt_secret:future_new_purpose", {}),
            "Важное сообщение о подписке или оплате",
        )

    def test_bad_request_is_human_readable_and_terminal(self):
        text = render_critical_delivery_alert(
            delivery_type="access_restore_invite", delivery_key="access:private-key",
            telegram_id=777123456, reason="telegram_bad_request_terminal",
            blocked=False, retryable=False, safe_user_ref=self.safe_ref,
        )
        self.assertIn("Ссылка для восстановления доступа", text)
        self.assertIn("Telegram сообщил, что чат с пользователем недоступен", text)
        self.assertIn("Повторная отправка: не выполняется", text)
        self.assertNotIn("access:private-key", text)

    def test_gift_critical_labels_are_specific(self):
        self.assertEqual(
            human_delivery_label("gift_certificate_recipient"),
            "Подарочный сертификат получателю",
        )
        self.assertEqual(
            human_delivery_label("gift_redeemed_recipient"),
            "Сообщение получателю об активации подарка",
        )
        self.assertEqual(
            human_delivery_label("gift_refunded_recipient"),
            "Сообщение получателю о возврате подарка",
        )

    async def test_retryable_alert_says_automatic_retry_without_changing_dedupe_key(self):
        notify = AsyncMock()
        escalation = {
            "stage": "critical_attempt_3", "retry_age": timedelta(minutes=20),
            "critical": True, "delivery_hash": "safehash1",
        }
        with patch.object(main, "notify_admins", notify):
            await main.notify_retryable_outbox_failure(
                "stripe:evt_secret:payment_failed", "stripe_user_message", 3,
                RuntimeError("raw private error"),
                {"reason": "telegram_network_error", "retryable": True}, escalation,
                telegram_id=777123456, payload={"text": "sub_secret"},
            )
        call = notify.await_args
        self.assertIn("Повторная отправка: будет выполнена автоматически", call.args[0])
        self.assertIn("Временная ошибка Telegram", call.args[0])
        self.assertEqual(call.kwargs["alert_key"], "outbox-retry:safehash1:critical_attempt_3")
        self.assertTrue(call.kwargs["dedupe_forever"])
        self.assertNotIn("raw private error", call.args[0])
        self.assertNotIn("sub_secret", call.args[0])

    async def test_permanent_alert_keeps_policy_and_hides_technical_details(self):
        notify = AsyncMock()
        with patch.object(main, "notify_admins", notify):
            await main.notify_permanent_outbox_failure(
                "stripe:evt_secret:invoice_payment_failed", "stripe_user_message", 1,
                RuntimeError("private"), blocked=True, telegram_id=777123456,
                payload={"text": "cus_secret"}, reason="telegram_forbidden_user_delivery",
            )
        call = notify.await_args
        self.assertEqual(call.kwargs["alert_key"], f"outbox-permanent:{main.safe_delivery_hash('stripe:evt_secret:invoice_payment_failed')}")
        self.assertEqual(call.kwargs["severity"], "CRITICAL")
        self.assertTrue(call.kwargs["dedupe_forever"])
        self.assertNotIn("RuntimeError", call.args[0])
        self.assertNotIn("private", call.args[0])
        self.assertNotIn("cus_secret", call.args[0])


if __name__ == "__main__":
    unittest.main()
