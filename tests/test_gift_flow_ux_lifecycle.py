import asyncio
import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from zoneinfo import ZoneInfo

os.environ.update({
    "BOT_TOKEN": "123456:TEST_TOKEN", "DATABASE_URL": "postgresql://user:pass@localhost/test",
    "GROUP_ID": "-100123", "ADMIN_IDS": "1,2", "STRIPE_API_KEY": "sk_test_dummy",
    "STRIPE_WEBHOOK_SECRET": "test_webhook_secret", "WEBHOOK_SECRET": "telegram_secret",
    "YOUR_DOMAIN": "https://club.example", "PRICE_TRIAL": "price_trial", "PRICE_1M": "price_1m",
    "PRICE_6M": "price_6m", "PRICE_12M": "price_12m", "BOT_USERNAME": "ClubGiftBot",
    "GIFT_TOKEN_SECRET": "unit-test-gift-token-secret-32chars",
    "GIFT_PRICE_1M": "price_gift_1m", "GIFT_PRICE_6M": "price_gift_6m", "GIFT_PRICE_12M": "price_gift_12m",
})

import main
from weekly_report import build_weekly_report_text


class Connection:
    def __init__(self, fetches=None):
        self.cursor_obj = Cursor(fetches or [])

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


class Cursor:
    def __init__(self, fetches):
        self.fetches = list(fetches)
        self.queries = []

    def execute(self, query, params=None):
        self.queries.append((query, params))

    def fetchone(self):
        return self.fetches.pop(0) if self.fetches else None

    def fetchall(self):
        return self.fetches.pop(0) if self.fetches else []

    def close(self):
        pass


class GiftFlowUxLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        await main.bot.session.close()

    def test_purchaser_instruction_prepares_forward_and_hides_internal_reference(self):
        text = main.build_gift_buyer_paid_text({"public_reference": "GIFT-SECRET12345678"})
        self.assertIn("Следующим сообщением", text)
        self.assertIn("Перешлите следующее сообщение целиком", text)
        self.assertIn("Вам больше ничего делать не нужно", text)
        self.assertNotIn("GIFT-", text)

    def test_certificate_is_self_contained_and_fallback_is_labelled(self):
        row = {
            "recipient_name": "Анна", "sender_name": "Виктория",
            "gift_message": "С любовью", "tariff_code": "gift_1m",
        }
        caption = main.gift_certificate_delivery_caption(
            main.gift_certificate_caption(row),
            "https://t.me/ClubGiftBot?start=gift_token",
        )
        self.assertIn("🎁 Вам подарили доступ", caption)
        self.assertIn("1. Нажмите «Активировать подарок»", caption)
        self.assertIn("Start / Начать", caption)
        self.assertIn("Если кнопка не открывается, используйте эту ссылку:", caption)

    async def test_deep_link_shows_one_confirmation_without_duplicate_certificate(self):
        token = main.generate_gift_token("GIFT-ABCD1234ABCD1234", 1)
        row = {
            "public_reference": "GIFT-ABCD1234ABCD1234", "token_version": 1,
            "token_hash": main.gift_token_hash(token), "status": "paid_unclaimed",
            "recipient_telegram_id": None, "tariff_code": "gift_1m", "sender_name": "Виктория",
        }
        message = SimpleNamespace(from_user=SimpleNamespace(id=777), answer=AsyncMock())
        state = SimpleNamespace(update_data=AsyncMock())
        with patch.object(main, "get_db_conn", return_value=Connection()), \
             patch.object(main, "fetch_gift_by_public_reference_version", return_value=row), \
             patch.object(main, "enqueue_gift_certificate_delivery") as enqueue_certificate:
            await main.show_gift_deep_link(message, state, token)
        enqueue_certificate.assert_not_called()
        message.answer.assert_awaited_once()
        self.assertIn("Подарок найден", message.answer.await_args.args[0])
        self.assertEqual(
            message.answer.await_args.kwargs["reply_markup"].inline_keyboard[0][0].callback_data,
            "gift_activate:GIFT-ABCD1234ABCD1234",
        )

    def test_redemption_copy_has_single_member_or_invite_success_path(self):
        expiry = datetime(2026, 9, 30)
        member = main.build_gift_redeemed_recipient_text({}, expiry)
        invite = main.build_gift_redeemed_invite_text(expiry)
        self.assertIn("уже состоите в клубе", member)
        self.assertNotIn("Ссылка для входа придёт", member)
        self.assertIn("Нажмите кнопку ниже", invite)
        self.assertIn("действует 24 часа", invite)
        self.assertNotIn("подписка", member.lower() + invite.lower())
        self.assertNotIn("восстанов", member.lower() + invite.lower())

    def test_weekly_report_has_dedicated_gift_actor_and_recipient_lifecycle(self):
        period_start = datetime(2026, 8, 3, tzinfo=ZoneInfo("Europe/Moscow"))
        period_end = datetime(2026, 8, 10, tzinfo=ZoneInfo("Europe/Moscow"))
        gifts = [
            {
                "paid_at": datetime(2026, 8, 4, 9, tzinfo=timezone.utc),
                "telegram_id": 123456, "username": "buyer", "tariff_code": "gift_1m",
                "amount_total": 5000, "currency": "eur", "status": "paid_unclaimed",
            },
            {
                "paid_at": datetime(2026, 8, 5, 9, tzinfo=timezone.utc),
                "telegram_id": 654321, "tariff_code": "gift_6m", "amount_total": 9000,
                "currency": "eur", "status": "redeemed", "recipient_telegram_id": 777888,
                "recipient_username": "recipient", "applied_expiry": datetime(2027, 2, 1),
            },
        ]
        text = build_weekly_report_text(
            period_start, period_end,
            {"revenue_by_currency": {"EUR": 14000}, "tariff_counts": {"gift_1m": 1, "gift_6m": 1}},
            buyers=[], gifts=gifts,
        )
        self.assertIn("🎁 Подарки", text)
        self.assertIn("Оплатил: @buyer", text)
        self.assertIn("Получатель: ещё не активировал подарок", text)
        self.assertIn("Получатель: @recipient", text)
        self.assertIn("Статус: активирован", text)
        self.assertIn("Доступ получателя до:", text)
        self.assertEqual(text.count("Выручка: 140,00 €"), 1)
        self.assertNotIn("telegram_id: 654321", text)

    async def test_gift_recipient_gets_source_aware_48_hour_reminder(self):
        expiry = datetime.utcnow() + timedelta(hours=36)
        db = Connection(fetches=[[
            (777, expiry, False, None, None, False, False, False, None, None, True),
        ], []])
        send_message = AsyncMock()
        with patch.object(main, "get_db_conn", return_value=db), \
             patch.object(main.bot, "send_message", send_message), \
             patch.object(main, "set_subscription_reminder_sent") as mark_sent, \
             patch.object(main, "notify_subscription_check_admins_if_needed", AsyncMock()):
            await main.check_subscriptions_and_reminders()
        text = send_message.await_args.args[1]
        self.assertIn("подарочный доступ", text)
        self.assertIn(expiry.strftime("%d.%m.%Y"), text)
        self.assertNotIn("автопрод", text.lower())
        self.assertNotIn("ошиб", text.lower())
        self.assertIsNotNone(send_message.await_args.kwargs["reply_markup"])
        mark_sent.assert_called_once_with(777)

    def test_admin_gift_copy_uses_only_safe_refs(self):
        text = main.gift_admin_text("🎁 Gift redeemed", {
            "public_reference": "GIFT-ABCD1234ABCD1234",
            "purchaser_telegram_id": 123456789,
            "recipient_telegram_id": 987654321,
            "tariff_code": "gift_1m",
            "status": "redeemed",
        })
        self.assertIn("Gift ref: id_***CD1234", text)
        self.assertIn("Purchaser: id_***456789", text)
        self.assertIn("Recipient: id_***654321", text)
        self.assertNotIn("GIFT-ABCD1234ABCD1234", text)
        self.assertNotIn("123456789", text)
        self.assertNotIn("987654321", text)

    def test_gift_technical_notifications_are_admin_only(self):
        with patch.object(main, "ADMIN_IDS", [1, 2]):
            self.assertTrue(main.gift_admin_recipient_allowed(1))
            self.assertTrue(main.gift_admin_recipient_allowed(2))
            self.assertFalse(main.gift_admin_recipient_allowed(777))


if __name__ == "__main__":
    unittest.main()
