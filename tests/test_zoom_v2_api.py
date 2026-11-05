import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.test_aiogram3_bootstrap import FakeMiniAppRequest, import_main


class ZoomV2ApiTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.main = import_main()

    def setUp(self):
        self.flag = patch.dict(os.environ, {"ZOOM_V2_API_ENABLED": "true"})
        self.flag.start()

    def tearDown(self):
        self.flag.stop()

    def route_handler(self, app, method, path):
        for route in app.router.routes():
            if route.method == method and route.resource.canonical == path:
                return route.handler
        self.fail(f"route not found: {method} {path}")

    async def test_routes_are_feature_flagged_and_require_member_auth(self):
        app = self.main.create_app()
        handler = self.route_handler(app, "GET", "/api/member/zoom-v2/classes")
        missing = await self.main.miniapp_admin_auth_middleware(
            FakeMiniAppRequest(app, path="/api/member/zoom-v2/classes"), handler
        )
        self.assertEqual(missing.status, 401)

        session = SimpleNamespace(telegram_id=41, session_id="member-41")
        async def member_runner(function, *args, **kwargs):
            if function is self.main.authenticate_member_session:
                return session
            return {"items": []}
        with patch.object(
            self.main, "run_sync_db", new=AsyncMock(side_effect=member_runner),
        ):
            allowed = await self.main.miniapp_admin_auth_middleware(
                FakeMiniAppRequest(
                    app, "Bearer member-token", path="/api/member/zoom-v2/classes",
                ), handler,
            )
        self.assertEqual(allowed.status, 200)

        with patch.dict(os.environ, {"ZOOM_V2_API_ENABLED": "false"}), \
             patch.object(self.main, "load_member_session", return_value=session), \
             patch.object(self.main, "list_v2_member_classes") as service:
            disabled = await self.main.miniapp_admin_auth_middleware(
                FakeMiniAppRequest(
                    app, "Bearer member-token", path="/api/member/zoom-v2/classes",
                ), handler,
            )
        self.assertEqual(disabled.status, 404)
        self.assertEqual(json.loads(disabled.text), {"error": "zoom_v2_not_enabled"})
        service.assert_not_called()

    async def test_member_booking_uses_authenticated_identity_and_is_idempotent(self):
        app = self.main.create_app()
        path = "/api/member/zoom-v2/classes/{class_id}/book"
        handler = self.route_handler(app, "POST", path)
        request = FakeMiniAppRequest(
            app, path="/api/member/zoom-v2/classes/class-a/book",
            method="POST", match_info={"class_id": "class-a"},
        )
        request["miniapp_member"] = SimpleNamespace(telegram_id=71)
        fresh = {
            "booking_id": "booking-a", "booking_status": "booked_subscription",
            "access_status": "booked_subscription", "idempotent": False,
        }
        repeated = {**fresh, "idempotent": True}
        runner = AsyncMock(side_effect=[fresh, repeated])
        with patch.object(self.main, "run_sync_db", new=runner):
            first = await handler(request)
            second = await handler(request)
        self.assertEqual(first.status, 201)
        self.assertEqual(second.status, 200)
        for call in runner.await_args_list:
            self.assertEqual(call.args[2:4], (71, "class-a"))

    async def test_access_required_and_reconcile_expose_no_secrets(self):
        app = self.main.create_app()
        member = SimpleNamespace(telegram_id=72)
        access_required = {
            "booking_id": "booking-b", "access_status": "access_required",
            "payment_required": True, "idempotent": False,
        }
        renewed = {
            "booking_id": "booking-b", "access_status": "booked_subscription",
            "payment_required": False, "changed": True,
        }
        book = self.route_handler(
            app, "POST", "/api/member/zoom-v2/classes/{class_id}/book"
        )
        reconcile = self.route_handler(
            app, "POST", "/api/member/zoom-v2/classes/{class_id}/reconcile"
        )
        request = FakeMiniAppRequest(
            app, method="POST", match_info={"class_id": "class-b"},
        )
        request["miniapp_member"] = member
        with patch.object(
            self.main, "run_sync_db", new=AsyncMock(side_effect=[access_required, renewed]),
        ):
            pending_response = await book(request)
            renewed_response = await reconcile(request)
        self.assertEqual(json.loads(pending_response.text)["access_status"], "access_required")
        self.assertEqual(json.loads(renewed_response.text)["access_status"], "booked_subscription")
        combined = pending_response.text + renewed_response.text
        self.assertNotIn("zoom_url", combined)
        self.assertNotIn("stripe", combined.lower())
        self.assertNotIn("client_secret", combined)

    async def test_list_statuses_are_safe_and_scoped_to_current_member(self):
        app = self.main.create_app()
        handler = self.route_handler(app, "GET", "/api/member/zoom-v2/classes")
        request = FakeMiniAppRequest(app, path="/api/member/zoom-v2/classes")
        request["miniapp_member"] = SimpleNamespace(telegram_id=81)
        items = {"items": [
            {"class_id": "a", "booking_status": "booked_subscription"},
            {
                "class_id": "b", "booking_status": "access_required",
                "access_unavailable_reason": "subscription_does_not_cover_class_start",
                "available_actions": [
                    {"type": "renew_subscription"},
                    {"type": "authorize_one_time", "amount": 1000, "currency": "eur"},
                ],
            },
            {"class_id": "c", "booking_status": "authorization_pending"},
            {"class_id": "d", "booking_status": "cancelled"},
        ]}
        runner = AsyncMock(return_value=items)
        with patch.object(self.main, "run_sync_db", new=runner):
            response = await handler(request)
        self.assertEqual(response.status, 200)
        self.assertEqual(runner.await_args.args[2], 81)
        body = json.loads(response.text)
        self.assertEqual(
            [item["booking_status"] for item in body["items"]],
            ["booked_subscription", "access_required", "authorization_pending", "cancelled"],
        )
        self.assertNotIn("zoom_url", response.text)
        self.assertNotIn("stripe_payment_intent", response.text)

    async def test_admin_creation_requires_admin_session_and_hides_zoom_url(self):
        app = self.main.create_app()
        path = "/api/admin/zoom-v2/classes"
        handler = self.route_handler(app, "POST", path)
        missing = await self.main.miniapp_admin_auth_middleware(
            FakeMiniAppRequest(app, path=path, method="POST"), handler
        )
        self.assertEqual(missing.status, 401)

        non_admin = SimpleNamespace(telegram_id=999, session_id="member-like")
        with patch.object(self.main, "load_miniapp_admin_session", return_value=non_admin):
            forbidden = await self.main.miniapp_admin_auth_middleware(
                FakeMiniAppRequest(app, "Bearer member-token", path=path, method="POST"),
                handler,
            )
        self.assertEqual(forbidden.status, 403)

        admin = SimpleNamespace(telegram_id=1, session_id="admin-1")
        payload = {
            "title": "Практика", "starts_at": "2026-11-08T10:00:00+00:00",
            "zoom_url": "https://zoom.example/private-room",
        }
        created = {
            "class_id": "class-v2", "booking_model": "authorization_v2",
            "lifecycle_status": "open", "duration_minutes": 60,
            "price_amount": 1000, "currency": "eur", "unlimited_capacity": True,
        }
        async def admin_runner(function, *args, **kwargs):
            if function is self.main.authenticate_miniapp_session:
                return admin
            return created
        runner = AsyncMock(side_effect=admin_runner)
        with patch.object(self.main, "run_sync_db", new=runner):
            response = await self.main.miniapp_admin_auth_middleware(
                FakeMiniAppRequest(
                    app, "Bearer admin-token", path=path, method="POST",
                    json_data=payload,
                ), handler,
            )
        self.assertEqual(response.status, 201)
        self.assertNotIn("zoom_url", response.text)
        self.assertNotIn("private-room", response.text)
        service_call = next(
            call for call in runner.await_args_list
            if call.args[0] is self.main.create_v2_class
        )
        self.assertEqual(service_call.args[2], 1)

    async def test_access_check_returns_reason_without_join_url(self):
        app = self.main.create_app()
        handler = self.route_handler(
            app, "GET", "/api/member/zoom-v2/classes/{class_id}/access"
        )
        request = FakeMiniAppRequest(app, match_info={"class_id": "class-c"})
        request["miniapp_member"] = SimpleNamespace(telegram_id=91)
        with patch.object(
            self.main, "run_sync_db",
            new=AsyncMock(return_value={"allowed": False, "reason": "subscription_access_expired"}),
        ):
            response = await handler(request)
        self.assertEqual(json.loads(response.text), {
            "allowed": False, "reason": "subscription_access_expired",
        })
        self.assertNotIn("url", response.text.lower())


if __name__ == "__main__":
    unittest.main()
