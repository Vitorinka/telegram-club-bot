# Critical Bot Safety Migration Notes

## New Tables

- `checkout_sessions`: persistent Checkout lifecycle, Stripe session/customer/subscription IDs, stable `idempotency_key`, status and errors.
- `trial_redemptions`: one trial redemption per `telegram_id`; duplicate trial webhooks are marked processed without granting extra access.
- `stripe_identity_conflicts`: audit table for duplicate Stripe customer/subscription ownership before unique indexes are created.
- `admin_action_requests`: DB-backed pending admin confirmations with TTL.
- `scheduled_job_runs`: distributed lease for scheduled jobs by slot.
- `message_delivery_events`: idempotency lease for free lesson and follow-up sends.
- `admin_alerts`: durable critical admin alerts and delivery status.
- `bot_invite_links`: only invite links created by the bot are stored and later eligible for safe revoke.

## New Columns

- `stripe_events.event_created_at`, `stripe_events.event_type`, `stripe_events.object_id`.
- `users.last_successful_invoice_created_at`.
- `users.last_subscription_state_event_created_at`.
- `users.last_payment_failure_event_created_at`.
- `users.manual_sync_at`.

## Indexes

- `checkout_sessions_one_open_tariff`: one `creating/open/creation_unknown` checkout per `(telegram_id, tariff_code)`.
- Stripe identity unique indexes are created only when the startup audit finds no duplicate IDs:
  - `users_unique_stripe_subscription`
  - `users_unique_stripe_customer`
  - `stripe_links_unique_subscription_user`

## Rollout Risks

- Startup now fails closed if critical env vars are missing. Verify Railway variables before deploying.
- Existing duplicate Stripe IDs will skip unique index creation and create `stripe_identity_conflicts`; resolve manually before relying on DB uniqueness.
- `BACKUP_TELEGRAM_ENABLED=false` by default means DB dumps are not sent to Telegram unless encrypted sending is explicitly enabled.
- `send_db_backup` runs `pg_dump` with host/port/user/dbname argv only; password is passed through `PGPASSWORD` and SSL through `PGSSLMODE=require`.
- `creation_unknown` Checkout rows preserve the original Stripe idempotency key after ambiguous network/API connection errors so retries do not create a second DB row or a different Stripe request.
- `/revoke_invite_links` revokes only active links present in `bot_invite_links`; external/unknown links are intentionally ignored.
- Scheduled job locks reduce duplicate work across replicas, but Railway should remain on one replica until the first production run is observed.
- Free lesson delivery is best-effort idempotent. Absolute exactly-once is impossible if the process crashes after Telegram accepts a message but before DB commit.

## Manual Railway Verification

1. Confirm all required env vars exist: `BOT_TOKEN`, `DATABASE_URL`, `GROUP_ID`, `ADMIN_IDS`, Stripe keys, webhook secret, domain, and price IDs.
2. Deploy to one replica only.
3. Watch startup logs for `STRIPE_IDENTITY_CONFLICTS_FOUND`; if present, inspect `stripe_identity_conflicts`.
4. Run `/bot_health` in a private chat with the bot.
5. Run `/duplicate_subscriptions` in private admin chat.
6. Create a test Checkout twice for the same user/tariff and confirm the existing open link is reused or creation is blocked while processing. For simulated Stripe timeouts, confirm `creation_unknown` keeps the same `idempotency_key`.
7. Complete a test trial and replay the same webhook payload/event ID; confirm access is not extended twice.
8. Add a paid test user to the group and confirm they remain; add an unpaid test account and confirm removal.
9. Trigger `/weekly_report_current` and verify no future-period callbacks are attached.
10. Run backup job with `BACKUP_TELEGRAM_ENABLED=false`; verify no `.sql` file is sent and logs do not expose `DATABASE_URL` or `PGPASSWORD`.

## Rollback Plan

1. Revert application code to the previous Railway deployment.
2. Keep newly added tables and columns; they are additive and safe to leave in place.
3. If checkout creation is impacted, temporarily reduce Railway to one replica and inspect `checkout_sessions` rows with `status IN ('creating','creation_unknown','open')`.
4. If Stripe identity unique indexes were created and block a manual correction, resolve duplicated rows first rather than dropping the indexes blindly.
