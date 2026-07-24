# PostgreSQL restore rehearsal

This project must not restore directly into production as a first step.

Recommended rehearsal flow:

1. Create a fresh temporary PostgreSQL database.
2. Stop any bot process that points at that temporary database.
3. Restore the encrypted backup into the temporary database only.
4. Verify row counts for `users`, `payment_events`, `stripe_links`, `checkout_sessions`, `stripe_events`, and `access_events`.
5. Run read-only consistency checks:
   - duplicate `stripe_customer_id` / `stripe_subscription_id`;
   - `paid = TRUE AND expiry_date < NOW()`;
   - `auto_renew = TRUE AND stripe_subscription_id IS NULL`;
   - stale `stripe_events` in processing state.
6. Compare Stripe IDs against Stripe Dashboard manually before any production cutover.
7. Only after rehearsal succeeds, schedule a production restore window with a fresh backup and rollback plan.

Never run restore commands from the live Railway service shell unless the target database URL is explicitly verified as non-production.

