CREATE TABLE IF NOT EXISTS stripe_identity_conflicts (
    id BIGSERIAL PRIMARY KEY,
    conflict_type TEXT NOT NULL,
    stripe_id TEXT,
    telegram_ids TEXT,
    details TEXT,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS stripe_identity_conflicts_active_unique
ON stripe_identity_conflicts (conflict_type, stripe_id, telegram_ids)
WHERE resolved IS NOT TRUE;

DO $$
DECLARE
    conflict_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO conflict_count
    FROM (
        SELECT stripe_subscription_id
        FROM users
        WHERE stripe_subscription_id IS NOT NULL
        GROUP BY stripe_subscription_id
        HAVING COUNT(*) > 1
        UNION ALL
        SELECT stripe_customer_id
        FROM users
        WHERE stripe_customer_id IS NOT NULL
        GROUP BY stripe_customer_id
        HAVING COUNT(*) > 1
        UNION ALL
        SELECT stripe_subscription_id
        FROM stripe_links
        WHERE stripe_subscription_id IS NOT NULL
        GROUP BY stripe_subscription_id
        HAVING COUNT(DISTINCT telegram_id) > 1
    ) conflicts;

    IF conflict_count > 0 THEN
        RAISE EXCEPTION 'Unresolved Stripe identity conflicts block unique identity guards: %', conflict_count;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS users_unique_stripe_subscription
ON users (stripe_subscription_id)
WHERE stripe_subscription_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS users_unique_stripe_customer
ON users (stripe_customer_id)
WHERE stripe_customer_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS stripe_links_unique_subscription_user
ON stripe_links (stripe_subscription_id)
WHERE stripe_subscription_id IS NOT NULL;
