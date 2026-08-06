DO $$
DECLARE
    duplicate_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT stripe_event_id
        FROM payment_events
        WHERE stripe_event_id IS NOT NULL
        GROUP BY stripe_event_id
        HAVING COUNT(*) > 1
    ) duplicates;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'Payment integrity guard blocked: duplicate payment_events.stripe_event_id count=%', duplicate_count;
    END IF;

    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT stripe_session_id
        FROM checkout_sessions
        WHERE stripe_session_id IS NOT NULL
        GROUP BY stripe_session_id
        HAVING COUNT(*) > 1
    ) duplicates;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'Payment integrity guard blocked: duplicate checkout_sessions.stripe_session_id count=%', duplicate_count;
    END IF;

    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT idempotency_key
        FROM checkout_sessions
        WHERE idempotency_key IS NOT NULL
        GROUP BY idempotency_key
        HAVING COUNT(*) > 1
    ) duplicates;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'Payment integrity guard blocked: duplicate checkout_sessions.idempotency_key count=%', duplicate_count;
    END IF;

    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT stripe_customer_id
        FROM users
        WHERE stripe_customer_id IS NOT NULL
        GROUP BY stripe_customer_id
        HAVING COUNT(*) > 1
    ) duplicates;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'Payment integrity guard blocked: duplicate users.stripe_customer_id count=%', duplicate_count;
    END IF;

    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT stripe_subscription_id
        FROM users
        WHERE stripe_subscription_id IS NOT NULL
        GROUP BY stripe_subscription_id
        HAVING COUNT(*) > 1
    ) duplicates;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'Payment integrity guard blocked: duplicate users.stripe_subscription_id count=%', duplicate_count;
    END IF;

    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT stripe_subscription_id
        FROM stripe_links
        WHERE stripe_subscription_id IS NOT NULL
        GROUP BY stripe_subscription_id
        HAVING COUNT(DISTINCT telegram_id) > 1
    ) duplicates;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'Payment integrity guard blocked: stripe_links.stripe_subscription_id belongs to multiple Telegram users count=%', duplicate_count;
    END IF;

    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT event_id
        FROM stripe_events
        WHERE event_id IS NOT NULL
        GROUP BY event_id
        HAVING COUNT(*) > 1
    ) duplicates;
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'Payment integrity guard blocked: duplicate stripe_events.event_id count=%', duplicate_count;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = 'public'
          AND tbl.relname = 'payment_events'
          AND i.indisunique
          AND i.indnkeyatts = 1
          AND pg_get_expr(i.indpred, i.indrelid) = '(stripe_event_id IS NOT NULL)'
          AND (
                SELECT array_agg(a.attname::TEXT ORDER BY ord.ordinality)
                FROM unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ordinality)
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ord.attnum
              ) = ARRAY['stripe_event_id']
    ) THEN
        CREATE UNIQUE INDEX payment_events_unique_stripe_event_id
        ON payment_events(stripe_event_id)
        WHERE stripe_event_id IS NOT NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = 'public'
          AND tbl.relname = 'checkout_sessions'
          AND i.indisunique
          AND i.indnkeyatts = 1
          AND pg_get_expr(i.indpred, i.indrelid) = '(stripe_session_id IS NOT NULL)'
          AND (
                SELECT array_agg(a.attname::TEXT ORDER BY ord.ordinality)
                FROM unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ordinality)
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ord.attnum
              ) = ARRAY['stripe_session_id']
    ) THEN
        CREATE UNIQUE INDEX checkout_sessions_unique_stripe_session_id
        ON checkout_sessions(stripe_session_id)
        WHERE stripe_session_id IS NOT NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = 'public'
          AND tbl.relname = 'checkout_sessions'
          AND i.indisunique
          AND i.indnkeyatts = 1
          AND pg_get_expr(i.indpred, i.indrelid) = '(idempotency_key IS NOT NULL)'
          AND (
                SELECT array_agg(a.attname::TEXT ORDER BY ord.ordinality)
                FROM unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ordinality)
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ord.attnum
              ) = ARRAY['idempotency_key']
    ) THEN
        CREATE UNIQUE INDEX checkout_sessions_unique_idempotency_key
        ON checkout_sessions(idempotency_key)
        WHERE idempotency_key IS NOT NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = 'public'
          AND idx.relname = 'users_unique_stripe_subscription'
          AND tbl.relname = 'users'
          AND i.indisunique
          AND i.indnkeyatts = 1
          AND pg_get_expr(i.indpred, i.indrelid) = '(stripe_subscription_id IS NOT NULL)'
          AND (
                SELECT array_agg(a.attname::TEXT ORDER BY ord.ordinality)
                FROM unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ordinality)
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ord.attnum
              ) = ARRAY['stripe_subscription_id']
    ) THEN
        RAISE EXCEPTION 'Payment integrity guard missing or invalid: users_unique_stripe_subscription';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = 'public'
          AND idx.relname = 'users_unique_stripe_customer'
          AND tbl.relname = 'users'
          AND i.indisunique
          AND i.indnkeyatts = 1
          AND pg_get_expr(i.indpred, i.indrelid) = '(stripe_customer_id IS NOT NULL)'
          AND (
                SELECT array_agg(a.attname::TEXT ORDER BY ord.ordinality)
                FROM unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ordinality)
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ord.attnum
              ) = ARRAY['stripe_customer_id']
    ) THEN
        RAISE EXCEPTION 'Payment integrity guard missing or invalid: users_unique_stripe_customer';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = 'public'
          AND idx.relname = 'stripe_links_unique_subscription_user'
          AND tbl.relname = 'stripe_links'
          AND i.indisunique
          AND i.indnkeyatts = 1
          AND pg_get_expr(i.indpred, i.indrelid) = '(stripe_subscription_id IS NOT NULL)'
          AND (
                SELECT array_agg(a.attname::TEXT ORDER BY ord.ordinality)
                FROM unnest(i.indkey) WITH ORDINALITY AS ord(attnum, ordinality)
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ord.attnum
              ) = ARRAY['stripe_subscription_id']
    ) THEN
        RAISE EXCEPTION 'Payment integrity guard missing or invalid: stripe_links_unique_subscription_user';
    END IF;
END $$;
