SELECT
    'duplicate payment_events.stripe_event_id' AS check_name,
    stripe_event_id AS safe_identifier,
    COUNT(*) AS row_count
FROM payment_events
WHERE stripe_event_id IS NOT NULL
GROUP BY stripe_event_id
HAVING COUNT(*) > 1;

SELECT
    'duplicate checkout_sessions.stripe_session_id' AS check_name,
    stripe_session_id AS safe_identifier,
    COUNT(*) AS row_count
FROM checkout_sessions
WHERE stripe_session_id IS NOT NULL
GROUP BY stripe_session_id
HAVING COUNT(*) > 1;

SELECT
    'duplicate checkout_sessions.idempotency_key' AS check_name,
    idempotency_key AS safe_identifier,
    COUNT(*) AS row_count
FROM checkout_sessions
WHERE idempotency_key IS NOT NULL
GROUP BY idempotency_key
HAVING COUNT(*) > 1;

SELECT
    'duplicate users.stripe_customer_id' AS check_name,
    stripe_customer_id AS safe_identifier,
    COUNT(*) AS row_count
FROM users
WHERE stripe_customer_id IS NOT NULL
GROUP BY stripe_customer_id
HAVING COUNT(*) > 1;

SELECT
    'duplicate users.stripe_subscription_id' AS check_name,
    stripe_subscription_id AS safe_identifier,
    COUNT(*) AS row_count
FROM users
WHERE stripe_subscription_id IS NOT NULL
GROUP BY stripe_subscription_id
HAVING COUNT(*) > 1;

SELECT
    'stripe_links subscription on multiple telegram users' AS check_name,
    stripe_subscription_id AS safe_identifier,
    COUNT(DISTINCT telegram_id) AS telegram_user_count
FROM stripe_links
WHERE stripe_subscription_id IS NOT NULL
GROUP BY stripe_subscription_id
HAVING COUNT(DISTINCT telegram_id) > 1;

SELECT
    'users and stripe_links identity contradiction' AS check_name,
    COALESCE(u.stripe_subscription_id, u.stripe_customer_id, sl.stripe_subscription_id, sl.stripe_customer_id) AS safe_identifier,
    md5(u.telegram_id::TEXT) AS users_telegram_hash,
    md5(sl.telegram_id::TEXT) AS stripe_links_telegram_hash
FROM users u
JOIN stripe_links sl
  ON (
        u.stripe_subscription_id IS NOT NULL
        AND u.stripe_subscription_id = sl.stripe_subscription_id
     )
  OR (
        u.stripe_customer_id IS NOT NULL
        AND u.stripe_customer_id = sl.stripe_customer_id
     )
WHERE u.telegram_id <> sl.telegram_id;

SELECT
    'stripe_events processing older than 10 minutes' AS check_name,
    event_id AS safe_identifier,
    event_type,
    event_created_at,
    processed_at
FROM stripe_events
WHERE processed IS FALSE
  AND COALESCE(processed_at, event_created_at, NOW() - INTERVAL '11 minutes') < NOW() - INTERVAL '10 minutes'
ORDER BY COALESCE(processed_at, event_created_at) NULLS FIRST;

SELECT
    'duplicate invoice payment history' AS check_name,
    invoice_id AS safe_identifier,
    COUNT(*) AS row_count
FROM payment_events
WHERE invoice_id IS NOT NULL
  AND payment_status = 'succeeded'
GROUP BY invoice_id
HAVING COUNT(*) > 1;

SELECT
    'duplicate checkout session payment history' AS check_name,
    checkout_session_id AS safe_identifier,
    COUNT(*) AS row_count
FROM payment_events
WHERE checkout_session_id IS NOT NULL
  AND payment_status = 'succeeded'
GROUP BY checkout_session_id
HAVING COUNT(*) > 1;

SELECT
    'paid user with expired expiry and no grace' AS check_name,
    md5(telegram_id::TEXT) AS telegram_hash,
    expiry_date,
    grace_period_end
FROM users
WHERE paid IS TRUE
  AND expiry_date IS NOT NULL
  AND expiry_date <= NOW()
  AND (grace_period_end IS NULL OR grace_period_end <= NOW());

SELECT
    'suspicious payment_failed state' AS check_name,
    md5(telegram_id::TEXT) AS telegram_hash,
    paid,
    expiry_date,
    payment_failed_at,
    grace_period_end,
    stripe_subscription_id
FROM users
WHERE payment_failed IS TRUE
  AND (
        payment_failed_at IS NULL
        OR (paid IS TRUE AND expiry_date IS NOT NULL AND expiry_date > NOW() AND grace_period_end IS NULL)
      );
