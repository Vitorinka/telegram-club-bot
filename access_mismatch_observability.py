TRUSTED_SUBSCRIPTION_PAYMENT_KINDS = (
    "initial_subscription",
    "recurring",
    "adjustment",
    "out_of_band",
)


CURRENT_ACCESS_MISMATCH_CTE = """
WITH ranked_active_links AS (
    SELECT
        sl.*,
        ROW_NUMBER() OVER (
            PARTITION BY sl.telegram_id
            ORDER BY sl.updated_at DESC NULLS LAST, sl.id DESC
        ) AS current_rank
    FROM stripe_links sl
    JOIN users identity_user
      ON identity_user.telegram_id = sl.telegram_id
     AND identity_user.stripe_subscription_id = sl.stripe_subscription_id
     AND identity_user.stripe_customer_id IS NOT NULL
     AND sl.stripe_customer_id IS NOT NULL
     AND identity_user.stripe_customer_id = sl.stripe_customer_id
    WHERE sl.is_active IS TRUE
      AND sl.status IN ('active', 'trialing')
      AND sl.stripe_subscription_id IS NOT NULL
), current_access AS (
    SELECT
        u.telegram_id,
        u.paid,
        u.expiry_date,
        u.stripe_customer_id AS user_customer_id,
        u.stripe_subscription_id AS user_subscription_id,
        sl.stripe_customer_id AS link_customer_id,
        sl.stripe_subscription_id,
        sl.status AS link_status,
        proof.stripe_event_id AS payment_event_id,
        proof.period_end AS payment_period_end
    FROM users u
    JOIN ranked_active_links sl
      ON sl.telegram_id = u.telegram_id
     AND sl.current_rank = 1
    LEFT JOIN LATERAL (
        SELECT pe.stripe_event_id, pe.period_end
        FROM payment_events pe
        WHERE pe.telegram_id = u.telegram_id
          AND pe.stripe_subscription_id = sl.stripe_subscription_id
          AND pe.stripe_customer_id = sl.stripe_customer_id
          AND pe.event_type = 'invoice.payment_succeeded'
          AND pe.payment_status = 'succeeded'
          AND pe.payment_kind = ANY(%s)
          AND pe.amount_paid > 0
          AND pe.period_end IS NOT NULL
          AND pe.period_end > NOW()
          AND pe.stripe_event_id IS NOT NULL
          AND pe.invoice_id IS NOT NULL
        ORDER BY pe.period_end DESC, pe.id DESC
        LIMIT 1
    ) proof ON TRUE
)
"""


def load_access_mismatch_counts(cur):
    cur.execute(
        CURRENT_ACCESS_MISMATCH_CTE
        + """
        SELECT
            COUNT(*) FILTER (WHERE paid IS FALSE) AS active_local_unpaid,
            COUNT(*) FILTER (
                WHERE expiry_date IS NULL OR expiry_date <= NOW()
            ) AS active_missing_or_stale_expiry,
            COUNT(*) FILTER (
                WHERE paid IS FALSE AND payment_event_id IS NOT NULL
            ) AS active_unpaid_with_local_payment_proof
        FROM current_access
        """,
        (list(TRUSTED_SUBSCRIPTION_PAYMENT_KINDS),),
    )
    row = cur.fetchone() or (0, 0, 0)
    return {
        "active_local_unpaid": int(row[0] or 0),
        "active_missing_or_stale_expiry": int(row[1] or 0),
        "active_unpaid_with_local_payment_proof": int(row[2] or 0),
    }


def load_access_mismatch_samples(cur, limit=20):
    bounded_limit = max(1, min(int(limit), 50))
    cur.execute(
        CURRENT_ACCESS_MISMATCH_CTE
        + """
        SELECT
            telegram_id,
            stripe_subscription_id,
            link_status,
            paid,
            expiry_date,
            payment_event_id,
            payment_period_end
        FROM current_access
        WHERE paid IS FALSE
           OR expiry_date IS NULL
           OR expiry_date <= NOW()
        ORDER BY
            (paid IS FALSE AND payment_event_id IS NOT NULL) DESC,
            (expiry_date IS NULL OR expiry_date <= NOW()) DESC,
            telegram_id
        LIMIT %s
        """,
        (list(TRUSTED_SUBSCRIPTION_PAYMENT_KINDS), bounded_limit),
    )
    return cur.fetchall()
