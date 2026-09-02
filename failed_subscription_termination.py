import hashlib
import uuid
from datetime import datetime, timedelta


TERMINAL_STATUSES = frozenset(("completed", "superseded", "manual_review"))
RETRYABLE_STATUSES = frozenset((
    "pending", "processing", "stripe_cancelled", "collection_stopped",
    "telegram_failed", "telegram_removed", "retryable_failed",
))


def operation_reference(subscription_id):
    digest = hashlib.sha256(str(subscription_id).encode("utf-8")).hexdigest()[:20]
    return f"fst-{digest}-{uuid.uuid4().hex[:12]}"


def claim_termination(cur, telegram_id, subscription_id, reason, invoice_id, owner_id, access_expiry,
                      now=None, lease_minutes=30):
    now = now or datetime.utcnow()
    lease_until = now + timedelta(minutes=lease_minutes)
    cur.execute(
        """
        INSERT INTO failed_subscription_terminations (
            operation_id, telegram_id, stripe_subscription_id, failed_invoice_id,
            reason, status, owner_id, claim_generation, lease_until, access_expiry,
            attempt_count, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, 'processing', %s, 1, %s, %s, 1, %s, %s)
        ON CONFLICT (stripe_subscription_id) DO NOTHING
        RETURNING operation_id, claim_generation, status, stripe_cancelled_at,
                  collection_stopped_at, telegram_banned_at, telegram_removed_at, db_finalized_at, access_expiry
        """,
        (operation_reference(subscription_id), int(telegram_id), subscription_id, invoice_id,
         reason, owner_id, lease_until, access_expiry, now, now),
    )
    row = cur.fetchone()
    if row:
        return row

    cur.execute(
        """
        SELECT operation_id, telegram_id, status, owner_id, claim_generation, lease_until,
               stripe_cancelled_at, collection_stopped_at, telegram_banned_at, telegram_removed_at,
               db_finalized_at, access_expiry
        FROM failed_subscription_terminations
        WHERE stripe_subscription_id = %s
        FOR UPDATE
        """,
        (subscription_id,),
    )
    existing = cur.fetchone()
    if not existing or int(existing[1]) != int(telegram_id) or existing[2] in TERMINAL_STATUSES:
        return None
    if existing[2] == "processing" and existing[5] and existing[5] > now:
        return None
    generation = int(existing[4] or 0) + 1
    cur.execute(
        """
        UPDATE failed_subscription_terminations
        SET status = 'processing', owner_id = %s, claim_generation = %s,
            lease_until = %s, attempt_count = attempt_count + 1,
            last_error_category = NULL, updated_at = %s,
            failed_invoice_id = COALESCE(failed_invoice_id, %s)
        WHERE operation_id = %s
        RETURNING operation_id, claim_generation, status, stripe_cancelled_at,
                  collection_stopped_at, telegram_banned_at, telegram_removed_at, db_finalized_at, access_expiry
        """,
        (owner_id, generation, lease_until, now, invoice_id, existing[0]),
    )
    return cur.fetchone()


def fenced_phase(cur, operation_id, owner_id, generation, status, timestamp_column=None):
    allowed_columns = {
        "stripe_cancelled_at", "collection_stopped_at", "telegram_banned_at", "telegram_removed_at",
        "db_finalized_at", "completed_at",
    }
    assignment = ""
    if timestamp_column:
        if timestamp_column not in allowed_columns:
            raise ValueError("invalid termination timestamp column")
        assignment = f", {timestamp_column} = COALESCE({timestamp_column}, NOW())"
    cur.execute(
        f"""
        UPDATE failed_subscription_terminations
        SET status = %s, updated_at = NOW(), last_error_category = NULL {assignment}
        WHERE operation_id = %s AND owner_id = %s AND claim_generation = %s
          AND lease_until > (NOW() AT TIME ZONE 'UTC')
          AND status NOT IN ('completed', 'superseded', 'manual_review')
        RETURNING operation_id
        """,
        (status, operation_id, owner_id, int(generation)),
    )
    return cur.fetchone() is not None


def fenced_invoice_reference(cur, operation_id, owner_id, generation, invoice_id):
    cur.execute(
        """
        UPDATE failed_subscription_terminations
        SET failed_invoice_id = COALESCE(failed_invoice_id, %s), updated_at = NOW()
        WHERE operation_id = %s AND owner_id = %s AND claim_generation = %s
          AND lease_until > (NOW() AT TIME ZONE 'UTC')
          AND status NOT IN ('completed', 'superseded', 'manual_review')
          AND (failed_invoice_id IS NULL OR failed_invoice_id = %s)
        RETURNING failed_invoice_id
        """,
        (invoice_id, operation_id, owner_id, int(generation), invoice_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


def fenced_failure(cur, operation_id, owner_id, generation, category, terminal=False):
    status = "manual_review" if terminal else "retryable_failed"
    cur.execute(
        """
        UPDATE failed_subscription_terminations
        SET status = %s, last_error_category = LEFT(%s, 200), lease_until = NULL,
            completed_at = CASE WHEN %s THEN NOW() ELSE completed_at END, updated_at = NOW()
        WHERE operation_id = %s AND owner_id = %s AND claim_generation = %s
          AND status NOT IN ('completed', 'superseded', 'manual_review')
        RETURNING operation_id
        """,
        (status, category, bool(terminal), operation_id, owner_id, int(generation)),
    )
    return cur.fetchone() is not None
