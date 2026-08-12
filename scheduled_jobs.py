from datetime import datetime, timedelta
import hashlib
import json
import logging
import os
import socket
import uuid


OWNER_ID = os.getenv("RAILWAY_REPLICA_ID") or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def log_stale_delivery_claim(delivery_key, action):
    delivery_hash = hashlib.sha256(str(delivery_key).encode("utf-8")).hexdigest()[:16]
    logging.warning(
        "MESSAGE_DELIVERY_STALE_CLAIM: action=%s, delivery_key_hash=%s",
        action,
        delivery_hash,
    )


def claim_scheduled_job(cur, job_key, job_name, schedule_slot, owner_id=None, now=None, lease_minutes=30, manual_retry=False):
    owner_id = owner_id or OWNER_ID
    now = now or datetime.utcnow()
    lease_until = now + timedelta(minutes=lease_minutes)
    cur.execute(
        """
        INSERT INTO scheduled_job_runs (
            job_key, job_name, schedule_slot, status, owner_id, lease_until, started_at, updated_at
        )
        VALUES (%s, %s, %s, 'running', %s, %s, %s, %s)
        ON CONFLICT (job_key) DO UPDATE SET
            status = 'running',
            owner_id = EXCLUDED.owner_id,
            lease_until = EXCLUDED.lease_until,
            updated_at = EXCLUDED.updated_at,
            error_text = NULL
        WHERE scheduled_job_runs.status = 'failed'
           OR (%s IS TRUE AND scheduled_job_runs.status = 'failed')
           OR (
                scheduled_job_runs.status = 'running'
                AND scheduled_job_runs.lease_until < %s
           )
        RETURNING job_key
        """,
        (job_key, job_name, schedule_slot, owner_id, lease_until, now, now, manual_retry, now),
    )
    if cur.fetchone():
        return "claimed"
    cur.execute("SELECT status FROM scheduled_job_runs WHERE job_key = %s", (job_key,))
    row = cur.fetchone()
    if row and row[0] == "completed":
        return "duplicate_completed"
    if row and row[0] == "running":
        return "already_running"
    return row[0] if row else "not_claimed"


def complete_scheduled_job(cur, job_key):
    cur.execute(
        """
        UPDATE scheduled_job_runs
        SET status = 'completed', completed_at = NOW(), updated_at = NOW()
        WHERE job_key = %s
        """,
        (job_key,),
    )


def fail_scheduled_job(cur, job_key, error_text):
    cur.execute(
        """
        UPDATE scheduled_job_runs
        SET status = 'failed', error_text = LEFT(%s, 1000), updated_at = NOW()
        WHERE job_key = %s
        """,
        (str(error_text), job_key),
    )


def claim_message_delivery(cur, delivery_key, telegram_id, delivery_type, now=None, lease_minutes=10):
    now = now or datetime.utcnow()
    lease_until = now + timedelta(minutes=lease_minutes)
    cur.execute(
        """
        INSERT INTO message_delivery_events (
            delivery_key, telegram_id, delivery_type, status, attempt_count, claimed_at, lease_until,
            claim_generation
        )
        VALUES (%s, %s, %s, 'processing', 1, %s, %s, 1)
        ON CONFLICT (delivery_key) DO UPDATE SET
            status = 'processing',
            attempt_count = message_delivery_events.attempt_count + 1,
            claimed_at = EXCLUDED.claimed_at,
            lease_until = EXCLUDED.lease_until,
            claim_generation = message_delivery_events.claim_generation + 1,
            last_error = NULL
        WHERE (
                message_delivery_events.status = 'failed'
                AND COALESCE(message_delivery_events.next_attempt_at, %s) <= %s
              )
           OR (
                message_delivery_events.status = 'processing'
                AND message_delivery_events.lease_until < %s
           )
        RETURNING claim_generation
        """,
        (delivery_key, int(telegram_id), delivery_type, now, lease_until, now, now, now),
    )
    claimed_row = cur.fetchone()
    if claimed_row:
        return "claimed", claimed_row[0]
    cur.execute("SELECT status FROM message_delivery_events WHERE delivery_key = %s", (delivery_key,))
    row = cur.fetchone()
    if row and row[0] == "sent":
        return "already_sent", None
    if row and row[0] == "processing":
        return "already_processing", None
    return (row[0] if row else "not_claimed"), None


def enqueue_message_delivery(cur, delivery_key, telegram_id, delivery_type, payload=None, next_attempt_at=None):
    payload_json = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
    cur.execute(
        """
        INSERT INTO message_delivery_events (
            delivery_key, telegram_id, delivery_type, status, attempt_count, last_error, payload_json, next_attempt_at
        )
        VALUES (%s, %s, %s, 'pending', 0, NULL, %s, COALESCE(%s, NOW()))
        ON CONFLICT (delivery_key) DO UPDATE SET
            telegram_id = EXCLUDED.telegram_id,
            delivery_type = EXCLUDED.delivery_type,
            payload_json = COALESCE(message_delivery_events.payload_json, EXCLUDED.payload_json),
            next_attempt_at = COALESCE(message_delivery_events.next_attempt_at, EXCLUDED.next_attempt_at)
        WHERE message_delivery_events.status NOT IN ('sent', 'permanently_failed')
        RETURNING delivery_key
        """,
        (delivery_key, int(telegram_id), delivery_type, payload_json, next_attempt_at),
    )
    return cur.fetchone() is not None


def claim_pending_message_deliveries(cur, limit=25, now=None, lease_minutes=10):
    now = now or datetime.utcnow()
    lease_until = now + timedelta(minutes=lease_minutes)
    cur.execute(
        """
        WITH due AS (
            SELECT delivery_key
            FROM message_delivery_events
            WHERE (
                    status IN ('pending', 'failed')
                    AND COALESCE(next_attempt_at, NOW()) <= NOW()
                  )
               OR (
                    status = 'processing'
                    AND lease_until < NOW()
                  )
            ORDER BY next_attempt_at NULLS FIRST, delivery_key
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE message_delivery_events
        SET status = 'processing',
            attempt_count = attempt_count + 1,
            claimed_at = %s,
            lease_until = %s,
            claim_generation = claim_generation + 1,
            last_error = NULL
        WHERE delivery_key IN (SELECT delivery_key FROM due)
        RETURNING delivery_key, telegram_id, delivery_type, payload_json, attempt_count, invite_link,
                  claim_generation
        """,
        (limit, now, lease_until),
    )
    return cur.fetchall()


def mark_delivery_sent(cur, delivery_key, claim_generation):
    cur.execute(
        """
        UPDATE message_delivery_events
        SET status = 'sent',
            sent_at = NOW(),
            lease_until = NULL,
            next_attempt_at = NULL
        WHERE delivery_key = %s
          AND status = 'processing'
          AND claim_generation = %s
        RETURNING delivery_key
        """,
        (delivery_key, claim_generation),
    )
    return "sent" if cur.fetchone() else "not_owner"


def mark_delivery_cancelled(cur, delivery_key, claim_generation, reason="cancelled"):
    cur.execute(
        """
        UPDATE message_delivery_events
        SET status = 'cancelled',
            last_error = LEFT(%s, 500),
            lease_until = NULL,
            next_attempt_at = NULL
        WHERE delivery_key = %s
          AND status = 'processing'
          AND claim_generation = %s
        RETURNING delivery_key
        """,
        (str(reason), delivery_key, claim_generation),
    )
    return "cancelled" if cur.fetchone() else "not_owner"


def cancel_message_delivery(cur, delivery_key, reason="cancelled"):
    """Cancel a logical delivery due to external business state, not claim ownership."""
    cur.execute(
        """
        UPDATE message_delivery_events
        SET status = 'cancelled',
            last_error = LEFT(%s, 500),
            lease_until = NULL,
            next_attempt_at = NULL
        WHERE delivery_key = %s
          AND status IN ('pending', 'failed', 'processing')
        RETURNING delivery_key
        """,
        (str(reason), delivery_key),
    )
    return "cancelled" if cur.fetchone() else "unchanged"


def mark_delivery_failed(
    cur,
    delivery_key,
    claim_generation,
    error_text,
    retry_delay_minutes=15,
    permanently_failed=False,
):
    status = "permanently_failed" if permanently_failed else "failed"
    next_attempt_sql = "NULL" if permanently_failed else "NOW() + (%s * INTERVAL '1 minute')"
    params = (
        (status, str(error_text), delivery_key, claim_generation)
        if permanently_failed
        else (status, str(error_text), retry_delay_minutes, delivery_key, claim_generation)
    )
    cur.execute(
        f"""
        UPDATE message_delivery_events
        SET status = %s,
            last_error = LEFT(%s, 500),
            lease_until = NULL,
            next_attempt_at = {next_attempt_sql}
        WHERE delivery_key = %s
          AND status = 'processing'
          AND claim_generation = %s
        RETURNING delivery_key
        """,
        params,
    )
    return status if cur.fetchone() else "not_owner"


def save_delivery_invite_link(cur, delivery_key, claim_generation, invite_link):
    cur.execute(
        """
        UPDATE message_delivery_events
        SET invite_link = COALESCE(invite_link, %s)
        WHERE delivery_key = %s
          AND status = 'processing'
          AND claim_generation = %s
        RETURNING invite_link
        """,
        (invite_link, delivery_key, claim_generation),
    )
    row = cur.fetchone()
    return ("saved", row[0]) if row else ("not_owner", None)


async def process_claimed_delivery(
    get_conn,
    delivery_key,
    telegram_id,
    delivery_type,
    send_func,
    blocked_exc=(),
    success_update_sql=None,
    success_update_params=None,
    attempt_count=1,
    classify_error_func=None,
    log_failure_func=None,
    terminal_error_callback=None,
    retryable_error_callback=None,
    retryable_state_func=None,
):
    claim_conn = get_conn()
    claim_cur = claim_conn.cursor()
    try:
        claim, claim_generation = claim_message_delivery(
            claim_cur,
            delivery_key,
            telegram_id,
            delivery_type,
        )
        claimed_attempt_count = None
        if claim == "claimed":
            claim_cur.execute("SELECT attempt_count FROM message_delivery_events WHERE delivery_key = %s", (delivery_key,))
            row = claim_cur.fetchone()
            claimed_attempt_count = row[0] if row else None
        claim_conn.commit()
    finally:
        claim_cur.close()
        claim_conn.close()

    if claim != "claimed":
        return claim

    if claimed_attempt_count is not None:
        attempt_count = claimed_attempt_count

    return await process_already_claimed_delivery(
        get_conn,
        delivery_key,
        telegram_id,
        delivery_type,
        send_func,
        claim_generation,
        blocked_exc=blocked_exc,
        success_update_sql=success_update_sql,
        success_update_params=success_update_params,
        attempt_count=attempt_count,
        classify_error_func=classify_error_func,
        log_failure_func=log_failure_func,
        terminal_error_callback=terminal_error_callback,
        retryable_error_callback=retryable_error_callback,
        retryable_state_func=retryable_state_func,
    )


async def process_already_claimed_delivery(
    get_conn,
    delivery_key,
    telegram_id,
    delivery_type,
    send_func,
    claim_generation,
    blocked_exc=(),
    success_update_sql=None,
    success_update_params=None,
    attempt_count=1,
    classify_error_func=None,
    log_failure_func=None,
    terminal_error_callback=None,
    retryable_error_callback=None,
    retryable_state_func=None,
):
    try:
        await send_func()
    except blocked_exc as exc:
        if classify_error_func:
            decision = classify_error_func(exc, attempt_count=attempt_count, sending_user_message=True)
        else:
            decision = {
                "blocked": True,
                "retryable": False,
                "permanently_failed": True,
                "retry_delay_minutes": None,
            }
        return await _mark_claimed_delivery_failed(
            get_conn,
            delivery_key,
            telegram_id,
            delivery_type,
            attempt_count,
            claim_generation,
            exc,
            decision,
            log_failure_func,
            terminal_error_callback,
            retryable_error_callback,
            retryable_state_func,
        )
    except Exception as exc:
        if classify_error_func:
            decision = classify_error_func(exc, attempt_count=attempt_count, sending_user_message=True)
        else:
            decision = {
                "blocked": False,
                "retryable": True,
                "permanently_failed": False,
                "retry_delay_minutes": 15,
            }
        return await _mark_claimed_delivery_failed(
            get_conn,
            delivery_key,
            telegram_id,
            delivery_type,
            attempt_count,
            claim_generation,
            exc,
            decision,
            log_failure_func,
            terminal_error_callback,
            retryable_error_callback,
            retryable_state_func,
        )

    sent_conn = get_conn()
    sent_cur = sent_conn.cursor()
    try:
        sent_cur.execute(
            success_update_sql or
            """
            UPDATE users
            SET video_sent = TRUE,
                video_sent_at = NOW()
            WHERE telegram_id = %s
            """,
            success_update_params or (int(telegram_id),),
        )
        sent_result = mark_delivery_sent(sent_cur, delivery_key, claim_generation)
        if sent_result == "sent":
            sent_conn.commit()
        else:
            sent_conn.rollback()
            log_stale_delivery_claim(delivery_key, "mark_sent")
            return "stale_claim"
    finally:
        sent_cur.close()
        sent_conn.close()
    return "sent"


async def _mark_claimed_delivery_failed(
    get_conn,
    delivery_key,
    telegram_id,
    delivery_type,
    attempt_count,
    claim_generation,
    exc,
    decision,
    log_failure_func=None,
    terminal_error_callback=None,
    retryable_error_callback=None,
    retryable_state_func=None,
):
    if log_failure_func:
        log_failure_func(delivery_key, delivery_type, attempt_count, exc, decision)
    retryable_state = None
    fail_conn = get_conn()
    fail_cur = fail_conn.cursor()
    try:
        if decision.get("blocked"):
            fail_cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (int(telegram_id),))
        retry_delay = decision.get("retry_delay_minutes")
        if retry_delay is None and not decision.get("permanently_failed", False):
            retry_delay = 15
        failed_result = mark_delivery_failed(
            fail_cur,
            delivery_key,
            claim_generation,
            exc,
            retry_delay_minutes=retry_delay,
            permanently_failed=decision.get("permanently_failed", False),
        )
        if failed_result in ("failed", "permanently_failed"):
            if decision.get("retryable") and retryable_state_func:
                retryable_state = retryable_state_func(
                    fail_cur, delivery_key, delivery_type, attempt_count
                )
            fail_conn.commit()
        else:
            fail_conn.rollback()
            log_stale_delivery_claim(delivery_key, "mark_failed")
            return "stale_claim"
    finally:
        fail_cur.close()
        fail_conn.close()

    if terminal_error_callback and decision.get("permanently_failed"):
        await terminal_error_callback(exc, decision, attempt_count)
    elif retryable_error_callback and decision.get("retryable"):
        await retryable_error_callback(exc, decision, attempt_count, retryable_state)

    if decision.get("blocked"):
        return "blocked"
    if decision.get("permanently_failed"):
        return "permanently_failed"
    return "failed"
