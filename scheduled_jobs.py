from datetime import datetime, timedelta
import json
import os
import socket
import uuid


OWNER_ID = os.getenv("RAILWAY_REPLICA_ID") or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


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
            delivery_key, telegram_id, delivery_type, status, attempt_count, claimed_at, lease_until
        )
        VALUES (%s, %s, %s, 'processing', 1, %s, %s)
        ON CONFLICT (delivery_key) DO UPDATE SET
            status = 'processing',
            attempt_count = message_delivery_events.attempt_count + 1,
            claimed_at = EXCLUDED.claimed_at,
            lease_until = EXCLUDED.lease_until,
            last_error = NULL
        WHERE message_delivery_events.status IN ('failed')
           OR (
                message_delivery_events.status = 'processing'
                AND message_delivery_events.lease_until < %s
           )
        RETURNING delivery_key
        """,
        (delivery_key, int(telegram_id), delivery_type, now, lease_until, now),
    )
    if cur.fetchone():
        return "claimed"
    cur.execute("SELECT status FROM message_delivery_events WHERE delivery_key = %s", (delivery_key,))
    row = cur.fetchone()
    if row and row[0] == "sent":
        return "already_sent"
    if row and row[0] == "processing":
        return "already_processing"
    return row[0] if row else "not_claimed"


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
            last_error = NULL
        WHERE delivery_key IN (SELECT delivery_key FROM due)
        RETURNING delivery_key, telegram_id, delivery_type, payload_json, attempt_count, invite_link
        """,
        (limit, now, lease_until),
    )
    return cur.fetchall()


def mark_delivery_sent(cur, delivery_key):
    cur.execute(
        """
        UPDATE message_delivery_events
        SET status = 'sent',
            sent_at = NOW(),
            lease_until = NULL,
            next_attempt_at = NULL
        WHERE delivery_key = %s
        """,
        (delivery_key,),
    )


def mark_delivery_failed(cur, delivery_key, error_text, retry_delay_minutes=15, permanently_failed=False):
    status = "permanently_failed" if permanently_failed else "failed"
    next_attempt_sql = "NULL" if permanently_failed else "NOW() + (%s * INTERVAL '1 minute')"
    params = (
        (status, str(error_text), delivery_key)
        if permanently_failed
        else (status, str(error_text), retry_delay_minutes, delivery_key)
    )
    cur.execute(
        f"""
        UPDATE message_delivery_events
        SET status = %s,
            last_error = LEFT(%s, 500),
            lease_until = NULL,
            next_attempt_at = {next_attempt_sql}
        WHERE delivery_key = %s
        """,
        params,
    )


def save_delivery_invite_link(cur, delivery_key, invite_link):
    cur.execute(
        """
        UPDATE message_delivery_events
        SET invite_link = COALESCE(invite_link, %s)
        WHERE delivery_key = %s
        RETURNING invite_link
        """,
        (invite_link, delivery_key),
    )
    row = cur.fetchone()
    return row[0] if row else invite_link


async def process_claimed_delivery(
    get_conn,
    delivery_key,
    telegram_id,
    delivery_type,
    send_func,
    blocked_exc=(),
    success_update_sql=None,
    success_update_params=None,
):
    claim_conn = get_conn()
    claim_cur = claim_conn.cursor()
    try:
        claim = claim_message_delivery(claim_cur, delivery_key, telegram_id, delivery_type)
        claim_conn.commit()
    finally:
        claim_cur.close()
        claim_conn.close()

    if claim != "claimed":
        return claim

    try:
        await send_func()
    except blocked_exc as exc:
        fail_conn = get_conn()
        fail_cur = fail_conn.cursor()
        try:
            fail_cur.execute("UPDATE users SET blocked_bot = TRUE WHERE telegram_id = %s", (int(telegram_id),))
            mark_delivery_failed(fail_cur, delivery_key, exc)
            fail_conn.commit()
        finally:
            fail_cur.close()
            fail_conn.close()
        return "blocked"
    except Exception as exc:
        fail_conn = get_conn()
        fail_cur = fail_conn.cursor()
        try:
            mark_delivery_failed(fail_cur, delivery_key, exc)
            fail_conn.commit()
        finally:
            fail_cur.close()
            fail_conn.close()
        return "failed"

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
        mark_delivery_sent(sent_cur, delivery_key)
        sent_conn.commit()
    finally:
        sent_cur.close()
        sent_conn.close()
    return "sent"
