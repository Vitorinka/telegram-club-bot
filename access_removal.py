from datetime import datetime, timedelta


TERMINAL_REMOVAL_STATUSES = {"removed", "manual_review"}


def access_removal_job_key(telegram_id, source="subscription_expired"):
    return f"{source}:{int(telegram_id)}"


def ensure_access_removal_schema(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS access_removal_jobs (
            job_key TEXT PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            reason TEXT,
            last_error TEXT,
            attempt_count INTEGER DEFAULT 0,
            lease_until TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            completed_at TIMESTAMP
        );
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS access_removal_jobs_status_idx
        ON access_removal_jobs (status, updated_at);
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS access_removal_jobs_telegram_id_idx
        ON access_removal_jobs (telegram_id);
        """
    )


def claim_access_removal(cur, telegram_id, source="subscription_expired", reason=None, now=None, lease_minutes=15):
    now = now or datetime.utcnow()
    lease_until = now + timedelta(minutes=lease_minutes)
    job_key = access_removal_job_key(telegram_id, source)
    cur.execute(
        """
        INSERT INTO access_removal_jobs (
            job_key, telegram_id, source, status, reason, attempt_count,
            lease_until, created_at, updated_at
        )
        VALUES (%s, %s, %s, 'processing', %s, 1, %s, %s, %s)
        ON CONFLICT (job_key) DO UPDATE SET
            status = 'processing',
            reason = COALESCE(EXCLUDED.reason, access_removal_jobs.reason),
            attempt_count = access_removal_jobs.attempt_count + 1,
            lease_until = EXCLUDED.lease_until,
            updated_at = EXCLUDED.updated_at,
            last_error = NULL
        WHERE access_removal_jobs.status IN ('pending', 'failed')
           OR (
                access_removal_jobs.status = 'processing'
                AND access_removal_jobs.lease_until < %s
           )
        RETURNING job_key, status, attempt_count
        """,
        (job_key, int(telegram_id), source, reason, lease_until, now, now, now),
    )
    row = cur.fetchone()
    if row:
        return {"status": "claimed", "job_key": row[0], "attempt_count": row[2]}

    cur.execute("SELECT status, attempt_count FROM access_removal_jobs WHERE job_key = %s", (job_key,))
    existing = cur.fetchone()
    if not existing:
        return {"status": "not_claimed", "job_key": job_key, "attempt_count": 0}
    if existing[0] in TERMINAL_REMOVAL_STATUSES:
        return {"status": f"duplicate_{existing[0]}", "job_key": job_key, "attempt_count": existing[1]}
    return {"status": existing[0], "job_key": job_key, "attempt_count": existing[1]}


def mark_access_removal_removed(cur, job_key):
    cur.execute(
        """
        UPDATE access_removal_jobs
        SET status = 'removed',
            completed_at = NOW(),
            updated_at = NOW(),
            last_error = NULL
        WHERE job_key = %s
        """,
        (job_key,),
    )


def mark_access_removal_failed(cur, job_key, error_text, manual_review=False):
    cur.execute(
        """
        UPDATE access_removal_jobs
        SET status = %s,
            last_error = LEFT(%s, 1000),
            updated_at = NOW(),
            lease_until = NULL
        WHERE job_key = %s
        """,
        ("manual_review" if manual_review else "failed", str(error_text), job_key),
    )


def count_unconfirmed_removals_query():
    return """
        SELECT COUNT(*)
        FROM access_removal_jobs
        WHERE status IN ('processing', 'failed', 'manual_review')
    """

