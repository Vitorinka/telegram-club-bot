from datetime import datetime, timedelta


def ensure_notification_outbox_schema(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_outbox (
            id BIGSERIAL PRIMARY KEY,
            idempotency_key TEXT UNIQUE NOT NULL,
            recipient_id BIGINT,
            recipient_type TEXT NOT NULL DEFAULT 'telegram_user',
            notification_type TEXT NOT NULL,
            stripe_event_id TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            attempt_count INTEGER DEFAULT 0,
            lease_until TIMESTAMP,
            last_error TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            sent_at TIMESTAMP
        );
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS notification_outbox_status_idx
        ON notification_outbox (status, updated_at);
        """
    )


def notification_key(notification_type, recipient_id, stripe_event_id=None):
    event_part = stripe_event_id or "no_event"
    return f"{notification_type}:{int(recipient_id)}:{event_part}"


def claim_notification(cur, idempotency_key, recipient_id, notification_type, stripe_event_id=None, now=None, lease_minutes=10):
    now = now or datetime.utcnow()
    lease_until = now + timedelta(minutes=lease_minutes)
    cur.execute(
        """
        INSERT INTO notification_outbox (
            idempotency_key, recipient_id, notification_type, stripe_event_id,
            status, attempt_count, lease_until, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, 'sending', 1, %s, %s, %s)
        ON CONFLICT (idempotency_key) DO UPDATE SET
            status = 'sending',
            attempt_count = notification_outbox.attempt_count + 1,
            lease_until = EXCLUDED.lease_until,
            updated_at = EXCLUDED.updated_at,
            last_error = NULL
        WHERE notification_outbox.status IN ('pending', 'failed')
           OR (
                notification_outbox.status = 'sending'
                AND notification_outbox.lease_until < %s
           )
        RETURNING idempotency_key
        """,
        (idempotency_key, int(recipient_id), notification_type, stripe_event_id, lease_until, now, now, now),
    )
    if cur.fetchone():
        return "claimed"
    cur.execute("SELECT status FROM notification_outbox WHERE idempotency_key = %s", (idempotency_key,))
    row = cur.fetchone()
    if row and row[0] == "sent":
        return "already_sent"
    if row and row[0] == "sending":
        return "already_sending"
    return row[0] if row else "not_claimed"


def mark_notification_sent(cur, idempotency_key):
    cur.execute(
        """
        UPDATE notification_outbox
        SET status = 'sent',
            sent_at = NOW(),
            updated_at = NOW(),
            lease_until = NULL
        WHERE idempotency_key = %s
        """,
        (idempotency_key,),
    )


def mark_notification_failed(cur, idempotency_key, error_text):
    cur.execute(
        """
        UPDATE notification_outbox
        SET status = 'failed',
            last_error = LEFT(%s, 1000),
            updated_at = NOW(),
            lease_until = NULL
        WHERE idempotency_key = %s
        """,
        (str(error_text), idempotency_key),
    )

