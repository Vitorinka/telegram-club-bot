import hashlib
import hmac
import secrets
import uuid
from dataclasses import dataclass


MINIAPP_SESSION_TTL_HOURS = 8
MINIAPP_MAX_ACTIVE_SESSIONS = 5
MINIAPP_SESSION_TOKEN_BYTES = 32


class MiniAppSessionError(Exception):
    def __init__(self, category, status=401):
        super().__init__(category)
        self.category = str(category)
        self.status = int(status)


@dataclass(frozen=True)
class MiniAppSession:
    session_id: str
    telegram_id: int
    expires_at: object


def hash_session_token(token):
    if not isinstance(token, str) or not token:
        raise MiniAppSessionError("session_token_invalid")
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def parse_bearer_authorization(value):
    if not isinstance(value, str):
        raise MiniAppSessionError("session_authorization_missing")
    scheme, separator, token = value.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token or " " in token:
        raise MiniAppSessionError("session_authorization_invalid")
    return token


def cleanup_expired_miniapp_sessions(cursor):
    cursor.execute(
        """
        DELETE FROM miniapp_admin_sessions
        WHERE expires_at <= NOW()
        """
    )


def create_miniapp_admin_session(get_connection, telegram_id):
    telegram_id = int(telegram_id)
    raw_token = secrets.token_urlsafe(MINIAPP_SESSION_TOKEN_BYTES)
    token_hash = hash_session_token(raw_token)
    session_id = str(uuid.uuid4())
    conn = get_connection()
    cur = conn.cursor()
    try:
        cleanup_expired_miniapp_sessions(cur)
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (telegram_id,))
        cur.execute(
            """
            UPDATE miniapp_admin_sessions
            SET revoked_at = NOW()
            WHERE session_id IN (
                SELECT session_id
                FROM miniapp_admin_sessions
                WHERE telegram_id = %s
                  AND revoked_at IS NULL
                  AND expires_at > NOW()
                ORDER BY created_at DESC, session_id DESC
                OFFSET %s
            )
            """,
            (telegram_id, MINIAPP_MAX_ACTIVE_SESSIONS - 1),
        )
        cur.execute(
            """
            INSERT INTO miniapp_admin_sessions (
                session_id, token_hash, telegram_id, created_at, expires_at
            )
            VALUES (%s, %s, %s, NOW(), NOW() + (%s * INTERVAL '1 hour'))
            RETURNING expires_at
            """,
            (session_id, token_hash, telegram_id, MINIAPP_SESSION_TTL_HOURS),
        )
        expires_at = cur.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    return raw_token, MiniAppSession(session_id, telegram_id, expires_at)


def load_miniapp_admin_session(get_connection, raw_token):
    presented_hash = hash_session_token(raw_token)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT session_id, token_hash, telegram_id, expires_at,
                   revoked_at IS NULL AND expires_at > NOW()
            FROM miniapp_admin_sessions
            WHERE token_hash = %s
            """,
            (presented_hash,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if row is None or not hmac.compare_digest(row[1], presented_hash):
        raise MiniAppSessionError("session_unknown")
    session_id, _stored_hash, telegram_id, expires_at, active = row
    if not active:
        raise MiniAppSessionError("session_inactive")
    return MiniAppSession(str(session_id), int(telegram_id), expires_at)


def revoke_miniapp_admin_session(get_connection, session_id):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE miniapp_admin_sessions
            SET revoked_at = COALESCE(revoked_at, NOW())
            WHERE session_id = %s
            """,
            (session_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
