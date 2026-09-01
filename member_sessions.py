import hashlib
import hmac
import secrets
import uuid
from dataclasses import dataclass

MEMBER_SESSION_TTL_HOURS = 2
MEMBER_MAX_ACTIVE_SESSIONS = 5

class MemberSessionError(Exception):
    def __init__(self, category, status=401):
        super().__init__(category); self.category=category; self.status=status

@dataclass(frozen=True)
class MemberSession:
    session_id: str
    telegram_id: int
    first_name: str | None
    expires_at: object

def _hash(token):
    if not isinstance(token,str) or not token: raise MemberSessionError("member_session_invalid")
    return hashlib.sha256(token.encode()).hexdigest()

def create_member_session(get_connection, telegram_id, first_name=None):
    if first_name is not None:
        if not isinstance(first_name, str):
            raise MemberSessionError("member_profile_invalid", 400)
        first_name = first_name.strip()
        if not first_name or len(first_name) > 128:
            raise MemberSessionError("member_profile_invalid", 400)
    raw=secrets.token_urlsafe(32); digest=_hash(raw); session_id=str(uuid.uuid4())
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("DELETE FROM miniapp_member_sessions WHERE expires_at<=NOW()")
        cur.execute("SELECT pg_advisory_xact_lock(%s)",(int(telegram_id),))
        cur.execute("""UPDATE miniapp_member_sessions SET revoked_at=NOW() WHERE session_id IN
          (SELECT session_id FROM miniapp_member_sessions WHERE telegram_id=%s AND revoked_at IS NULL AND expires_at>NOW()
           ORDER BY created_at DESC,session_id DESC OFFSET %s)""",(int(telegram_id),MEMBER_MAX_ACTIVE_SESSIONS-1))
        cur.execute("""INSERT INTO miniapp_member_sessions(session_id,token_hash,telegram_id,first_name,expires_at)
          VALUES(%s,%s,%s,%s,NOW()+(%s*INTERVAL '1 hour')) RETURNING expires_at""",
          (session_id,digest,int(telegram_id),first_name,MEMBER_SESSION_TTL_HOURS))
        expires=cur.fetchone()[0]; conn.commit(); return raw,MemberSession(session_id,int(telegram_id),first_name,expires)
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()

def load_member_session(get_connection, raw):
    digest=_hash(raw); conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SELECT session_id,token_hash,telegram_id,first_name,expires_at,revoked_at IS NULL AND expires_at>NOW() FROM miniapp_member_sessions WHERE token_hash=%s",(digest,)); row=cur.fetchone()
    finally: cur.close(); conn.close()
    if not row or not hmac.compare_digest(row[1],digest): raise MemberSessionError("member_session_unknown")
    if not row[5]: raise MemberSessionError("member_session_inactive")
    return MemberSession(str(row[0]),int(row[2]),row[3],row[4])
