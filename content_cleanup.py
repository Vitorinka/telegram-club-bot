import hashlib
import hmac
import json
import uuid
from datetime import datetime, timedelta

from admin_security import cancel_admin_action, complete_admin_action, make_action_request
from content_cms import ContentCmsError


DRAFT_DELETE_ACTION = "content_draft_delete"
MEDIA_REMOVE_ACTION = "content_media_remove"
PREVIEW_TTL = timedelta(minutes=10)


def _uuid(value, category):
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        raise ContentCmsError(category) from None


def _fingerprint(secret, values):
    encoded = json.dumps(values, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), encoded, hashlib.sha256).hexdigest()


def _draft_state(cur, content_id, lock=False):
    cur.execute(
        """SELECT content_id,title,status,version,revision_number,deleted_at,
                  (SELECT COUNT(*) FROM content_media m
                   WHERE m.content_id=content_items.content_id AND m.deleted_at IS NULL)
           FROM content_items WHERE content_id=%s""" + (" FOR UPDATE" if lock else ""),
        (content_id,),
    )
    row = cur.fetchone()
    if not row or row[5] is not None:
        raise ContentCmsError("content_not_found", 404)
    return {"content_id":str(row[0]), "title":row[1], "status":row[2],
            "version":int(row[3]), "revision_number":int(row[4]), "media_count":int(row[6])}


def _media_state(cur, content_id, media_id, lock=False):
    cur.execute(
        """SELECT c.content_id,c.status,c.version,c.deleted_at,m.media_id,m.media_type,m.version
           FROM content_items c JOIN content_media m ON m.content_id=c.content_id
           WHERE c.content_id=%s AND m.media_id=%s AND m.deleted_at IS NULL"""
        + (" FOR UPDATE OF c,m" if lock else ""), (content_id, media_id),
    )
    row = cur.fetchone()
    if not row or row[3] is not None:
        raise ContentCmsError("content_media_not_found", 404)
    return {"content_id":str(row[0]), "status":row[1], "content_version":int(row[2]),
            "media_id":str(row[4]), "media_type":row[5], "media_version":int(row[6])}


def _create_preview(get_connection, admin_id, action_type, state, secret, now):
    expires = now + PREVIEW_TTL
    payload = {**state, "fingerprint":_fingerprint(secret, state),
               "preview_created_at":now.isoformat(), "preview_expires_at":expires.isoformat()}
    conn = get_connection(); cur = conn.cursor()
    try:
        action_id = make_action_request(cur, admin_id, action_type, payload, ttl_minutes=10, now=now)
        conn.commit()
        return {**state, "action_id":action_id, "preview_expires_at":expires.isoformat()}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def preview_draft_delete(get_connection, content_id, admin_id, expected_version, secret, now=None):
    content_id = _uuid(content_id, "invalid_content_id"); now = now or datetime.utcnow()
    conn = get_connection(); cur = conn.cursor()
    try:
        state = _draft_state(cur, content_id)
        if state["status"] != "draft": raise ContentCmsError("content_not_deletable", 409)
        if state["version"] != expected_version: raise ContentCmsError("content_version_changed", 409)
        conn.rollback()
    finally:
        cur.close(); conn.close()
    return _create_preview(get_connection, admin_id, DRAFT_DELETE_ACTION, state, secret, now)


def preview_media_remove(get_connection, content_id, media_id, admin_id, expected_version, secret, now=None):
    content_id = _uuid(content_id, "invalid_content_id"); media_id = _uuid(media_id, "invalid_media_id"); now = now or datetime.utcnow()
    conn = get_connection(); cur = conn.cursor()
    try:
        state = _media_state(cur, content_id, media_id)
        if state["status"] != "draft": raise ContentCmsError("content_media_not_removable", 409)
        if state["content_version"] != expected_version: raise ContentCmsError("content_version_changed", 409)
        conn.rollback()
    finally:
        cur.close(); conn.close()
    return _create_preview(get_connection, admin_id, MEDIA_REMOVE_ACTION, state, secret, now)


def _confirm(get_connection, action_id, admin_id, expected_type, secret, loader, apply, now):
    action_id = _uuid(action_id, "invalid_action_id")
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET LOCAL statement_timeout=5000"); cur.execute("SET LOCAL lock_timeout='2s'")
        cur.execute("SELECT action_type,payload_json,status,expires_at FROM admin_action_requests WHERE action_id=%s AND admin_id=%s FOR UPDATE", (action_id,int(admin_id)))
        row = cur.fetchone()
        if not row or row[0] != expected_type: raise ContentCmsError("content_action_not_found", 404)
        if row[2] == "completed": conn.commit(); return {"status":"completed"}
        if row[2] != "pending" or row[3] <= now: raise ContentCmsError("content_action_not_confirmable", 409)
        try: payload = json.loads(row[1])
        except (TypeError, ValueError, json.JSONDecodeError): raise ContentCmsError("content_state_changed", 409) from None
        state = loader(cur, payload, True)
        expected = {key:payload.get(key) for key in state}
        if expected != state or not isinstance(payload.get("fingerprint"), str) or not hmac.compare_digest(payload["fingerprint"], _fingerprint(secret, state)):
            raise ContentCmsError("content_state_changed", 409)
        apply(cur, state, now)
        cur.execute("UPDATE admin_action_requests SET status='processing' WHERE action_id=%s AND admin_id=%s AND status='pending'", (action_id,int(admin_id)))
        if cur.rowcount != 1: raise ContentCmsError("content_action_not_confirmable", 409)
        complete_admin_action(cur, action_id); conn.commit()
        return {**state, "status":"completed"}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def confirm_draft_delete(get_connection, action_id, admin_id, secret, now=None):
    def loader(cur, payload, lock): return _draft_state(cur, _uuid(payload.get("content_id"), "content_state_changed"), lock)
    def apply(cur, state, timestamp):
        if state["status"] != "draft": raise ContentCmsError("content_not_deletable", 409)
        cur.execute("UPDATE content_items SET deleted_at=%s,deleted_by_telegram_id=%s,updated_at=%s WHERE content_id=%s AND status='draft' AND deleted_at IS NULL AND version=%s", (timestamp,int(admin_id),timestamp,state["content_id"],state["version"]))
        if cur.rowcount != 1: raise ContentCmsError("content_state_changed", 409)
    return _confirm(get_connection, action_id, admin_id, DRAFT_DELETE_ACTION, secret, loader, apply, now or datetime.utcnow())


def confirm_media_remove(get_connection, action_id, admin_id, secret, now=None):
    def loader(cur, payload, lock): return _media_state(cur, _uuid(payload.get("content_id"), "content_state_changed"), _uuid(payload.get("media_id"), "content_state_changed"), lock)
    def apply(cur, state, timestamp):
        if state["status"] != "draft": raise ContentCmsError("content_media_not_removable", 409)
        cur.execute("UPDATE content_media SET deleted_at=%s,updated_at=%s WHERE media_id=%s AND content_id=%s AND deleted_at IS NULL AND version=%s", (timestamp,timestamp,state["media_id"],state["content_id"],state["media_version"]))
        if cur.rowcount != 1: raise ContentCmsError("content_state_changed", 409)
        cur.execute("UPDATE content_items SET version=version+1,updated_at=%s WHERE content_id=%s AND status='draft' AND deleted_at IS NULL AND version=%s", (timestamp,state["content_id"],state["content_version"]))
        if cur.rowcount != 1: raise ContentCmsError("content_state_changed", 409)
    return _confirm(get_connection, action_id, admin_id, MEDIA_REMOVE_ACTION, secret, loader, apply, now or datetime.utcnow())


def cancel_cleanup(get_connection, action_id, admin_id, expected_type):
    action_id = _uuid(action_id, "invalid_action_id"); conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SELECT action_type,status FROM admin_action_requests WHERE action_id=%s AND admin_id=%s FOR UPDATE", (action_id,int(admin_id)))
        row=cur.fetchone()
        if not row or row[0] != expected_type: raise ContentCmsError("content_action_not_found",404)
        if row[1] == "cancelled": conn.commit(); return {"status":"cancelled"}
        if row[1] != "pending" or not cancel_admin_action(cur,action_id,admin_id): raise ContentCmsError("content_action_not_cancellable",409)
        conn.commit(); return {"status":"cancelled"}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()
