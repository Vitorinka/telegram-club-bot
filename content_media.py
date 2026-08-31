import hashlib
import uuid
from datetime import datetime, timedelta

from admin_security import claim_admin_action, complete_admin_action, fail_admin_action, make_action_request
from content_cms import CONTENT_TYPES
from schedule_uploads import schedule_upload_content_type


CONTENT_MEDIA_ACTION_TYPE = "content_media_attach"
CONTENT_MEDIA_UPLOAD_TTL_MINUTES = 30
CONTENT_MEDIA_INFLIGHT_SECONDS = 120
COVER_MAX_BYTES = 10 * 1024 * 1024
VIDEO_MAX_BYTES = 20 * 1024 * 1024
MEDIA_TYPES = frozenset({"cover", "video"})


def media_allowed_for_content(content_type, media_type):
    return content_type in CONTENT_TYPES and not (
        content_type == "recipe" and media_type != "cover"
    )


class ContentMediaError(ValueError):
    def __init__(self, category, status=400):
        super().__init__(category)
        self.category = category
        self.status = int(status)


def validate_uuid(value, category):
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        raise ContentMediaError(category) from None


def detect_media_mime(media_type, data):
    if media_type == "cover":
        return schedule_upload_content_type(data)
    if media_type == "video":
        if len(data) < 12 or data[4:8] != b"ftyp":
            return None
        brand = data[8:12]
        if brand not in {b"isom", b"iso2", b"mp41", b"mp42", b"avc1", b"M4V ", b"dash"}:
            return None
        return "video/mp4"
    return None


def validate_media_bytes(media_type, data):
    if media_type not in MEDIA_TYPES:
        raise ContentMediaError("invalid_content_media_type")
    if not isinstance(data, bytes) or not data:
        raise ContentMediaError("content_media_missing")
    limit = COVER_MAX_BYTES if media_type == "cover" else VIDEO_MAX_BYTES
    if len(data) > limit:
        raise ContentMediaError("content_media_too_large", 413)
    mime = detect_media_mime(media_type, data)
    if mime is None:
        raise ContentMediaError("unsupported_content_media", 415)
    return mime


def _safe_upload(row):
    (upload_id, admin_id, content_id, expected_version, media_type, mime_type,
     byte_size, sha256, existing_id, status, action_id, applied_media_id,
     created_at, expires_at, failure_category) = row
    return {
        "upload_id": str(upload_id), "admin_telegram_id": int(admin_id),
        "content_id": str(content_id), "expected_content_version": int(expected_version),
        "media_type": media_type, "mime_type": mime_type,
        "size_bytes": int(byte_size), "sha256_reference": sha256[:16],
        "existing_media": existing_id is not None, "status": status,
        "action_id": str(action_id) if action_id else None,
        "media_id": str(applied_media_id) if applied_media_id else None,
        "created_at": created_at.isoformat(), "expires_at": expires_at.isoformat(),
        "failure_category": failure_category,
    }


UPLOAD_SELECT = """
SELECT upload_id, admin_telegram_id, content_id, expected_content_version,
       media_type, mime_type, byte_size, sha256, expected_existing_media_id,
       status, action_id, applied_media_id, created_at, expires_at, failure_category
FROM content_media_uploads
"""


def _safe_media(row):
    (media_id, content_id, media_type, mime, size, sha256, sort_order, version,
     created_by, replaces_id, created_at, updated_at) = row
    return {
        "media_id": str(media_id), "content_id": str(content_id),
        "media_type": media_type, "mime_type": mime, "size_bytes": int(size),
        "sha256_reference": sha256[:16], "sort_order": int(sort_order),
        "version": int(version), "created_by_telegram_id": int(created_by),
        "replaces_media_id": str(replaces_id) if replaces_id else None,
        "created_at": created_at.isoformat(), "updated_at": updated_at.isoformat(),
        "has_media": True,
    }


MEDIA_SELECT = """
SELECT media_id, content_id, media_type, mime_type, size_bytes, sha256,
       sort_order, version, created_by_telegram_id, replaces_media_id,
       created_at, updated_at
FROM content_media
"""


def _expire(cur):
    cur.execute("""
        UPDATE content_media_uploads
        SET status='expired', media_bytes=''::bytea, byte_size=0,
            telegram_file_id=NULL, consumed_at=NOW(), updated_at=NOW(),
            failure_category='upload_expired'
        WHERE expires_at <= NOW() AND status IN ('pending','confirmed')
    """)


def create_media_upload(get_connection, admin_id, content_id, media_type, data):
    content_id = validate_uuid(content_id, "invalid_content_id")
    mime = validate_media_bytes(media_type, data)
    upload_id = str(uuid.uuid4())
    digest = hashlib.sha256(data).hexdigest()
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET LOCAL statement_timeout = 5000")
        _expire(cur)
        cur.execute("SELECT title, status, version, content_type FROM content_items WHERE content_id=%s FOR UPDATE", (content_id,))
        content = cur.fetchone()
        if not content:
            raise ContentMediaError("content_not_found", 404)
        if content[1] != "draft" or content[3] not in CONTENT_TYPES:
            raise ContentMediaError("content_not_editable", 409)
        if not media_allowed_for_content(content[3], media_type):
            raise ContentMediaError("invalid_content_media_type")
        cur.execute("SELECT media_id FROM content_media WHERE content_id=%s AND media_type=%s AND deleted_at IS NULL", (content_id, media_type))
        existing = cur.fetchone()
        now = datetime.utcnow()
        cur.execute("""
            INSERT INTO content_media_uploads (
                upload_id, admin_telegram_id, content_id, expected_content_version,
                media_type, media_bytes, mime_type, byte_size, sha256,
                expected_existing_media_id, status, created_at, updated_at, expires_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s,%s,%s)
        """, (upload_id, int(admin_id), content_id, int(content[2]), media_type,
              data, mime, len(data), digest, existing[0] if existing else None,
              now, now, now + timedelta(minutes=CONTENT_MEDIA_UPLOAD_TTL_MINUTES)))
        cur.execute(UPLOAD_SELECT + " WHERE upload_id=%s", (upload_id,))
        result = _safe_upload(cur.fetchone())
        result["content_title"] = content[0]
        conn.commit(); return result
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def get_media_upload(get_connection, upload_id, admin_id, include_bytes=False):
    upload_id = validate_uuid(upload_id, "invalid_upload_id")
    conn = get_connection(); cur = conn.cursor()
    try:
        _expire(cur)
        if include_bytes:
            cur.execute("SELECT media_bytes,mime_type,media_type,status FROM content_media_uploads WHERE upload_id=%s AND admin_telegram_id=%s", (upload_id, int(admin_id)))
            row = cur.fetchone(); conn.commit()
            if not row: return None
            if row[3] not in ('pending','confirmed','uploading','uploaded'):
                raise ContentMediaError("content_media_upload_unavailable", 410)
            return {"media_bytes": bytes(row[0]), "mime_type": row[1], "media_type": row[2]}
        cur.execute(UPLOAD_SELECT + " WHERE upload_id=%s AND admin_telegram_id=%s", (upload_id, int(admin_id)))
        row = cur.fetchone(); conn.commit()
        return _safe_upload(row) if row else None
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def cancel_media_upload(get_connection, upload_id, admin_id):
    upload_id = validate_uuid(upload_id, "invalid_upload_id")
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE content_media_uploads SET status='cancelled', media_bytes=''::bytea,
                byte_size=0, telegram_file_id=NULL, consumed_at=NOW(), updated_at=NOW()
            WHERE upload_id=%s AND admin_telegram_id=%s AND status='pending'
            RETURNING upload_id
        """, (upload_id, int(admin_id)))
        if not cur.fetchone():
            cur.execute("SELECT status FROM content_media_uploads WHERE upload_id=%s AND admin_telegram_id=%s", (upload_id, int(admin_id)))
            row = cur.fetchone()
            if not row: conn.rollback(); return None
            if row[0] != 'cancelled': raise ContentMediaError("content_media_upload_not_cancellable", 409)
        conn.commit(); return {"upload_id": upload_id, "status": "cancelled"}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def ensure_media_action(get_connection, upload_id, admin_id):
    upload_id = validate_uuid(upload_id, "invalid_upload_id")
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT content_id,media_type,expected_content_version,status,action_id,expires_at FROM content_media_uploads WHERE upload_id=%s AND admin_telegram_id=%s FOR UPDATE", (upload_id, int(admin_id)))
        row = cur.fetchone()
        if not row: conn.rollback(); return None
        content_id, media_type, expected_version, status, action_id, expires_at = row
        if action_id:
            cur.execute("SELECT status FROM admin_action_requests WHERE action_id=%s AND admin_id=%s", (action_id, int(admin_id)))
            action = cur.fetchone(); conn.commit()
            return {"action_id": str(action_id), "status": action[0] if action else status}
        if status != 'pending' or expires_at <= datetime.utcnow():
            raise ContentMediaError("content_media_upload_not_confirmable", 409)
        action_id = make_action_request(cur, admin_id, CONTENT_MEDIA_ACTION_TYPE, {
            "content_id": str(content_id), "upload_id": upload_id,
            "media_type": media_type, "expected_content_version": int(expected_version),
            "expected_content_status": "draft",
        }, ttl_minutes=15)
        cur.execute("UPDATE content_media_uploads SET action_id=%s,status='confirmed',updated_at=NOW() WHERE upload_id=%s AND status='pending'", (action_id, upload_id))
        conn.commit(); return {"action_id": str(action_id), "status": "pending"}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def prepare_media_execution(get_connection, upload_id, action_id, admin_id):
    upload_id = validate_uuid(upload_id, "invalid_upload_id")
    action_id = validate_uuid(action_id, "invalid_action_id")
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"content-media-upload:{upload_id}",))
        cur.execute("""
            SELECT status,media_bytes,mime_type,media_type,telegram_file_id,
                   upload_started_at,content_id,expected_content_version,applied_media_id
            FROM content_media_uploads
            WHERE upload_id=%s AND action_id=%s AND admin_telegram_id=%s FOR UPDATE
        """, (upload_id, action_id, int(admin_id)))
        row = cur.fetchone()
        if not row: conn.rollback(); return {"mode":"missing"}
        status,data,mime,media_type,file_id,started,content_id,expected_version,applied_id = row
        cur.execute("SELECT status,action_type FROM admin_action_requests WHERE action_id=%s AND admin_id=%s FOR UPDATE", (action_id, int(admin_id)))
        action = cur.fetchone()
        if not action or action[1] != CONTENT_MEDIA_ACTION_TYPE: conn.rollback(); return {"mode":"missing"}
        if status == 'applied' or action[0] == 'completed':
            conn.commit(); return {"mode":"completed", "media_id": str(applied_id) if applied_id else None}
        if action[0] == 'pending':
            claimed = claim_admin_action(cur, action_id, admin_id)
            if claimed["status"] != "claimed": conn.commit(); return {"mode":claimed["status"]}
        elif action[0] != 'processing': conn.commit(); return {"mode":action[0]}
        if status == 'uploaded' and file_id: conn.commit(); return {"mode":"apply"}
        if status == 'uploading':
            if started and started > datetime.utcnow() - timedelta(seconds=CONTENT_MEDIA_INFLIGHT_SECONDS):
                conn.commit(); return {"mode":"processing"}
            cur.execute("UPDATE content_media_uploads SET status='failed',media_bytes=''::bytea,byte_size=0,telegram_file_id=NULL,consumed_at=NOW(),updated_at=NOW(),failure_category='telegram_upload_outcome_unknown' WHERE upload_id=%s", (upload_id,))
            fail_admin_action(cur, action_id); conn.commit()
            return {"mode":"failed", "failure_category":"telegram_upload_outcome_unknown"}
        if status != 'confirmed': conn.commit(); return {"mode":status}
        cur.execute("SELECT status,version,content_type FROM content_items WHERE content_id=%s FOR UPDATE", (content_id,))
        content = cur.fetchone()
        if (not content or content[0] != 'draft' or content[1] != expected_version
                or not media_allowed_for_content(content[2], media_type)):
            cur.execute("UPDATE content_media_uploads SET status='failed',media_bytes=''::bytea,byte_size=0,consumed_at=NOW(),updated_at=NOW(),failure_category='content_version_changed' WHERE upload_id=%s", (upload_id,))
            fail_admin_action(cur, action_id); conn.commit()
            return {"mode":"failed", "failure_category":"content_version_changed"}
        cur.execute("UPDATE content_media_uploads SET status='uploading',upload_started_at=NOW(),updated_at=NOW() WHERE upload_id=%s AND status='confirmed'", (upload_id,))
        conn.commit(); return {"mode":"upload", "media_bytes":bytes(data), "mime_type":mime, "media_type":media_type, "content_id":str(content_id)}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def record_telegram_upload(get_connection, upload_id, action_id, admin_id, file_id):
    if not file_id: raise ContentMediaError("telegram_file_id_missing", 502)
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("UPDATE content_media_uploads SET status='uploaded',telegram_file_id=%s,telegram_uploaded_at=NOW(),updated_at=NOW() WHERE upload_id=%s AND action_id=%s AND admin_telegram_id=%s AND status='uploading' RETURNING upload_id", (str(file_id), upload_id, action_id, int(admin_id)))
        if not cur.fetchone(): raise ContentMediaError("content_media_upload_state_conflict", 409)
        conn.commit()
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def fail_media_upload(get_connection, upload_id, action_id, admin_id, category):
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("UPDATE content_media_uploads SET status='failed',media_bytes=''::bytea,byte_size=0,telegram_file_id=NULL,consumed_at=NOW(),updated_at=NOW(),failure_category=%s WHERE upload_id=%s AND action_id=%s AND admin_telegram_id=%s AND status<>'applied'", (category, upload_id, action_id, int(admin_id)))
        fail_admin_action(cur, action_id); conn.commit()
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def apply_media_upload(get_connection, upload_id, action_id, admin_id):
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"content-media-upload:{upload_id}",))
        cur.execute("""
            SELECT content_id,expected_content_version,media_type,mime_type,byte_size,
                   sha256,expected_existing_media_id,status,telegram_file_id,applied_media_id
            FROM content_media_uploads
            WHERE upload_id=%s AND action_id=%s AND admin_telegram_id=%s FOR UPDATE
        """, (upload_id, action_id, int(admin_id)))
        row = cur.fetchone()
        if not row: conn.rollback(); return {"status":"missing"}
        content_id,expected_version,media_type,mime,size,digest,expected_existing,status,file_id,applied_id = row
        if status == 'applied':
            complete_admin_action(cur, action_id); conn.commit()
            return {"status":"completed", "media_id":str(applied_id)}
        if status != 'uploaded' or not file_id: raise ContentMediaError("content_media_upload_not_ready", 409)
        cur.execute("SELECT status,version,content_type FROM content_items WHERE content_id=%s FOR UPDATE", (content_id,))
        content = cur.fetchone()
        cur.execute("SELECT media_id,version FROM content_media WHERE content_id=%s AND media_type=%s AND deleted_at IS NULL FOR UPDATE", (content_id, media_type))
        current = cur.fetchone()
        current_id = current[0] if current else None
        if (not content or content[0] != 'draft' or content[1] != expected_version
                or not media_allowed_for_content(content[2], media_type)
                or current_id != expected_existing):
            cur.execute("UPDATE content_media_uploads SET status='failed',media_bytes=''::bytea,byte_size=0,telegram_file_id=NULL,consumed_at=NOW(),updated_at=NOW(),failure_category='content_version_changed' WHERE upload_id=%s", (upload_id,))
            fail_admin_action(cur, action_id); conn.commit()
            return {"status":"failed", "failure_category":"content_version_changed"}
        media_id = str(uuid.uuid4()); next_version = (current[1] + 1) if current else 1
        if current:
            cur.execute("UPDATE content_media SET deleted_at=NOW(),updated_at=NOW() WHERE media_id=%s AND deleted_at IS NULL", (current_id,))
        cur.execute("""
            INSERT INTO content_media (media_id,content_id,media_type,storage_kind,
                server_reference,mime_type,size_bytes,sha256,sort_order,version,
                created_by_telegram_id,replaces_media_id,created_at,updated_at)
            VALUES (%s,%s,%s,'telegram_file_id',%s,%s,%s,%s,0,%s,%s,%s,NOW(),NOW())
        """, (media_id, content_id, media_type, file_id, mime, size, digest,
              next_version, int(admin_id), current_id))
        cur.execute("UPDATE content_items SET version=version+1,updated_at=NOW() WHERE content_id=%s AND version=%s", (content_id, expected_version))
        cur.execute("UPDATE content_media_uploads SET status='applied',media_bytes=''::bytea,byte_size=0,telegram_file_id=NULL,applied_media_id=%s,applied_at=NOW(),consumed_at=NOW(),updated_at=NOW() WHERE upload_id=%s AND status='uploaded'", (media_id, upload_id))
        complete_admin_action(cur, action_id); conn.commit()
        return {"status":"completed", "media_id":media_id, "content_version":expected_version+1}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def list_content_media(get_connection, content_id):
    content_id = validate_uuid(content_id, "invalid_content_id")
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute("SET LOCAL statement_timeout=5000")
        cur.execute(MEDIA_SELECT + " WHERE content_id=%s AND deleted_at IS NULL ORDER BY media_type,sort_order,media_id", (content_id,))
        result = [_safe_media(row) for row in cur.fetchall()]; conn.rollback(); return result
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def get_media_reference(get_connection, content_id, media_id):
    content_id = validate_uuid(content_id, "invalid_content_id"); media_id = validate_uuid(media_id, "invalid_media_id")
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute("SET LOCAL statement_timeout=5000")
        cur.execute("SELECT media_type,mime_type,size_bytes,server_reference FROM content_media WHERE content_id=%s AND media_id=%s AND deleted_at IS NULL", (content_id, media_id))
        row=cur.fetchone(); conn.rollback()
        if not row: return None
        return {"media_type":row[0], "mime_type":row[1], "size_bytes":int(row[2]), "server_reference":row[3]}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()
