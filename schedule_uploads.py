import hashlib
import re
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from admin_security import (
    claim_admin_action,
    complete_admin_action,
    fail_admin_action,
    make_action_request,
)


MOSCOW_TZ = ZoneInfo("Europe/Moscow")
SCHEDULE_UPLOAD_MAX_BYTES = 10 * 1024 * 1024
SCHEDULE_UPLOAD_TTL_MINUTES = 30
SCHEDULE_UPLOAD_ACTION_TYPE = "schedule_upload_replace"
SCHEDULE_UPLOAD_INFLIGHT_SECONDS = 120
MONTH_PATTERN = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")
UPLOAD_ID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


class ScheduleUploadError(ValueError):
    def __init__(self, category, status=400):
        super().__init__(category)
        self.category = category
        self.status = int(status)


def schedule_upload_content_type(data):
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def validate_schedule_upload_month(value, now=None):
    month = str(value or "")
    if not MONTH_PATTERN.fullmatch(month):
        raise ScheduleUploadError("invalid_schedule_month")
    current = now or datetime.now(MOSCOW_TZ)
    if current.tzinfo is not None:
        current = current.astimezone(MOSCOW_TZ)
    current_index = current.year * 12 + current.month - 1
    year, month_number = (int(part) for part in month.split("-"))
    selected_index = year * 12 + month_number - 1
    if selected_index < current_index - 12 or selected_index > current_index + 24:
        raise ScheduleUploadError("schedule_month_out_of_range")
    return month


def validate_upload_id(value):
    upload_id = str(value or "").lower()
    if not UPLOAD_ID_PATTERN.fullmatch(upload_id):
        raise ScheduleUploadError("invalid_upload_id")
    return upload_id


def lock_club_schedule_month(cur, month):
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%s))",
        (f"club_schedule:{month}",),
    )


def upsert_club_schedule(cur, month_key, file_id, admin_id):
    month_key = str(month_key or "")
    if not MONTH_PATTERN.fullmatch(month_key):
        raise ValueError("schedule_month_invalid")
    if not file_id:
        raise ValueError("schedule_file_id_missing")
    lock_club_schedule_month(cur, month_key)
    cur.execute(
        """
        INSERT INTO club_schedules (
            schedule_month, telegram_file_id, uploaded_by_telegram_id,
            created_at, updated_at
        )
        VALUES (%s, %s, %s, NOW(), NOW())
        ON CONFLICT (schedule_month) DO UPDATE
        SET telegram_file_id = EXCLUDED.telegram_file_id,
            uploaded_by_telegram_id = EXCLUDED.uploaded_by_telegram_id,
            updated_at = NOW()
        RETURNING schedule_month
        """,
        (month_key, str(file_id), int(admin_id)),
    )
    return cur.fetchone()[0]


def _expire_staged_uploads(cur):
    cur.execute("""
        UPDATE miniapp_schedule_uploads
        SET status = 'expired', image_bytes = ''::bytea, byte_size = 0,
            consumed_at = NOW(), updated_at = NOW(),
            failure_category = 'upload_expired'
        WHERE expires_at <= NOW()
          AND status IN ('pending', 'confirmed')
    """)


def _public_upload(row):
    (
        upload_id, admin_id, month, content_type, byte_size, sha256,
        expected_exists, expected_updated_at, status, action_id,
        created_at, expires_at, failure_category,
    ) = row
    return {
        "upload_id": str(upload_id),
        "admin_telegram_id": int(admin_id),
        "schedule_month": month,
        "content_type": content_type,
        "byte_size": int(byte_size),
        "sha256_reference": sha256[:16],
        "existing_schedule": bool(expected_exists),
        "existing_schedule_reference": month if expected_exists else None,
        "expected_updated_at": (
            expected_updated_at.isoformat() if expected_updated_at else None
        ),
        "status": status,
        "action_id": str(action_id) if action_id else None,
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "failure_category": failure_category,
    }


PUBLIC_UPLOAD_SELECT = """
    SELECT upload_id, admin_telegram_id, schedule_month, content_type,
           byte_size, sha256, expected_schedule_exists,
           expected_schedule_updated_at, status, action_id, created_at,
           expires_at, failure_category
    FROM miniapp_schedule_uploads
"""


def create_schedule_upload(get_connection, admin_id, month, image_bytes, now=None):
    month = validate_schedule_upload_month(month, now=now)
    if not isinstance(image_bytes, bytes) or not image_bytes:
        raise ScheduleUploadError("schedule_image_missing")
    if len(image_bytes) > SCHEDULE_UPLOAD_MAX_BYTES:
        raise ScheduleUploadError("schedule_image_too_large", status=413)
    content_type = schedule_upload_content_type(image_bytes)
    if content_type is None:
        raise ScheduleUploadError("unsupported_schedule_image", status=415)
    created_at = now or datetime.utcnow()
    if created_at.tzinfo is not None:
        created_at = created_at.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    upload_id = str(uuid.uuid4())
    digest = hashlib.sha256(image_bytes).hexdigest()
    conn = get_connection()
    cur = conn.cursor()
    try:
        _expire_staged_uploads(cur)
        lock_club_schedule_month(cur, month)
        cur.execute(
            "SELECT updated_at FROM club_schedules WHERE schedule_month = %s FOR UPDATE",
            (month,),
        )
        existing = cur.fetchone()
        cur.execute("""
            INSERT INTO miniapp_schedule_uploads (
                upload_id, admin_telegram_id, schedule_month, image_bytes,
                content_type, byte_size, sha256, expected_schedule_exists,
                expected_schedule_updated_at, status, created_at, updated_at,
                expires_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                'pending', %s, %s, %s
            )
        """, (
            upload_id, int(admin_id), month, image_bytes, content_type,
            len(image_bytes), digest, bool(existing), existing[0] if existing else None,
            created_at, created_at,
            created_at + timedelta(minutes=SCHEDULE_UPLOAD_TTL_MINUTES),
        ))
        cur.execute(f"{PUBLIC_UPLOAD_SELECT} WHERE upload_id = %s", (upload_id,))
        result = _public_upload(cur.fetchone())
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def get_schedule_upload(get_connection, upload_id, admin_id, include_bytes=False):
    upload_id = validate_upload_id(upload_id)
    conn = get_connection()
    cur = conn.cursor()
    try:
        _expire_staged_uploads(cur)
        if include_bytes:
            cur.execute("""
                SELECT image_bytes, content_type, status, expires_at
                FROM miniapp_schedule_uploads
                WHERE upload_id = %s AND admin_telegram_id = %s
            """, (upload_id, int(admin_id)))
            row = cur.fetchone()
            conn.commit()
            if not row:
                return None
            if row[2] not in ('pending', 'confirmed', 'uploading', 'uploaded'):
                raise ScheduleUploadError("schedule_upload_unavailable", status=410)
            return {"image_bytes": bytes(row[0]), "content_type": row[1]}
        cur.execute(
            f"{PUBLIC_UPLOAD_SELECT} WHERE upload_id = %s AND admin_telegram_id = %s",
            (upload_id, int(admin_id)),
        )
        row = cur.fetchone()
        conn.commit()
        return _public_upload(row) if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def cancel_schedule_upload(get_connection, upload_id, admin_id):
    upload_id = validate_upload_id(upload_id)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE miniapp_schedule_uploads
            SET status = 'cancelled', image_bytes = ''::bytea, byte_size = 0,
                consumed_at = NOW(), updated_at = NOW()
            WHERE upload_id = %s AND admin_telegram_id = %s
              AND status = 'pending'
            RETURNING upload_id
        """, (upload_id, int(admin_id)))
        changed = cur.fetchone() is not None
        if not changed:
            cur.execute("""
                SELECT status FROM miniapp_schedule_uploads
                WHERE upload_id = %s AND admin_telegram_id = %s
            """, (upload_id, int(admin_id)))
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None
            if row[0] != "cancelled":
                raise ScheduleUploadError("schedule_upload_not_cancellable", status=409)
        conn.commit()
        return {"upload_id": upload_id, "status": "cancelled"}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def ensure_schedule_upload_action(get_connection, upload_id, admin_id):
    upload_id = validate_upload_id(upload_id)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT schedule_month, status, action_id, expires_at
            FROM miniapp_schedule_uploads
            WHERE upload_id = %s AND admin_telegram_id = %s
            FOR UPDATE
        """, (upload_id, int(admin_id)))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        month, status, action_id, expires_at = row
        if action_id is not None:
            cur.execute(
                "SELECT status FROM admin_action_requests WHERE action_id = %s AND admin_id = %s",
                (action_id, int(admin_id)),
            )
            action_row = cur.fetchone()
            conn.commit()
            return {
                "action_id": str(action_id),
                "status": action_row[0] if action_row else status,
            }
        if status != "pending" or expires_at <= datetime.utcnow():
            raise ScheduleUploadError("schedule_upload_not_confirmable", status=409)
        if action_id is None:
            action_id = make_action_request(
                cur,
                admin_id,
                SCHEDULE_UPLOAD_ACTION_TYPE,
                {
                    "admin_id": int(admin_id),
                    "schedule_month": month,
                    "upload_id": upload_id,
                },
                ttl_minutes=15,
            )
            cur.execute("""
                UPDATE miniapp_schedule_uploads
                SET action_id = %s, status = 'confirmed', updated_at = NOW()
                WHERE upload_id = %s AND status = 'pending'
            """, (action_id, upload_id))
        conn.commit()
        return {"action_id": str(action_id), "status": "pending"}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def prepare_schedule_upload_execution(get_connection, upload_id, action_id, admin_id):
    upload_id = validate_upload_id(upload_id)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"schedule-upload:{upload_id}",))
        cur.execute("""
            SELECT status, image_bytes, content_type, telegram_file_id,
                   upload_started_at, schedule_month
            FROM miniapp_schedule_uploads
            WHERE upload_id = %s AND action_id = %s AND admin_telegram_id = %s
            FOR UPDATE
        """, (upload_id, action_id, int(admin_id)))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return {"mode": "missing"}
        status, image_bytes, content_type, file_id, upload_started_at, month = row
        cur.execute("""
            SELECT status, action_type FROM admin_action_requests
            WHERE action_id = %s AND admin_id = %s FOR UPDATE
        """, (action_id, int(admin_id)))
        action = cur.fetchone()
        if not action or action[1] != SCHEDULE_UPLOAD_ACTION_TYPE:
            conn.rollback()
            return {"mode": "missing"}
        if status == "applied" or action[0] == "completed":
            conn.commit()
            return {"mode": "completed", "schedule_month": month}
        if action[0] == "pending":
            claimed = claim_admin_action(cur, action_id, admin_id)
            if claimed["status"] != "claimed":
                conn.commit()
                return {"mode": claimed["status"]}
        elif action[0] != "processing":
            conn.commit()
            return {"mode": action[0]}
        if status == "uploaded" and file_id:
            conn.commit()
            return {"mode": "apply", "schedule_month": month}
        if status == "uploading":
            if upload_started_at and upload_started_at > datetime.utcnow() - timedelta(seconds=SCHEDULE_UPLOAD_INFLIGHT_SECONDS):
                conn.commit()
                return {"mode": "processing", "schedule_month": month}
            cur.execute("""
                UPDATE miniapp_schedule_uploads
                SET status = 'failed', image_bytes = ''::bytea, byte_size = 0,
                    telegram_file_id = NULL, consumed_at = NOW(), updated_at = NOW(),
                    failure_category = 'telegram_upload_outcome_unknown'
                WHERE upload_id = %s
            """, (upload_id,))
            fail_admin_action(cur, action_id)
            conn.commit()
            return {"mode": "failed", "failure_category": "telegram_upload_outcome_unknown"}
        if status != "confirmed":
            conn.commit()
            return {"mode": status}
        cur.execute("""
            UPDATE miniapp_schedule_uploads
            SET status = 'uploading', upload_started_at = NOW(), updated_at = NOW()
            WHERE upload_id = %s AND status = 'confirmed'
        """, (upload_id,))
        conn.commit()
        return {
            "mode": "upload", "image_bytes": bytes(image_bytes),
            "content_type": content_type, "schedule_month": month,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def record_schedule_telegram_upload(get_connection, upload_id, action_id, admin_id, file_id):
    if not file_id:
        raise ScheduleUploadError("telegram_file_id_missing", status=502)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE miniapp_schedule_uploads
            SET status = 'uploaded', telegram_file_id = %s,
                telegram_uploaded_at = NOW(), updated_at = NOW()
            WHERE upload_id = %s AND action_id = %s
              AND admin_telegram_id = %s AND status = 'uploading'
            RETURNING schedule_month
        """, (str(file_id), upload_id, action_id, int(admin_id)))
        row = cur.fetchone()
        if not row:
            raise ScheduleUploadError("schedule_upload_state_conflict", status=409)
        conn.commit()
        return row[0]
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def fail_schedule_upload(get_connection, upload_id, action_id, admin_id, category):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE miniapp_schedule_uploads
            SET status = 'failed', image_bytes = ''::bytea, byte_size = 0,
                telegram_file_id = NULL, consumed_at = NOW(), updated_at = NOW(),
                failure_category = %s
            WHERE upload_id = %s AND action_id = %s
              AND admin_telegram_id = %s AND status <> 'applied'
        """, (category, upload_id, action_id, int(admin_id)))
        fail_admin_action(cur, action_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def apply_schedule_upload(get_connection, upload_id, action_id, admin_id):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"schedule-upload:{upload_id}",))
        cur.execute("""
            SELECT schedule_month, telegram_file_id, expected_schedule_exists,
                   expected_schedule_updated_at, status
            FROM miniapp_schedule_uploads
            WHERE upload_id = %s AND action_id = %s AND admin_telegram_id = %s
            FOR UPDATE
        """, (upload_id, action_id, int(admin_id)))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return {"status": "missing"}
        month, file_id, expected_exists, expected_updated_at, status = row
        if status == "applied":
            complete_admin_action(cur, action_id)
            conn.commit()
            return {"status": "completed", "schedule_month": month}
        if status != "uploaded" or not file_id:
            raise ScheduleUploadError("schedule_upload_not_ready", status=409)
        lock_club_schedule_month(cur, month)
        cur.execute(
            "SELECT updated_at FROM club_schedules WHERE schedule_month = %s FOR UPDATE",
            (month,),
        )
        current = cur.fetchone()
        stale = (
            (expected_exists and (not current or current[0] != expected_updated_at))
            or (not expected_exists and current is not None)
        )
        if stale:
            cur.execute("""
                UPDATE miniapp_schedule_uploads
                SET status = 'failed', image_bytes = ''::bytea, byte_size = 0,
                    telegram_file_id = NULL, consumed_at = NOW(), updated_at = NOW(),
                    failure_category = 'stale_schedule_preview'
                WHERE upload_id = %s
            """, (upload_id,))
            fail_admin_action(cur, action_id)
            conn.commit()
            return {"status": "failed", "failure_category": "stale_schedule_preview"}
        upsert_club_schedule(cur, month, file_id, admin_id)
        cur.execute("""
            UPDATE miniapp_schedule_uploads
            SET status = 'applied', image_bytes = ''::bytea, byte_size = 0,
                telegram_file_id = NULL, applied_at = NOW(), consumed_at = NOW(),
                updated_at = NOW()
            WHERE upload_id = %s AND status = 'uploaded'
        """, (upload_id,))
        complete_admin_action(cur, action_id)
        conn.commit()
        return {"status": "completed", "schedule_month": month}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def get_schedule_upload_action(get_connection, action_id, admin_id):
    try:
        action_uuid = str(uuid.UUID(str(action_id)))
    except (TypeError, ValueError):
        raise ScheduleUploadError("invalid_action_id") from None
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT action.action_id, action.admin_id, action.action_type, action.status,
                   action.created_at, action.completed_at,
                   upload.schedule_month, upload.status, upload.failure_category
            FROM admin_action_requests action
            JOIN miniapp_schedule_uploads upload ON upload.action_id = action.action_id
            WHERE action.action_id = %s AND action.admin_id = %s
        """, (action_uuid, int(admin_id)))
        row = cur.fetchone()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    if not row:
        return None
    return {
        "action_id": str(row[0]), "admin_id": int(row[1]),
        "action_type": row[2], "status": row[3],
        "created_at": row[4].isoformat(),
        "completed_at": row[5].isoformat() if row[5] else None,
        "schedule_month": row[6], "upload_status": row[7],
        "failure_category": row[8],
    }
