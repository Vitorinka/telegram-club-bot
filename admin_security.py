import json
import uuid
from datetime import datetime, timedelta
from functools import wraps


PRIVATE_ADMIN_MESSAGE = "Используйте эту команду в личном чате с ботом"


def is_private_admin_message(message, admin_ids):
    target = getattr(message, "message", message)
    user_id = getattr(getattr(message, "from_user", None), "id", None)
    chat_type = getattr(getattr(target, "chat", None), "type", None)
    return user_id in set(admin_ids or []) and chat_type == "private"


def admin_private_only(admin_ids):
    def decorator(func):
        @wraps(func)
        async def wrapper(message, *args, **kwargs):
            target = getattr(message, "message", message)
            user_id = getattr(getattr(message, "from_user", None), "id", None)
            chat_type = getattr(getattr(target, "chat", None), "type", None)
            if user_id not in set(admin_ids or []):
                return None
            if chat_type != "private":
                if hasattr(message, "answer") and hasattr(message, "message"):
                    await message.answer(PRIVATE_ADMIN_MESSAGE, show_alert=True)
                else:
                    await message.reply(PRIVATE_ADMIN_MESSAGE)
                return None
            return await func(message, *args, **kwargs)

        return wrapper

    return decorator


def make_action_request(cur, admin_id, action_type, payload, ttl_minutes=10, now=None):
    now = now or datetime.utcnow()
    action_id = str(uuid.uuid4())
    expires_at = now + timedelta(minutes=ttl_minutes)
    payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    cur.execute(
        """
        INSERT INTO admin_action_requests (
            action_id, admin_id, action_type, payload_json, status, created_at, expires_at
        )
        VALUES (%s, %s, %s, %s, 'pending', %s, %s)
        RETURNING action_id
        """,
        (action_id, int(admin_id), action_type, payload_json, now, expires_at),
    )
    return action_id


def claim_admin_action(cur, action_id, admin_id, now=None):
    now = now or datetime.utcnow()
    cur.execute(
        """
        UPDATE admin_action_requests
        SET status = 'processing'
        WHERE action_id = %s
          AND admin_id = %s
          AND status = 'pending'
          AND expires_at > %s
        RETURNING action_type, payload_json
        """,
        (action_id, int(admin_id), now),
    )
    row = cur.fetchone()
    if not row:
        cur.execute(
            "SELECT status FROM admin_action_requests WHERE action_id = %s AND admin_id = %s",
            (action_id, int(admin_id)),
        )
        status_row = cur.fetchone()
        return {"status": status_row[0] if status_row else "missing", "payload": None, "action_type": None}
    return {"status": "claimed", "action_type": row[0], "payload": json.loads(row[1])}


def complete_admin_action(cur, action_id):
    cur.execute(
        """
        UPDATE admin_action_requests
        SET status = 'completed', completed_at = NOW()
        WHERE action_id = %s AND status = 'processing'
        """,
        (action_id,),
    )


def fail_admin_action(cur, action_id):
    cur.execute(
        "UPDATE admin_action_requests SET status = 'failed', completed_at = NOW() "
        "WHERE action_id = %s AND status = 'processing'",
        (action_id,),
    )


def admin_action_confirmation_keyboard(action_id):
    return {
        "confirm": f"admin_action:confirm:{action_id}",
        "cancel": f"admin_action:cancel:{action_id}",
    }


def broadcast_preview(recipient_count, text, limit=300):
    text = text or ""
    preview = text[:limit]
    if len(text) > limit:
        preview += "..."
    return {
        "recipient_count": int(recipient_count),
        "length": len(text),
        "preview": preview,
    }


def cancel_admin_action(cur, action_id, admin_id, now=None):
    now = now or datetime.utcnow()
    cur.execute(
        """
        UPDATE admin_action_requests
        SET status = 'cancelled', completed_at = %s
        WHERE action_id = %s
          AND admin_id = %s
          AND status = 'pending'
        RETURNING action_id
        """,
        (now, action_id, int(admin_id)),
    )
    return cur.fetchone() is not None
