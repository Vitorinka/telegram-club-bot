import re


MAX_NOTIFICATION_KEY_LENGTH = 200
NOTIFICATION_KEY_RE = re.compile(r"^[a-z][a-z0-9_-]*:[A-Za-z0-9][A-Za-z0-9:._-]*$")
NOTIFICATION_ACTIONS = frozenset({"read", "resolve", "reopen", "archive"})


class AdminNotificationAcknowledgementError(ValueError):
    def __init__(self, category, status=400):
        super().__init__(category)
        self.category = category
        self.status = int(status)


def validate_notification_key(value):
    if not isinstance(value, str):
        raise AdminNotificationAcknowledgementError("invalid_notification_key")
    key = value.strip()
    if key != value or not key or len(key) > MAX_NOTIFICATION_KEY_LENGTH or not NOTIFICATION_KEY_RE.fullmatch(key):
        raise AdminNotificationAcknowledgementError("invalid_notification_key")
    return key


def list_admin_notification_acknowledgements(get_connection, admin_telegram_id):
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute(
            """
            SELECT notification_key, read_at, resolved_at, archived_at
            FROM admin_notification_acknowledgements
            WHERE admin_telegram_id = %s
            ORDER BY notification_key
            """,
            (int(admin_telegram_id),),
        )
        rows = cur.fetchall()
        keys = [str(row[0]) for row in rows if row[1] is not None]
        conn.rollback()
        return {
            "notification_keys": keys,
            "states": [{
                "notification_key": str(row[0]),
                "read_at": row[1].isoformat() if row[1] else None,
                "resolved_at": row[2].isoformat() if row[2] else None,
                "archived_at": row[3].isoformat() if row[3] else None,
            } for row in rows],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close(); conn.close()


def acknowledge_admin_notification(get_connection, admin_telegram_id, notification_key):
    key = validate_notification_key(notification_key)
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO admin_notification_acknowledgements (
                admin_telegram_id, notification_key, read_at
            ) VALUES (%s, %s, NOW())
            ON CONFLICT (admin_telegram_id, notification_key)
            DO UPDATE SET read_at = admin_notification_acknowledgements.read_at
            RETURNING read_at
            """,
            (int(admin_telegram_id), key),
        )
        read_at = cur.fetchone()[0]
        conn.commit()
        return {"notification_key": key, "read_at": read_at.isoformat()}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close(); conn.close()


def update_admin_notification_state(
    get_connection, admin_telegram_id, notification_key, action
):
    key = validate_notification_key(notification_key)
    action = str(action or "")
    if action not in NOTIFICATION_ACTIONS:
        raise AdminNotificationAcknowledgementError("invalid_notification_action")
    assignments = {
        "read": "read_at = COALESCE(admin_notification_acknowledgements.read_at, NOW())",
        "resolve": "read_at = COALESCE(admin_notification_acknowledgements.read_at, NOW()), resolved_at = COALESCE(admin_notification_acknowledgements.resolved_at, NOW()), archived_at = NULL",
        "reopen": "resolved_at = NULL, archived_at = NULL",
        "archive": "read_at = COALESCE(admin_notification_acknowledgements.read_at, NOW()), archived_at = COALESCE(admin_notification_acknowledgements.archived_at, NOW())",
    }
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO admin_notification_acknowledgements (
                admin_telegram_id, notification_key, read_at,
                resolved_at, archived_at, updated_at
            ) VALUES (
                %s, %s,
                NOW(),
                CASE WHEN %s = 'resolve' THEN NOW() ELSE NULL END,
                CASE WHEN %s = 'archive' THEN NOW() ELSE NULL END,
                NOW()
            )
            ON CONFLICT (admin_telegram_id, notification_key) DO UPDATE SET
            """ + assignments[action] + ", updated_at = NOW()\n" +
            "RETURNING read_at, resolved_at, archived_at",
            (int(admin_telegram_id), key, action, action),
        )
        read_at, resolved_at, archived_at = cur.fetchone()
        conn.commit()
        return {
            "notification_key": key,
            "read_at": read_at.isoformat() if read_at else None,
            "resolved_at": resolved_at.isoformat() if resolved_at else None,
            "archived_at": archived_at.isoformat() if archived_at else None,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close(); conn.close()
