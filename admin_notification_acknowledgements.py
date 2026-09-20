import re


MAX_NOTIFICATION_KEY_LENGTH = 200
NOTIFICATION_KEY_RE = re.compile(r"^[a-z][a-z0-9_-]*:[A-Za-z0-9][A-Za-z0-9:._-]*$")


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
            SELECT notification_key
            FROM admin_notification_acknowledgements
            WHERE admin_telegram_id = %s
            ORDER BY notification_key
            """,
            (int(admin_telegram_id),),
        )
        keys = [str(row[0]) for row in cur.fetchall()]
        conn.rollback()
        return {"notification_keys": keys}
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
