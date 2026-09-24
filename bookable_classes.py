import uuid
import base64
import binascii
import json
from datetime import datetime, timezone
from urllib.parse import urlsplit


CLASS_STATUSES = frozenset({"draft", "open", "confirmed", "cancelled", "completed"})
TITLE_MAX = 120
DESCRIPTION_MAX = 5000
ADMIN_CLASSES_DEFAULT_LIMIT = 50
ADMIN_CLASSES_MAX_LIMIT = 100


class BookableClassError(ValueError):
    def __init__(self, category, status=400):
        super().__init__(category)
        self.category = category
        self.status = int(status)


def _iso(value):
    return value.isoformat() if value else None


def _datetime(value, category):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise BookableClassError(category) from None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _integer(value, category, minimum, maximum):
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise BookableClassError(category) from None
    if result < minimum or result > maximum:
        raise BookableClassError(category)
    return result


def validate_class_payload(payload):
    if not isinstance(payload, dict):
        raise BookableClassError("invalid_request")
    title = str(payload.get("title") or "").strip()
    description = str(payload.get("description") or "").strip() or None
    if not 1 <= len(title) <= TITLE_MAX:
        raise BookableClassError("invalid_title")
    if description and len(description) > DESCRIPTION_MAX:
        raise BookableClassError("invalid_description")
    starts_at = _datetime(payload.get("starts_at"), "invalid_starts_at")
    booking_deadline = _datetime(payload.get("booking_deadline"), "invalid_booking_deadline")
    if booking_deadline >= starts_at:
        raise BookableClassError("invalid_booking_deadline")
    zoom_url = str(payload.get("zoom_url") or "").strip()
    parsed = urlsplit(zoom_url)
    if parsed.scheme != "https" or not parsed.netloc or len(zoom_url) > 2048:
        raise BookableClassError("invalid_zoom_url")
    capacity = _integer(payload.get("capacity"), "invalid_capacity", 1, 500)
    minimum = _integer(payload.get("minimum_participants", 3), "invalid_minimum_participants", 1, capacity)
    return {
        "title": title,
        "description": description,
        "starts_at": starts_at,
        "duration_minutes": _integer(payload.get("duration_minutes"), "invalid_duration", 5, 480),
        "zoom_url": zoom_url,
        "price_amount": _integer(payload.get("price_amount", 1000), "invalid_price", 1, 1000000),
        "currency": "eur",
        "capacity": capacity,
        "minimum_participants": minimum,
        "booking_deadline": booking_deadline,
    }


def _projection(row, *, admin=False, viewer_booking_status=None):
    (
        class_id, title, description, starts_at, duration_minutes, zoom_url,
        price_amount, currency, capacity, minimum_participants,
        booking_deadline, status, paid_count, created_at, updated_at,
    ) = row
    show_join = bool(status == "confirmed" and viewer_booking_status == "paid")
    result = {
        "class_id": str(class_id), "title": title, "description": description,
        "starts_at": _iso(starts_at), "duration_minutes": int(duration_minutes),
        "price_amount": int(price_amount), "currency": currency,
        "capacity": int(capacity), "minimum_participants": int(minimum_participants),
        "booking_deadline": _iso(booking_deadline), "status": status,
        "paid_bookings": int(paid_count or 0), "viewer_booking_status": viewer_booking_status,
        "zoom_url": zoom_url if admin or show_join else None,
        "created_at": _iso(created_at), "updated_at": _iso(updated_at),
    }
    return result


CLASS_SELECT = """
    SELECT c.class_id,c.title,c.description,c.starts_at,c.duration_minutes,c.zoom_url,
           c.price_amount,c.currency,c.capacity,c.minimum_participants,
           c.booking_deadline,c.status,
           COUNT(b.booking_id) FILTER (WHERE b.status='paid') AS paid_count,
           c.created_at,c.updated_at
    FROM bookable_classes c
    LEFT JOIN class_bookings b ON b.class_id=c.class_id
"""


def _admin_classes_cursor(value):
    if not value:
        return None
    try:
        encoded = str(value)
        starts_at, class_id = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode())
        return _datetime(starts_at, "invalid_cursor"), str(uuid.UUID(class_id))
    except (ValueError, TypeError, binascii.Error, json.JSONDecodeError, UnicodeDecodeError):
        raise BookableClassError("invalid_cursor") from None


def _encode_admin_classes_cursor(starts_at, class_id):
    payload = json.dumps([_iso(starts_at), str(class_id)], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def list_admin_classes(get_connection, *, limit=ADMIN_CLASSES_DEFAULT_LIMIT, cursor=None):
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        raise BookableClassError("invalid_limit") from None
    if limit < 1:
        raise BookableClassError("invalid_limit")
    limit = min(limit, ADMIN_CLASSES_MAX_LIMIT)
    position = _admin_classes_cursor(cursor)
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        where = ""
        params = []
        if position:
            where = " WHERE (c.starts_at,c.class_id) > (%s,%s)"
            params.extend(position)
        params.append(limit + 1)
        cur.execute(CLASS_SELECT + where + " GROUP BY c.class_id ORDER BY c.starts_at,c.class_id LIMIT %s", params)
        rows = cur.fetchall(); conn.rollback()
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = _encode_admin_classes_cursor(visible[-1][3], visible[-1][0]) if has_more and visible else None
        return {"items": [_projection(row, admin=True) for row in visible], "has_more": has_more, "next_cursor": next_cursor}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def create_admin_class(get_connection, admin_id, payload):
    values = validate_class_payload(payload); class_id = uuid.uuid4()
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO bookable_classes (
                class_id,title,description,starts_at,duration_minutes,zoom_url,
                price_amount,currency,capacity,minimum_participants,
                booking_deadline,status,created_by_telegram_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'draft',%s)
            """,
            (str(class_id), values["title"], values["description"], values["starts_at"],
             values["duration_minutes"], values["zoom_url"], values["price_amount"],
             values["currency"], values["capacity"], values["minimum_participants"],
             values["booking_deadline"], int(admin_id)),
        )
        conn.commit()
        return {"class_id": str(class_id), "status": "draft"}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def update_admin_class(get_connection, class_id, payload):
    """Edit a future class while no captured booking makes it immutable."""
    values = validate_class_payload(payload)
    try:
        class_uuid = uuid.UUID(str(class_id))
    except ValueError:
        raise BookableClassError("invalid_class_id") from None
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT status, starts_at,
                   EXISTS (SELECT 1 FROM class_bookings b
                           WHERE b.class_id=c.class_id)
            FROM bookable_classes c WHERE class_id=%s FOR UPDATE
            """, (str(class_uuid),),
        )
        row = cur.fetchone()
        if not row:
            raise BookableClassError("class_not_found", 404)
        if row[0] not in ("draft", "open") or row[2]:
            raise BookableClassError("class_edit_conflict", 409)
        cur.execute(
            """
            UPDATE bookable_classes SET
                title=%s,description=%s,starts_at=%s,duration_minutes=%s,
                zoom_url=%s,price_amount=%s,currency=%s,capacity=%s,
                minimum_participants=%s,booking_deadline=%s,updated_at=NOW()
            WHERE class_id=%s
            """,
            (values["title"], values["description"], values["starts_at"],
             values["duration_minutes"], values["zoom_url"], values["price_amount"],
             values["currency"], values["capacity"], values["minimum_participants"],
             values["booking_deadline"], str(class_uuid)),
        )
        conn.commit()
        return {"class_id": str(class_uuid), "status": row[0]}
    except BookableClassError:
        conn.rollback(); raise
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def list_admin_class_bookings(get_connection, class_id):
    try:
        class_uuid = uuid.UUID(str(class_id))
    except ValueError:
        raise BookableClassError("invalid_class_id") from None
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SELECT 1 FROM bookable_classes WHERE class_id=%s", (str(class_uuid),))
        if not cur.fetchone():
            conn.rollback(); raise BookableClassError("class_not_found", 404)
        cur.execute(
            """
            SELECT b.booking_id,b.telegram_id,u.username,u.first_name,b.status,
                   b.amount,b.currency,b.created_at,b.paid_at,b.refunded_at
            FROM class_bookings b
            LEFT JOIN users u ON u.telegram_id=b.telegram_id
            WHERE b.class_id=%s
            ORDER BY b.created_at,b.booking_id
            """, (str(class_uuid),),
        )
        items = [{
            "booking_id": str(row[0]), "telegram_id": int(row[1]),
            "username": row[2], "first_name": row[3], "status": row[4],
            "amount": int(row[5]), "currency": row[6],
            "created_at": _iso(row[7]), "paid_at": _iso(row[8]),
            "refunded_at": _iso(row[9]),
        } for row in cur.fetchall()]
        conn.rollback()
        return {"items": items}
    except BookableClassError:
        raise
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def list_member_bookings(get_connection, telegram_id):
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute(
            """
            SELECT c.class_id,c.title,c.description,c.starts_at,c.duration_minutes,c.zoom_url,
                   c.price_amount,c.currency,c.capacity,c.minimum_participants,
                   c.booking_deadline,c.status,
                   (SELECT COUNT(*) FROM class_bookings paid
                    WHERE paid.class_id=c.class_id AND paid.status='paid'),
                   c.created_at,c.updated_at,b.status,b.checkout_url,b.created_at,b.paid_at,b.refunded_at
            FROM class_bookings b JOIN bookable_classes c ON c.class_id=b.class_id
            WHERE b.telegram_id=%s
            ORDER BY c.starts_at DESC,c.class_id
            """, (int(telegram_id),),
        )
        items = []
        for row in cur.fetchall():
            item = _projection(row[:15], viewer_booking_status=row[15])
            item.update({"checkout_url": row[16] if row[15] == "checkout_open" else None,
                         "booking_created_at": _iso(row[17]), "paid_at": _iso(row[18]),
                         "refunded_at": _iso(row[19])})
            items.append(item)
        conn.rollback()
        now = datetime.utcnow()
        return {"upcoming": [item for item in items if datetime.fromisoformat(item["starts_at"]) > now and item["status"] not in ("cancelled", "completed")],
                "history": [item for item in items if datetime.fromisoformat(item["starts_at"]) <= now or item["status"] in ("cancelled", "completed")]}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def update_admin_class_status(get_connection, class_id, target):
    if target not in CLASS_STATUSES - {"confirmed"}:
        raise BookableClassError("invalid_class_status")
    try: class_uuid = uuid.UUID(str(class_id))
    except ValueError: raise BookableClassError("invalid_class_id") from None
    transitions = {
        "open": ("draft",), "cancelled": ("draft", "open", "confirmed"),
        "completed": ("confirmed",), "draft": ("open",),
    }
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE bookable_classes SET status=%s,updated_at=NOW(),
                cancelled_at=CASE WHEN %s='cancelled' THEN NOW() ELSE cancelled_at END,
                completed_at=CASE WHEN %s='completed' THEN NOW() ELSE completed_at END
            WHERE class_id=%s AND status=ANY(%s)
              AND (%s <> 'open' OR (booking_deadline > NOW() AND starts_at > NOW()))
              AND (%s <> 'draft' OR NOT EXISTS (
                    SELECT 1 FROM class_bookings b WHERE b.class_id=bookable_classes.class_id
              ))
            RETURNING status
            """,
            (target, target, target, str(class_uuid), list(transitions[target]), target, target),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback(); raise BookableClassError("class_transition_conflict", 409)
        if target == "cancelled":
            cur.execute(
                """
                INSERT INTO class_refund_operations (
                    operation_id,booking_id,stripe_payment_intent_id,status
                )
                SELECT gen_random_uuid(),booking_id,stripe_payment_intent_id,'pending'
                FROM class_bookings
                WHERE class_id=%s AND status='paid' AND stripe_payment_intent_id IS NOT NULL
                ON CONFLICT (booking_id) DO NOTHING
                """, (str(class_uuid),),
            )
        conn.commit(); return {"class_id": str(class_uuid), "status": target}
    except BookableClassError: raise
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def list_member_classes(get_connection, telegram_id):
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute(
            CLASS_SELECT + """
            WHERE c.status IN ('open','confirmed') AND c.starts_at > NOW()
            GROUP BY c.class_id ORDER BY c.starts_at,c.class_id
            """
        )
        rows = cur.fetchall()
        cur.execute(
            "SELECT class_id,status FROM class_bookings WHERE telegram_id=%s",
            (int(telegram_id),),
        )
        states = {row[0]: row[1] for row in cur.fetchall()}; conn.rollback()
        return {"items": [_projection(row, viewer_booking_status=states.get(row[0])) for row in rows]}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def prepare_class_booking(get_connection, telegram_id, class_id):
    try: class_uuid = uuid.UUID(str(class_id))
    except ValueError: raise BookableClassError("invalid_class_id") from None
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT price_amount,currency,capacity,status,booking_deadline,
                   (SELECT COUNT(*) FROM class_bookings WHERE class_id=%s AND status='paid')
            FROM bookable_classes WHERE class_id=%s FOR UPDATE
            """, (str(class_uuid), str(class_uuid)),
        )
        row = cur.fetchone()
        if not row: raise BookableClassError("class_not_found", 404)
        amount,currency,capacity,status,deadline,paid_count = row
        if status not in ("open", "confirmed") or deadline <= datetime.utcnow():
            raise BookableClassError("booking_closed", 409)
        if int(paid_count or 0) >= int(capacity):
            raise BookableClassError("class_full", 409)
        cur.execute(
            """
            INSERT INTO class_bookings (booking_id,class_id,telegram_id,status,amount,currency)
            VALUES (%s,%s,%s,'pending',%s,%s)
            ON CONFLICT (class_id,telegram_id) DO UPDATE SET
                status=CASE
                    WHEN class_bookings.status='checkout_open'
                     AND class_bookings.checkout_expires_at <= NOW()
                    THEN 'pending' ELSE class_bookings.status END,
                stripe_checkout_session_id=CASE
                    WHEN class_bookings.status='checkout_open'
                     AND class_bookings.checkout_expires_at <= NOW()
                    THEN NULL ELSE class_bookings.stripe_checkout_session_id END,
                checkout_url=CASE
                    WHEN class_bookings.status='checkout_open'
                     AND class_bookings.checkout_expires_at <= NOW()
                    THEN NULL ELSE class_bookings.checkout_url END,
                checkout_expires_at=CASE
                    WHEN class_bookings.status='checkout_open'
                     AND class_bookings.checkout_expires_at <= NOW()
                    THEN NULL ELSE class_bookings.checkout_expires_at END,
                checkout_generation=class_bookings.checkout_generation + CASE
                    WHEN class_bookings.status='checkout_open'
                     AND class_bookings.checkout_expires_at <= NOW()
                    THEN 1 ELSE 0 END,
                updated_at=NOW()
            RETURNING booking_id,status,stripe_checkout_session_id,checkout_url,
                      checkout_expires_at,checkout_generation
            """,
            (str(uuid.uuid4()), str(class_uuid), int(telegram_id), int(amount), currency),
        )
        booking = cur.fetchone()
        if booking[1] not in ("pending", "checkout_open"):
            raise BookableClassError("booking_already_finalized", 409)
        conn.commit()
        return {
            "booking_id": str(booking[0]), "status": booking[1],
            "stripe_checkout_session_id": booking[2], "checkout_url": booking[3],
            "checkout_expires_at": _iso(booking[4]), "amount": int(amount),
            "currency": currency, "checkout_generation": int(booking[5]),
        }
    except BookableClassError:
        conn.rollback(); raise
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def mark_booking_checkout_open(get_connection, booking_id, session_id, url, expires_at):
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE class_bookings SET status='checkout_open',
                stripe_checkout_session_id=%s,checkout_url=%s,
                checkout_expires_at=%s,updated_at=NOW()
            WHERE booking_id=%s AND status IN ('pending','checkout_open')
            RETURNING booking_id
            """, (session_id, url, expires_at, str(uuid.UUID(str(booking_id)))),
        )
        if not cur.fetchone():
            conn.rollback(); raise BookableClassError("booking_state_changed", 409)
        conn.commit()
    except BookableClassError: raise
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def apply_booking_checkout_completed(cur, session):
    metadata = session.get("metadata") or {}
    booking_id = metadata.get("booking_id")
    if metadata.get("payment_kind") != "class_booking" or not booking_id:
        return None
    cur.execute(
        """
        SELECT b.booking_id,b.class_id,b.status,b.amount,b.currency,
               c.minimum_participants,c.status,b.stripe_checkout_session_id,
               c.capacity
        FROM class_bookings b JOIN bookable_classes c ON c.class_id=b.class_id
        WHERE b.booking_id=%s FOR UPDATE OF b,c
        """, (str(uuid.UUID(str(booking_id))),),
    )
    row = cur.fetchone()
    if not row: raise BookableClassError("booking_not_found", 404)
    if not row[7] or str(session.get("id")) != str(row[7]):
        raise BookableClassError("booking_session_mismatch")
    amount = int(session.get("amount_total") or 0); currency = str(session.get("currency") or "")
    if amount != int(row[3]) or currency != row[4] or session.get("payment_status") != "paid":
        raise BookableClassError("booking_payment_mismatch")
    if row[2] == "paid": return {"booking_id": str(row[0]), "status": "paid"}
    cur.execute(
        "SELECT COUNT(*) FROM class_bookings WHERE class_id=%s AND status='paid'",
        (row[1],),
    )
    paid_before = int(cur.fetchone()[0])
    if row[6] not in ("open", "confirmed") or paid_before >= int(row[8]):
        payment_intent = session.get("payment_intent")
        if not payment_intent:
            raise BookableClassError("booking_payment_intent_missing")
        cur.execute(
            """
            UPDATE class_bookings SET status='manual_review',
                stripe_payment_intent_id=%s,updated_at=NOW()
            WHERE booking_id=%s AND status IN ('pending','checkout_open')
            """, (payment_intent, row[0]),
        )
        cur.execute(
            """
            INSERT INTO class_refund_operations (
                operation_id,booking_id,stripe_payment_intent_id,status
            ) VALUES (gen_random_uuid(),%s,%s,'pending')
            ON CONFLICT (booking_id) DO NOTHING
            """, (row[0], payment_intent),
        )
        return {"booking_id": str(row[0]), "status": "refund_pending"}
    cur.execute(
        """
        UPDATE class_bookings SET status='paid',stripe_payment_intent_id=%s,
            paid_at=COALESCE(paid_at,NOW()),updated_at=NOW()
        WHERE booking_id=%s AND status IN ('pending','checkout_open')
        """, (session.get("payment_intent"), row[0]),
    )
    paid_count = paid_before + 1
    if paid_count >= int(row[5]) and row[6] == "open":
        cur.execute("UPDATE bookable_classes SET status='confirmed',confirmed_at=NOW(),updated_at=NOW() WHERE class_id=%s AND status='open'", (row[1],))
    return {"booking_id": str(row[0]), "status": "paid", "paid_count": paid_count}


def cancel_underfilled_classes(cur):
    cur.execute(
        """
        WITH underfilled AS MATERIALIZED (
            SELECT c.class_id FROM bookable_classes c
            WHERE c.status='open' AND c.booking_deadline <= NOW()
              AND (SELECT COUNT(*) FROM class_bookings b
                   WHERE b.class_id=c.class_id AND b.status='paid')
                  < c.minimum_participants
            ORDER BY c.booking_deadline,c.class_id
            FOR UPDATE OF c SKIP LOCKED
        ), cancelled AS (
            UPDATE bookable_classes c
            SET status='cancelled',cancelled_at=NOW(),updated_at=NOW()
            FROM underfilled u
            WHERE c.class_id=u.class_id AND c.status='open'
            RETURNING c.class_id
        )
        INSERT INTO class_refund_operations (
            operation_id,booking_id,stripe_payment_intent_id,status
        )
        SELECT gen_random_uuid(),b.booking_id,b.stripe_payment_intent_id,'pending'
        FROM class_bookings b JOIN cancelled c ON c.class_id=b.class_id
        WHERE b.status='paid' AND b.stripe_payment_intent_id IS NOT NULL
        ON CONFLICT (booking_id) DO NOTHING
        RETURNING booking_id
        """
    )
    return [str(row[0]) for row in cur.fetchall()]
