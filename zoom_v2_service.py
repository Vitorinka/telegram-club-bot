"""Persistence and access decisions for authorization-based Zoom bookings.

The service is intentionally isolated from the legacy capture-now functions in
``bookable_classes.py`` and has no Stripe SDK dependency.  Payment creation and
Zoom URL delivery belong to later stages.
"""

import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

from member_catalog import member_access
from zoom_v2_contract import (
    BOOKING_MODEL_V2,
    CLASS_DURATION_MINUTES,
    GUEST_PRICE_AMOUNT,
    GUEST_PRICE_CURRENCY,
    can_access_zoom,
    classify_booking_access,
    reconcile_subscription_booking,
    validate_class_contract,
)


LEGACY_CAPACITY_SENTINEL = 500
TITLE_MAX = 120
DESCRIPTION_MAX = 5000
LIST_MAX = 100


class ZoomV2ServiceError(ValueError):
    def __init__(self, category, status=400):
        super().__init__(category)
        self.category = category
        self.status = int(status)


def _aware_datetime(value, category):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ZoomV2ServiceError(category) from None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ZoomV2ServiceError(category)
    return value.astimezone(timezone.utc)


def _db_datetime(value):
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _uuid(value, category):
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        raise ZoomV2ServiceError(category) from None


def _access_snapshot(get_connection, telegram_id, now):
    access = member_access(
        get_connection, int(telegram_id), now=_db_datetime(now),
    )
    expires_at = access.get("expires_at")
    if expires_at:
        expires_at = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        else:
            expires_at = expires_at.astimezone(timezone.utc)
    return bool(access.get("has_active_access")), expires_at


def _booking_projection(row, *, idempotent=False):
    return {
        "booking_id": str(row[0]),
        "class_id": str(row[1]),
        "booking_model": row[2],
        "booking_type": row[3],
        "access_status": row[4],
        "payment_state": row[5],
        "settlement_status": row[6],
        "payment_required": row[4] == "access_required",
        "idempotent": bool(idempotent),
    }


def _member_class_projection(row):
    (
        class_id, title, description, starts_at, duration_minutes,
        lifecycle_status, booking_id, access_status, payment_state,
    ) = row
    if lifecycle_status == "cancelled":
        display_status = "cancelled"
        unavailable_reason = "class_cancelled"
    elif access_status == "authorization_pending" or payment_state in (
        "authorization_creating", "requires_action",
    ):
        display_status = "authorization_pending"
        unavailable_reason = "payment_authorization_required"
    elif access_status == "booked_subscription":
        display_status = "booked_subscription"
        unavailable_reason = None
    elif access_status == "access_required":
        display_status = "access_required"
        unavailable_reason = "subscription_does_not_cover_class_start"
    else:
        display_status = None
        unavailable_reason = "booking_required"
    actions = []
    if display_status == "access_required":
        actions = [
            {"type": "renew_subscription"},
            {
                "type": "authorize_one_time",
                "amount": GUEST_PRICE_AMOUNT,
                "currency": GUEST_PRICE_CURRENCY,
            },
        ]
    return {
        "class_id": str(class_id),
        "title": title,
        "description": description,
        "starts_at": starts_at.replace(tzinfo=timezone.utc).isoformat(),
        "duration_minutes": int(duration_minutes),
        "class_status": lifecycle_status,
        "booking_id": str(booking_id) if booking_id else None,
        "booking_status": display_status,
        "access_unavailable_reason": unavailable_reason,
        "available_actions": actions,
    }


def list_v2_member_classes(get_connection, telegram_id, *, now=None, limit=50):
    """List future v2 classes with only the requesting member's booking."""
    now = _aware_datetime(now or datetime.now(timezone.utc), "invalid_now")
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        raise ZoomV2ServiceError("invalid_limit") from None
    if limit < 1 or limit > LIST_MAX:
        raise ZoomV2ServiceError("invalid_limit")
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute(
            """
            SELECT c.class_id,c.title,c.description,c.starts_at,
                   c.duration_minutes,c.lifecycle_status,
                   b.booking_id,b.access_status,b.payment_state
            FROM bookable_classes c
            LEFT JOIN class_bookings b
              ON b.class_id=c.class_id
             AND b.telegram_id=%s
             AND b.booking_model='authorization_v2'
            WHERE c.booking_model='authorization_v2'
              AND c.starts_at>%s
              AND c.lifecycle_status IN ('open','cancelled')
            ORDER BY c.starts_at,c.class_id
            LIMIT %s
            """,
            (int(telegram_id), _db_datetime(now), limit),
        )
        result = {"items": [_member_class_projection(row) for row in cur.fetchall()]}
        conn.rollback()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close(); conn.close()


def create_v2_class(
    get_connection, admin_telegram_id, *, title, starts_at, zoom_url,
    description=None, published_at=None
):
    """Create and publish a v2 class without exposing it to legacy booking code."""
    starts_at = _aware_datetime(starts_at, "invalid_starts_at")
    published_at = _aware_datetime(
        published_at or datetime.now(timezone.utc), "invalid_published_at"
    )
    validate_class_contract(
        published_at=published_at,
        starts_at=starts_at,
        duration_minutes=CLASS_DURATION_MINUTES,
        capacity=None,
    )
    title = str(title or "").strip()
    description = str(description or "").strip() or None
    if not 1 <= len(title) <= TITLE_MAX:
        raise ZoomV2ServiceError("invalid_title")
    if description and len(description) > DESCRIPTION_MAX:
        raise ZoomV2ServiceError("invalid_description")
    zoom_url = str(zoom_url or "").strip()
    parsed = urlsplit(zoom_url)
    if parsed.scheme != "https" or not parsed.netloc or len(zoom_url) > 2048:
        raise ZoomV2ServiceError("invalid_zoom_url")

    class_id = str(uuid.uuid4())
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO bookable_classes (
                class_id,title,description,starts_at,duration_minutes,zoom_url,
                price_amount,currency,capacity,minimum_participants,
                booking_deadline,status,created_by_telegram_id,
                booking_model,lifecycle_status,published_at,unlimited_capacity
            ) VALUES (
                %s,%s,%s,%s,60,%s,1000,'eur',%s,3,%s,'draft',%s,
                'authorization_v2','open',%s,TRUE
            )
            """,
            (
                class_id, title, description, _db_datetime(starts_at), zoom_url,
                LEGACY_CAPACITY_SENTINEL,
                _db_datetime(starts_at - timedelta(seconds=1)),
                int(admin_telegram_id), _db_datetime(published_at),
            ),
        )
        conn.commit()
        return {
            "class_id": class_id,
            "booking_model": BOOKING_MODEL_V2,
            "lifecycle_status": "open",
            "starts_at": starts_at.isoformat(),
            "duration_minutes": CLASS_DURATION_MINUTES,
            "price_amount": GUEST_PRICE_AMOUNT,
            "currency": GUEST_PRICE_CURRENCY,
            "unlimited_capacity": True,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close(); conn.close()


def create_or_get_v2_booking(get_connection, telegram_id, class_id, *, now=None):
    """Create one free/preliminary booking; repeated/concurrent calls converge."""
    class_id = _uuid(class_id, "invalid_class_id")
    now = _aware_datetime(now or datetime.now(timezone.utc), "invalid_now")
    has_access, expires_at = _access_snapshot(get_connection, telegram_id, now)

    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT booking_model,lifecycle_status,starts_at
            FROM bookable_classes
            WHERE class_id=%s
            FOR UPDATE
            """,
            (class_id,),
        )
        class_row = cur.fetchone()
        if not class_row:
            raise ZoomV2ServiceError("class_not_found", 404)
        booking_model, lifecycle_status, starts_at_db = class_row
        if booking_model != BOOKING_MODEL_V2:
            raise ZoomV2ServiceError("legacy_class_not_supported", 409)
        if lifecycle_status != "open" or starts_at_db <= _db_datetime(now):
            raise ZoomV2ServiceError("booking_closed", 409)
        starts_at = starts_at_db.replace(tzinfo=timezone.utc)
        access_status = classify_booking_access(
            has_active_access=has_access,
            access_expires_at=expires_at,
            class_starts_at=starts_at,
        )
        booking_type = "subscription" if access_status == "booked_subscription" else "pending"
        booking_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO class_bookings (
                booking_id,class_id,telegram_id,status,amount,currency,
                booking_model,booking_type,access_status,payment_state,
                payment_generation,settlement_status,last_access_check_at
            ) VALUES (
                %s,%s,%s,'pending',1000,'eur','authorization_v2',%s,%s,
                'none',0,'none',%s
            )
            ON CONFLICT (class_id,telegram_id) DO NOTHING
            RETURNING booking_id,class_id,booking_model,booking_type,
                      access_status,payment_state,settlement_status
            """,
            (
                booking_id, class_id, int(telegram_id), booking_type,
                access_status, _db_datetime(now),
            ),
        )
        row = cur.fetchone()
        idempotent = row is None
        if row is None:
            cur.execute(
                """
                SELECT booking_id,class_id,booking_model,booking_type,
                       access_status,payment_state,settlement_status
                FROM class_bookings
                WHERE class_id=%s AND telegram_id=%s
                FOR UPDATE
                """,
                (class_id, int(telegram_id)),
            )
            row = cur.fetchone()
            if not row or row[2] != BOOKING_MODEL_V2:
                raise ZoomV2ServiceError("legacy_booking_conflict", 409)
        conn.commit()
        return _booking_projection(row, idempotent=idempotent)
    except ZoomV2ServiceError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close(); conn.close()


def reconcile_v2_booking_access(get_connection, telegram_id, class_id, *, now=None):
    """Re-evaluate subscription coverage after renewal or expiry."""
    class_id = _uuid(class_id, "invalid_class_id")
    now = _aware_datetime(now or datetime.now(timezone.utc), "invalid_now")
    has_access, expires_at = _access_snapshot(get_connection, telegram_id, now)
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT b.booking_id,b.class_id,b.booking_model,b.booking_type,
                   b.access_status,b.payment_state,b.settlement_status,c.starts_at,
                   c.booking_model,c.lifecycle_status
            FROM class_bookings b
            JOIN bookable_classes c ON c.class_id=b.class_id
            WHERE b.class_id=%s AND b.telegram_id=%s
            FOR UPDATE OF b,c
            """,
            (class_id, int(telegram_id)),
        )
        row = cur.fetchone()
        if not row:
            raise ZoomV2ServiceError("booking_not_found", 404)
        if row[2] != BOOKING_MODEL_V2 or row[8] != BOOKING_MODEL_V2:
            raise ZoomV2ServiceError("legacy_booking_not_supported", 409)
        if row[9] in ("cancelled", "completed"):
            raise ZoomV2ServiceError("class_not_access_reconcilable", 409)
        starts_at = row[7].replace(tzinfo=timezone.utc)
        target = reconcile_subscription_booking(
            row[4], has_active_access=has_access,
            access_expires_at=expires_at, class_starts_at=starts_at,
        )
        target_type = "subscription" if target == "booked_subscription" else "pending"
        changed = target != row[4] or target_type != row[3]
        if changed:
            cur.execute(
                """
                UPDATE class_bookings
                SET access_status=%s,booking_type=%s,last_access_check_at=%s,
                    updated_at=NOW()
                WHERE booking_id=%s AND booking_model='authorization_v2'
                """,
                (target, target_type, _db_datetime(now), str(row[0])),
            )
        else:
            cur.execute(
                """
                UPDATE class_bookings SET last_access_check_at=%s
                WHERE booking_id=%s AND booking_model='authorization_v2'
                """,
                (_db_datetime(now), str(row[0])),
            )
        conn.commit()
        projected = list(row[:7])
        projected[3] = target_type
        projected[4] = target
        result = _booking_projection(projected, idempotent=not changed)
        result["changed"] = changed
        return result
    except ZoomV2ServiceError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close(); conn.close()


def check_v2_zoom_access(get_connection, telegram_id, class_id, *, now=None):
    """Return an access decision only; never return or log the Zoom URL."""
    class_id = _uuid(class_id, "invalid_class_id")
    now = _aware_datetime(now or datetime.now(timezone.utc), "invalid_now")
    has_access, expires_at = _access_snapshot(get_connection, telegram_id, now)
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute(
            """
            SELECT c.booking_model,c.lifecycle_status,c.starts_at,
                   b.booking_model,b.access_status,b.payment_state,
                   b.authorization_expires_at
            FROM bookable_classes c
            LEFT JOIN class_bookings b
              ON b.class_id=c.class_id AND b.telegram_id=%s
            WHERE c.class_id=%s
            """,
            (int(telegram_id), class_id),
        )
        row = cur.fetchone()
        conn.rollback()
        if not row:
            raise ZoomV2ServiceError("class_not_found", 404)
        if row[0] != BOOKING_MODEL_V2:
            raise ZoomV2ServiceError("legacy_class_not_supported", 409)
        if row[3] is None:
            return {"allowed": False, "reason": "booking_required"}
        if row[3] != BOOKING_MODEL_V2:
            raise ZoomV2ServiceError("legacy_booking_conflict", 409)
        starts_at = row[2].replace(tzinfo=timezone.utc)
        authorization_expires_at = row[6]
        if authorization_expires_at is not None:
            authorization_expires_at = authorization_expires_at.replace(tzinfo=timezone.utc)
        allowed = can_access_zoom(
            class_state=row[1], booking_state=row[4], now=now,
            class_starts_at=starts_at, has_active_access=has_access,
            access_expires_at=expires_at, payment_state=row[5],
            authorization_expires_at=authorization_expires_at,
        )
        if allowed:
            reason = "access_confirmed"
        elif row[1] == "cancelled":
            reason = "class_cancelled"
        elif row[4] == "access_required":
            reason = "access_required"
        elif row[4] == "booked_subscription":
            reason = "subscription_access_expired"
        else:
            reason = "guest_payment_not_confirmed"
        return {"allowed": allowed, "reason": reason}
    finally:
        cur.close(); conn.close()
