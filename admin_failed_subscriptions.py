import base64
import hashlib
import json
import re
from datetime import datetime

from admin_security import cancel_admin_action, claim_admin_action, complete_admin_action, fail_admin_action, make_action_request
from admin_users import mask_stripe_identifier
from failed_subscription_termination import RETRYABLE_STATUSES, TERMINAL_STATUSES


FILTERS = frozenset({"all", "attention", "retryable", "processing", "stale", "manual_review", "completed", "superseded"})
PUBLIC_STATUSES = frozenset({
    "pending", "processing", "stripe_cancelled", "collection_stopped",
    "telegram_failed", "telegram_removed", "retryable_failed",
    "completed", "superseded", "manual_review",
})
REASON_LABELS = {
    "user_cancelled_after_payment_failure": "Пользователь отменил после ошибки оплаты",
    "grace_period_expired": "Истёк grace-период",
}
PUBLIC_ERROR_CATEGORIES = frozenset({
    "stripe_cancel_failed", "failed_invoice_missing",
    "failed_invoice_identity_mismatch", "failed_invoice_already_paid",
    "failed_invoice_status_unverifiable", "failed_invoice_void_failed",
    "telegram_admin", "telegram_removal_failed",
})
OPERATION_ID = re.compile(r"^[A-Za-z0-9_-]{8,100}$")
ACTION_TYPE = "failed_subscription_retry"
DEFAULT_LIMIT = 25
MAX_LIMIT = 50
STATEMENT_TIMEOUT_MS = 5000


class AdminFailedSubscriptionsError(ValueError):
    def __init__(self, category, status=400):
        super().__init__(category)
        self.category = category
        self.status = int(status)


def _iso(value):
    return value.isoformat() if value else None


def _validate_operation_id(value):
    value = str(value or "")
    if not OPERATION_ID.fullmatch(value):
        raise AdminFailedSubscriptionsError("invalid_operation_id")
    return value


def _limit(value):
    try:
        value = DEFAULT_LIMIT if value in (None, "") else int(value)
    except (TypeError, ValueError):
        raise AdminFailedSubscriptionsError("invalid_limit") from None
    if value < 1 or value > MAX_LIMIT:
        raise AdminFailedSubscriptionsError("invalid_limit")
    return value


def _cursor_encode(updated_at, operation_id):
    raw = json.dumps([updated_at.isoformat(), operation_id], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _cursor_decode(value):
    if not value:
        return None
    try:
        raw = base64.urlsafe_b64decode(str(value) + "=" * (-len(str(value)) % 4))
        timestamp, operation_id = json.loads(raw)
        timestamp = datetime.fromisoformat(timestamp)
        operation_id = _validate_operation_id(operation_id)
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        raise AdminFailedSubscriptionsError("invalid_cursor") from None
    return timestamp, operation_id


def _stale(status, lease_until, now):
    return status == "processing" and (lease_until is None or lease_until <= now)


def _retry_allowed(status, lease_until, now):
    return status in RETRYABLE_STATUSES and not (status == "processing" and not _stale(status, lease_until, now))


def _needs_attention(status, lease_until, now, db_finalized_at=None):
    return (status == "completed" and not db_finalized_at) or status == "manual_review" or _stale(status, lease_until, now) or (
        status in RETRYABLE_STATUSES and status != "processing"
    )


def _phase(row):
    status = row[4]
    if status in ("superseded", "manual_review"):
        return status
    if row[18]:
        return "completed"
    if row[17] or row[16]:
        return "waiting_for_db"
    if row[15]:
        return "waiting_for_telegram"
    if row[14]:
        return "waiting_for_invoice"
    return "waiting_for_stripe"


def _safe_status(value):
    return value if value in PUBLIC_STATUSES else "unknown"


def _safe_category(value):
    return value if value in PUBLIC_ERROR_CATEGORIES else ("unknown_error" if value else None)


def _projection(row, detail=False):
    now = row[20]
    status = _safe_status(row[4])
    result = {
        "operation_id": row[0], "telegram_id": int(row[1]),
        "username": row[2], "first_name": row[3], "status": status,
        "reason": row[5] if row[5] in REASON_LABELS else "unknown",
        "reason_label": REASON_LABELS.get(row[5], "Другая причина"),
        "attempt_count": int(row[6] or 0), "created_at": _iso(row[7]),
        "updated_at": _iso(row[8]), "lease_until": _iso(row[9]),
        "stale": _stale(status, row[9], now),
        "needs_attention": _needs_attention(status, row[9], now, row[18]),
        "retry_allowed": _retry_allowed(status, row[9], now),
        "current_phase": _phase(row), "access_expiry": _iso(row[10]),
        "stripe": {
            "subscription_id": mask_stripe_identifier(row[11]),
            "failed_invoice_id": mask_stripe_identifier(row[12]),
        },
    }
    if detail:
        result.update({
            "stripe_cancelled_at": _iso(row[14]),
            "collection_stopped_at": _iso(row[15]),
            "telegram_banned_at": _iso(row[16]),
            "telegram_removed_at": _iso(row[17]),
            "db_finalized_at": _iso(row[18]), "completed_at": _iso(row[19]),
            "last_error_category": _safe_category(row[13]),
            "claim_generation": int(row[21] or 0),
        })
    return result


SELECT = """
SELECT f.operation_id, f.telegram_id, u.username, u.first_name, f.status,
       f.reason, f.attempt_count, f.created_at, f.updated_at, f.lease_until,
       f.access_expiry, f.stripe_subscription_id, f.failed_invoice_id,
       f.last_error_category, f.stripe_cancelled_at, f.collection_stopped_at,
       f.telegram_banned_at, f.telegram_removed_at, f.db_finalized_at,
       f.completed_at, (NOW() AT TIME ZONE 'UTC'), f.claim_generation
FROM failed_subscription_terminations f
LEFT JOIN users u ON u.telegram_id=f.telegram_id
"""


def _where_for(state):
    retryable = "('pending','processing','stripe_cancelled','collection_stopped','telegram_failed','telegram_removed','retryable_failed')"
    mapping = {
        "all": None,
        "attention": f"(f.status='manual_review' OR (f.status='completed' AND f.db_finalized_at IS NULL) OR f.status IN {retryable} AND (f.status<>'processing' OR f.lease_until IS NULL OR f.lease_until <= (NOW() AT TIME ZONE 'UTC')))",
        "retryable": f"f.status IN {retryable}",
        "processing": "f.status='processing' AND f.lease_until > (NOW() AT TIME ZONE 'UTC')",
        "stale": "f.status='processing' AND (f.lease_until IS NULL OR f.lease_until <= (NOW() AT TIME ZONE 'UTC'))",
        "manual_review": "f.status='manual_review'", "completed": "f.status='completed'",
        "superseded": "f.status='superseded'",
    }
    return mapping[state]


def list_failed_subscriptions(get_connection, *, state="all", limit=25, cursor=None):
    if state not in FILTERS:
        raise AdminFailedSubscriptionsError("invalid_filter")
    limit = _limit(limit); cursor = _cursor_decode(cursor)
    clauses, params = [], []
    if _where_for(state): clauses.append(_where_for(state))
    if cursor:
        clauses.append("(f.updated_at,f.operation_id)<(%s,%s)"); params.extend(cursor)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute(f"SET LOCAL statement_timeout={STATEMENT_TIMEOUT_MS}")
        cur.execute(SELECT + where + " ORDER BY f.updated_at DESC,f.operation_id DESC LIMIT %s", tuple(params+[limit+1]))
        rows=cur.fetchall()
        cur.execute("""SELECT
          COUNT(*) FILTER (WHERE status='manual_review' OR (status='completed' AND db_finalized_at IS NULL) OR (status IN ('pending','stripe_cancelled','collection_stopped','telegram_failed','telegram_removed','retryable_failed')) OR (status='processing' AND (lease_until IS NULL OR lease_until <= (NOW() AT TIME ZONE 'UTC')))),
          COUNT(*) FILTER (WHERE status IN ('pending','processing','stripe_cancelled','collection_stopped','telegram_failed','telegram_removed','retryable_failed')),
          COUNT(*) FILTER (WHERE status='manual_review'),
          COUNT(*) FILTER (WHERE status='processing' AND (lease_until IS NULL OR lease_until <= (NOW() AT TIME ZONE 'UTC'))),
          COUNT(*) FILTER (WHERE status='processing' AND lease_until > (NOW() AT TIME ZONE 'UTC'))
          FROM failed_subscription_terminations""")
        summary=cur.fetchone(); conn.rollback()
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()
    page=rows[:limit]; more=len(rows)>limit
    return {"items":[_projection(r) for r in page], "has_more":more,
            "next_cursor":_cursor_encode(page[-1][8],page[-1][0]) if more else None,
            "summary":dict(zip(("attention","retryable","manual_review","stale","processing"),map(int,summary)))}


def get_failed_subscription(get_connection, operation_id):
    operation_id=_validate_operation_id(operation_id); conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute(f"SET LOCAL statement_timeout={STATEMENT_TIMEOUT_MS}")
        cur.execute(SELECT+" WHERE f.operation_id=%s",(operation_id,)); row=cur.fetchone(); conn.rollback()
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()
    return _projection(row,True) if row else None


def _fingerprint(row):
    raw="|".join(str(row[index] or "") for index in (
        0, 1, 4, 5, 6, 8, 9, 11, 12, 14, 15, 16, 17, 18, 19, 21,
    ))
    return hashlib.sha256(raw.encode()).hexdigest()


def create_retry_preview(get_connection, admin_id, operation_id):
    operation_id=_validate_operation_id(operation_id); conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute(SELECT+" WHERE f.operation_id=%s FOR UPDATE OF f",(operation_id,)); row=cur.fetchone()
        if not row: raise AdminFailedSubscriptionsError("operation_not_found",404)
        if not _retry_allowed(_safe_status(row[4]),row[9],row[20]):
            raise AdminFailedSubscriptionsError("operation_not_retryable",409)
        action_id=make_action_request(cur,admin_id,ACTION_TYPE,{"operation_id":operation_id,"telegram_id":int(row[1]),"state_fingerprint":_fingerprint(row)})
        conn.commit(); result=_projection(row,True); result["action_id"]=action_id; return result
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()


def claim_retry_action(get_connection, admin_id, action_id, expected_operation_id):
    expected_operation_id=_validate_operation_id(expected_operation_id)
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SELECT action_type,payload_json,status FROM admin_action_requests WHERE action_id=%s AND admin_id=%s FOR UPDATE",(action_id,int(admin_id)))
        action=cur.fetchone()
        try: preclaim_payload=json.loads(action[1]) if action else {}
        except (TypeError,ValueError,json.JSONDecodeError): preclaim_payload={}
        if not action or action[0]!=ACTION_TYPE or action[2]!="pending":
            conn.rollback(); raise AdminFailedSubscriptionsError("action_not_pending",409)
        if preclaim_payload.get("operation_id")!=expected_operation_id:
            conn.rollback(); raise AdminFailedSubscriptionsError("operation_action_mismatch",409)
        claim=claim_admin_action(cur,action_id,admin_id)
        if claim["status"]!="claimed" or claim["action_type"]!=ACTION_TYPE:
            conn.rollback(); raise AdminFailedSubscriptionsError("action_not_pending",409)
        payload=claim["payload"] or {}; operation_id=_validate_operation_id(payload.get("operation_id"))
        if operation_id!=expected_operation_id:
            conn.rollback(); raise AdminFailedSubscriptionsError("operation_action_mismatch",409)
        cur.execute(SELECT+" WHERE f.operation_id=%s FOR UPDATE OF f",(operation_id,)); row=cur.fetchone()
        if (not row or int(row[1])!=int(payload.get("telegram_id",0)) or
                _fingerprint(row)!=payload.get("state_fingerprint") or
                not _retry_allowed(_safe_status(row[4]),row[9],row[20])):
            fail_admin_action(cur,action_id); conn.commit()
            raise AdminFailedSubscriptionsError("operation_state_changed",409)
        conn.commit()
        return {"action_id":action_id,"operation_id":operation_id,"telegram_id":int(row[1]),"subscription_id":row[11],"reason":row[5]}
    except AdminFailedSubscriptionsError: raise
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()


def finish_retry_action(get_connection, action_id, success):
    conn=get_connection(); cur=conn.cursor()
    try:
        (complete_admin_action if success else fail_admin_action)(cur,action_id); conn.commit()
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()


def cancel_retry_action(get_connection, admin_id, action_id, expected_operation_id):
    expected_operation_id=_validate_operation_id(expected_operation_id)
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SELECT action_type,payload_json,status FROM admin_action_requests WHERE action_id=%s AND admin_id=%s FOR UPDATE",(action_id,int(admin_id)))
        row=cur.fetchone()
        try: payload=json.loads(row[1]) if row else {}
        except (TypeError,ValueError,json.JSONDecodeError): payload={}
        if not row or row[0]!=ACTION_TYPE or row[2]!="pending" or payload.get("operation_id")!=expected_operation_id:
            conn.rollback(); raise AdminFailedSubscriptionsError("operation_action_mismatch",409)
        result=cancel_admin_action(cur,action_id,admin_id); conn.commit(); return result
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()
