"""Durable Stripe manual-authorization lifecycle for Zoom v2."""
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone

AMOUNT = 1000
CURRENCY = "eur"
PAYMENT_KIND = "class_booking_authorization_v2"
CONSENT_VERSION = "zoom-v2-guest-v1"
TERMS_CODE = "guest-10eur-cancel-24h-v1"
SAFETY_MARGIN = timedelta(hours=24)


class ZoomV2PaymentError(ValueError):
    def __init__(self, category, status=400):
        super().__init__(category); self.category=category; self.status=status


def terms_hash():
    payload={"amount":AMOUNT,"currency":CURRENCY,"cancel_hours":24,
             "late_or_no_show_capture":True,"organizer_cancel_no_capture":True,
             "version":CONSENT_VERSION,"code":TERMS_CODE}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(",",":")).encode()).hexdigest()


def payment_terms():
    return {"amount":AMOUNT,"currency":CURRENCY,"consent_version":CONSENT_VERSION,
            "terms_code":TERMS_CODE,"terms_hash":terms_hash(),"free_cancellation_hours":24,
            "late_cancellation_or_no_show_may_be_captured":True,
            "organizer_cancellation_is_not_captured":True}


def prepare_authorization(get_connection, telegram_id, class_id, *, consent_version,
                          consent_hash, explicit_consent, now=None):
    if explicit_consent is not True or consent_version != CONSENT_VERSION or consent_hash != terms_hash():
        raise ZoomV2PaymentError("invalid_payment_consent")
    now=(now or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(tzinfo=None)
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("""SELECT b.booking_id,b.booking_model,b.booking_type,b.access_status,
                       b.payment_generation,b.stripe_payment_intent_id,c.starts_at,c.duration_minutes,
                       c.lifecycle_status
                       FROM class_bookings b JOIN bookable_classes c ON c.class_id=b.class_id
                       WHERE b.class_id=%s AND b.telegram_id=%s FOR UPDATE OF b,c""",
                    (class_id,int(telegram_id)))
        row=cur.fetchone()
        if not row: raise ZoomV2PaymentError("booking_not_found",404)
        if row[1] != "authorization_v2": raise ZoomV2PaymentError("legacy_booking_not_supported",409)
        if row[3] == "booked_subscription": raise ZoomV2PaymentError("subscription_booking_does_not_require_payment",409)
        if row[8] != "open": raise ZoomV2PaymentError("class_not_open",409)
        cur.execute("SELECT operation_id,idempotency_key,status,reconciliation_status FROM class_payment_operations WHERE booking_id=%s AND operation_type='authorize' AND payment_generation=%s",(str(row[0]),row[4]))
        op=cur.fetchone()
        if op:
            conn.commit()
            return {"booking_id":str(row[0]),"operation_id":str(op[0]),"idempotency_key":op[1],
                    "generation":int(row[4]),"existing":True,
                    "request_uncertain":op[3]=="request_uncertain"}
        generation=int(row[4])+1; operation_id=str(uuid.uuid4())
        key=f"zoom-v2:authorize:{row[0]}:{generation}"
        class_end=row[6]+timedelta(minutes=int(row[7])); cutoff=row[6]-timedelta(hours=24)
        cur.execute("""UPDATE class_bookings SET booking_type='guest',access_status='authorization_pending',
                    payment_state='authorization_creating',payment_generation=%s,consent_version=%s,
                    consented_at=%s,consent_terms_hash=%s,consent_terms_code=%s,
                    consent_class_ends_at=%s,consent_cancellation_cutoff_at=%s,
                    authorization_reconciliation_status='pending',updated_at=NOW()
                    WHERE booking_id=%s""",
                    (generation,CONSENT_VERSION,now,terms_hash(),TERMS_CODE,class_end,cutoff,str(row[0])))
        cur.execute("""INSERT INTO class_payment_operations(operation_id,booking_id,operation_type,
                    payment_generation,idempotency_key,status,reconciliation_status,request_started_at)
                    VALUES (%s,%s,'authorize',%s,%s,'processing','pending',%s)""",
                    (operation_id,str(row[0]),generation,key,now))
        conn.commit(); return {"booking_id":str(row[0]),"operation_id":operation_id,
                              "generation":generation,"idempotency_key":key,"existing":False}
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()


def attach_payment_intent(get_connection, operation_id, payment_intent_id, stripe_request_id=None):
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("""SELECT o.booking_id,o.payment_generation,b.payment_generation,b.stripe_payment_intent_id
                       FROM class_payment_operations o JOIN class_bookings b ON b.booking_id=o.booking_id
                       WHERE o.operation_id=%s FOR UPDATE OF o,b""",(operation_id,)); row=cur.fetchone()
        if not row or row[1]!=row[2]: raise ZoomV2PaymentError("stale_payment_generation",409)
        if row[3] and row[3]!=payment_intent_id: raise ZoomV2PaymentError("payment_identity_mismatch",409)
        cur.execute("UPDATE class_bookings SET stripe_payment_intent_id=%s WHERE booking_id=%s",(payment_intent_id,str(row[0])))
        cur.execute("""UPDATE class_payment_operations SET stripe_payment_intent_id=%s,stripe_request_id=COALESCE(%s,stripe_request_id),
                    response_recorded_at=NOW(),reconciliation_status='pending',updated_at=NOW() WHERE operation_id=%s""",
                    (payment_intent_id,stripe_request_id,operation_id)); conn.commit()
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()


def mark_request_uncertain(get_connection, operation_id):
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("UPDATE class_payment_operations SET reconciliation_status='request_uncertain',updated_at=NOW() WHERE operation_id=%s AND status='processing'",(operation_id,)); conn.commit()
    finally: cur.close(); conn.close()


def authorization_owner(get_connection, telegram_id, class_id):
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute("""SELECT b.booking_id,b.stripe_payment_intent_id,b.payment_state,
                    b.access_status,o.operation_id,o.status,o.reconciliation_status
                    FROM class_bookings b LEFT JOIN class_payment_operations o ON o.booking_id=b.booking_id
                    AND o.operation_type='authorize' AND o.payment_generation=b.payment_generation
                    WHERE b.class_id=%s AND b.telegram_id=%s AND b.booking_model='authorization_v2'""",(class_id,int(telegram_id)))
        row=cur.fetchone(); conn.rollback()
        if not row: raise ZoomV2PaymentError("booking_not_found",404)
        return {"booking_id":str(row[0]),"payment_intent_id":row[1],"payment_state":row[2],
                "access_status":row[3],"operation_id":str(row[4]) if row[4] else None,
                "operation_status":row[5],"reconciliation_status":row[6]}
    finally: cur.close(); conn.close()


def apply_stripe_authorization(get_connection, payment_intent, *, event_id=None):
    """Fence and persist a retrieved PI with expanded latest_charge."""
    def value(obj, name, default=None):
        return obj.get(name, default) if isinstance(obj, dict) else getattr(obj, name, default)
    pi_id=value(payment_intent,"id"); metadata=value(payment_intent,"metadata",{}) or {}
    booking_id=metadata.get("booking_id"); generation=int(metadata.get("payment_generation",-1))
    status=value(payment_intent,"status"); amount=value(payment_intent,"amount")
    currency=str(value(payment_intent,"currency","")).lower()
    capture_method=value(payment_intent,"capture_method")
    capturable=int(value(payment_intent,"amount_capturable",0) or 0)
    charge=value(payment_intent,"latest_charge")
    details=value(charge,"payment_method_details",{}) or {}
    card=value(details,"card",{}) or {}; capture_before=value(card,"capture_before")
    if capture_before is not None:
        capture_before=datetime.fromtimestamp(int(capture_before),timezone.utc).replace(tzinfo=None)
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("""SELECT b.payment_generation,b.stripe_payment_intent_id,b.consent_class_ends_at,
                       EXISTS(SELECT 1 FROM class_payment_operations c WHERE c.booking_id=b.booking_id
                              AND c.operation_type='cancel' AND c.payment_generation=b.payment_generation
                              AND c.status='completed'),
                       EXISTS(SELECT 1 FROM class_payment_operations a WHERE a.booking_id=b.booking_id
                              AND a.operation_type='authorize' AND a.payment_generation=b.payment_generation)
                       FROM class_bookings b WHERE b.booking_id=%s FOR UPDATE""",(booking_id,)); row=cur.fetchone()
        if not row or row[0]!=generation or not row[4] or (row[1] and row[1]!=pi_id):
            raise ZoomV2PaymentError("payment_identity_mismatch",409)
        if row[1] is None:
            cur.execute("UPDATE class_bookings SET stripe_payment_intent_id=%s WHERE booking_id=%s AND payment_generation=%s",
                        (pi_id,booking_id,generation))
            cur.execute("UPDATE class_payment_operations SET stripe_payment_intent_id=%s,response_recorded_at=COALESCE(response_recorded_at,NOW()) WHERE booking_id=%s AND operation_type='authorize' AND payment_generation=%s",
                        (pi_id,booking_id,generation))
        valid_identity=(amount==AMOUNT and currency==CURRENCY and capture_method=="manual" and
                        metadata.get("payment_kind")==PAYMENT_KIND)
        sufficient=(capture_before is not None and row[2] is not None and capture_before>=row[2]+SAFETY_MARGIN)
        if row[3]:
            cur.execute("UPDATE class_bookings SET payment_state='cancelled',authorization_reconciliation_status='cancelled' WHERE booking_id=%s",(booking_id,))
            op_status="superseded"; reconciliation_status="cancelled"
            result="authorization_already_cancelled"
        elif status=="requires_capture" and valid_identity and capturable==AMOUNT and sufficient:
            cur.execute("""UPDATE class_bookings SET access_status='booked_guest',payment_state='authorized',
                        stripe_capture_before=%s,capture_before_verified_at=NOW(),
                        capture_before_source='stripe_charge_card_capture_before',
                        authorization_reconciliation_status='verified',authorization_last_reconciled_at=NOW(),
                        authorization_review_reason=NULL WHERE booking_id=%s AND payment_generation=%s""",
                        (capture_before,booking_id,generation))
            op_status="completed"; reconciliation_status="verified"; result="booked_guest"
        else:
            reason=("invalid_payment_identity" if not valid_identity or
                    (status=="requires_capture" and capturable!=AMOUNT) else
                    status if status!="requires_capture" else
                    "missing_capture_before" if capture_before is None else
                    "insufficient_authorization_window" if not sufficient else status)
            payment_state={"requires_action":"requires_action","requires_payment_method":"retryable_failed",
                           "canceled":"cancelled"}.get(status,"retryable_failed")
            cur.execute("""UPDATE class_bookings SET payment_state=%s,
                        authorization_reconciliation_status=%s,authorization_last_reconciled_at=NOW(),
                        authorization_review_reason=%s WHERE booking_id=%s AND payment_generation=%s""",
                        (payment_state,"manual_review" if reason=="invalid_payment_identity" else
                         "missing_capture_before" if capture_before is None else
                         "insufficient_window" if not sufficient else "retryable_failed",
                         reason,booking_id,generation))
            op_status="manual_review" if reason=="invalid_payment_identity" else "retryable_failed"
            reconciliation_status="manual_review" if op_status=="manual_review" else "retryable_failed"
            result=reason
        cur.execute("""UPDATE class_payment_operations SET status=%s,reconciliation_status=%s,
                    stripe_event_id=COALESCE(%s,stripe_event_id),stripe_last_checked_at=NOW(),updated_at=NOW(),
                    completed_at=CASE WHEN %s='completed' THEN NOW() ELSE completed_at END
                    WHERE booking_id=%s AND operation_type='authorize' AND payment_generation=%s""",
                    (op_status,reconciliation_status,
                     event_id,op_status,booking_id,generation)); conn.commit()
        return {"status":result,"cancel_required": status=="requires_capture" and result in (
            "missing_capture_before", "insufficient_authorization_window",
        )}
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()


def prepare_cancel_operation(get_connection, booking_id, generation, reason):
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SELECT payment_generation,stripe_payment_intent_id FROM class_bookings WHERE booking_id=%s FOR UPDATE",(booking_id,)); row=cur.fetchone()
        if not row or row[0]!=generation or not row[1]: raise ZoomV2PaymentError("stale_payment_generation",409)
        key=f"zoom-v2:cancel:{booking_id}:{generation}"
        cur.execute("""INSERT INTO class_payment_operations(operation_id,booking_id,operation_type,payment_generation,
                    idempotency_key,status,stripe_payment_intent_id,reconciliation_status,manual_review_reason)
                    VALUES (%s,%s,'cancel',%s,%s,'pending',%s,'pending',%s)
                    ON CONFLICT (booking_id,operation_type,payment_generation) DO NOTHING""",
                    (str(uuid.uuid4()),booking_id,generation,key,row[1],reason))
        cur.execute("SELECT operation_id,idempotency_key,stripe_payment_intent_id,status FROM class_payment_operations WHERE booking_id=%s AND operation_type='cancel' AND payment_generation=%s",
                    (booking_id,generation)); op=cur.fetchone(); conn.commit()
        return {"operation_id":str(op[0]),"idempotency_key":op[1],"payment_intent_id":op[2],"status":op[3]}
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()


def finish_cancel_operation(get_connection, operation_id, *, success, error=None):
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("""UPDATE class_payment_operations SET status=%s,reconciliation_status=%s,last_error=%s,
                    completed_at=CASE WHEN %s THEN NOW() ELSE completed_at END,updated_at=NOW()
                    WHERE operation_id=%s RETURNING booking_id,payment_generation""",
                    ("completed" if success else "retryable_failed","cancelled" if success else "retryable_failed",
                     None if success else str(error)[:2000],success,operation_id)); row=cur.fetchone()
        if success and row:
            cur.execute("""UPDATE class_bookings SET access_status='access_required',payment_state='cancelled',
                        authorization_reconciliation_status='cancelled' WHERE booking_id=%s AND payment_generation=%s""",row)
        conn.commit()
    except Exception: conn.rollback(); raise
    finally: cur.close(); conn.close()
