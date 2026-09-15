import re
import uuid
from datetime import datetime

from checkout_safety import has_active_access

MEMBER_TYPES={"lesson","meditation","recipe","nutrition_material"}
SLUG=re.compile(r"^[a-z][a-z0-9_]{0,47}$")

class MemberCatalogError(Exception):
    def __init__(self, category,status=400): super().__init__(category); self.category=category; self.status=status

def _access(row, now=None):
    if not row: return {"has_active_access":False,"expires_at":None}
    now = now or datetime.utcnow()
    paid, expiry_date, payment_failed, grace_period_end = row[:4]
    billing_shutdown_confirmed = bool(row[4]) if len(row) > 4 else False
    if billing_shutdown_confirmed:
        active = False
    elif payment_failed and grace_period_end and grace_period_end <= now:
        # A failed-renewal grace is a hard entitlement deadline.  A genuinely
        # newer access grant remains valid if it extends beyond that deadline.
        active = bool(paid and expiry_date and expiry_date > now and expiry_date > grace_period_end)
    else:
        active=has_active_access(
            paid, expiry_date, payment_failed=payment_failed,
            grace_period_end=grace_period_end, now=now,
        )
    effective=grace_period_end if payment_failed and grace_period_end and grace_period_end>now else expiry_date
    return {"has_active_access":bool(active),"expires_at":effective.isoformat() if active and effective else None}

def member_access(get_connection, telegram_id, now=None):
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute(
            """
            SELECT u.paid, u.expiry_date, u.payment_failed, u.grace_period_end,
                   EXISTS (
                       SELECT 1
                       FROM failed_subscription_terminations fst
                       WHERE fst.telegram_id = u.telegram_id
                         AND fst.stripe_cancelled_at IS NOT NULL
                         AND fst.collection_stopped_at IS NOT NULL
                         AND fst.status <> 'superseded'
                         AND fst.access_expiry IS NOT NULL
                         AND (
                             u.stripe_subscription_id IS NULL
                             OR u.stripe_subscription_id = fst.stripe_subscription_id
                         )
                         AND (
                             u.expiry_date IS NULL
                             OR u.expiry_date <= fst.access_expiry
                         )
                   ) AS billing_shutdown_confirmed
            FROM users u
            WHERE u.telegram_id = %s
            """,
            (int(telegram_id),),
        )
        row=cur.fetchone(); conn.rollback()
        return _access(row,now)
    finally: cur.close(); conn.close()

SELECT="""SELECT c.content_id,c.content_type,c.title,c.description,c.duration_seconds,c.sort_order,
 cover.media_id,video.media_id,audio.media_id,
 ARRAY(SELECT cc.slug FROM content_item_categories cic JOIN content_categories cc USING(category_id) WHERE cic.content_id=c.content_id ORDER BY COALESCE(cic.sort_order,cc.sort_order),cc.slug),
 ARRAY(SELECT cc.title FROM content_item_categories cic JOIN content_categories cc USING(category_id) WHERE cic.content_id=c.content_id ORDER BY COALESCE(cic.sort_order,cc.sort_order),cc.slug),
 c.access_level
 FROM content_items c
 LEFT JOIN content_media cover ON cover.content_id=c.content_id AND cover.media_type='cover' AND cover.deleted_at IS NULL
 LEFT JOIN content_media video ON video.content_id=c.content_id AND video.media_type='video' AND video.deleted_at IS NULL
 LEFT JOIN content_media audio ON audio.content_id=c.content_id AND audio.media_type='audio' AND audio.deleted_at IS NULL"""

def _item(row,locked):
    return {"content_id":str(row[0]),"content_type":row[1],"title":row[2],"description":row[3],"duration_seconds":row[4],"sort_order":int(row[5]),"cover_media_id":str(row[6]) if row[6] else None,"video_media_id":str(row[7]) if row[7] else None,"audio_media_id":str(row[8]) if row[8] else None,"has_cover":bool(row[6]),"has_video":bool(row[7]),"has_audio":bool(row[8]),"categories":[{"slug":s,"title":t} for s,t in zip(row[9] or [],row[10] or [])],"locked":bool(locked),"access_level":row[11]}

def list_member_catalog(get_connection, telegram_id, *, content_type="lesson",category=None,query="",limit=50):
    if content_type not in MEMBER_TYPES: raise MemberCatalogError("invalid_content_type")
    if category is not None and (not isinstance(category,str) or not SLUG.fullmatch(category)): raise MemberCatalogError("invalid_category")
    query=(query or "").strip()
    if len(query)>120: raise MemberCatalogError("invalid_query")
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        raise MemberCatalogError("invalid_limit") from None
    if limit < 1 or limit > 50:
        raise MemberCatalogError("invalid_limit")
    access=member_access(get_connection,telegram_id); conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute("SET LOCAL statement_timeout=5000")
        cur.execute(SELECT+""" WHERE c.status='published' AND c.deleted_at IS NULL AND c.content_type=%s
          AND (%s OR c.access_level='free') AND (%s IS NULL OR EXISTS
          (SELECT 1 FROM content_item_categories cic JOIN content_categories cc USING(category_id) WHERE cic.content_id=c.content_id AND cc.slug=%s))
          AND (%s='' OR c.title ILIKE '%%'||%s||'%%')
          ORDER BY CASE c.access_level WHEN 'free' THEN 0 ELSE 1 END,c.sort_order,c.updated_at DESC,c.content_id LIMIT %s""",
          (content_type,access["has_active_access"],category,category,query,query,limit))
        items=[_item(r,False) for r in cur.fetchall()]; conn.rollback()
        return {"items":items,"access":{"has_active_access":access["has_active_access"],"expires_at":access["expires_at"]},"published_only":True}
    finally: cur.close(); conn.close()

def get_member_content(get_connection,telegram_id,content_id):
    try: content_id=str(uuid.UUID(str(content_id)))
    except Exception: raise MemberCatalogError("invalid_content_id") from None
    access=member_access(get_connection,telegram_id); conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute(SELECT+" WHERE c.content_id=%s AND c.status='published' AND c.deleted_at IS NULL",(content_id,)); row=cur.fetchone()
        if not row: conn.rollback(); return None
        result=_item(row,False)
        content_allowed = access["has_active_access"] or result["access_level"] == "free"
        if not content_allowed:
            conn.rollback(); return None
        if result["content_type"]=='recipe':
            cur.execute("SELECT name,amount FROM recipe_ingredients WHERE content_id=%s ORDER BY sort_order,ingredient_id",(content_id,)); result["ingredients"]=[{"name":r[0],"amount":r[1]} for r in cur.fetchall()]
            cur.execute("SELECT step_number,instruction FROM recipe_steps WHERE content_id=%s ORDER BY step_number,step_id",(content_id,)); result["steps"]=[{"step_number":int(r[0]),"instruction":r[1]} for r in cur.fetchall()]
        if result["content_type"]=='nutrition_material':
            cur.execute("SELECT body FROM nutrition_material_bodies WHERE content_id=%s",(content_id,)); body=cur.fetchone(); result["body"]=body[0] if body else ""
        conn.rollback(); return result
    finally: cur.close(); conn.close()

def list_member_categories(get_connection,content_type,telegram_id):
    if content_type not in MEMBER_TYPES: raise MemberCatalogError("invalid_content_type")
    access = member_access(get_connection, telegram_id)
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute("""SELECT cc.slug,cc.title,cc.group_slug,COUNT(c.content_id)
          FROM content_categories cc LEFT JOIN content_item_categories cic ON cic.category_id=cc.category_id
          LEFT JOIN content_items c ON c.content_id=cic.content_id AND c.status='published' AND c.deleted_at IS NULL
            AND (%s OR c.access_level='free')
          WHERE cc.content_type=%s AND cc.is_active=TRUE GROUP BY cc.slug,cc.title,cc.group_slug,cc.sort_order ORDER BY cc.sort_order""",
          (access["has_active_access"], content_type))
        rows=[{"slug":r[0],"title":r[1],"group":r[2],"count":int(r[3])} for r in cur.fetchall()]; conn.rollback(); return {"items":rows}
    finally: cur.close(); conn.close()
