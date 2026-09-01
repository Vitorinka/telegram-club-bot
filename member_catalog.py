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
    active=has_active_access(row[0],row[1],payment_failed=row[2],grace_period_end=row[3],now=now)
    effective=row[3] if row[2] and row[3] and row[3]>(now or datetime.utcnow()) else row[1]
    return {"has_active_access":bool(active),"expires_at":effective.isoformat() if active and effective else None}

def member_access(get_connection, telegram_id, now=None):
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute(
            "SELECT paid,expiry_date,payment_failed,grace_period_end "
            "FROM users WHERE telegram_id=%s",
            (int(telegram_id),),
        )
        row=cur.fetchone(); conn.rollback()
        return _access(row,now)
    finally: cur.close(); conn.close()

SELECT="""SELECT c.content_id,c.content_type,c.title,c.description,c.duration_seconds,c.sort_order,
 cover.media_id,video.media_id,audio.media_id,
 ARRAY(SELECT cc.slug FROM content_item_categories cic JOIN content_categories cc USING(category_id) WHERE cic.content_id=c.content_id ORDER BY COALESCE(cic.sort_order,cc.sort_order),cc.slug),
 ARRAY(SELECT cc.title FROM content_item_categories cic JOIN content_categories cc USING(category_id) WHERE cic.content_id=c.content_id ORDER BY COALESCE(cic.sort_order,cc.sort_order),cc.slug)
 FROM content_items c
 LEFT JOIN content_media cover ON cover.content_id=c.content_id AND cover.media_type='cover' AND cover.deleted_at IS NULL
 LEFT JOIN content_media video ON video.content_id=c.content_id AND video.media_type='video' AND video.deleted_at IS NULL
 LEFT JOIN content_media audio ON audio.content_id=c.content_id AND audio.media_type='audio' AND audio.deleted_at IS NULL"""

def _item(row,locked):
    return {"content_id":str(row[0]),"content_type":row[1],"title":row[2],"description":row[3],"duration_seconds":row[4],"sort_order":int(row[5]),"cover_media_id":str(row[6]) if row[6] else None,"video_media_id":str(row[7]) if row[7] else None,"audio_media_id":str(row[8]) if row[8] else None,"has_cover":bool(row[6]),"has_video":bool(row[7]),"has_audio":bool(row[8]),"categories":[{"slug":s,"title":t} for s,t in zip(row[9] or [],row[10] or [])],"locked":bool(locked)}

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
        cur.execute(SELECT+""" WHERE c.status='published' AND c.content_type=%s AND (%s IS NULL OR EXISTS
          (SELECT 1 FROM content_item_categories cic JOIN content_categories cc USING(category_id) WHERE cic.content_id=c.content_id AND cc.slug=%s))
          AND (%s='' OR c.title ILIKE '%%'||%s||'%%') ORDER BY c.sort_order,c.updated_at DESC,c.content_id LIMIT %s""",
          (content_type,category,category,query,query,limit))
        items=[_item(r,not access["has_active_access"]) for r in cur.fetchall()]; conn.rollback()
        return {"items":items,"access":{"has_active_access":access["has_active_access"],"expires_at":access["expires_at"]},"published_only":True}
    finally: cur.close(); conn.close()

def get_member_content(get_connection,telegram_id,content_id):
    try: content_id=str(uuid.UUID(str(content_id)))
    except Exception: raise MemberCatalogError("invalid_content_id") from None
    access=member_access(get_connection,telegram_id); conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute(SELECT+" WHERE c.content_id=%s AND c.status='published'",(content_id,)); row=cur.fetchone()
        if not row: conn.rollback(); return None
        result=_item(row,not access["has_active_access"])
        if access["has_active_access"] and result["content_type"]=='recipe':
            cur.execute("SELECT name,amount FROM recipe_ingredients WHERE content_id=%s ORDER BY sort_order,ingredient_id",(content_id,)); result["ingredients"]=[{"name":r[0],"amount":r[1]} for r in cur.fetchall()]
            cur.execute("SELECT step_number,instruction FROM recipe_steps WHERE content_id=%s ORDER BY step_number,step_id",(content_id,)); result["steps"]=[{"step_number":int(r[0]),"instruction":r[1]} for r in cur.fetchall()]
        if access["has_active_access"] and result["content_type"]=='nutrition_material':
            cur.execute("SELECT body FROM nutrition_material_bodies WHERE content_id=%s",(content_id,)); body=cur.fetchone(); result["body"]=body[0] if body else ""
        conn.rollback(); return result
    finally: cur.close(); conn.close()

def list_member_categories(get_connection,content_type):
    if content_type not in MEMBER_TYPES: raise MemberCatalogError("invalid_content_type")
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute("""SELECT cc.slug,cc.title,cc.group_slug,COUNT(c.content_id)
          FROM content_categories cc LEFT JOIN content_item_categories cic ON cic.category_id=cc.category_id
          LEFT JOIN content_items c ON c.content_id=cic.content_id AND c.status='published'
          WHERE cc.content_type=%s AND cc.is_active=TRUE GROUP BY cc.slug,cc.title,cc.group_slug,cc.sort_order ORDER BY cc.sort_order""",(content_type,))
        rows=[{"slug":r[0],"title":r[1],"group":r[2],"count":int(r[3])} for r in cur.fetchall()]; conn.rollback(); return {"items":rows}
    finally: cur.close(); conn.close()
