import uuid

from content_cms import ContentCmsError

MAX_CATEGORIES_PER_ITEM = 20

def _ids(values):
    if not isinstance(values, list) or len(values) > MAX_CATEGORIES_PER_ITEM:
        raise ContentCmsError("invalid_categories")
    try:
        result = [str(uuid.UUID(str(value))) for value in values]
    except (TypeError, ValueError, AttributeError):
        raise ContentCmsError("invalid_categories") from None
    if len(result) != len(set(result)):
        raise ContentCmsError("duplicate_category")
    return result

def categories_for_content_cur(cur, content_id):
    cur.execute("""SELECT cc.category_id,cc.slug,cc.title,cc.group_slug,cc.sort_order
      FROM content_item_categories cic JOIN content_categories cc USING(category_id)
      WHERE cic.content_id=%s ORDER BY COALESCE(cic.sort_order,cc.sort_order),cc.slug""", (content_id,))
    return [{"id":str(r[0]),"slug":r[1],"title":r[2],"group":r[3],"sort_order":int(r[4])} for r in cur.fetchall()]

def validate_categories_cur(cur, content_type, values):
    ids = _ids(values)
    if not ids: return []
    cur.execute("SELECT category_id,content_type,is_active FROM content_categories WHERE category_id=ANY(%s::uuid[])", (ids,))
    rows = cur.fetchall()
    if len(rows) != len(ids): raise ContentCmsError("unknown_category")
    by_id = {str(r[0]):r for r in rows}
    if any(by_id[i][1] != content_type for i in ids): raise ContentCmsError("category_content_type_mismatch")
    if any(not by_id[i][2] for i in ids): raise ContentCmsError("inactive_category")
    return ids

def replace_categories_cur(cur, content_id, content_type, values):
    ids = validate_categories_cur(cur, content_type, values)
    cur.execute("DELETE FROM content_item_categories WHERE content_id=%s", (content_id,))
    for position, category_id in enumerate(ids):
        cur.execute("INSERT INTO content_item_categories(content_id,category_id,sort_order) VALUES(%s,%s,%s)", (content_id,category_id,position))
    return ids

def list_categories(get_connection, content_type):
    if content_type not in {"lesson","meditation","recipe","nutrition_material"}: raise ContentCmsError("invalid_content_type")
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute("SET LOCAL statement_timeout=5000")
        cur.execute("SELECT category_id,slug,title,group_slug,sort_order FROM content_categories WHERE content_type=%s AND is_active=TRUE ORDER BY sort_order,slug",(content_type,))
        rows=[{"id":str(r[0]),"slug":r[1],"title":r[2],"group":r[3],"sort_order":int(r[4])} for r in cur.fetchall()]
        conn.rollback(); return {"items":rows,"read_only":True}
    finally: cur.close(); conn.close()

def get_content_categories(get_connection, content_id):
    conn=get_connection(); cur=conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute("SET LOCAL statement_timeout=5000")
        result=categories_for_content_cur(cur, content_id); conn.rollback(); return result
    finally: cur.close(); conn.close()
