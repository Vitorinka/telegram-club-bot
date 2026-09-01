import hashlib
import hmac
import json
import uuid
from datetime import datetime, timedelta

from admin_security import cancel_admin_action, complete_admin_action, make_action_request
from content_cms import ContentCmsError


PUBLISH_ACTION_TYPE = "content_publish"
ARCHIVE_ACTION_TYPE = "content_archive"
PREVIEW_TTL = timedelta(minutes=10)


def _uuid(value, category="invalid_content_id"):
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        raise ContentCmsError(category) from None


def _iso(value):
    return value.isoformat() if value else None


def _load_state(cur, content_id, *, lock=False):
    cur.execute("""
        SELECT content_id,content_type,category,title,description,duration_seconds,
               sort_order,status,version,published_at,archived_at
        FROM content_items WHERE content_id=%s
    """ + (" FOR UPDATE" if lock else ""), (content_id,))
    row = cur.fetchone()
    if not row:
        raise ContentCmsError("content_not_found", 404)
    state = {
        "content_id": str(row[0]), "content_type": row[1], "category": row[2],
        "title": row[3], "description": row[4], "duration_seconds": row[5],
        "sort_order": int(row[6]), "status": row[7], "version": int(row[8]),
        "published_at": row[9], "archived_at": row[10],
    }
    cur.execute("""
        SELECT media_id,media_type,version,sort_order,mime_type,size_bytes,sha256
        FROM content_media WHERE content_id=%s AND deleted_at IS NULL
        ORDER BY media_type,sort_order,media_id
    """ + (" FOR UPDATE" if lock else ""), (content_id,))
    state["media"] = [{
        "media_id": str(item[0]), "media_type": item[1],
        "version": int(item[2]), "sort_order": int(item[3]),
        "mime_type": item[4], "size_bytes": int(item[5]), "sha256": item[6],
    } for item in cur.fetchall()]
    from content_taxonomy import categories_for_content_cur
    state["categories"] = categories_for_content_cur(cur, content_id)
    state["ingredients"] = []
    state["steps"] = []
    state["nutrition_body"] = None
    if state["content_type"] == "recipe":
        cur.execute("""
            SELECT name,amount,sort_order FROM recipe_ingredients
            WHERE content_id=%s ORDER BY sort_order,ingredient_id
        """ + (" FOR UPDATE" if lock else ""), (content_id,))
        state["ingredients"] = [{"name": r[0], "amount": r[1], "sort_order": int(r[2])} for r in cur.fetchall()]
        cur.execute("""
            SELECT step_number,instruction FROM recipe_steps
            WHERE content_id=%s ORDER BY step_number,step_id
        """ + (" FOR UPDATE" if lock else ""), (content_id,))
        state["steps"] = [{"step_number": int(r[0]), "instruction": r[1]} for r in cur.fetchall()]
    elif state["content_type"] == "nutrition_material":
        cur.execute("SELECT body FROM nutrition_material_bodies WHERE content_id=%s" + (" FOR UPDATE" if lock else ""), (content_id,))
        body = cur.fetchone()
        state["nutrition_body"] = body[0] if body else None
    return state


def _fingerprint(state, secret):
    material = {
        "content_id": state["content_id"], "content_type": state["content_type"],
        "category": state["category"], "title": state["title"],
        "description": state["description"], "duration_seconds": state["duration_seconds"],
        "sort_order": state["sort_order"], "status": state["status"],
        "version": state["version"], "published_at": _iso(state["published_at"]),
        "archived_at": _iso(state["archived_at"]), "media": state["media"],
        "ingredients": state["ingredients"], "steps": state["steps"],
        "nutrition_body": state["nutrition_body"],
        "categories": state["categories"],
    }
    encoded = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), encoded, hashlib.sha256).hexdigest()


def _validate_complete(state):
    media_types = {item["media_type"] for item in state["media"]}
    content_type = state["content_type"]
    if not state["title"]:
        raise ContentCmsError("content_publish_incomplete", 409)
    if content_type == "lesson" and (not state["duration_seconds"] or "video" not in media_types):
        raise ContentCmsError("content_publish_incomplete", 409)
    if content_type == "meditation" and (
        not state["duration_seconds"] or not media_types.intersection({"audio", "video"})
    ):
        raise ContentCmsError("content_publish_incomplete", 409)
    if content_type == "recipe" and (not state["ingredients"] or not state["steps"]):
        raise ContentCmsError("content_publish_incomplete", 409)
    if content_type == "nutrition_material" and not (state["nutrition_body"] or "").strip():
        raise ContentCmsError("content_publish_incomplete", 409)


def _safe_preview(state, action_id, expires_at):
    media = [{
        "media_id": item["media_id"], "media_type": item["media_type"],
        "version": item["version"], "mime_type": item["mime_type"],
        "size_bytes": item["size_bytes"],
    } for item in state["media"]]
    domain = {}
    if state["content_type"] == "recipe":
        domain = {"ingredients": state["ingredients"], "steps": state["steps"]}
    elif state["content_type"] == "nutrition_material":
        domain = {"body": state["nutrition_body"]}
    return {
        "action_id": action_id, "content_id": state["content_id"],
        "content_type": state["content_type"], "title": state["title"],
        "category": state["category"], "description": state["description"],
        "duration_seconds": state["duration_seconds"], "media": media,
        "domain": domain, "warnings": [], "expected_version": state["version"],
        "categories": state["categories"],
        "preview_expires_at": expires_at.isoformat(),
    }


def create_lifecycle_preview(get_connection, content_id, admin_id, expected_version, secret, *, archive=False, now=None):
    content_id = _uuid(content_id)
    if isinstance(expected_version, bool) or not isinstance(expected_version, int) or expected_version < 1:
        raise ContentCmsError("invalid_expected_version")
    now = now or datetime.utcnow()
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET LOCAL statement_timeout=5000"); cur.execute("SET LOCAL lock_timeout='2s'")
        state = _load_state(cur, content_id, lock=True)
        required_status = "published" if archive else "draft"
        if state["status"] != required_status:
            raise ContentCmsError("content_not_archivable" if archive else "content_not_publishable", 409)
        if state["version"] != expected_version:
            raise ContentCmsError("content_publish_state_changed", 409)
        if not archive:
            _validate_complete(state)
        expires_at = now + PREVIEW_TTL
        action_type = ARCHIVE_ACTION_TYPE if archive else PUBLISH_ACTION_TYPE
        action_id = make_action_request(cur, admin_id, action_type, {
            "content_id": content_id, "expected_version": expected_version,
            "expected_fingerprint": _fingerprint(state, secret),
            "preview_created_at": now.isoformat(), "preview_expires_at": expires_at.isoformat(),
        }, ttl_minutes=10, now=now)
        result = _safe_preview(state, action_id, expires_at)
        conn.commit(); return result
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def _parse_action(row, expected_type, content_id, now):
    if not row:
        raise ContentCmsError("content_action_not_found", 404)
    action_type, raw_payload, status, expires_at = row
    if action_type != expected_type:
        raise ContentCmsError("content_action_not_found", 404)
    try:
        payload = json.loads(raw_payload)
    except (TypeError, ValueError, json.JSONDecodeError):
        raise ContentCmsError("content_publish_state_changed", 409) from None
    if not isinstance(payload, dict) or payload.get("content_id") != content_id:
        raise ContentCmsError("content_publish_state_changed", 409)
    if status == "pending" and expires_at <= now:
        raise ContentCmsError("content_publish_preview_expired", 409)
    return status, payload


def _insert_snapshot(cur, state, action_id, admin_id, event_type, now):
    version_id = str(uuid.uuid4())
    result_version = state["version"] + 1
    snapshot_status = "published" if event_type == "publish" else "archived"
    published_at = now if event_type == "publish" else state["published_at"]
    archived_at = now if event_type == "archive" else None
    cur.execute("""
        INSERT INTO content_item_versions (
            version_id,content_id,content_version,event_type,content_type,status,
            title,description,category,duration_seconds,sort_order,published_at,
            archived_at,action_id,created_by_telegram_id,created_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (version_id,state["content_id"],result_version,event_type,state["content_type"],
          snapshot_status,state["title"],state["description"],state["category"],
          state["duration_seconds"],state["sort_order"],published_at,archived_at,
          action_id,int(admin_id),now))
    for item in state["media"]:
        cur.execute("""
            INSERT INTO content_item_version_media
                (version_id,media_id,media_type,media_version,sort_order,mime_type,size_bytes,sha256)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (version_id,item["media_id"],item["media_type"],item["version"],
              item["sort_order"],item["mime_type"],item["size_bytes"],item["sha256"]))
    for position, item in enumerate(state["categories"], 1):
        cur.execute("""INSERT INTO content_item_version_categories
          (version_id,position,category_id,content_type,slug,title,group_slug,sort_order)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
          (version_id,position,item["id"],state["content_type"],item["slug"],item["title"],item["group"],item["sort_order"]))
    for position, item in enumerate(state["ingredients"], 1):
        cur.execute("INSERT INTO content_item_version_recipe_ingredients (version_id,position,name,amount,sort_order) VALUES (%s,%s,%s,%s,%s)",
                    (version_id,position,item["name"],item["amount"],item["sort_order"]))
    for position, item in enumerate(state["steps"], 1):
        cur.execute("INSERT INTO content_item_version_recipe_steps (version_id,position,step_number,instruction) VALUES (%s,%s,%s,%s)",
                    (version_id,position,item["step_number"],item["instruction"]))
    if state["nutrition_body"] is not None:
        cur.execute("INSERT INTO content_item_version_nutrition (version_id,body) VALUES (%s,%s)", (version_id,state["nutrition_body"]))
    return version_id, result_version, published_at, archived_at


def confirm_lifecycle(get_connection, content_id, action_id, admin_id, secret, *, archive=False, now=None):
    content_id = _uuid(content_id); action_id = _uuid(action_id, "invalid_action_id")
    now = now or datetime.utcnow(); expected_type = ARCHIVE_ACTION_TYPE if archive else PUBLISH_ACTION_TYPE
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET LOCAL statement_timeout=5000"); cur.execute("SET LOCAL lock_timeout='2s'")
        cur.execute("SELECT action_type,payload_json,status,expires_at FROM admin_action_requests WHERE action_id=%s AND admin_id=%s FOR UPDATE", (action_id,int(admin_id)))
        status, payload = _parse_action(cur.fetchone(), expected_type, content_id, now)
        if status == "completed":
            cur.execute("SELECT version_id,content_version,status,created_at FROM content_item_versions WHERE action_id=%s", (action_id,))
            row = cur.fetchone(); conn.commit()
            if not row: raise ContentCmsError("content_publish_state_changed", 409)
            return {"status":"completed", "version_id":str(row[0]), "version":int(row[1]), "content_status":row[2], "completed_at":_iso(row[3])}
        if status != "pending":
            raise ContentCmsError("content_action_not_confirmable", 409)
        state = _load_state(cur, content_id, lock=True)
        required_status = "published" if archive else "draft"
        identity = payload.get("expected_fingerprint")
        if (
            state["status"] != required_status
            or payload.get("expected_version") != state["version"]
            or not isinstance(identity, str)
            or not hmac.compare_digest(identity, _fingerprint(state, secret))
        ):
            raise ContentCmsError("content_publish_state_changed", 409)
        if not archive:
            _validate_complete(state)
        version_id, result_version, published_at, archived_at = _insert_snapshot(
            cur, state, action_id, admin_id, "archive" if archive else "publish", now
        )
        if archive:
            cur.execute("UPDATE content_items SET status='archived',archived_at=%s,version=%s,updated_at=%s WHERE content_id=%s AND status='published' AND version=%s",
                        (archived_at,result_version,now,content_id,state["version"]))
        else:
            cur.execute("UPDATE content_items SET status='published',published_at=%s,version=%s,updated_at=%s WHERE content_id=%s AND status='draft' AND version=%s",
                        (published_at,result_version,now,content_id,state["version"]))
        if cur.rowcount != 1:
            raise ContentCmsError("content_publish_state_changed", 409)
        cur.execute("UPDATE admin_action_requests SET status='processing' WHERE action_id=%s AND admin_id=%s AND status='pending'", (action_id,int(admin_id)))
        if cur.rowcount != 1:
            raise ContentCmsError("content_action_not_confirmable", 409)
        complete_admin_action(cur, action_id)
        conn.commit()
        return {"status":"completed", "version_id":version_id, "version":result_version, "content_status":"archived" if archive else "published", "completed_at":now.isoformat()}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def cancel_lifecycle(get_connection, content_id, action_id, admin_id, *, archive=False):
    content_id = _uuid(content_id); action_id = _uuid(action_id, "invalid_action_id")
    expected_type = ARCHIVE_ACTION_TYPE if archive else PUBLISH_ACTION_TYPE
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT action_type,payload_json,status FROM admin_action_requests WHERE action_id=%s AND admin_id=%s FOR UPDATE", (action_id,int(admin_id)))
        row = cur.fetchone()
        if not row or row[0] != expected_type:
            raise ContentCmsError("content_action_not_found", 404)
        try: payload = json.loads(row[1])
        except (TypeError, ValueError, json.JSONDecodeError): payload = {}
        if payload.get("content_id") != content_id:
            raise ContentCmsError("content_action_not_found", 404)
        if row[2] == "cancelled":
            conn.commit(); return {"status":"cancelled", "action_id":action_id}
        if row[2] != "pending" or not cancel_admin_action(cur, action_id, admin_id):
            raise ContentCmsError("content_action_not_cancellable", 409)
        conn.commit(); return {"status":"cancelled", "action_id":action_id}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def list_versions(get_connection, content_id):
    content_id = _uuid(content_id); conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute("SET LOCAL statement_timeout=5000")
        cur.execute("SELECT version_id,content_version,event_type,status,created_by_telegram_id,created_at FROM content_item_versions WHERE content_id=%s ORDER BY content_version DESC", (content_id,))
        items = [{"version_id":str(r[0]),"version":int(r[1]),"event_type":r[2],"status":r[3],"admin_id":int(r[4]),"created_at":_iso(r[5])} for r in cur.fetchall()]
        conn.rollback(); return {"items":items,"read_only":True}
    except Exception:
        conn.rollback(); raise
    finally: cur.close(); conn.close()


def get_version(get_connection, content_id, version_id):
    content_id = _uuid(content_id); version_id = _uuid(version_id, "invalid_version_id")
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY"); cur.execute("SET LOCAL statement_timeout=5000")
        cur.execute("""
            SELECT version_id,content_version,event_type,content_type,status,title,description,
                   category,duration_seconds,sort_order,published_at,archived_at,
                   created_by_telegram_id,created_at
            FROM content_item_versions WHERE content_id=%s AND version_id=%s
        """, (content_id,version_id)); row=cur.fetchone()
        if not row: conn.rollback(); return None
        result={"version_id":str(row[0]),"version":int(row[1]),"event_type":row[2],"content_type":row[3],"status":row[4],"title":row[5],"description":row[6],"category":row[7],"duration_seconds":row[8],"sort_order":int(row[9]),"published_at":_iso(row[10]),"archived_at":_iso(row[11]),"admin_id":int(row[12]),"created_at":_iso(row[13])}
        cur.execute("SELECT media_id,media_type,media_version,sort_order,mime_type,size_bytes FROM content_item_version_media WHERE version_id=%s ORDER BY media_type", (version_id,))
        result["media"]=[{"media_id":str(r[0]),"media_type":r[1],"version":int(r[2]),"sort_order":int(r[3]),"mime_type":r[4],"size_bytes":int(r[5])} for r in cur.fetchall()]
        cur.execute("SELECT category_id,slug,title,group_slug,sort_order FROM content_item_version_categories WHERE version_id=%s ORDER BY position", (version_id,))
        result["categories"]=[{"id":str(r[0]),"slug":r[1],"title":r[2],"group":r[3],"sort_order":int(r[4])} for r in cur.fetchall()]
        cur.execute("SELECT name,amount,sort_order FROM content_item_version_recipe_ingredients WHERE version_id=%s ORDER BY position", (version_id,))
        result["ingredients"]=[{"name":r[0],"amount":r[1],"sort_order":int(r[2])} for r in cur.fetchall()]
        cur.execute("SELECT step_number,instruction FROM content_item_version_recipe_steps WHERE version_id=%s ORDER BY position", (version_id,))
        result["steps"]=[{"step_number":int(r[0]),"instruction":r[1]} for r in cur.fetchall()]
        cur.execute("SELECT body FROM content_item_version_nutrition WHERE version_id=%s", (version_id,)); body=cur.fetchone(); result["nutrition_body"]=body[0] if body else None
        conn.rollback(); return result
    except Exception:
        conn.rollback(); raise
    finally: cur.close(); conn.close()
