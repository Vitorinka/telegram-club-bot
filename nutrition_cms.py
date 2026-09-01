import uuid

from content_cms import ContentCmsError, validate_create_payload, validate_update_payload


MAX_NUTRITION_BODY_LENGTH = 30000


def _content_id(value):
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        raise ContentCmsError("invalid_content_id") from None


def normalize_nutrition_body(value):
    if not isinstance(value, str):
        raise ContentCmsError("invalid_nutrition_body")
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    value = "\n".join(line.rstrip() for line in value.split("\n")).strip()
    if not value or len(value) > MAX_NUTRITION_BODY_LENGTH:
        raise ContentCmsError("invalid_nutrition_body")
    return value


def _body_projection(row):
    if not row:
        return None
    return {
        "content_id": str(row[0]),
        "body": row[1],
        "created_at": row[2].isoformat(),
        "updated_at": row[3].isoformat(),
    }


def create_nutrition_draft(get_connection, admin_id, payload):
    if not isinstance(payload, dict) or set(payload) - {
        "content_type", "title", "category", "description", "duration_seconds", "body",
    } or "body" not in payload:
        raise ContentCmsError("invalid_content_payload")
    metadata = validate_create_payload({key: value for key, value in payload.items() if key != "body"})
    if metadata["content_type"] != "nutrition_material" or metadata["duration_seconds"] is not None:
        raise ContentCmsError("invalid_nutrition_material")
    body = normalize_nutrition_body(payload["body"])
    content_id = str(uuid.uuid4())
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET LOCAL statement_timeout = 5000")
        cur.execute("SET LOCAL lock_timeout = '2s'")
        cur.execute("""
            INSERT INTO content_items (
                content_id,content_type,category,title,description,duration_seconds,
                sort_order,status,version,created_by_telegram_id,logical_content_id,
                revision_number,created_at,updated_at
            ) VALUES (%s,'nutrition_material',%s,%s,%s,NULL,0,'draft',1,%s,%s,1,NOW(),NOW())
            RETURNING version
        """, (content_id, metadata["category"], metadata["title"], metadata["description"], int(admin_id), content_id))
        version = int(cur.fetchone()[0])
        cur.execute("""
            INSERT INTO nutrition_material_bodies (content_id,body,created_at,updated_at)
            VALUES (%s,%s,NOW(),NOW())
        """, (content_id, body))
        conn.commit()
        return {"content_id": content_id, "content_type": "nutrition_material", "status": "draft", "version": version}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def get_nutrition_body(get_connection, content_id):
    content_id = _content_id(content_id)
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SET LOCAL statement_timeout = 5000")
        cur.execute("""
            SELECT b.content_id,b.body,b.created_at,b.updated_at
            FROM nutrition_material_bodies b
            JOIN content_items c ON c.content_id=b.content_id
            WHERE b.content_id=%s AND c.content_type='nutrition_material'
        """, (content_id,))
        result = _body_projection(cur.fetchone())
        conn.rollback(); return result
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def update_nutrition_draft(get_connection, content_id, payload):
    content_id = _content_id(content_id)
    if not isinstance(payload, dict) or "body" not in payload:
        raise ContentCmsError("invalid_nutrition_payload")
    body = normalize_nutrition_body(payload["body"])
    metadata_payload = {key: value for key, value in payload.items() if key != "body"}
    expected_version, values = validate_update_payload(metadata_payload)
    if values.get("duration_seconds") is not None:
        raise ContentCmsError("invalid_nutrition_material")
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET LOCAL statement_timeout = 5000")
        cur.execute("SET LOCAL lock_timeout = '2s'")
        cur.execute("SELECT content_type,status,version FROM content_items WHERE content_id=%s FOR UPDATE", (content_id,))
        row = cur.fetchone()
        if not row:
            raise ContentCmsError("content_not_found", 404)
        if row[0] != "nutrition_material" or row[1] != "draft":
            raise ContentCmsError("content_not_editable", 409)
        if int(row[2]) != expected_version:
            raise ContentCmsError("content_version_changed", 409)
        assignments = [f"{field}=%s" for field in values]
        cur.execute(
            "UPDATE content_items SET " + ",".join(assignments)
            + ",version=version+1,updated_at=NOW() WHERE content_id=%s AND version=%s AND status='draft' RETURNING version",
            tuple(list(values.values()) + [content_id, expected_version]),
        )
        updated = cur.fetchone()
        if not updated:
            raise ContentCmsError("content_version_changed", 409)
        cur.execute("""
            INSERT INTO nutrition_material_bodies (content_id,body,created_at,updated_at)
            VALUES (%s,%s,NOW(),NOW())
            ON CONFLICT (content_id) DO UPDATE SET body=EXCLUDED.body,updated_at=NOW()
        """, (content_id, body))
        conn.commit()
        return {"content_id": content_id, "content_type": "nutrition_material", "status": "draft", "version": int(updated[0]), "body": body}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()
