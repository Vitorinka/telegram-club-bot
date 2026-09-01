import uuid

from content_cms import ContentCmsError, get_cms_content


def _uuid(value):
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        raise ContentCmsError("invalid_content_id") from None


def create_published_revision(get_connection, content_id, admin_id):
    content_id = _uuid(content_id)
    new_id = str(uuid.uuid4())
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SET LOCAL statement_timeout=5000")
        cur.execute("SET LOCAL lock_timeout='2s'")
        cur.execute(
            """
            SELECT content_type,category,title,description,duration_seconds,
                   sort_order,status,logical_content_id,revision_number
            FROM content_items WHERE content_id=%s FOR UPDATE
            """,
            (content_id,),
        )
        source = cur.fetchone()
        if not source:
            raise ContentCmsError("content_not_found", 404)
        if source[6] != "published":
            raise ContentCmsError("content_revision_source_not_published", 409)
        logical_id = str(source[7])
        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (f"content-revision:{logical_id}",))
        cur.execute(
            "SELECT content_id FROM content_items WHERE logical_content_id=%s AND status='draft' FOR UPDATE",
            (logical_id,),
        )
        existing = cur.fetchone()
        if existing:
            conn.commit()
            return get_cms_content(get_connection, existing[0])
        revision_number = int(source[8]) + 1
        cur.execute(
            """
            INSERT INTO content_items (
                content_id,content_type,category,title,description,duration_seconds,
                sort_order,status,version,created_by_telegram_id,logical_content_id,
                revision_of,revision_number,created_at,updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,'draft',1,%s,%s,%s,%s,NOW(),NOW())
            """,
            (new_id, source[0], source[1], source[2], source[3], source[4],
             source[5], int(admin_id), logical_id, content_id, revision_number),
        )
        cur.execute(
            """INSERT INTO content_item_categories(content_id,category_id,sort_order,created_at)
               SELECT %s,category_id,sort_order,NOW() FROM content_item_categories WHERE content_id=%s""",
            (new_id, content_id),
        )
        cur.execute(
            """INSERT INTO recipe_ingredients(ingredient_id,content_id,name,amount,sort_order,created_at,updated_at)
               SELECT gen_random_uuid(),%s,name,amount,sort_order,NOW(),NOW()
               FROM recipe_ingredients WHERE content_id=%s""",
            (new_id, content_id),
        )
        cur.execute(
            """INSERT INTO recipe_steps(step_id,content_id,step_number,instruction,created_at,updated_at)
               SELECT gen_random_uuid(),%s,step_number,instruction,NOW(),NOW()
               FROM recipe_steps WHERE content_id=%s""",
            (new_id, content_id),
        )
        cur.execute(
            """INSERT INTO nutrition_material_bodies(content_id,body,created_at,updated_at)
               SELECT %s,body,NOW(),NOW() FROM nutrition_material_bodies WHERE content_id=%s""",
            (new_id, content_id),
        )
        cur.execute(
            """INSERT INTO content_media(
                 media_id,content_id,media_type,storage_kind,server_reference,mime_type,
                 size_bytes,sha256,sort_order,version,created_by_telegram_id,
                 replaces_media_id,created_at,updated_at)
               SELECT gen_random_uuid(),%s,media_type,storage_kind,server_reference,mime_type,
                 size_bytes,sha256,sort_order,1,%s,media_id,NOW(),NOW()
               FROM content_media WHERE content_id=%s AND deleted_at IS NULL""",
            (new_id, int(admin_id), content_id),
        )
        conn.commit()
        return get_cms_content(get_connection, new_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
