import uuid

from recipe_cms import get_recipe_structure
from nutrition_cms import get_nutrition_body


MEMBER_PREVIEW_LIMIT = 50
MEMBER_PREVIEW_STATUSES = ("draft", "published")
MEMBER_PREVIEW_CONTENT_TYPES = frozenset({"lesson", "meditation", "recipe", "nutrition_material"})


class MemberPreviewError(ValueError):
    def __init__(self, category, status=400):
        super().__init__(category)
        self.category = category
        self.status = int(status)


def _content_id(value):
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        raise MemberPreviewError("invalid_content_id") from None


def _item(row):
    (
        content_id, content_type, title, description, category, duration_seconds,
        status, sort_order, cover_media_id, video_media_id, audio_media_id,
        category_slugs, category_titles,
    ) = row
    return {
        "content_id": str(content_id),
        "content_type": content_type,
        "title": title,
        "description": description,
        "category": category,
        "duration_seconds": duration_seconds,
        "status": status,
        "sort_order": int(sort_order),
        "cover_media_id": str(cover_media_id) if cover_media_id else None,
        "has_cover": cover_media_id is not None,
        "has_video": video_media_id is not None,
        "audio_media_id": str(audio_media_id) if audio_media_id else None,
        "has_audio": audio_media_id is not None,
        "categories": [
            {"slug": slug, "title": title}
            for slug, title in zip(category_slugs or [], category_titles or [])
        ],
    }


MEMBER_CONTENT_SELECT = """
    SELECT c.content_id, c.content_type, c.title, c.description, c.category,
           c.duration_seconds, c.status, c.sort_order,
           cover.media_id, video.media_id, audio.media_id,
           ARRAY(SELECT cc.slug FROM content_item_categories cic JOIN content_categories cc USING(category_id) WHERE cic.content_id=c.content_id ORDER BY COALESCE(cic.sort_order,cc.sort_order),cc.slug),
           ARRAY(SELECT cc.title FROM content_item_categories cic JOIN content_categories cc USING(category_id) WHERE cic.content_id=c.content_id ORDER BY COALESCE(cic.sort_order,cc.sort_order),cc.slug)
    FROM content_items c
    LEFT JOIN content_media cover
      ON cover.content_id = c.content_id
     AND cover.media_type = 'cover'
     AND cover.deleted_at IS NULL
    LEFT JOIN content_media video
      ON video.content_id = c.content_id
     AND video.media_type = 'video'
     AND video.deleted_at IS NULL
    LEFT JOIN content_media audio
      ON audio.content_id = c.content_id
     AND audio.media_type = 'audio'
     AND audio.deleted_at IS NULL
"""


def _begin_read(cur):
    cur.execute("SET TRANSACTION READ ONLY")
    cur.execute("SET LOCAL statement_timeout = 5000")


def _preview_content_type(value):
    if value not in MEMBER_PREVIEW_CONTENT_TYPES:
        raise MemberPreviewError("invalid_content_type")
    return value


def list_member_preview_content(get_connection, *, limit=MEMBER_PREVIEW_LIMIT,
                                content_type="lesson", category=None):
    content_type = _preview_content_type(content_type)
    if isinstance(limit, bool):
        raise MemberPreviewError("invalid_limit")
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        raise MemberPreviewError("invalid_limit") from None
    if limit < 1 or limit > MEMBER_PREVIEW_LIMIT:
        raise MemberPreviewError("invalid_limit")
    conn = get_connection(); cur = conn.cursor()
    try:
        _begin_read(cur)
        if category is not None and (not isinstance(category,str) or len(category)>48):
            raise MemberPreviewError("invalid_category")
        category_clause = """ AND (%s IS NULL OR EXISTS (
          SELECT 1 FROM content_item_categories cic JOIN content_categories cc USING(category_id)
          WHERE cic.content_id=c.content_id AND cc.slug=%s AND cc.content_type=c.content_type))"""
        cur.execute(
            MEMBER_CONTENT_SELECT + """
            WHERE c.content_type = %s
              AND c.status IN ('draft', 'published')
            """ + category_clause + """
            ORDER BY c.sort_order ASC, c.updated_at DESC, c.content_id ASC
            LIMIT %s
            """,
            (content_type, category, category, limit),
        )
        items = [_item(row) for row in cur.fetchall()]
        conn.rollback()
        return {"items": items, "read_only": True, "preview": True}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def get_member_preview_content(get_connection, content_id, *, content_type=None):
    content_id = _content_id(content_id)
    if content_type is not None:
        content_type = _preview_content_type(content_type)
    conn = get_connection(); cur = conn.cursor()
    try:
        _begin_read(cur)
        cur.execute(
            MEMBER_CONTENT_SELECT + """
            WHERE c.content_id = %s
              AND c.content_type = COALESCE(%s, c.content_type)
              AND c.content_type IN ('lesson', 'meditation', 'recipe', 'nutrition_material')
              AND c.status IN ('draft', 'published')
            """,
            (content_id, content_type),
        )
        row = cur.fetchone()
        conn.rollback()
        result = _item(row) if row else None
        if result and result["content_type"] == "recipe":
            structure = get_recipe_structure(get_connection, content_id)
            result.update(structure or {"ingredients": [], "steps": []})
        if result and result["content_type"] == "nutrition_material":
            body = get_nutrition_body(get_connection, content_id)
            result["body"] = body["body"] if body else ""
        return result
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def get_member_preview_home(get_connection):
    conn = get_connection(); cur = conn.cursor()
    try:
        _begin_read(cur)
        cur.execute(
            MEMBER_CONTENT_SELECT + """
            WHERE c.content_type = 'lesson'
              AND c.status IN ('draft', 'published')
            ORDER BY c.updated_at DESC, c.content_id ASC
            LIMIT 6
            """
        )
        latest = [_item(row) for row in cur.fetchall()]
        cur.execute(
            MEMBER_CONTENT_SELECT + """
            WHERE c.content_type = 'meditation'
              AND c.status IN ('draft', 'published')
            ORDER BY c.updated_at DESC, c.content_id ASC
            LIMIT 6
            """
        )
        latest_meditations = [_item(row) for row in cur.fetchall()]
        cur.execute(
            MEMBER_CONTENT_SELECT + """
            WHERE c.content_type = 'recipe'
              AND c.status IN ('draft', 'published')
            ORDER BY c.updated_at DESC, c.content_id ASC
            LIMIT 6
            """
        )
        latest_recipes = [_item(row) for row in cur.fetchall()]
        cur.execute(
            MEMBER_CONTENT_SELECT + """
            WHERE c.content_type = 'nutrition_material'
              AND c.status IN ('draft', 'published')
            ORDER BY c.updated_at DESC, c.content_id ASC
            LIMIT 6
            """
        )
        latest_nutrition_materials = [_item(row) for row in cur.fetchall()]
        cur.execute("""SELECT cc.slug,cc.title,COUNT(*) FROM content_categories cc
          JOIN content_item_categories cic USING(category_id) JOIN content_items c USING(content_id)
          WHERE cc.content_type='lesson' AND cc.is_active=TRUE AND c.status IN ('draft','published')
          GROUP BY cc.slug,cc.title,cc.sort_order ORDER BY cc.sort_order,cc.slug""")
        categories = [
            {"category": row[0], "title": row[1], "count": int(row[2])}
            for row in cur.fetchall()
        ]
        cur.execute("SELECT COUNT(*) FROM content_items WHERE content_type='lesson' AND status IN ('draft','published')")
        total = int(cur.fetchone()[0])
        conn.rollback()
        return {
            "latest_lessons": latest,
            "latest_meditations": latest_meditations,
            "latest_recipes": latest_recipes,
            "latest_nutrition_materials": latest_nutrition_materials,
            "categories": categories,
            "total_lessons": total,
            "read_only": True,
            "preview": True,
        }
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()
