import uuid


MEMBER_PREVIEW_LIMIT = 50
MEMBER_PREVIEW_STATUSES = ("draft", "published")


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
        content_id, title, description, category, duration_seconds,
        status, sort_order, cover_media_id, video_media_id,
    ) = row
    return {
        "content_id": str(content_id),
        "title": title,
        "description": description,
        "category": category,
        "duration_seconds": duration_seconds,
        "status": status,
        "sort_order": int(sort_order),
        "cover_media_id": str(cover_media_id) if cover_media_id else None,
        "has_cover": cover_media_id is not None,
        "has_video": video_media_id is not None,
    }


MEMBER_CONTENT_SELECT = """
    SELECT c.content_id, c.title, c.description, c.category,
           c.duration_seconds, c.status, c.sort_order,
           cover.media_id, video.media_id
    FROM content_items c
    LEFT JOIN content_media cover
      ON cover.content_id = c.content_id
     AND cover.media_type = 'cover'
     AND cover.deleted_at IS NULL
    LEFT JOIN content_media video
      ON video.content_id = c.content_id
     AND video.media_type = 'video'
     AND video.deleted_at IS NULL
"""


def _begin_read(cur):
    cur.execute("SET TRANSACTION READ ONLY")
    cur.execute("SET LOCAL statement_timeout = 5000")


def list_member_preview_content(get_connection, *, limit=MEMBER_PREVIEW_LIMIT):
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
        cur.execute(
            MEMBER_CONTENT_SELECT + """
            WHERE c.content_type = 'lesson'
              AND c.status IN ('draft', 'published')
            ORDER BY c.sort_order ASC, c.updated_at DESC, c.content_id ASC
            LIMIT %s
            """,
            (limit,),
        )
        items = [_item(row) for row in cur.fetchall()]
        conn.rollback()
        return {"items": items, "read_only": True, "preview": True}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def get_member_preview_content(get_connection, content_id):
    content_id = _content_id(content_id)
    conn = get_connection(); cur = conn.cursor()
    try:
        _begin_read(cur)
        cur.execute(
            MEMBER_CONTENT_SELECT + """
            WHERE c.content_id = %s
              AND c.content_type = 'lesson'
              AND c.status IN ('draft', 'published')
            """,
            (content_id,),
        )
        row = cur.fetchone()
        conn.rollback()
        return _item(row) if row else None
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
        cur.execute("""
            SELECT COALESCE(category, 'other'), COUNT(*)
            FROM content_items
            WHERE content_type = 'lesson'
              AND status IN ('draft', 'published')
            GROUP BY COALESCE(category, 'other')
            ORDER BY COALESCE(category, 'other')
        """)
        categories = [
            {"category": row[0], "count": int(row[1])}
            for row in cur.fetchall()
        ]
        total = sum(item["count"] for item in categories)
        conn.rollback()
        return {
            "latest_lessons": latest,
            "categories": categories,
            "total_lessons": total,
            "read_only": True,
            "preview": True,
        }
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()
