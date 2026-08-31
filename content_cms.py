import re
import uuid


CONTENT_STATEMENT_TIMEOUT_MS = 5000
DEFAULT_CONTENT_LIMIT = 25
MAX_CONTENT_LIMIT = 50
MAX_TITLE_LENGTH = 120
MAX_DESCRIPTION_LENGTH = 5000
MAX_DURATION_SECONDS = 86400
MAX_SORT_ORDER = 100000
CATEGORY_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
CONTENT_TYPES = frozenset({"lesson", "meditation", "recipe"})
CONTENT_STATUSES = frozenset({"draft", "published", "archived"})
CREATE_FIELDS = frozenset({
    "content_type", "title", "category", "description", "duration_seconds",
})
EDITABLE_FIELDS = frozenset({
    "title", "category", "description", "duration_seconds", "sort_order",
})


class ContentCmsError(ValueError):
    def __init__(self, category, status=400):
        super().__init__(category)
        self.category = category
        self.status = status


def _iso(value):
    return value.isoformat() if value else None


def _projection(row):
    (
        content_id, content_type, category, title, description,
        duration_seconds, sort_order, status, version,
        created_by_telegram_id, created_at, updated_at, published_at,
        archived_at,
    ) = row
    return {
        "content_id": str(content_id),
        "content_type": content_type,
        "category": category,
        "title": title,
        "description": description,
        "duration_seconds": duration_seconds,
        "sort_order": sort_order,
        "status": status,
        "version": version,
        "created_by_telegram_id": created_by_telegram_id,
        "created_at": _iso(created_at),
        "updated_at": _iso(updated_at),
        "published_at": _iso(published_at),
        "archived_at": _iso(archived_at),
        "has_media": False,
        "source": "cms",
    }


CONTENT_SELECT = """
    SELECT content_id, content_type, category, title, description,
           duration_seconds, sort_order, status, version,
           created_by_telegram_id, created_at, updated_at,
           published_at, archived_at
    FROM content_items
"""


def _normalize_optional_text(value, max_length, error):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContentCmsError(error)
    normalized = value.strip()
    if not normalized:
        return None
    if len(normalized) > max_length:
        raise ContentCmsError(error)
    return normalized


def _normalize_title(value):
    if not isinstance(value, str):
        raise ContentCmsError("invalid_title")
    title = " ".join(value.split())
    if not title or len(title) > MAX_TITLE_LENGTH:
        raise ContentCmsError("invalid_title")
    return title


def _normalize_category(value):
    category = _normalize_optional_text(value, 48, "invalid_category")
    if category is not None and not CATEGORY_PATTERN.fullmatch(category):
        raise ContentCmsError("invalid_category")
    return category


def _normalize_duration(value):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContentCmsError("invalid_duration")
    if value < 1 or value > MAX_DURATION_SECONDS:
        raise ContentCmsError("invalid_duration")
    return value


def _normalize_sort_order(value):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContentCmsError("invalid_sort_order")
    if abs(value) > MAX_SORT_ORDER:
        raise ContentCmsError("invalid_sort_order")
    return value


def validate_create_payload(payload):
    if not isinstance(payload, dict) or set(payload) - CREATE_FIELDS:
        raise ContentCmsError("invalid_content_payload")
    if payload.get("content_type") not in CONTENT_TYPES:
        raise ContentCmsError("invalid_content_type")
    return {
        "content_type": payload["content_type"],
        "title": _normalize_title(payload.get("title")),
        "category": _normalize_category(payload.get("category")),
        "description": _normalize_optional_text(
            payload.get("description"), MAX_DESCRIPTION_LENGTH,
            "invalid_description",
        ),
        "duration_seconds": _normalize_duration(payload.get("duration_seconds")),
    }


def validate_update_payload(payload):
    if not isinstance(payload, dict):
        raise ContentCmsError("invalid_content_payload")
    unknown = set(payload) - EDITABLE_FIELDS - {"expected_version"}
    if unknown or "expected_version" not in payload:
        raise ContentCmsError("invalid_content_payload")
    version = payload["expected_version"]
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ContentCmsError("invalid_expected_version")
    values = {}
    if "title" in payload:
        values["title"] = _normalize_title(payload["title"])
    if "category" in payload:
        values["category"] = _normalize_category(payload["category"])
    if "description" in payload:
        values["description"] = _normalize_optional_text(
            payload["description"], MAX_DESCRIPTION_LENGTH,
            "invalid_description",
        )
    if "duration_seconds" in payload:
        values["duration_seconds"] = _normalize_duration(
            payload["duration_seconds"]
        )
    if "sort_order" in payload:
        values["sort_order"] = _normalize_sort_order(payload["sort_order"])
    if not values:
        raise ContentCmsError("no_content_changes")
    return version, values


def _parse_content_id(value):
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        raise ContentCmsError("invalid_content_id") from None


def _begin_read(cur):
    cur.execute("SET TRANSACTION READ ONLY")
    cur.execute(f"SET LOCAL statement_timeout = {CONTENT_STATEMENT_TIMEOUT_MS}")


def _begin_write(cur):
    cur.execute(f"SET LOCAL statement_timeout = {CONTENT_STATEMENT_TIMEOUT_MS}")
    cur.execute("SET LOCAL lock_timeout = '2s'")


def create_content_draft(get_connection, admin_id, payload):
    values = validate_create_payload(payload)
    content_id = str(uuid.uuid4())
    conn = get_connection(); cur = conn.cursor()
    try:
        _begin_write(cur)
        cur.execute(
            """
            INSERT INTO content_items (
                content_id, content_type, category, title, description,
                duration_seconds, sort_order, status, version,
                created_by_telegram_id, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, 0, 'draft', 1, %s, NOW(), NOW())
            RETURNING content_id, content_type, category, title, description,
                      duration_seconds, sort_order, status, version,
                      created_by_telegram_id, created_at, updated_at,
                      published_at, archived_at
            """,
            (content_id, values["content_type"], values["category"],
             values["title"], values["description"],
             values["duration_seconds"], int(admin_id)),
        )
        result = _projection(cur.fetchone())
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close(); conn.close()


def list_cms_content(get_connection, *, status="all", limit=25):
    if status != "all" and status not in CONTENT_STATUSES:
        raise ContentCmsError("invalid_status")
    if isinstance(limit, bool):
        raise ContentCmsError("invalid_limit")
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        raise ContentCmsError("invalid_limit") from None
    if limit < 1 or limit > MAX_CONTENT_LIMIT:
        raise ContentCmsError("invalid_limit")
    conn = get_connection(); cur = conn.cursor()
    try:
        _begin_read(cur)
        where = "" if status == "all" else " WHERE status = %s"
        params = () if status == "all" else (status,)
        cur.execute(
            CONTENT_SELECT + where + " ORDER BY updated_at DESC, content_id DESC LIMIT %s",
            params + (limit,),
        )
        items = [_projection(row) for row in cur.fetchall()]
        conn.rollback()
        return {"items": items, "read_only": True}
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def get_cms_content(get_connection, content_id):
    content_id = _parse_content_id(content_id)
    conn = get_connection(); cur = conn.cursor()
    try:
        _begin_read(cur)
        cur.execute(CONTENT_SELECT + " WHERE content_id = %s", (content_id,))
        row = cur.fetchone()
        conn.rollback()
        return _projection(row) if row else None
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def update_content_draft(get_connection, content_id, payload):
    content_id = _parse_content_id(content_id)
    expected_version, values = validate_update_payload(payload)
    conn = get_connection(); cur = conn.cursor()
    try:
        _begin_write(cur)
        cur.execute(
            "SELECT status, version FROM content_items WHERE content_id = %s FOR UPDATE",
            (content_id,),
        )
        current = cur.fetchone()
        if not current:
            raise ContentCmsError("content_not_found", 404)
        if current[0] != "draft":
            raise ContentCmsError("content_not_editable", 409)
        if current[1] != expected_version:
            raise ContentCmsError("content_version_changed", 409)
        assignments = [f"{field} = %s" for field in values]
        params = list(values.values())
        assignments.extend(["version = version + 1", "updated_at = NOW()"])
        cur.execute(
            "UPDATE content_items SET " + ", ".join(assignments)
            + " WHERE content_id = %s AND status = 'draft' AND version = %s RETURNING "
            + CONTENT_SELECT.split("FROM content_items")[0].replace("SELECT", "", 1).strip(),
            tuple(params + [content_id, expected_version]),
        )
        row = cur.fetchone()
        if not row:
            raise ContentCmsError("content_version_changed", 409)
        result = _projection(row)
        conn.commit()
        return result
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()
