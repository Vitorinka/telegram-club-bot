import uuid

from content_cms import ContentCmsError


MAX_INGREDIENTS = 100
MAX_STEPS = 50
MAX_INGREDIENT_NAME = 200
MAX_INGREDIENT_AMOUNT = 100
MAX_INSTRUCTION = 2000
MAX_RECIPE_ORDER = 100000


def _uuid(value):
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        raise ContentCmsError("invalid_content_id") from None


def _text(value, maximum, category, *, optional=False):
    if value is None and optional:
        return None
    if not isinstance(value, str):
        raise ContentCmsError(category)
    value = " ".join(value.split())
    if not value:
        if optional:
            return None
        raise ContentCmsError(category)
    if len(value) > maximum:
        raise ContentCmsError(category)
    return value
def validate_recipe_payload(payload):
    if not isinstance(payload, dict) or set(payload) != {"expected_version", "ingredients", "steps"}:
        raise ContentCmsError("invalid_recipe_payload")
    version = payload["expected_version"]
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ContentCmsError("invalid_expected_version")
    ingredients = payload["ingredients"]
    steps = payload["steps"]
    if not isinstance(ingredients, list) or len(ingredients) > MAX_INGREDIENTS:
        raise ContentCmsError("invalid_ingredients")
    if not isinstance(steps, list) or len(steps) > MAX_STEPS:
        raise ContentCmsError("invalid_steps")
    normalized_ingredients = []
    ingredient_orders = set()
    for item in ingredients:
        if not isinstance(item, dict) or set(item) != {"name", "amount", "sort_order"}:
            raise ContentCmsError("invalid_ingredient")
        order = item["sort_order"]
        if isinstance(order, bool) or not isinstance(order, int) or not 0 <= order <= MAX_RECIPE_ORDER or order in ingredient_orders:
            raise ContentCmsError("invalid_ingredient_order")
        ingredient_orders.add(order)
        normalized_ingredients.append({
            "name": _text(item["name"], MAX_INGREDIENT_NAME, "invalid_ingredient_name"),
            "amount": _text(item["amount"], MAX_INGREDIENT_AMOUNT, "invalid_ingredient_amount", optional=True),
            "sort_order": order,
        })
    normalized_steps = []
    step_numbers = set()
    for item in steps:
        if not isinstance(item, dict) or set(item) != {"step_number", "instruction"}:
            raise ContentCmsError("invalid_recipe_step")
        number = item["step_number"]
        if isinstance(number, bool) or not isinstance(number, int) or number < 1 or number > MAX_STEPS or number in step_numbers:
            raise ContentCmsError("invalid_step_number")
        step_numbers.add(number)
        normalized_steps.append({
            "step_number": number,
            "instruction": _text(item["instruction"], MAX_INSTRUCTION, "invalid_step_instruction"),
        })
    return version, normalized_ingredients, normalized_steps


def _projection(cur, content_id):
    cur.execute("""
        SELECT ingredient_id, name, amount, sort_order
        FROM recipe_ingredients WHERE content_id=%s
        ORDER BY sort_order, ingredient_id
    """, (content_id,))
    ingredients = [{
        "ingredient_id": str(row[0]), "name": row[1], "amount": row[2],
        "sort_order": int(row[3]),
    } for row in cur.fetchall()]
    cur.execute("""
        SELECT step_id, step_number, instruction
        FROM recipe_steps WHERE content_id=%s
        ORDER BY step_number, step_id
    """, (content_id,))
    steps = [{
        "step_id": str(row[0]), "step_number": int(row[1]),
        "instruction": row[2],
    } for row in cur.fetchall()]
    return {"ingredients": ingredients, "steps": steps}


def get_recipe_structure(get_connection, content_id):
    content_id = _uuid(content_id)
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SET LOCAL statement_timeout = 5000")
        cur.execute("SELECT content_type FROM content_items WHERE content_id=%s", (content_id,))
        row = cur.fetchone()
        if not row or row[0] != "recipe":
            conn.rollback(); return None
        result = _projection(cur, content_id)
        conn.rollback(); return result
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def replace_recipe_structure(get_connection, content_id, payload):
    content_id = _uuid(content_id)
    expected_version, ingredients, steps = validate_recipe_payload(payload)
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SET LOCAL statement_timeout = 5000")
        cur.execute("SET LOCAL lock_timeout = '2s'")
        cur.execute("SELECT content_type,status,version FROM content_items WHERE content_id=%s FOR UPDATE", (content_id,))
        row = cur.fetchone()
        if not row:
            raise ContentCmsError("content_not_found", 404)
        if row[0] != "recipe" or row[1] != "draft":
            raise ContentCmsError("content_not_editable", 409)
        if int(row[2]) != expected_version:
            raise ContentCmsError("content_version_changed", 409)
        cur.execute("DELETE FROM recipe_ingredients WHERE content_id=%s", (content_id,))
        for item in ingredients:
            cur.execute("""
                INSERT INTO recipe_ingredients
                    (ingredient_id,content_id,name,amount,sort_order,created_at,updated_at)
                VALUES (%s,%s,%s,%s,%s,NOW(),NOW())
            """, (str(uuid.uuid4()), content_id, item["name"], item["amount"], item["sort_order"]))
        cur.execute("DELETE FROM recipe_steps WHERE content_id=%s", (content_id,))
        for item in steps:
            cur.execute("""
                INSERT INTO recipe_steps
                    (step_id,content_id,step_number,instruction,created_at,updated_at)
                VALUES (%s,%s,%s,%s,NOW(),NOW())
            """, (str(uuid.uuid4()), content_id, item["step_number"], item["instruction"]))
        cur.execute("""
            UPDATE content_items SET version=version+1,updated_at=NOW()
            WHERE content_id=%s AND version=%s AND status='draft'
            RETURNING version
        """, (content_id, expected_version))
        updated = cur.fetchone()
        if not updated:
            raise ContentCmsError("content_version_changed", 409)
        result = _projection(cur, content_id)
        result["version"] = int(updated[0])
        conn.commit(); return result
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()
