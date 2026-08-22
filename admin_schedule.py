import base64
import json
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


MOSCOW_TZ = ZoneInfo("Europe/Moscow")
SCHEDULE_STATEMENT_TIMEOUT_MS = 5000
DEFAULT_SCHEDULE_LIMIT = 25
MAX_SCHEDULE_LIMIT = 50
SCHEDULE_STATUSES = frozenset({"all", "upcoming", "past"})
MONTH_NAMES = (
    "январь", "февраль", "март", "апрель", "май", "июнь",
    "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь",
)
MONTH_PATTERN = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")


class AdminScheduleQueryError(ValueError):
    pass


def _iso(value):
    return value.isoformat() if value else None


def schedule_month_label(month_key):
    if not isinstance(month_key, str) or not MONTH_PATTERN.fullmatch(month_key):
        raise AdminScheduleQueryError("invalid_schedule_id")
    year, month = (int(part) for part in month_key.split("-"))
    return f"{MONTH_NAMES[month - 1].capitalize()} {year}"


def current_moscow_month(now=None):
    current = now or datetime.now(MOSCOW_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=MOSCOW_TZ)
    else:
        current = current.astimezone(MOSCOW_TZ)
    return current.strftime("%Y-%m")


def parse_schedule_date(value, *, default, error):
    if value in (None, ""):
        return default
    try:
        parsed = date.fromisoformat(str(value))
    except (TypeError, ValueError):
        raise AdminScheduleQueryError(error) from None
    return parsed


def parse_schedule_limit(value):
    if value in (None, ""):
        return DEFAULT_SCHEDULE_LIMIT
    try:
        limit = int(value)
    except (TypeError, ValueError):
        raise AdminScheduleQueryError("invalid_limit") from None
    if limit < 1 or limit > MAX_SCHEDULE_LIMIT:
        raise AdminScheduleQueryError("invalid_limit")
    return limit


def validate_schedule_status(value):
    status = str(value or "all")
    if status not in SCHEDULE_STATUSES:
        raise AdminScheduleQueryError("invalid_status")
    return status


def schedule_window(from_value=None, to_value=None, now=None):
    current = now or datetime.now(MOSCOW_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=MOSCOW_TZ)
    else:
        current = current.astimezone(MOSCOW_TZ)
    today = current.date()
    start = parse_schedule_date(
        from_value, default=today - timedelta(days=7), error="invalid_from"
    )
    end = parse_schedule_date(
        to_value, default=today + timedelta(days=60), error="invalid_to"
    )
    if start > end:
        raise AdminScheduleQueryError("invalid_date_range")
    if (end - start).days > 730:
        raise AdminScheduleQueryError("date_range_too_large")
    return start, end


def encode_schedule_cursor(schedule_month):
    payload = json.dumps([str(schedule_month)], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_schedule_cursor(value):
    if not value:
        return None
    try:
        raw = str(value)
        decoded = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
        values = json.loads(decoded)
        if not isinstance(values, list) or len(values) != 1:
            raise ValueError
        month_key = values[0]
        if not isinstance(month_key, str) or not MONTH_PATTERN.fullmatch(month_key):
            raise ValueError
    except (TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
        raise AdminScheduleQueryError("invalid_cursor") from None
    return month_key


def _begin_read_only(cur):
    cur.execute("SET TRANSACTION READ ONLY")
    cur.execute(f"SET LOCAL statement_timeout = {SCHEDULE_STATEMENT_TIMEOUT_MS}")


def _schedule_projection(row, current_month):
    schedule_month, created_at, updated_at = row
    return {
        "schedule_id": schedule_month,
        "title": f"Расписание на {schedule_month_label(schedule_month).lower()}",
        "schedule_month": schedule_month,
        "period_label": schedule_month_label(schedule_month),
        "status": "upcoming" if schedule_month >= current_month else "past",
        "published": True,
        "source": "telegram_image",
        "has_image": True,
        "has_join_link": False,
        "created_at": _iso(created_at),
        "updated_at": _iso(updated_at),
    }


def list_admin_schedule(
    get_connection, *, from_value=None, to_value=None, status="all",
    limit=25, cursor=None, now=None,
):
    limit = parse_schedule_limit(limit)
    status = validate_schedule_status(status)
    start, end = schedule_window(from_value, to_value, now=now)
    current_month = current_moscow_month(now)
    cursor_month = decode_schedule_cursor(cursor)
    start_month = start.strftime("%Y-%m")
    end_month = end.strftime("%Y-%m")
    clauses = ["schedule_month >= %s", "schedule_month <= %s"]
    params = [start_month, end_month]
    if status == "upcoming":
        clauses.append("schedule_month >= %s")
        params.append(current_month)
    elif status == "past":
        clauses.append("schedule_month < %s")
        params.append(current_month)
    if cursor_month:
        clauses.append("schedule_month > %s")
        params.append(cursor_month)
    params.append(limit + 1)
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute(
            f"""
            SELECT schedule_month, created_at, updated_at
            FROM club_schedules
            WHERE {' AND '.join(clauses)}
            ORDER BY schedule_month ASC
            LIMIT %s
            """,
            tuple(params),
        )
        rows = cur.fetchall()
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE schedule_month = %s),
                COUNT(*) FILTER (WHERE schedule_month >= %s AND schedule_month <= %s),
                COUNT(*) FILTER (WHERE schedule_month >= %s)
            FROM club_schedules
            """,
            (
                current_month,
                current_month,
                _shift_month(current_month, 2),
                current_month,
            ),
        )
        summary = cur.fetchone()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    has_more = len(rows) > limit
    page = rows[:limit]
    return {
        "items": [_schedule_projection(row, current_month) for row in page],
        "next_cursor": encode_schedule_cursor(page[-1][0]) if has_more else None,
        "has_more": has_more,
        "window": {"from": start.isoformat(), "to": end.isoformat()},
        "timezone": "Europe/Moscow",
        "summary": {
            "current_month": int(summary[0] or 0),
            "next_three_months": int(summary[1] or 0),
            "total_future": int(summary[2] or 0),
        },
    }


def _shift_month(month_key, offset):
    year, month = (int(part) for part in month_key.split("-"))
    absolute = year * 12 + month - 1 + int(offset)
    shifted_year, shifted_month = divmod(absolute, 12)
    return f"{shifted_year:04d}-{shifted_month + 1:02d}"


def get_admin_schedule_details(get_connection, schedule_id, now=None):
    if not isinstance(schedule_id, str) or not MONTH_PATTERN.fullmatch(schedule_id):
        raise AdminScheduleQueryError("invalid_schedule_id")
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute(
            """
            SELECT schedule_month, created_at, updated_at
            FROM club_schedules WHERE schedule_month = %s
            """,
            (schedule_id,),
        )
        row = cur.fetchone()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    if row is None:
        return None
    result = _schedule_projection(row, current_moscow_month(now))
    result.update({
        "description": None,
        "category": None,
        "duration_minutes": None,
        "recurrence": None,
        "platform": "Telegram",
        "technical_information": "Изображение расписания загружено",
        "timezone": "Europe/Moscow",
    })
    return result


def get_admin_schedule_image_file_id(get_connection, schedule_id):
    if not isinstance(schedule_id, str) or not MONTH_PATTERN.fullmatch(schedule_id):
        raise AdminScheduleQueryError("invalid_schedule_id")
    conn = get_connection()
    cur = conn.cursor()
    try:
        _begin_read_only(cur)
        cur.execute(
            "SELECT telegram_file_id FROM club_schedules WHERE schedule_month = %s",
            (schedule_id,),
        )
        row = cur.fetchone()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    return row[0] if row else None
