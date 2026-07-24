from datetime import datetime, timedelta


AUTHORIZED_GROUP_STATUSES = {"member", "administrator", "creator"}


def invite_link_options(source, telegram_id, now=None):
    now = now or datetime.utcnow()
    return {
        "name": f"{source}_{telegram_id}",
        "expire_date": now + timedelta(hours=24),
        "member_limit": 1,
    }


def is_group_admin_member(status):
    return status in ("administrator", "creator")


def group_join_decision(user_id, is_bot, is_admin, access_active, db_error=False):
    if is_bot:
        return "preserve_bot"
    if is_admin:
        return "preserve_admin"
    if db_error:
        return "preserve_db_error"
    if access_active:
        return "authorized"
    return "remove_unauthorized"


def save_bot_invite_link(cur, invite_link, source, telegram_id=None, expires_at=None):
    cur.execute(
        """
        INSERT INTO bot_invite_links (
            invite_link, source, telegram_id, status, expires_at
        )
        VALUES (%s, %s, %s, 'active', %s)
        ON CONFLICT (invite_link) DO UPDATE SET
            source = EXCLUDED.source,
            telegram_id = EXCLUDED.telegram_id,
            status = 'active',
            expires_at = EXCLUDED.expires_at,
            revoked_at = NULL
        """,
        (invite_link, source, telegram_id, expires_at),
    )


def load_active_bot_invite_links(cur, limit=100):
    cur.execute(
        """
        SELECT invite_link
        FROM bot_invite_links
        WHERE status = 'active'
          AND (expires_at IS NULL OR expires_at > NOW())
        ORDER BY created_at ASC
        LIMIT %s
        """,
        (int(limit),),
    )
    return [row[0] for row in cur.fetchall()]


def mark_bot_invite_link_revoked(cur, invite_link):
    cur.execute(
        """
        UPDATE bot_invite_links
        SET status = 'revoked',
            revoked_at = NOW()
        WHERE invite_link = %s
        """,
        (invite_link,),
    )
