CREATE TABLE IF NOT EXISTS miniapp_member_sessions (
    session_id UUID PRIMARY KEY,
    token_hash TEXT NOT NULL UNIQUE,
    telegram_id BIGINT NOT NULL,
    first_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    CONSTRAINT miniapp_member_sessions_expiry_check CHECK (expires_at > created_at),
    CONSTRAINT miniapp_member_sessions_first_name_check CHECK (
        first_name IS NULL OR char_length(first_name) BETWEEN 1 AND 128
    )
);
CREATE INDEX IF NOT EXISTS miniapp_member_sessions_expires_at_idx ON miniapp_member_sessions(expires_at);
CREATE INDEX IF NOT EXISTS miniapp_member_sessions_telegram_id_idx ON miniapp_member_sessions(telegram_id);
