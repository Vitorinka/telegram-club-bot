CREATE TABLE IF NOT EXISTS miniapp_admin_sessions (
    session_id UUID PRIMARY KEY,
    token_hash TEXT NOT NULL UNIQUE,
    telegram_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ NULL,
    CONSTRAINT miniapp_admin_sessions_expiry_check
        CHECK (expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS miniapp_admin_sessions_expires_at_idx
    ON miniapp_admin_sessions (expires_at);

CREATE INDEX IF NOT EXISTS miniapp_admin_sessions_telegram_id_idx
    ON miniapp_admin_sessions (telegram_id);
