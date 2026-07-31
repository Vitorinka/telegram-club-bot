CREATE TABLE IF NOT EXISTS aiogram_fsm_states (
    bot_id BIGINT NOT NULL,
    chat_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    thread_id BIGINT NOT NULL DEFAULT 0,
    business_connection_id TEXT NOT NULL DEFAULT '',
    destiny TEXT NOT NULL DEFAULT 'default',
    state TEXT,
    data_json TEXT NOT NULL DEFAULT '{}',
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (bot_id, chat_id, user_id, thread_id, business_connection_id, destiny)
);

ALTER TABLE aiogram_fsm_states ADD COLUMN IF NOT EXISTS bot_id BIGINT;
ALTER TABLE aiogram_fsm_states ADD COLUMN IF NOT EXISTS chat_id BIGINT;
ALTER TABLE aiogram_fsm_states ADD COLUMN IF NOT EXISTS user_id BIGINT;
ALTER TABLE aiogram_fsm_states ADD COLUMN IF NOT EXISTS thread_id BIGINT NOT NULL DEFAULT 0;
ALTER TABLE aiogram_fsm_states ADD COLUMN IF NOT EXISTS business_connection_id TEXT NOT NULL DEFAULT '';
ALTER TABLE aiogram_fsm_states ADD COLUMN IF NOT EXISTS destiny TEXT NOT NULL DEFAULT 'default';
ALTER TABLE aiogram_fsm_states ADD COLUMN IF NOT EXISTS state TEXT;
ALTER TABLE aiogram_fsm_states ADD COLUMN IF NOT EXISTS data_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE aiogram_fsm_states ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

CREATE INDEX IF NOT EXISTS aiogram_fsm_states_updated_at_idx
ON aiogram_fsm_states (updated_at);
