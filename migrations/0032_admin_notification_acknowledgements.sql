CREATE TABLE IF NOT EXISTS admin_notification_acknowledgements (
    admin_telegram_id BIGINT NOT NULL,
    notification_key TEXT NOT NULL,
    read_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (admin_telegram_id, notification_key),
    CONSTRAINT admin_notification_acknowledgements_key_length_check
        CHECK (char_length(notification_key) BETWEEN 1 AND 200)
);

CREATE INDEX IF NOT EXISTS admin_notification_acknowledgements_read_at_idx
ON admin_notification_acknowledgements (read_at);
