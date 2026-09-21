ALTER TABLE admin_notification_acknowledgements
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS admin_notification_acknowledgements_resolved_idx
ON admin_notification_acknowledgements (admin_telegram_id, resolved_at)
WHERE resolved_at IS NOT NULL AND archived_at IS NULL;

CREATE INDEX IF NOT EXISTS admin_notification_acknowledgements_archived_idx
ON admin_notification_acknowledgements (admin_telegram_id, archived_at)
WHERE archived_at IS NOT NULL;
