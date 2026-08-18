ALTER TABLE gift_access_grants
    ADD COLUMN IF NOT EXISTS certificate_name TEXT;
