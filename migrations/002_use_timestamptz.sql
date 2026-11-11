-- Store all timestamps with time zone.
-- TIMESTAMP (without time zone) dropped the offset Node sends, so an app running
-- in UTC-3 against a UTC database saved expiry times 3 hours early and
-- dedup entries expired before their 24h TTL. Re-running this is a no-op.

ALTER TABLE messages
  ALTER COLUMN created_at TYPE TIMESTAMPTZ,
  ALTER COLUMN updated_at TYPE TIMESTAMPTZ,
  ALTER COLUMN webhook_received_at TYPE TIMESTAMPTZ;

ALTER TABLE webhook_requests
  ALTER COLUMN created_at TYPE TIMESTAMPTZ;

ALTER TABLE message_deduplication
  ALTER COLUMN created_at TYPE TIMESTAMPTZ,
  ALTER COLUMN expires_at TYPE TIMESTAMPTZ;
