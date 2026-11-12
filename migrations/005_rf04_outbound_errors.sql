-- RF-04: Outbound WhatsApp message failures.
-- One row per failed send attempt (cause, attempt number, recipient).
-- Re-runnable: every statement is idempotent.

CREATE TABLE IF NOT EXISTS outbound_message_errors (
  id SERIAL PRIMARY KEY,
  recipient VARCHAR(255) NOT NULL,
  message_type VARCHAR(50) NOT NULL,
  attempt INT NOT NULL,
  retryable BOOLEAN NOT NULL,
  error_code VARCHAR(100),
  error_message TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_outbound_errors_recipient ON outbound_message_errors(recipient);
CREATE INDEX IF NOT EXISTS idx_outbound_errors_created_at ON outbound_message_errors(created_at);
