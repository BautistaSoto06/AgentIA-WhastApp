-- Create tables for message processing and deduplication

-- Messages table: stores all received messages for audit and processing
CREATE TABLE IF NOT EXISTS messages (
  id SERIAL PRIMARY KEY,
  message_id VARCHAR(255) NOT NULL UNIQUE,
  sender VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL,
  status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  webhook_received_at TIMESTAMP NOT NULL
);

-- Webhook requests table: audit log of all webhook calls
CREATE TABLE IF NOT EXISTS webhook_requests (
  id SERIAL PRIMARY KEY,
  message_id VARCHAR(255),
  request_body JSONB NOT NULL,
  signature VARCHAR(255),
  status_code INT,
  response_body JSONB,
  ip_address VARCHAR(45),
  user_agent TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (message_id) REFERENCES messages(message_id) ON DELETE SET NULL
);

-- Message deduplication table: fast lookup for duplicate detection with TTL
CREATE TABLE IF NOT EXISTS message_deduplication (
  id SERIAL PRIMARY KEY,
  message_id VARCHAR(255) NOT NULL UNIQUE,
  response_payload JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP NOT NULL
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id);
CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);
CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);
CREATE INDEX IF NOT EXISTS idx_webhook_requests_message_id ON webhook_requests(message_id);
CREATE INDEX IF NOT EXISTS idx_webhook_requests_created_at ON webhook_requests(created_at);
CREATE INDEX IF NOT EXISTS idx_message_dedup_message_id ON message_deduplication(message_id);
CREATE INDEX IF NOT EXISTS idx_message_dedup_expires_at ON message_deduplication(expires_at);

-- Create function to auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for updated_at (dropped first so the migration can be re-run)
DROP TRIGGER IF EXISTS update_messages_updated_at ON messages;
CREATE TRIGGER update_messages_updated_at
BEFORE UPDATE ON messages
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
