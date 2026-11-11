-- Add message_bursts table and burst_id column to messages

-- Create message_bursts table for tracking aggregated bursts
CREATE TABLE IF NOT EXISTS message_bursts (
  id SERIAL PRIMARY KEY,
  sender VARCHAR(255) NOT NULL,
  combined_text TEXT NOT NULL,
  message_ids TEXT[] NOT NULL,
  status VARCHAR(50) DEFAULT 'processing' CHECK (status IN ('processing', 'completed', 'failed')),
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  processed_at TIMESTAMPTZ
);

-- Add burst_id column to messages table if it doesn't exist
ALTER TABLE messages ADD COLUMN IF NOT EXISTS burst_id INT REFERENCES message_bursts(id) ON DELETE SET NULL;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_message_bursts_sender ON message_bursts(sender);
CREATE INDEX IF NOT EXISTS idx_message_bursts_status ON message_bursts(status);
CREATE INDEX IF NOT EXISTS idx_message_bursts_created_at ON message_bursts(created_at);
CREATE INDEX IF NOT EXISTS idx_messages_burst_id ON messages(burst_id);
