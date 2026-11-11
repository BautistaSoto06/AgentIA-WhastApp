import { getPool } from '../database';
import logger from '../logger';

// Persist a received message and its webhook audit row.
// Returns false when the message row already existed (a redelivery that slipped
// past the idempotency check), so callers don't process it twice.
export async function storeMessage(
  messageId: string,
  sender: string,
  payload: any,
  webhookReceivedAt: Date
): Promise<boolean> {
  try {
    const pool = getPool();
    const result = await pool.query(
      `INSERT INTO messages (message_id, sender, payload, webhook_received_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (message_id) DO NOTHING`,
      [messageId, sender, JSON.stringify(payload), webhookReceivedAt]
    );

    // Store webhook request log
    await pool.query(
      `INSERT INTO webhook_requests (message_id, request_body)
       VALUES ($1, $2)`,
      [messageId, JSON.stringify(payload)]
    );

    return result.rowCount === 1;
  } catch (error) {
    logger.error('Error storing message', {
      error: error instanceof Error ? error.message : String(error),
      messageId,
    });
    throw error;
  }
}
