import { getRedisClient } from '../redis';
import { getPool } from '../database';
import logger from '../logger';

const DEDUP_TTL_SECONDS = 24 * 60 * 60; // 24 hours
const REDIS_KEY_PREFIX = 'msg:';

interface DeduplicationResult {
  isDuplicate: boolean;
  response?: any;
}

export async function checkIdempotency(messageId: string): Promise<DeduplicationResult> {
  const startTime = Date.now();

  try {
    // Try Redis first for fast lookup
    try {
      const redisClient = getRedisClient();
      const cachedResponse = await redisClient.get(`${REDIS_KEY_PREFIX}${messageId}`);

      if (cachedResponse) {
        const duration = Date.now() - startTime;
        logger.debug('Idempotency check via Redis (duplicate)', {
          messageId,
          duration: `${duration}ms`,
        });
        return {
          isDuplicate: true,
          response: JSON.parse(cachedResponse),
        };
      }
    } catch (redisError) {
      logger.warn('Redis unavailable for idempotency check, falling back to PostgreSQL', {
        error: redisError instanceof Error ? redisError.message : String(redisError),
        messageId,
      });
    }

    // Fall back to PostgreSQL if Redis fails or no cache hit
    const pool = getPool();
    const result = await pool.query(
      'SELECT response_payload FROM message_deduplication WHERE message_id = $1 AND expires_at > NOW()',
      [messageId]
    );

    if (result.rows.length > 0) {
      const duration = Date.now() - startTime;
      logger.debug('Idempotency check via PostgreSQL (duplicate)', {
        messageId,
        duration: `${duration}ms`,
      });
      return {
        isDuplicate: true,
        response: result.rows[0].response_payload,
      };
    }

    const duration = Date.now() - startTime;
    logger.debug('Idempotency check passed (new message)', {
      messageId,
      duration: `${duration}ms`,
    });

    return { isDuplicate: false };
  } catch (error) {
    logger.error('Error checking idempotency', {
      error: error instanceof Error ? error.message : String(error),
      messageId,
    });
    throw error;
  }
}

export async function storeIdempotencyEntry(
  messageId: string,
  response: any
): Promise<void> {
  try {
    const expiresAt = new Date(Date.now() + DEDUP_TTL_SECONDS * 1000);
    const responseJson = JSON.stringify(response);

    // Store in PostgreSQL
    const pool = getPool();
    await pool.query(
      `INSERT INTO message_deduplication (message_id, response_payload, expires_at)
       VALUES ($1, $2, $3)
       ON CONFLICT (message_id) DO UPDATE SET
         response_payload = $2,
         expires_at = $3`,
      [messageId, responseJson, expiresAt]
    );

    // Try to cache in Redis as well
    try {
      const redisClient = getRedisClient();
      await redisClient.setEx(
        `${REDIS_KEY_PREFIX}${messageId}`,
        DEDUP_TTL_SECONDS,
        responseJson
      );
    } catch (redisError) {
      logger.warn('Failed to cache idempotency entry in Redis', {
        error: redisError instanceof Error ? redisError.message : String(redisError),
        messageId,
      });
      // Don't throw - PostgreSQL is the source of truth
    }

    logger.debug('Idempotency entry stored', {
      messageId,
      expiresAt: expiresAt.toISOString(),
    });
  } catch (error) {
    logger.error('Error storing idempotency entry', {
      error: error instanceof Error ? error.message : String(error),
      messageId,
    });
    throw error;
  }
}

export async function cleanupExpiredEntries(): Promise<number> {
  try {
    const pool = getPool();
    const result = await pool.query(
      'DELETE FROM message_deduplication WHERE expires_at <= NOW()'
    );

    logger.info('Cleaned up expired deduplication entries', {
      deletedCount: result.rowCount,
    });

    return result.rowCount || 0;
  } catch (error) {
    logger.error('Error cleaning up expired entries', {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}
