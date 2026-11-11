import Queue from 'bull';
import { getPool } from '../database';
import { getRedisClient } from '../redis';
import logger from '../logger';
import { config } from '../config';

// RF-02: messages a sender writes in quick succession are buffered in Redis and
// processed together once the sender pauses for `burst.windowMs` (or once
// `burst.maxWaitMs` has passed since the burst's first message).
//
// Per sender we keep:
//   burst:<sender>       list of buffered messages, in arrival order
//   burst:seq:<sender>   monotonic counter, bumped on every message (never reset)
//   burst:time:<sender>  ms timestamp of the burst's first message
// Every message schedules a delayed job tagged with its seq and the burst start.
// When a job fires it only acts if it is still the latest message of the same
// burst (or the cap was reached), so older jobs become no-ops.

export interface BurstJobData {
  sender: string;
  seq: number;
  firstMessageTime: number;
}

export interface BurstMessage {
  id: string;
  from: string;
  timestamp: string;
  type: string;
  text?: string;
  mediaId?: string;
  payload: any;
}

const REDIS_BURST_PREFIX = 'burst:';
const REDIS_SEQ_PREFIX = 'burst:seq:';
const REDIS_TIME_PREFIX = 'burst:time:';
const SEQ_TTL_MS = 7 * 24 * 60 * 60 * 1000;

// KEYS: list, seq, time. ARGV: message json, now, burst key TTL ms, seq TTL ms.
// Returns { seq, firstMessageTime } for the message just added.
const ADD_SCRIPT = `
  local seq = redis.call('INCR', KEYS[2])
  redis.call('PEXPIRE', KEYS[2], ARGV[4])
  redis.call('SET', KEYS[3], ARGV[2], 'NX', 'PX', ARGV[3])
  redis.call('RPUSH', KEYS[1], ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  return { seq, redis.call('GET', KEYS[3]) }
`;

// KEYS: list, seq, time. ARGV: job seq, job burst start, cap reached (1/0).
// Returns the buffered messages and clears the burst, or nil when this job
// belongs to an already processed burst or a newer message is still pending.
const TAKE_SCRIPT = `
  local start = redis.call('GET', KEYS[3])
  if not start or start ~= ARGV[2] then
    return nil
  end
  local current = tonumber(redis.call('GET', KEYS[2]) or '0')
  if current ~= tonumber(ARGV[1]) and ARGV[3] ~= '1' then
    return nil
  end
  local messages = redis.call('LRANGE', KEYS[1], 0, -1)
  redis.call('DEL', KEYS[1], KEYS[3])
  return messages
`;

let burstQueue: Queue.Queue<BurstJobData>;

function burstKeys(sender: string): string[] {
  return [
    `${REDIS_BURST_PREFIX}${sender}`,
    `${REDIS_SEQ_PREFIX}${sender}`,
    `${REDIS_TIME_PREFIX}${sender}`,
  ];
}

export function initializeBurstQueue(redisUrl: string): Queue.Queue<BurstJobData> {
  if (burstQueue) {
    return burstQueue;
  }

  burstQueue = new Queue<BurstJobData>('bursts', redisUrl, {
    settings: {
      maxStalledCount: 2,
      lockRenewTime: 5000,
      lockDuration: 30000,
    },
  });

  burstQueue.on('error', (error) => {
    logger.error('Burst queue error', {
      error: error instanceof Error ? error.message : String(error),
    });
  });

  logger.info('Burst queue initialized');
  return burstQueue;
}

export function getBurstQueue(): Queue.Queue<BurstJobData> {
  if (!burstQueue) {
    throw new Error('Burst queue not initialized. Call initializeBurstQueue first.');
  }
  return burstQueue;
}

export async function addMessageToBurst(sender: string, message: BurstMessage): Promise<void> {
  const { windowMs, maxWaitMs } = config.burst;

  try {
    const now = Date.now();
    // Keep the buffer around well past the cap so a slow worker still finds it
    const burstTtlMs = maxWaitMs + windowMs + 60000;

    const [seq, start] = (await getRedisClient().eval(ADD_SCRIPT, {
      keys: burstKeys(sender),
      arguments: [JSON.stringify(message), String(now), String(burstTtlMs), String(SEQ_TTL_MS)],
    })) as [number, string];

    const firstMessageTime = parseInt(start, 10);
    const delayMs = Math.max(0, Math.min(windowMs, firstMessageTime + maxWaitMs - now));

    await getBurstQueue().add(
      { sender, seq, firstMessageTime },
      {
        delay: delayMs,
        // Bull silently ignores an add whose jobId already exists, so include the
        // burst start: ids stay unique even if the seq counter is ever reset
        jobId: `burst-${sender}-${firstMessageTime}-${seq}`,
        attempts: 1,
        removeOnComplete: true,
        removeOnFail: true,
      }
    );

    logger.debug('Message added to burst', {
      sender,
      seq,
      messageId: message.id,
      delayMs,
    });
  } catch (error) {
    logger.error('Error adding message to burst', {
      error: error instanceof Error ? error.message : String(error),
      sender,
      messageId: message.id,
    });
    throw error;
  }
}

export function combineBurstText(messages: BurstMessage[]): string {
  return messages
    .map((m) => (m.type === 'text' && m.text ? m.text : `[${m.type}${m.mediaId ? `:${m.mediaId}` : ''}]`))
    .join('\n');
}

// Returns the burst id it processed, or null when the job was a no-op.
export async function processBurst(job: BurstJobData): Promise<number | null> {
  const { sender, seq, firstMessageTime } = job;
  const capReached = Date.now() - firstMessageTime >= config.burst.maxWaitMs;

  const raw = (await getRedisClient().eval(TAKE_SCRIPT, {
    keys: burstKeys(sender),
    arguments: [String(seq), String(firstMessageTime), capReached ? '1' : '0'],
  })) as string[] | null;

  if (!raw || raw.length === 0) {
    logger.debug('Burst job superseded, skipping', { sender, seq });
    return null;
  }

  const messages: BurstMessage[] = raw.map((m) => JSON.parse(m));
  const messageIds = messages.map((m) => m.id);
  const combinedText = combineBurstText(messages);
  const pool = getPool();

  // Record the burst before doing any work so a failure leaves a trace
  const burstResult = await pool.query(
    `INSERT INTO message_bursts (sender, combined_text, message_ids, status, created_at)
     VALUES ($1, $2, $3, 'processing', $4)
     RETURNING id`,
    [sender, combinedText, messageIds, new Date(firstMessageTime)]
  );
  const burstId: number = burstResult.rows[0].id;

  logger.info('Processing burst', {
    sender,
    burstId,
    messageCount: messages.length,
    waitedMs: Date.now() - firstMessageTime,
  });

  try {
    await pool.query(
      `UPDATE messages SET burst_id = $1, status = 'processing' WHERE message_id = ANY($2)`,
      [burstId, messageIds]
    );

    // Simulate AI processing (placeholder): one response for the whole burst
    await new Promise((resolve) => setTimeout(resolve, 1000));

    await pool.query(`UPDATE messages SET status = 'completed' WHERE burst_id = $1`, [burstId]);

    // Store webhook response on the latest request of each message
    await pool.query(
      `UPDATE webhook_requests wr SET status_code = $1, response_body = $2
       FROM (
         SELECT DISTINCT ON (message_id) id FROM webhook_requests
         WHERE message_id = ANY($3)
         ORDER BY message_id, created_at DESC, id DESC
       ) latest
       WHERE wr.id = latest.id`,
      [200, JSON.stringify({ processed: true, burstId }), messageIds]
    );

    await pool.query(
      `UPDATE message_bursts SET status = 'completed', processed_at = CURRENT_TIMESTAMP WHERE id = $1`,
      [burstId]
    );

    logger.info('Burst processing completed', { sender, burstId, messageCount: messages.length });
    return burstId;
  } catch (error) {
    logger.error('Burst processing failed', {
      error: error instanceof Error ? error.message : String(error),
      sender,
      burstId,
    });

    await pool.query(
      `UPDATE message_bursts SET status = 'failed', processed_at = CURRENT_TIMESTAMP WHERE id = $1`,
      [burstId]
    );
    await pool.query(`UPDATE messages SET status = 'failed' WHERE burst_id = $1`, [burstId]);
    throw error;
  }
}

export async function closeBurstQueue(): Promise<void> {
  if (burstQueue) {
    await burstQueue.close();
    logger.info('Burst queue closed');
  }
}
