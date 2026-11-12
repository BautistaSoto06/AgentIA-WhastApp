import Queue from 'bull';
import { getPool } from '../database';
import { getRedisClient } from '../redis';
import logger from '../logger';
import { config } from '../config';
import { getWhatsAppSender, isOutboundConfigured, OutboundMessage, SendOptions } from './whatsappSender';

// RF-03: Ensure per-client ordering while allowing parallel processing of different clients.
//
// Per sender we keep:
//   burst:<sender>       list of buffered messages, in arrival order
//   burst:seq:<sender>   monotonic counter, bumped on every message (never reset)
//   burst:time:<sender>  ms timestamp of the burst's first message
//   burst:lock:<sender>  lock held while a burst is processing (with TTL)
//   burst:queue:<sender> queue of burst job ids waiting for this sender
//
// Every message schedules a delayed job tagged with its seq and the burst start.
// When a job fires, TAKE_SCRIPT acquires the lock if available, ensuring only one
// burst per sender is processed at a time. If lock is held, the job is re-queued.
// Different senders process in parallel up to the global worker concurrency limit.

export interface BurstJobData {
  sender: string;
  seq: number;
  firstMessageTime: number;
  lockToken?: string; // Token for per-sender locking
  lockAttempts?: number; // Count of lock contention retries
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

// Exception thrown when a burst can't acquire its sender's lock
export class LockContentionError extends Error {
  constructor(sender: string, attemptNumber: number) {
    super(`Lock contention for sender ${sender}, attempt ${attemptNumber}`);
    this.name = 'LockContentionError';
  }
}

const REDIS_BURST_PREFIX = 'burst:';
const REDIS_SEQ_PREFIX = 'burst:seq:';
const REDIS_TIME_PREFIX = 'burst:time:';
const REDIS_LOCK_PREFIX = 'burst:lock:';
const SEQ_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const LOCK_TTL_MS = 120000; // 2 minutes lock timeout (failsafe)

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

// KEYS: list, seq, time, lock. ARGV: job seq, job burst start, cap reached (1/0), lock TTL ms, lock token.
// Returns the buffered messages if lock acquired and burst is ready, or nil when:
// - burst already processed (start mismatch)
// - newer message pending (unless cap reached)
// - lock held by another job
const TAKE_SCRIPT = `
  local start = redis.call('GET', KEYS[3])
  if not start or start ~= ARGV[2] then
    return nil
  end
  local current = tonumber(redis.call('GET', KEYS[2]) or '0')
  if current ~= tonumber(ARGV[1]) and ARGV[3] ~= '1' then
    return nil
  end
  if redis.call('GET', KEYS[4]) then
    return nil
  end
  redis.call('SET', KEYS[4], ARGV[5], 'PX', ARGV[4])
  local messages = redis.call('LRANGE', KEYS[1], 0, -1)
  redis.call('DEL', KEYS[1], KEYS[3])
  return messages
`;

// KEYS: lock. ARGV: lock token.
// Release lock only if it matches our token (prevent releasing other's lock).
const RELEASE_LOCK_SCRIPT = `
  if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
  end
  return 0
`;

let burstQueue: Queue.Queue<BurstJobData>;

// RF-04: produces the reply for a processed burst (the AI step is still a placeholder).
export type ReplyGenerator = (
  sender: string,
  combinedText: string,
  messages: BurstMessage[]
) => Promise<{ message: OutboundMessage; options?: SendOptions } | null>;

let replyGenerator: ReplyGenerator | undefined;

export function setReplyGenerator(fn: ReplyGenerator | undefined): void {
  replyGenerator = fn;
}

function burstKeys(sender: string): string[] {
  return [
    `${REDIS_BURST_PREFIX}${sender}`,
    `${REDIS_SEQ_PREFIX}${sender}`,
    `${REDIS_TIME_PREFIX}${sender}`,
    `${REDIS_LOCK_PREFIX}${sender}`,
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

    // Generate lock token for this job to track which job holds the sender lock
    const lockToken = `${sender}-${firstMessageTime}-${seq}-${Math.random().toString(36)}`;

    await getBurstQueue().add(
      { sender, seq, firstMessageTime, lockToken },
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
      lockToken,
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

// Returns the burst id it processed, or null when the job was a no-op (superseded).
// Throws LockContentionError when lock is held by another job.
export async function processBurst(job: BurstJobData): Promise<number | null> {
  const { sender, seq, firstMessageTime, lockToken = '' } = job;
  const capReached = Date.now() - firstMessageTime >= config.burst.maxWaitMs;

  const raw = (await getRedisClient().eval(TAKE_SCRIPT, {
    keys: burstKeys(sender),
    arguments: [String(seq), String(firstMessageTime), capReached ? '1' : '0', String(LOCK_TTL_MS), lockToken],
  })) as string[] | null;

  if (!raw) {
    // Lock is held: throw for retry
    logger.debug('Lock contention, will retry', { sender, seq, lockToken });
    throw new LockContentionError(sender, 0);
  }

  if (raw.length === 0) {
    // Burst was superseded
    logger.debug('Burst job superseded, skipping', { sender, seq, lockToken });
    return null;
  }

  const messages: BurstMessage[] = raw.map((m) => JSON.parse(m));
  const messageIds = messages.map((m) => m.id);
  const combinedText = combineBurstText(messages);
  const pool = getPool();
  const lockKey = `${REDIS_LOCK_PREFIX}${sender}`;

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
    lockToken,
  });

  try {
    await pool.query(
      `UPDATE messages SET burst_id = $1, status = 'processing' WHERE message_id = ANY($2)`,
      [burstId, messageIds]
    );

    // Simulate AI processing (placeholder): one response for the whole burst
    await new Promise((resolve) => setTimeout(resolve, 1000));

    await pool.query(`UPDATE messages SET status = 'completed' WHERE burst_id = $1`, [burstId]);

    // RF-04: deliver the reply (if a generator is registered and outbound is configured).
    // A send failure is already recorded by the sender and must not fail the burst.
    if (replyGenerator && isOutboundConfigured()) {
      try {
        const reply = await replyGenerator(sender, combinedText, messages);
        if (reply) await getWhatsAppSender().send(sender, reply.message, reply.options);
      } catch (sendErr) {
        logger.warn('Reply delivery failed', {
          sender,
          burstId,
          error: sendErr instanceof Error ? sendErr.message : String(sendErr),
        });
      }
    }

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
  } finally {
    // Always release lock, even on error
    try {
      await getRedisClient().eval(RELEASE_LOCK_SCRIPT, {
        keys: [lockKey],
        arguments: [lockToken],
      });
      logger.debug('Lock released', { sender, lockToken });
    } catch (err) {
      logger.error('Failed to release lock', {
        error: err instanceof Error ? err.message : String(err),
        sender,
        lockToken,
      });
    }
  }
}

export async function closeBurstQueue(): Promise<void> {
  if (burstQueue) {
    await burstQueue.close();
    logger.info('Burst queue closed');
  }
}
