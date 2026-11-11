import Queue from 'bull';
import { getPool } from '../database';
import logger from '../logger';

export interface MessageJobData {
  messageId: string;
  payload: any;
  sender: string;
  timestamp: string;
  webhookReceivedAt: string;
}

let messageQueue: Queue.Queue<MessageJobData>;

export function initializeMessageQueue(redisUrl: string): Queue.Queue<MessageJobData> {
  if (messageQueue) {
    return messageQueue;
  }

  messageQueue = new Queue<MessageJobData>('messages', redisUrl, {
    settings: {
      maxStalledCount: 2,
      lockRenewTime: 5000,
      lockDuration: 30000,
    },
  });

  messageQueue.on('error', (error) => {
    logger.error('Bull queue error', {
      error: error instanceof Error ? error.message : String(error),
    });
  });

  messageQueue.on('failed', (job, error) => {
    logger.error('Job failed', {
      jobId: job.id,
      error: error instanceof Error ? error.message : String(error),
    });
  });

  logger.info('Message queue initialized');
  return messageQueue;
}

export function getMessageQueue(): Queue.Queue<MessageJobData> {
  if (!messageQueue) {
    throw new Error('Message queue not initialized. Call initializeMessageQueue first.');
  }
  return messageQueue;
}

export async function enqueueMessage(data: MessageJobData): Promise<Queue.Job<MessageJobData>> {
  try {
    const queue = getMessageQueue();
    const job = await queue.add(data, {
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 2000,
      },
      removeOnComplete: true,
    });

    // Store message metadata in database
    const pool = getPool();
    await pool.query(
      `INSERT INTO messages (message_id, sender, payload, webhook_received_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (message_id) DO NOTHING`,
      [
        data.messageId,
        data.sender,
        JSON.stringify(data.payload),
        new Date(data.webhookReceivedAt),
      ]
    );

    // Store webhook request log
    await pool.query(
      `INSERT INTO webhook_requests (message_id, request_body)
       VALUES ($1, $2)`,
      [data.messageId, JSON.stringify(data.payload)]
    );

    logger.info('Message enqueued successfully', {
      messageId: data.messageId,
      jobId: job.id,
      sender: data.sender,
    });

    return job;
  } catch (error) {
    logger.error('Error enqueuing message', {
      error: error instanceof Error ? error.message : String(error),
      messageId: data.messageId,
    });
    throw error;
  }
}

export async function closeMessageQueue(): Promise<void> {
  if (messageQueue) {
    await messageQueue.close();
    logger.info('Message queue closed');
  }
}
