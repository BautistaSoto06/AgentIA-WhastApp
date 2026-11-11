import Queue from 'bull';
import dotenv from 'dotenv';
import logger from '../logger';
import { initializeDatabase, closeDatabase, getPool } from '../database';
import { initializeRedis, closeRedis } from '../redis';
import { MessageJobData } from '../services/messageEnqueuer';

dotenv.config();

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

let queue: Queue.Queue<MessageJobData>;
let isShuttingDown = false;

async function initializeWorker(): Promise<void> {
  try {
    logger.info('Initializing queue worker');

    await initializeDatabase();
    await initializeRedis();

    queue = new Queue<MessageJobData>('messages', REDIS_URL, {
      settings: {
        maxStalledCount: 2,
        lockRenewTime: 5000,
        lockDuration: 30000,
      },
    });

    // Process jobs with concurrency of 5
    await queue.process(5, async (job) => {
      return await processMessage(job);
    });

    queue.on('completed', (job) => {
      logger.info('Job completed', {
        jobId: job.id,
        messageId: job.data.messageId,
      });
    });

    queue.on('failed', (job, error) => {
      logger.error('Job failed', {
        jobId: job.id,
        messageId: job.data.messageId,
        error: error instanceof Error ? error.message : String(error),
        attempts: job.attemptsMade,
      });
    });

    logger.info('Queue worker initialized and listening for jobs');
  } catch (error) {
    logger.error('Failed to initialize queue worker', {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }
}

async function processMessage(job: Queue.Job<MessageJobData>): Promise<any> {
  const { messageId, payload, sender } = job.data;

  try {
    logger.info('Processing message job', {
      jobId: job.id,
      messageId,
      sender,
    });

    // Update message status to processing
    const pool = getPool();
    await pool.query(
      'UPDATE messages SET status = $1 WHERE message_id = $2',
      ['processing', messageId]
    );

    // Simulate AI processing (placeholder)
    // In a real scenario, this would call an AI service
    await new Promise((resolve) => setTimeout(resolve, 1000));

    logger.info('Message processing completed', {
      jobId: job.id,
      messageId,
      payload,
    });

    // Update message status to completed
    await pool.query(
      'UPDATE messages SET status = $1 WHERE message_id = $2',
      ['completed', messageId]
    );

    // Store webhook response on the latest request for this message
    // (PostgreSQL UPDATE has no ORDER BY/LIMIT, so pick the row in a subquery)
    await pool.query(
      `UPDATE webhook_requests SET status_code = $1, response_body = $2
       WHERE id = (
         SELECT id FROM webhook_requests
         WHERE message_id = $3
         ORDER BY created_at DESC, id DESC
         LIMIT 1
       )`,
      [200, JSON.stringify({ processed: true }), messageId]
    );

    return { success: true, messageId };
  } catch (error) {
    logger.error('Error processing message', {
      jobId: job.id,
      messageId,
      error: error instanceof Error ? error.message : String(error),
    });

    // Update message status to failed
    try {
      const pool = getPool();
      await pool.query(
        'UPDATE messages SET status = $1 WHERE message_id = $2',
        ['failed', messageId]
      );
    } catch (updateError) {
      logger.error('Failed to update message status', {
        error: updateError instanceof Error ? updateError.message : String(updateError),
      });
    }

    throw error;
  }
}

async function gracefulShutdown(signal: string): Promise<void> {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;

  logger.info(`Received ${signal}, starting graceful shutdown`);

  try {
    if (queue) {
      await queue.close();
      logger.info('Queue closed');
    }

    await closeDatabase();
    await closeRedis();

    logger.info('Graceful shutdown completed');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown', {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception', {
    error: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack : undefined,
  });
  process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled Rejection', {
    reason: reason instanceof Error ? reason.message : String(reason),
  });
  process.exit(1);
});

initializeWorker().catch((error) => {
  logger.error('Fatal error', {
    error: error instanceof Error ? error.message : String(error),
  });
  process.exit(1);
});
