import Queue from 'bull';
import dotenv from 'dotenv';
import logger from '../logger';
import { initializeDatabase, closeDatabase } from '../database';
import { initializeRedis, closeRedis } from '../redis';
import { BurstJobData, initializeBurstQueue, closeBurstQueue, processBurst, getBurstQueue, LockContentionError } from '../services/burstAggregator';

dotenv.config();

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

let queue: Queue.Queue<BurstJobData>;
let isShuttingDown = false;

async function initializeWorker(): Promise<void> {
  try {
    logger.info('Initializing burst worker');

    await initializeDatabase();
    await initializeRedis();

    queue = initializeBurstQueue(REDIS_URL);

    queue.on('completed', (job, burstId) => {
      if (burstId !== null) {
        logger.info('Burst job completed', { jobId: job.id, sender: job.data.sender, burstId });
      }
    });

    queue.on('failed', (job, error) => {
      logger.error('Burst job failed', {
        jobId: job.id,
        sender: job.data.sender,
        error: error instanceof Error ? error.message : String(error),
      });
    });

    // Process jobs with higher concurrency since ordering is enforced per-sender.
    // RF-03: Each sender is serialized, different senders process in parallel.
    // If a job can't acquire the lock (sender is busy), it throws LockContentionError
    // and is re-queued with exponential backoff.
    queue.process(20, async (job) => {
      try {
        const result = await processBurst(job.data);
        return result;
      } catch (error) {
        // Handle lock contention: re-queue with backoff
        if (error instanceof LockContentionError) {
          const attemptNumber = (job.data.lockAttempts || 0) + 1;

          if (attemptNumber <= 3) {
            // Exponential backoff: 100ms, 200ms, 400ms
            const delayMs = 100 * Math.pow(2, attemptNumber - 1);

            await getBurstQueue().add(
              { ...job.data, lockAttempts: attemptNumber },
              {
                delay: delayMs,
                jobId: `${job.id}-retry-${attemptNumber}`,
                attempts: 1,
                removeOnComplete: true,
                removeOnFail: true,
              }
            );

            logger.debug('Job re-queued due to lock contention', {
              jobId: job.id,
              sender: job.data.sender,
              delayMs,
              attempt: attemptNumber,
            });

            // Return success to not fail the original job
            return null;
          } else {
            logger.warn('Max lock contention retries exceeded', {
              jobId: job.id,
              sender: job.data.sender,
              attempts: attemptNumber,
            });
            throw error;
          }
        }

        // Other errors should fail the job
        throw error;
      }
    }).catch((error) => {
      logger.error('Burst worker stopped', {
        error: error instanceof Error ? error.message : String(error),
      });
    });
    await queue.isReady();

    // Written to stdout directly so supervisors (and tests) can detect readiness at any log level
    process.stdout.write('BURST_WORKER_READY\n');
    logger.info('Burst worker initialized and listening for jobs');
  } catch (error) {
    logger.error('Failed to initialize burst worker', {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }
}

async function gracefulShutdown(signal: string): Promise<void> {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;

  logger.info(`Received ${signal}, starting graceful shutdown`);

  try {
    await closeBurstQueue();
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
