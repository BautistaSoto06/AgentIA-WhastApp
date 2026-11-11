import Queue from 'bull';
import dotenv from 'dotenv';
import logger from '../logger';
import { initializeDatabase, closeDatabase } from '../database';
import { initializeRedis, closeRedis } from '../redis';
import { BurstJobData, initializeBurstQueue, closeBurstQueue, processBurst } from '../services/burstAggregator';

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

    // Process jobs with concurrency of 5. Bull's process() promise only settles
    // when the queue closes, so it is not awaited.
    queue.process(5, (job) => processBurst(job.data)).catch((error) => {
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
