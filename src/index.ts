import { Server } from 'http';
import { config } from './config';
import logger from './logger';
import { initializeDatabase, closeDatabase } from './database';
import { initializeRedis, closeRedis } from './redis';
import { createApp } from './app';
import { initializeMessageQueue, closeMessageQueue } from './services/messageEnqueuer';

let server: Server;

async function start(): Promise<void> {
  try {
    logger.info(`Starting ${config.appName} on port ${config.port}`);

    await initializeDatabase();
    await initializeRedis();
    initializeMessageQueue(config.redis.url);

    const app = createApp();

    server = app.listen(config.port, () => {
      logger.info(`Server running on port ${config.port}`);
    });
  } catch (error) {
    logger.error('Failed to start server', {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }
}

async function gracefulShutdown(signal: string): Promise<void> {
  logger.info(`Received ${signal}, starting graceful shutdown`);

  if (server) {
    server.close(async () => {
      logger.info('HTTP server closed');

      try {
        await closeMessageQueue();
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
    });

    setTimeout(() => {
      logger.error('Forced shutdown after 30 seconds');
      process.exit(1);
    }, 30000);
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

start();
