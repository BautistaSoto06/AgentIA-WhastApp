import { createClient, RedisClientType } from 'redis';
import { config } from './config';
import logger from './logger';

let redisClient: RedisClientType;

export async function initializeRedis(): Promise<void> {
  redisClient = createClient({
    url: config.redis.url,
    socket: {
      reconnectStrategy: (retries) => {
        const delay = Math.min(retries * 50, 500);
        return delay;
      },
    },
  });

  if (config.redis.db > 0) {
    redisClient.on('ready', async () => {
      await redisClient.select(config.redis.db);
    });
  }

  redisClient.on('error', (err) => {
    logger.error('Redis client error', { error: err.message });
  });

  redisClient.on('connect', () => {
    logger.info('Connected to Redis');
  });

  try {
    await redisClient.connect();
    logger.info('Redis client initialized');
  } catch (error) {
    logger.error('Failed to connect to Redis', {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

export function getRedisClient(): RedisClientType {
  if (!redisClient) {
    throw new Error('Redis client not initialized. Call initializeRedis first.');
  }
  return redisClient;
}

export async function closeRedis(): Promise<void> {
  if (redisClient) {
    await redisClient.quit();
    logger.info('Redis client closed');
  }
}
