import { Pool, PoolConfig } from 'pg';
import { config } from './config';
import logger from './logger';

let pool: Pool;

export async function initializeDatabase(): Promise<void> {
  const poolConfig: PoolConfig = {
    connectionString: config.database.url,
    min: config.database.poolMin,
    max: config.database.poolMax,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
  };

  pool = new Pool(poolConfig);

  pool.on('error', (err: Error) => {
    logger.error('Unexpected error on idle client', { error: err.message });
  });

  try {
    const client = await pool.connect();
    logger.info('Connected to PostgreSQL database');
    client.release();
  } catch (error) {
    logger.error('Failed to connect to PostgreSQL database', {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

export function getPool(): Pool {
  if (!pool) {
    throw new Error('Database pool not initialized. Call initializeDatabase first.');
  }
  return pool;
}

export async function closeDatabase(): Promise<void> {
  if (pool) {
    await pool.end();
    logger.info('Database connection pool closed');
  }
}
