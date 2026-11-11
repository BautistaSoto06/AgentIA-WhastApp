import dotenv from 'dotenv';

dotenv.config();

export const config = {
  port: parseInt(process.env.PORT || '3000', 10),
  nodeEnv: process.env.NODE_ENV || 'development',
  appName: process.env.APP_NAME || 'WhatsApp-Webhook-Handler',

  database: {
    url: process.env.DATABASE_URL || 'postgresql://user:password@localhost:5432/whatsapp_webhook',
    poolMin: parseInt(process.env.DATABASE_POOL_MIN || '2', 10),
    poolMax: parseInt(process.env.DATABASE_POOL_MAX || '10', 10),
  },

  redis: {
    url: process.env.REDIS_URL || 'redis://localhost:6379',
    db: parseInt(process.env.REDIS_DB || '0', 10),
  },

  logging: {
    level: process.env.LOG_LEVEL || 'info',
  },

  // Burst config uses getter to support tests that set env vars after module load
  get burst() {
    return {
      windowMs: parseInt(process.env.BURST_WINDOW_MS || '10000', 10),
      maxWaitMs: parseInt(process.env.BURST_MAX_WAIT_MS || '30000', 10),
    };
  },
};
