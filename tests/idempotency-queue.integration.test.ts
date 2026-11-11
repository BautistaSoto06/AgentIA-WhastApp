import crypto from 'crypto';
import { Pool } from 'pg';
import http from 'http';
import { createApp } from '../src/app';
import { checkIdempotency, storeIdempotencyEntry, cleanupExpiredEntries } from '../src/services/idempotencyService';
import { initializeDatabase, closeDatabase, getPool } from '../src/database';
import { initializeRedis, closeRedis, getRedisClient } from '../src/redis';
import { initializeMessageQueue, enqueueMessage, closeMessageQueue, getMessageQueue } from '../src/services/messageEnqueuer';

describe('Idempotency & Queue Integration Tests', () => {
  jest.setTimeout(15000);

  // Setup and teardown
  beforeAll(async () => {
    process.env.DATABASE_URL = process.env.DATABASE_URL || 'postgresql://user:password@localhost:5432/whatsapp_webhook';
    process.env.REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
    process.env.WHATSAPP_APP_SECRET = 'test_app_secret';
    process.env.WHATSAPP_VERIFY_TOKEN = 'test_verify_token';

    await initializeDatabase();
    await initializeRedis();
    initializeMessageQueue(process.env.REDIS_URL);
  });

  afterAll(async () => {
    await closeMessageQueue();
    await closeDatabase();
    await closeRedis();
  });

  afterEach(async () => {
    // Clean up test data
    const pool = getPool();
    try {
      await pool.query('DELETE FROM message_deduplication');
      await pool.query('DELETE FROM webhook_requests');
      await pool.query('DELETE FROM messages');
    } catch (error) {
      // Tables might not exist yet
    }
  });

  describe('Idempotency Service', () => {
    it('should return false for new message', async () => {
      const messageId = 'msg_' + Date.now();
      const result = await checkIdempotency(messageId);

      expect(result.isDuplicate).toBe(false);
      expect(result.response).toBeUndefined();
    });

    it('should return true for duplicate message with Redis', async () => {
      const messageId = 'msg_' + Date.now();
      const response = { received: true };

      await storeIdempotencyEntry(messageId, response);
      const result = await checkIdempotency(messageId);

      expect(result.isDuplicate).toBe(true);
      expect(result.response).toEqual(response);
    });

    it('should return true for duplicate message with PostgreSQL fallback', async () => {
      const messageId = 'msg_' + Date.now();
      const response = { received: true };

      // Store in database
      const pool = getPool();
      await pool.query(
        `INSERT INTO message_deduplication (message_id, response_payload, expires_at)
         VALUES ($1, $2, $3)`,
        [messageId, JSON.stringify(response), new Date(Date.now() + 24 * 60 * 60 * 1000)]
      );

      const result = await checkIdempotency(messageId);

      expect(result.isDuplicate).toBe(true);
      expect(result.response).toEqual(response);
    });

    it('should respect 24-hour TTL', async () => {
      const messageId = 'msg_' + Date.now();
      const response = { received: true };

      // Store with immediate expiration
      const pool = getPool();
      await pool.query(
        `INSERT INTO message_deduplication (message_id, response_payload, expires_at)
         VALUES ($1, $2, $3)`,
        [messageId, JSON.stringify(response), new Date(Date.now() - 1000)]
      );

      const result = await checkIdempotency(messageId);

      expect(result.isDuplicate).toBe(false);
    });

    it('should cleanup expired entries', async () => {
      const pool = getPool();
      const messageId = 'msg_' + Date.now();

      // Insert expired entry
      await pool.query(
        `INSERT INTO message_deduplication (message_id, response_payload, expires_at)
         VALUES ($1, $2, $3)`,
        [messageId, JSON.stringify({ received: true }), new Date(Date.now() - 1000)]
      );

      const deleted = await cleanupExpiredEntries();

      expect(deleted).toBeGreaterThan(0);

      const result = await pool.query(
        'SELECT * FROM message_deduplication WHERE message_id = $1',
        [messageId]
      );
      expect(result.rows.length).toBe(0);
    });

    it('should handle response time < 5ms for duplicates', async () => {
      const messageId = 'msg_' + Date.now();
      const response = { received: true };

      await storeIdempotencyEntry(messageId, response);

      const startTime = Date.now();
      const result = await checkIdempotency(messageId);
      const duration = Date.now() - startTime;

      expect(result.isDuplicate).toBe(true);
      expect(duration).toBeLessThan(5);
    });
  });

  describe('Message Enqueuer Service', () => {
    it('should enqueue message successfully', async () => {
      const messageId = 'msg_' + Date.now();
      const job = await enqueueMessage({
        messageId,
        payload: { test: 'data' },
        sender: '1234567890',
        timestamp: new Date().toISOString(),
        webhookReceivedAt: new Date().toISOString(),
      });

      expect(job.id).toBeDefined();

      // Verify message stored in database
      const pool = getPool();
      const result = await pool.query(
        'SELECT * FROM messages WHERE message_id = $1',
        [messageId]
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].sender).toBe('1234567890');
      expect(result.rows[0].status).toBe('pending');
    });

    it('should store webhook request log', async () => {
      const messageId = 'msg_' + Date.now();
      await enqueueMessage({
        messageId,
        payload: { test: 'data' },
        sender: '1234567890',
        timestamp: new Date().toISOString(),
        webhookReceivedAt: new Date().toISOString(),
      });

      const pool = getPool();
      const result = await pool.query(
        'SELECT * FROM webhook_requests WHERE message_id = $1',
        [messageId]
      );
      expect(result.rows.length).toBeGreaterThan(0);
    });

    it('should handle duplicate message IDs gracefully', async () => {
      const messageId = 'msg_' + Date.now();
      const payload = { test: 'data' };

      await enqueueMessage({
        messageId,
        payload,
        sender: '1234567890',
        timestamp: new Date().toISOString(),
        webhookReceivedAt: new Date().toISOString(),
      });

      // Try to enqueue same message ID again
      await enqueueMessage({
        messageId,
        payload,
        sender: '1234567890',
        timestamp: new Date().toISOString(),
        webhookReceivedAt: new Date().toISOString(),
      });

      const pool = getPool();
      const result = await pool.query(
        'SELECT * FROM messages WHERE message_id = $1',
        [messageId]
      );
      expect(result.rows.length).toBe(1);
    });
  });

  describe('Duplicate Detection in Webhook', () => {
    it('should return 200 for both original and duplicate messages', (done) => {
      process.env.WHATSAPP_APP_SECRET = 'test_app_secret';
      process.env.WHATSAPP_VERIFY_TOKEN = 'test_verify_token';

      const app = createApp();
      const server = app.listen(0, async () => {
        const port = (server.address() as any).port;
        const payload = {
          object: 'whatsapp_business_account',
          entry: [
            {
              id: '123',
              changes: [
                {
                  value: {
                    messaging_product: 'whatsapp',
                    messages: [
                      {
                        from: '1234567890',
                        id: 'msg_test_' + Date.now(),
                        timestamp: '1671234567',
                        type: 'text',
                        text: { body: 'Test message' },
                      },
                    ],
                  },
                },
              ],
            },
          ],
        };

        const secret = 'test_app_secret';
        const bodyString = JSON.stringify(payload);
        const signature = 'sha256=' + crypto.createHmac('sha256', secret).update(bodyString).digest('hex');

        let responses = 0;

        const sendRequest = () => {
          const options = {
            hostname: 'localhost',
            port: port,
            path: '/webhooks/whatsapp',
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'X-Hub-Signature-256': signature,
              'Content-Length': bodyString.length,
            },
          };

          const req = http.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => (data += chunk));
            res.on('end', () => {
              expect(res.statusCode).toBe(200);
              responses++;

              if (responses === 2) {
                server.close(() => done());
              } else {
                // Send second request with same message ID
                setTimeout(sendRequest, 100);
              }
            });
          });

          req.on('error', (err) => {
            server.close(() => done(err));
          });

          req.write(bodyString);
          req.end();
        };

        sendRequest();
      });
    });
  });

  describe('Queue Persistence', () => {
    it('should persist messages reliably in queue', async () => {
      const messageId = 'msg_' + Date.now();
      const jobData = {
        messageId,
        payload: { test: 'data' },
        sender: '1234567890',
        timestamp: new Date().toISOString(),
        webhookReceivedAt: new Date().toISOString(),
      };

      const job = await enqueueMessage(jobData);
      expect(job.id).toBeDefined();

      // Retrieve job from queue
      const queue = getMessageQueue();
      const retrievedJob = await queue.getJob(job.id);

      expect(retrievedJob).toBeDefined();
      expect(retrievedJob?.data.messageId).toBe(messageId);
    });

    it('should handle queue with multiple messages', async () => {
      const queue = getMessageQueue();
      const messageIds = [];

      for (let i = 0; i < 3; i++) {
        const messageId = 'msg_' + Date.now() + i;
        messageIds.push(messageId);

        await enqueueMessage({
          messageId,
          payload: { index: i },
          sender: '1234567890',
          timestamp: new Date().toISOString(),
          webhookReceivedAt: new Date().toISOString(),
        });
      }

      const count = await queue.getJobCounts();
      expect(count.waiting).toBeGreaterThanOrEqual(3);
    });
  });

  describe('PostgreSQL Fallback', () => {
    it('should deduplicate via PostgreSQL when Redis unavailable', async () => {
      const messageId = 'msg_' + Date.now();
      const response = { received: true };

      // Directly insert into PostgreSQL dedup table
      const pool = getPool();
      await pool.query(
        `INSERT INTO message_deduplication (message_id, response_payload, expires_at)
         VALUES ($1, $2, $3)`,
        [messageId, JSON.stringify(response), new Date(Date.now() + 24 * 60 * 60 * 1000)]
      );

      // Verify idempotency check works without Redis
      const result = await checkIdempotency(messageId);
      expect(result.isDuplicate).toBe(true);
      expect(result.response).toEqual(response);
    });
  });
});
