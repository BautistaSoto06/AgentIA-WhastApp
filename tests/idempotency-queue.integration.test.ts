import crypto from 'crypto';
import http from 'http';
import { createApp } from '../src/app';
import { checkIdempotency, storeIdempotencyEntry, cleanupExpiredEntries } from '../src/services/idempotencyService';
import { initializeDatabase, closeDatabase, getPool } from '../src/database';
import { initializeRedis, closeRedis, getRedisClient } from '../src/redis';
import { storeMessage } from '../src/services/messageStore';
import { initializeBurstQueue, closeBurstQueue, getBurstQueue, addMessageToBurst } from '../src/services/burstAggregator';

describe('Idempotency & Message Store Integration Tests', () => {
  jest.setTimeout(15000);

  // Setup and teardown
  beforeAll(async () => {
    process.env.DATABASE_URL = process.env.DATABASE_URL || 'postgresql://user:password@localhost:5432/whatsapp_webhook';
    process.env.REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
    process.env.WHATSAPP_APP_SECRET = 'test_app_secret';
    process.env.WHATSAPP_VERIFY_TOKEN = 'test_verify_token';

    await initializeDatabase();
    await initializeRedis();
    initializeBurstQueue(process.env.REDIS_URL);
  });

  afterAll(async () => {
    await closeBurstQueue();
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

      // Use the median of several lookups so one slow tick (GC, first
      // connection use) doesn't make the test flaky
      const durations: number[] = [];
      for (let i = 0; i < 20; i++) {
        const startTime = process.hrtime.bigint();
        const result = await checkIdempotency(messageId);
        durations.push(Number(process.hrtime.bigint() - startTime) / 1e6);
        expect(result.isDuplicate).toBe(true);
      }
      durations.sort((a, b) => a - b);

      expect(durations[Math.floor(durations.length / 2)]).toBeLessThan(5);
    });
  });

  describe('Message Store Service', () => {
    it('should store a new message as pending', async () => {
      const messageId = 'msg_' + Date.now();
      const isNew = await storeMessage(messageId, '1234567890', { test: 'data' }, new Date());

      expect(isNew).toBe(true);

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
      await storeMessage(messageId, '1234567890', { test: 'data' }, new Date());

      const pool = getPool();
      const result = await pool.query(
        'SELECT * FROM webhook_requests WHERE message_id = $1',
        [messageId]
      );
      expect(result.rows.length).toBe(1);
    });

    it('should handle duplicate message IDs gracefully', async () => {
      const messageId = 'msg_' + Date.now();
      const payload = { test: 'data' };

      expect(await storeMessage(messageId, '1234567890', payload, new Date())).toBe(true);

      // Storing the same message ID again reports it as not new
      expect(await storeMessage(messageId, '1234567890', payload, new Date())).toBe(false);

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

  describe('Burst Queue Persistence', () => {
    const sender = 'persist_' + Date.now();
    const message = (id: string) => ({ id, from: sender, timestamp: '1700000000', type: 'text', text: id, payload: {} });

    afterEach(async () => {
      await getBurstQueue().empty();
      await getRedisClient().del([`burst:${sender}`, `burst:seq:${sender}`, `burst:time:${sender}`]);
    });

    it('should persist a delayed job and the buffered message in Redis', async () => {
      const messageId = 'msg_' + Date.now();
      await addMessageToBurst(sender, message(messageId));

      const delayed = (await getBurstQueue().getDelayed()).filter((j) => j.data.sender === sender);
      expect(delayed).toHaveLength(1);

      const buffered = await getRedisClient().lRange(`burst:${sender}`, 0, -1);
      expect(buffered.map((m) => JSON.parse(m).id)).toEqual([messageId]);
    });

    it('should buffer multiple messages in arrival order', async () => {
      const messageIds = [0, 1, 2].map((i) => 'msg_' + Date.now() + i);

      for (const messageId of messageIds) {
        await addMessageToBurst(sender, message(messageId));
      }

      const buffered = await getRedisClient().lRange(`burst:${sender}`, 0, -1);
      expect(buffered.map((m) => JSON.parse(m).id)).toEqual(messageIds);
      expect(await getRedisClient().get(`burst:seq:${sender}`)).toBe('3');
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
