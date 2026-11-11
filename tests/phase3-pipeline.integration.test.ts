import crypto from 'crypto';
import http from 'http';
import path from 'path';
import { AddressInfo } from 'net';
import { spawn, spawnSync, ChildProcess } from 'child_process';
import { createApp } from '../src/app';
import { initializeDatabase, closeDatabase, getPool } from '../src/database';
import { initializeRedis, closeRedis, getRedisClient } from '../src/redis';
import { initializeMessageQueue, closeMessageQueue, getMessageQueue } from '../src/services/messageEnqueuer';
import * as redisModule from '../src/redis';
import { checkIdempotency, storeIdempotencyEntry } from '../src/services/idempotencyService';

// Phase 3 coverage: the full webhook -> idempotency -> DB -> queue -> worker pipeline,
// real duplicate suppression, Redis-down fallback and re-runnable migrations.
// Requires PostgreSQL and Redis (see DATABASE_URL / REDIS_URL).

const SECRET = 'test_app_secret';
const ROOT = path.join(__dirname, '..');

function sign(body: string): string {
  return 'sha256=' + crypto.createHmac('sha256', SECRET).update(body).digest('hex');
}

function postWebhook(server: http.Server, payload: object): Promise<number> {
  const body = JSON.stringify(payload);
  const { port } = server.address() as AddressInfo;
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: 'localhost',
        port,
        path: '/webhooks/whatsapp',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': String(Buffer.byteLength(body)),
          'X-Hub-Signature-256': sign(body),
        },
      },
      (res) => {
        res.resume();
        res.on('end', () => resolve(res.statusCode || 0));
      }
    );
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function textPayload(messageId: string, text = 'hola') {
  return {
    object: 'whatsapp_business_account',
    entry: [
      {
        id: 'waba',
        changes: [
          {
            value: {
              messaging_product: 'whatsapp',
              messages: [{ from: '5491100000000', id: messageId, timestamp: '1700000000', type: 'text', text: { body: text } }],
            },
            field: 'messages',
          },
        ],
      },
    ],
  };
}

async function waitFor<T>(fn: () => Promise<T | undefined>, timeoutMs: number, intervalMs = 100): Promise<T | undefined> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value !== undefined) return value;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return undefined;
}

async function messageStatus(messageId: string): Promise<string | undefined> {
  const result = await getPool().query('SELECT status FROM messages WHERE message_id = $1', [messageId]);
  return result.rows[0]?.status;
}

describe('Phase 3 - Idempotent pipeline', () => {
  jest.setTimeout(60000);
  let server: http.Server;

  beforeAll(async () => {
    process.env.WHATSAPP_APP_SECRET = SECRET;
    process.env.WHATSAPP_VERIFY_TOKEN = 'test_verify_token';
    await initializeDatabase();
    await initializeRedis();
    initializeMessageQueue(process.env.REDIS_URL || 'redis://localhost:6379');
    await new Promise<void>((resolve) => {
      server = createApp().listen(0, () => resolve());
    });
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await closeMessageQueue();
    await closeDatabase();
    await closeRedis();
  });

  beforeEach(async () => {
    const pool = getPool();
    await pool.query('DELETE FROM message_deduplication');
    await pool.query('DELETE FROM webhook_requests');
    await pool.query('DELETE FROM messages');
    await getMessageQueue().empty();
  });

  describe('Webhook end to end', () => {
    it('persists the message, audit log, dedup entry, Redis key and queue job', async () => {
      const id = 'wamid.e2e_' + Date.now();
      expect(await postWebhook(server, textPayload(id, 'quiero un turno'))).toBe(200);

      // Processing is asynchronous after the 200, so poll for the final write.
      const dedup = await waitFor(async () => {
        const r = await getPool().query('SELECT response_payload FROM message_deduplication WHERE message_id = $1', [id]);
        return r.rows[0];
      }, 5000);
      expect(dedup?.response_payload).toEqual({ received: true });

      const msg = await getPool().query('SELECT sender, payload, status FROM messages WHERE message_id = $1', [id]);
      expect(msg.rows[0]).toMatchObject({ sender: '5491100000000', status: 'pending' });
      expect(msg.rows[0].payload).toMatchObject({ id, type: 'text', text: 'quiero un turno' });

      const audit = await getPool().query('SELECT COUNT(*)::int AS n FROM webhook_requests WHERE message_id = $1', [id]);
      expect(audit.rows[0].n).toBe(1);

      expect(await getRedisClient().ttl(`msg:${id}`)).toBeGreaterThan(24 * 60 * 60 - 60);

      const jobs = await getMessageQueue().getWaiting();
      expect(jobs.map((j) => j.data.messageId)).toContain(id);
    });

    it('suppresses a duplicate delivery: one message row, one audit row, one job', async () => {
      const id = 'wamid.dup_' + Date.now();
      expect(await postWebhook(server, textPayload(id))).toBe(200);
      await waitFor(async () => ((await checkIdempotency(id)).isDuplicate ? true : undefined), 5000);

      expect(await postWebhook(server, textPayload(id))).toBe(200);
      await new Promise((r) => setTimeout(r, 500));

      const audit = await getPool().query('SELECT COUNT(*)::int AS n FROM webhook_requests WHERE message_id = $1', [id]);
      expect(audit.rows[0].n).toBe(1);
      const jobs = (await getMessageQueue().getWaiting()).filter((j) => j.data.messageId === id);
      expect(jobs).toHaveLength(1);
    });

    it('enqueues every message of a multi-message webhook', async () => {
      const ids = ['a', 'b', 'c'].map((s) => `wamid.multi_${s}_${Date.now()}`);
      const payload = textPayload(ids[0]);
      payload.entry[0].changes[0].value.messages = ids.map((id) => ({
        from: '5491100000000',
        id,
        timestamp: '1700000000',
        type: 'text',
        text: { body: id },
      }));
      expect(await postWebhook(server, payload)).toBe(200);

      const count = await waitFor(async () => {
        const r = await getPool().query('SELECT COUNT(*)::int AS n FROM message_deduplication WHERE message_id = ANY($1)', [ids]);
        return r.rows[0].n === 3 ? 3 : undefined;
      }, 5000);
      expect(count).toBe(3);
    });
  });

  describe('Redis unavailable', () => {
    afterEach(() => jest.restoreAllMocks());

    it('checkIdempotency falls back to PostgreSQL when Redis throws', async () => {
      const id = 'wamid.redisdown_' + Date.now();
      await getPool().query(
        'INSERT INTO message_deduplication (message_id, response_payload, expires_at) VALUES ($1, $2, $3)',
        [id, JSON.stringify({ received: true }), new Date(Date.now() + 3600_000)]
      );
      jest.spyOn(redisModule, 'getRedisClient').mockImplementation(() => {
        throw new Error('ECONNREFUSED');
      });

      const result = await checkIdempotency(id);
      expect(result).toEqual({ isDuplicate: true, response: { received: true } });
    });

    it('storeIdempotencyEntry still writes to PostgreSQL when Redis throws', async () => {
      const id = 'wamid.redisdown_store_' + Date.now();
      jest.spyOn(redisModule, 'getRedisClient').mockImplementation(() => {
        throw new Error('ECONNREFUSED');
      });

      await expect(storeIdempotencyEntry(id, { received: true })).resolves.toBeUndefined();
      const r = await getPool().query('SELECT 1 FROM message_deduplication WHERE message_id = $1', [id]);
      expect(r.rows).toHaveLength(1);
    });
  });

  describe('Queue worker', () => {
    let worker: ChildProcess | undefined;
    let workerLog = '';

    afterEach(() => {
      worker?.kill();
      worker = undefined;
    });

    it('processes a queued message and marks it completed', async () => {
      const id = 'wamid.worker_' + Date.now();
      expect(await postWebhook(server, textPayload(id))).toBe(200);
      await waitFor(async () => ((await checkIdempotency(id)).isDuplicate ? true : undefined), 5000);

      worker = spawn(process.execPath, ['-r', 'ts-node/register/transpile-only', 'src/queue/worker.ts'], {
        cwd: ROOT,
        env: { ...process.env, LOG_LEVEL: 'info' },
      });
      worker.stdout?.on('data', (d) => (workerLog += d));
      worker.stderr?.on('data', (d) => (workerLog += d));

      // The worker fakes 1s of AI work; the job is done once the audit row gets its response.
      const audit = await waitFor(async () => {
        const r = await getPool().query('SELECT status_code, response_body FROM webhook_requests WHERE message_id = $1', [id]);
        return r.rows[0]?.status_code ? r.rows[0] : undefined;
      }, 12000, 250);

      if (!audit) {
        // Surface the worker's own error so the failure is actionable.
        const errors = [...new Set(workerLog.split('\n').filter((l) => /Error processing message/.test(l)).map((l) => l.replace(/^.*?"error":/, '"error":')))];
        throw new Error(`audit row never updated; message status is "${await messageStatus(id)}"; worker errors:\n${errors.join('\n')}`);
      }

      expect(audit).toEqual({ status_code: 200, response_body: { processed: true } });
      expect(await messageStatus(id)).toBe('completed');
    });
  });

  describe('Migrations', () => {
    it('can be re-run against an already migrated database', () => {
      const run = spawnSync(process.execPath, ['scripts/migrate.js'], { cwd: ROOT, env: process.env, encoding: 'utf8' });
      expect(run.stderr + run.stdout).not.toMatch(/Migration failed/);
      expect(run.status).toBe(0);
    });
  });
});
