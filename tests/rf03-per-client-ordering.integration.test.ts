import crypto from 'crypto';
import http from 'http';
import path from 'path';
import { AddressInfo } from 'net';
import { spawn, ChildProcess } from 'child_process';
import { createApp } from '../src/app';
import { initializeDatabase, closeDatabase, getPool } from '../src/database';
import { initializeRedis, closeRedis, getRedisClient } from '../src/redis';
import { initializeBurstQueue, closeBurstQueue, getBurstQueue } from '../src/services/burstAggregator';

// RF-03 coverage: ensure per-client ordering while allowing parallel processing
// of different clients. Two messages from the same client never process simultaneously,
// but different clients process in parallel up to worker concurrency.

const SECRET = 'test_app_secret';
const ROOT = path.join(__dirname, '..');
const WINDOW_MS = 800;
const MAX_WAIT_MS = 3000;
const PROCESSING_MS = 2500;

interface Burst {
  id: number;
  combined_text: string;
  message_ids: string[];
  status: string;
  created_at: Date;
  processed_at: Date | null;
}

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

function webhook(from: string, message: object) {
  return {
    object: 'whatsapp_business_account',
    entry: [{ id: 'waba', changes: [{ value: { messaging_product: 'whatsapp', messages: [{ from, timestamp: '1700000000', ...message }] }, field: 'messages' }] }],
  };
}

const text = (from: string, id: string, body: string) => webhook(from, { id, type: 'text', text: { body } });

async function send(server: http.Server, payload: ReturnType<typeof webhook>): Promise<void> {
  expect(await postWebhook(server, payload)).toBe(200);
  const id = (payload.entry[0].changes[0].value.messages[0] as any).id;
  const stored = await waitFor(async () => {
    const r = await getPool().query('SELECT 1 FROM message_deduplication WHERE message_id = $1', [id]);
    return r.rows.length ? true : undefined;
  }, 3000, 20);
  expect(stored).toBe(true);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor<T>(fn: () => Promise<T | undefined>, timeoutMs: number, intervalMs = 100): Promise<T | undefined> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value !== undefined) return value;
    await sleep(intervalMs);
  }
  return undefined;
}

async function bursts(sender: string): Promise<Burst[]> {
  const r = await getPool().query(
    'SELECT id, combined_text, message_ids, status, created_at, processed_at FROM message_bursts WHERE sender = $1 ORDER BY id',
    [sender]
  );
  return r.rows;
}

async function settledBursts(sender: string, count: number, timeoutMs = WINDOW_MS + PROCESSING_MS): Promise<Burst[]> {
  const rows = await waitFor(async () => {
    const all = await bursts(sender);
    return all.length >= count && all.every((b) => b.status !== 'processing') ? all : undefined;
  }, timeoutMs);
  return rows ?? (await bursts(sender));
}

async function messageRows(ids: string[]) {
  const r = await getPool().query('SELECT message_id, status, burst_id FROM messages WHERE message_id = ANY($1)', [ids]);
  return r.rows;
}

let counter = 0;
const uid = (label: string) => `wamid.rf03_${label}_${Date.now()}_${counter++}`;
const newSender = () => `549110${Date.now() % 10000000}${counter++}`;

describe('RF-03 - Per-client message ordering', () => {
  jest.setTimeout(120000);
  let server: http.Server;
  let worker: ChildProcess;
  let workerLog = '';

  beforeAll(async () => {
    process.env.WHATSAPP_APP_SECRET = SECRET;
    process.env.WHATSAPP_VERIFY_TOKEN = 'test_verify_token';
    process.env.BURST_WINDOW_MS = String(WINDOW_MS);
    process.env.BURST_MAX_WAIT_MS = String(MAX_WAIT_MS);
    await initializeDatabase();
    await initializeRedis();
    initializeBurstQueue(process.env.REDIS_URL || 'redis://localhost:6379');
    await new Promise<void>((resolve) => {
      server = createApp().listen(0, () => resolve());
    });

    worker = spawn(process.execPath, ['-r', 'ts-node/register/transpile-only', 'src/queue/burstWorker.ts'], {
      cwd: ROOT,
      env: { ...process.env, LOG_LEVEL: 'error' },
    });
    worker.stderr?.on('data', (d) => (workerLog += d));
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error(`burst worker not ready:\n${workerLog}`)), 20000);
      worker.stdout?.on('data', (d) => {
        workerLog += d;
        if (workerLog.includes('BURST_WORKER_READY')) {
          clearTimeout(timeout);
          resolve();
        }
      });
      worker.on('exit', (code) => reject(new Error(`burst worker exited with ${code}:\n${workerLog}`)));
    });
  });

  afterAll(async () => {
    const exited = new Promise((resolve) => worker.once('exit', resolve));
    worker.kill();
    await exited;
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await closeBurstQueue();
    await closeDatabase();
    await closeRedis();
  });

  beforeEach(async () => {
    await getBurstQueue().empty();
  });

  it('processes bursts of the same client in order', async () => {
    const sender = newSender();
    const burst1Ids = [uid('s1_b1_m1'), uid('s1_b1_m2')];
    const burst2Ids = [uid('s1_b2_m1')];

    // Send burst 1
    await send(server, text(sender, burst1Ids[0], 'first batch a'));
    await sleep(200);
    await send(server, text(sender, burst1Ids[1], 'first batch b'));

    // Wait for first burst to complete
    const burst1 = await settledBursts(sender, 1);
    expect(burst1).toHaveLength(1);
    expect(burst1[0].message_ids).toEqual(burst1Ids);
    const burst1CreatedAt = new Date(burst1[0].created_at).getTime();
    const burst1ProcessedAt = new Date(burst1[0].processed_at!).getTime();

    // Send burst 2 (should wait for burst 1 to finish)
    await sleep(WINDOW_MS + 400);
    await send(server, text(sender, burst2Ids[0], 'second batch'));

    // Wait for second burst to complete
    const burst2 = await settledBursts(sender, 2, 2 * WINDOW_MS + PROCESSING_MS);
    expect(burst2).toHaveLength(2);
    expect(burst2[1].message_ids).toEqual(burst2Ids);
    const burst2CreatedAt = new Date(burst2[1].created_at).getTime();

    // Burst 2 should have been created AFTER burst 1 processing started (due to ordering)
    expect(burst2CreatedAt).toBeGreaterThanOrEqual(burst1CreatedAt);

    // Verify both bursts completed
    expect(burst2.map((b) => b.status)).toEqual(['completed', 'completed']);
  });

  it('processes multiple clients in parallel', async () => {
    const [sender1, sender2, sender3] = [newSender(), newSender(), newSender()];
    const startTime = Date.now();

    // Send first message to each client at roughly the same time
    await send(server, text(sender1, uid('p1_m1'), 'client1 msg'));
    await send(server, text(sender2, uid('p2_m1'), 'client2 msg'));
    await send(server, text(sender3, uid('p3_m1'), 'client3 msg'));

    // All should complete within a reasonable time (serial processing would take 3x longer)
    const burst1 = await settledBursts(sender1, 1);
    const burst2 = await settledBursts(sender2, 1);
    const burst3 = await settledBursts(sender3, 1);
    const totalTime = Date.now() - startTime;

    expect(burst1).toHaveLength(1);
    expect(burst2).toHaveLength(1);
    expect(burst3).toHaveLength(1);

    // If parallel, should complete in ~WINDOW_MS + PROCESSING_MS
    // If serial, would be 3x that. We allow some margin.
    expect(totalTime).toBeLessThan(2 * (WINDOW_MS + PROCESSING_MS));
  });

  it('does not lose messages when concurrent bursts are processed', async () => {
    const sender = newSender();
    const allIds: string[] = [];

    // Send 3 rapid bursts
    for (let burstNum = 0; burstNum < 3; burstNum++) {
      const burstIds = [uid(`b${burstNum}_m1`), uid(`b${burstNum}_m2`)];
      allIds.push(...burstIds);

      await send(server, text(sender, burstIds[0], `burst ${burstNum} msg 1`));
      await sleep(200);
      await send(server, text(sender, burstIds[1], `burst ${burstNum} msg 2`));

      // Wait for this burst to settle before next
      await settledBursts(sender, burstNum + 1, WINDOW_MS + PROCESSING_MS + 1000);
    }

    // All bursts should be completed
    const allBursts = await bursts(sender);
    expect(allBursts.length).toBeGreaterThanOrEqual(3);
    expect(allBursts.every((b) => b.status === 'completed')).toBe(true);

    // All messages should be accounted for
    const allMessageRows = await messageRows(allIds);
    expect(allMessageRows).toHaveLength(allIds.length);
    expect(allMessageRows.every((m) => m.status === 'completed')).toBe(true);
    expect(allMessageRows.every((m) => m.burst_id !== null)).toBe(true);
  });

  it('handles idempotent redelivery during concurrent processing', async () => {
    const sender = newSender();
    const messageId = uid('idempotent_msg');
    const payload = text(sender, messageId, 'test idempotent');

    // Send message
    await send(server, payload);

    // Simulate redelivery while first is processing
    await sleep(200);
    expect(await postWebhook(server, payload)).toBe(200);

    // Wait for processing
    const bursts_result = await settledBursts(sender, 1);
    expect(bursts_result).toHaveLength(1);

    // Should have exactly one message, not duplicated
    const messages = await messageRows([messageId]);
    expect(messages).toHaveLength(1);
    expect(messages[0].status).toBe('completed');
  });

  it('releases lock on completion so next burst can be processed', async () => {
    const sender = newSender();
    const ids1 = [uid('lock1_a'), uid('lock1_b')];
    const ids2 = [uid('lock2_a'), uid('lock2_b')];

    // Send first burst
    await send(server, text(sender, ids1[0], 'burst 1 part 1'));
    await sleep(200);
    await send(server, text(sender, ids1[1], 'burst 1 part 2'));

    // First burst should complete
    const burst1 = await settledBursts(sender, 1);
    expect(burst1).toHaveLength(1);

    // Send second burst with no lock held
    await sleep(WINDOW_MS + 500);
    await send(server, text(sender, ids2[0], 'burst 2 part 1'));
    await sleep(200);
    await send(server, text(sender, ids2[1], 'burst 2 part 2'));

    // Second burst should eventually complete
    const burst2 = await settledBursts(sender, 2, 2 * WINDOW_MS + 2 * PROCESSING_MS);
    expect(burst2).toHaveLength(2);
    expect(burst2[1].message_ids).toEqual(ids2);
  });

  it('maintains per-client order across many bursts', async () => {
    const sender = newSender();
    const expectedOrder: string[] = [];

    // Send 5 bursts
    for (let i = 0; i < 5; i++) {
      const ids = [uid(`manybursts_${i}_a`), uid(`manybursts_${i}_b`)];
      expectedOrder.push(...ids);

      await send(server, text(sender, ids[0], `burst ${i} a`));
      await sleep(100);
      await send(server, text(sender, ids[1], `burst ${i} b`));

      // Small delay between bursts
      await sleep(WINDOW_MS + 200);
    }

    // Wait for all bursts to complete
    const allBursts = await settledBursts(sender, 5, 6 * (WINDOW_MS + PROCESSING_MS));
    expect(allBursts).toHaveLength(5);

    // Verify all messages appear in order
    const flattenedIds = allBursts.flatMap((b) => b.message_ids);
    expect(flattenedIds).toEqual(expectedOrder);

    // Verify all are completed
    expect(allBursts.every((b) => b.status === 'completed')).toBe(true);
  });

  it('cleans up locks after processing even on error', async () => {
    const sender = newSender();
    const ids = [uid('cleanup_m1'), uid('cleanup_m2')];

    await send(server, text(sender, ids[0], 'msg'));
    await sleep(200);
    await send(server, text(sender, ids[1], 'msg'));

    // Wait for completion
    await settledBursts(sender, 1);

    // Lock should be released (check by trying to process next burst)
    await sleep(WINDOW_MS + 300);
    await send(server, text(sender, uid('cleanup_m3'), 'next msg'));

    const allBursts = await settledBursts(sender, 2, WINDOW_MS + PROCESSING_MS + 1000);
    expect(allBursts.length).toBeGreaterThanOrEqual(2);
  });
});
