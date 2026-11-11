import crypto from 'crypto';
import http from 'http';
import path from 'path';
import { AddressInfo } from 'net';
import { spawn, ChildProcess } from 'child_process';
import { createApp } from '../src/app';
import { initializeDatabase, closeDatabase, getPool } from '../src/database';
import { initializeRedis, closeRedis, getRedisClient } from '../src/redis';
import { initializeBurstQueue, closeBurstQueue, getBurstQueue } from '../src/services/burstAggregator';

// RF-02 coverage: messages a customer sends in a row are answered once, after a
// configurable quiet window, with a cap for customers who never pause.
// Requires PostgreSQL and Redis (see DATABASE_URL / REDIS_URL).

const SECRET = 'test_app_secret';
const ROOT = path.join(__dirname, '..');
// Short windows keep the suite fast; production defaults are 10s / 30s
const WINDOW_MS = 800;
const MAX_WAIT_MS = 3000;
// Fake AI step in the worker plus polling slack
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
const image = (from: string, id: string, mediaId: string) => webhook(from, { id, type: 'image', image: { id: mediaId } });

// Webhook processing continues after the 200, so wait until the message is buffered
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

// Resolves once `count` bursts for the sender exist and none is still processing
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
const uid = (label: string) => `wamid.rf02_${label}_${Date.now()}_${counter++}`;
const newSender = () => `549110${Date.now() % 10000000}${counter++}`;

describe('RF-02 - Message burst aggregation', () => {
  jest.setTimeout(60000);
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

    // One worker for the whole suite, as in production
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
    // Each test uses its own senders, so only clear what earlier suites left behind
    await getBurstQueue().empty();
  });

  it('answers "Hola", "tenés zapatillas", "talle 42" sent within 3s with a single response', async () => {
    const sender = newSender();
    const ids = [uid('a1'), uid('a2'), uid('a3')];

    await send(server, text(sender, ids[0], 'Hola'));
    await sleep(300);
    await send(server, text(sender, ids[1], 'tenés zapatillas'));
    await sleep(300);
    await send(server, text(sender, ids[2], 'talle 42'));

    const rows = await settledBursts(sender, 1);
    await sleep(WINDOW_MS); // no late second burst

    expect(await bursts(sender)).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      combined_text: 'Hola\ntenés zapatillas\ntalle 42',
      message_ids: ids,
      status: 'completed',
    });

    const messages = await messageRows(ids);
    expect(messages).toHaveLength(3);
    for (const m of messages) {
      expect(m).toMatchObject({ status: 'completed', burst_id: rows[0].id });
    }
  });

  it('waits for the window to elapse after the LAST message before processing', async () => {
    const sender = newSender();
    const ids = [uid('b1'), uid('b2')];

    await send(server, text(sender, ids[0], 'uno'));
    await sleep(WINDOW_MS * 0.6);
    await send(server, text(sender, ids[1], 'dos'));
    const lastSentAt = Date.now();

    // Past the first message's window but inside the last one's: nothing yet
    await sleep(WINDOW_MS * 0.6);
    expect(await bursts(sender)).toHaveLength(0);
    expect((await messageRows(ids)).map((m) => m.status)).toEqual(['pending', 'pending']);

    // The burst row is written when the worker takes the burst: it must not
    // appear before one full window after the last message
    const appearedAt = await waitFor(async () => ((await bursts(sender)).length ? Date.now() : undefined), WINDOW_MS + 1500, 25);
    expect(appearedAt).toBeDefined();
    expect(appearedAt! - lastSentAt).toBeGreaterThanOrEqual(WINDOW_MS - 50);

    const rows = await settledBursts(sender, 1);
    expect(rows).toHaveLength(1);
    expect(rows[0].message_ids).toEqual(ids);
  });

  it('answers messages separated by more than the window separately', async () => {
    const sender = newSender();
    const ids = [uid('c1'), uid('c2')];

    await send(server, text(sender, ids[0], 'primero'));
    await sleep(WINDOW_MS + 400);
    await send(server, text(sender, ids[1], 'segundo'));

    const rows = await settledBursts(sender, 2, 2 * WINDOW_MS + PROCESSING_MS);
    expect(rows.map((b) => b.message_ids)).toEqual([[ids[0]], [ids[1]]]);
    expect(rows.map((b) => b.status)).toEqual(['completed', 'completed']);
  });

  it('keeps bursts of interleaved senders apart', async () => {
    const [s1, s2] = [newSender(), newSender()];
    const [a1, b1, a2, b2] = [uid('d_a1'), uid('d_b1'), uid('d_a2'), uid('d_b2')];

    await send(server, text(s1, a1, 's1 uno'));
    await send(server, text(s2, b1, 's2 uno'));
    await send(server, text(s1, a2, 's1 dos'));
    await send(server, text(s2, b2, 's2 dos'));

    const [r1, r2] = [await settledBursts(s1, 1), await settledBursts(s2, 1)];
    expect(r1).toHaveLength(1);
    expect(r2).toHaveLength(1);
    expect(r1[0]).toMatchObject({ combined_text: 's1 uno\ns1 dos', message_ids: [a1, a2] });
    expect(r2[0]).toMatchObject({ combined_text: 's2 uno\ns2 dos', message_ids: [b1, b2] });
  });

  it('counts a WhatsApp redelivery inside a burst only once', async () => {
    const sender = newSender();
    const [first, second] = [uid('e1'), uid('e2')];

    await send(server, text(sender, first, 'hola'));
    expect(await postWebhook(server, text(sender, first, 'hola'))).toBe(200); // redelivery
    await send(server, text(sender, second, 'precio?'));

    const rows = await settledBursts(sender, 1);
    expect(rows).toHaveLength(1);
    expect(rows[0].message_ids).toEqual([first, second]);
    expect(rows[0].combined_text).toBe('hola\nprecio?');
  });

  it('processes a sender who never pauses once the max-wait cap is reached', async () => {
    const sender = newSender();
    const ids: string[] = [];
    const startedAt = Date.now();

    // Every half window, for longer than the cap: the window alone would never close
    while (Date.now() - startedAt < MAX_WAIT_MS + 1200) {
      const id = uid('f');
      ids.push(id);
      await send(server, text(sender, id, `msg ${ids.length}`));
      await sleep(WINDOW_MS / 2);
    }

    const first = await waitFor(async () => (await bursts(sender))[0], 1000);
    expect(first).toBeDefined();
    // The first burst was cut by the cap while the sender was still typing:
    // its first message is the sender's first, and it was taken ~MAX_WAIT_MS later
    expect(first!.message_ids[0]).toBe(ids[0]);
    expect(new Date(first!.created_at).getTime() - startedAt).toBeLessThan(500);
    expect(first!.message_ids.length).toBeGreaterThan(1);
    expect(first!.message_ids.length).toBeLessThan(ids.length);

    // Every message ends up in exactly one burst, in order
    const rows = await settledBursts(sender, 2);
    expect(rows.flatMap((b) => b.message_ids)).toEqual(ids);
  });

  it('keeps media messages in the burst with their ids', async () => {
    const sender = newSender();
    const ids = [uid('m1'), uid('m2'), uid('m3')];

    await send(server, text(sender, ids[0], 'mirá esta'));
    await send(server, image(sender, ids[1], 'media_123'));
    await send(server, text(sender, ids[2], 'la tenés?'));

    const rows = await settledBursts(sender, 1);
    expect(rows).toHaveLength(1);
    expect(rows[0].combined_text).toBe('mirá esta\n[image:media_123]\nla tenés?');
    expect(rows[0].message_ids).toEqual(ids);

    const stored = await getPool().query('SELECT payload FROM messages WHERE message_id = $1', [ids[1]]);
    expect(stored.rows[0].payload).toMatchObject({ type: 'image', mediaId: 'media_123' });
  });

  it('leaves no buffered state behind once a burst is processed', async () => {
    const sender = newSender();
    await send(server, text(sender, uid('g'), 'chau'));
    await settledBursts(sender, 1);

    expect(await getRedisClient().exists([`burst:${sender}`, `burst:time:${sender}`])).toBe(0);
  });
});
