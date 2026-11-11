import crypto from 'crypto';
import http from 'http';
import { AddressInfo } from 'net';
import { Request, Response } from 'express';
import { createApp } from '../src/app';
import { extractMessages, WebhookPayload } from '../src/webhooks/messageExtractor';
import { webhookErrorHandler, WebhookError } from '../src/webhooks/errorHandler';

// Phase 2 coverage: health/404 routing, GET verification, signature validation,
// message extraction and the webhook error handler. No DB/Redis required.

const SECRET = 'test_app_secret';
const VERIFY_TOKEN = 'test_verify_token';

interface HttpResult {
  status: number;
  body: string;
  headers: http.IncomingHttpHeaders;
}

function request(
  server: http.Server,
  method: string,
  path: string,
  body?: string,
  headers: Record<string, string> = {}
): Promise<HttpResult> {
  const { port } = server.address() as AddressInfo;
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: 'localhost',
        port,
        path,
        method,
        headers: body
          ? { 'Content-Type': 'application/json', 'Content-Length': String(Buffer.byteLength(body)), ...headers }
          : headers,
      },
      (res) => {
        let data = '';
        res.on('data', (chunk) => (data += chunk));
        res.on('end', () => resolve({ status: res.statusCode || 0, body: data, headers: res.headers }));
      }
    );
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

function sign(body: string, secret = SECRET): string {
  return 'sha256=' + crypto.createHmac('sha256', secret).update(body).digest('hex');
}

describe('Phase 2 - Webhook core', () => {
  let server: http.Server;

  beforeAll((done) => {
    process.env.WHATSAPP_APP_SECRET = SECRET;
    process.env.WHATSAPP_VERIFY_TOKEN = VERIFY_TOKEN;
    server = createApp().listen(0, done);
  });

  afterAll((done) => {
    server.close(() => done());
  });

  describe('App routing', () => {
    it('GET /health returns ok with an ISO timestamp', async () => {
      const res = await request(server, 'GET', '/health');
      expect(res.status).toBe(200);
      const json = JSON.parse(res.body);
      expect(json.status).toBe('ok');
      expect(new Date(json.timestamp).toISOString()).toBe(json.timestamp);
    });

    it('unknown routes return 404 JSON with the path', async () => {
      const res = await request(server, 'GET', '/does-not-exist');
      expect(res.status).toBe(404);
      expect(JSON.parse(res.body)).toEqual({ error: 'Not Found', path: '/does-not-exist' });
    });

    it('applies helmet security headers', async () => {
      const res = await request(server, 'GET', '/health');
      expect(res.headers['x-content-type-options']).toBe('nosniff');
    });
  });

  describe('GET /webhooks/whatsapp verification', () => {
    it('rejects a mode other than subscribe', async () => {
      const res = await request(
        server,
        'GET',
        `/webhooks/whatsapp?hub.mode=unsubscribe&hub.verify_token=${VERIFY_TOKEN}&hub.challenge=abc`
      );
      expect(res.status).toBe(403);
      expect(JSON.parse(res.body)).toEqual({ error: 'Invalid mode' });
    });

    it('rejects a missing token', async () => {
      const res = await request(server, 'GET', '/webhooks/whatsapp?hub.mode=subscribe&hub.challenge=abc');
      expect(res.status).toBe(403);
      expect(JSON.parse(res.body)).toEqual({ error: 'Invalid token' });
    });

    it('echoes the challenge verbatim on success', async () => {
      const res = await request(
        server,
        'GET',
        `/webhooks/whatsapp?hub.mode=subscribe&hub.verify_token=${VERIFY_TOKEN}&hub.challenge=1158201444`
      );
      expect(res.status).toBe(200);
      expect(res.body).toBe('1158201444');
    });
  });

  describe('POST /webhooks/whatsapp signature validation', () => {
    it('accepts a signature computed over the raw body (non-canonical JSON)', async () => {
      // Whitespace differs from JSON.stringify output, so this only passes if the raw body is used.
      const body = '{ "object" : "whatsapp_business_account",  "entry": [] }';
      const res = await request(server, 'POST', '/webhooks/whatsapp', body, { 'X-Hub-Signature-256': sign(body) });
      expect(res.status).toBe(200);
      expect(JSON.parse(res.body)).toEqual({ received: true });
    });

    it('rejects a same-length signature made with the wrong secret', async () => {
      const body = JSON.stringify({ object: 'whatsapp_business_account', entry: [] });
      const res = await request(server, 'POST', '/webhooks/whatsapp', body, {
        'X-Hub-Signature-256': sign(body, 'wrong_secret'),
      });
      expect(res.status).toBe(401);
      expect(JSON.parse(res.body)).toEqual({ error: 'Invalid signature' });
    });

    it('rejects a valid signature when the body was tampered with', async () => {
      const original = JSON.stringify({ object: 'whatsapp_business_account', entry: [] });
      const tampered = JSON.stringify({ object: 'whatsapp_business_account', entry: [{ id: 'x', changes: [] }] });
      const res = await request(server, 'POST', '/webhooks/whatsapp', tampered, {
        'X-Hub-Signature-256': sign(original),
      });
      expect(res.status).toBe(401);
    });

    it('rejects a signature without the sha256= prefix', async () => {
      const body = JSON.stringify({ object: 'whatsapp_business_account', entry: [] });
      const res = await request(server, 'POST', '/webhooks/whatsapp', body, {
        'X-Hub-Signature-256': sign(body).replace('sha256=', ''),
      });
      expect(res.status).toBe(401);
    });
  });

  describe('extractMessages', () => {
    const wrap = (messages: any[]): WebhookPayload => ({
      object: 'whatsapp_business_account',
      entry: [{ id: '1', changes: [{ value: { messages }, field: 'messages' }] }],
    });
    const base = { from: '5491100000000', id: 'wamid.1', timestamp: '1700000000' };

    it('returns [] when entry, changes or messages are missing', () => {
      expect(extractMessages({})).toEqual([]);
      expect(extractMessages({ entry: [{ id: '1' } as any] })).toEqual([]);
      expect(extractMessages({ entry: [{ id: '1', changes: [{ value: {}, field: 'statuses' }] }] })).toEqual([]);
    });

    it('extracts text messages', () => {
      const [msg] = extractMessages(wrap([{ ...base, type: 'text', text: { body: 'hola' } }]));
      expect(msg).toMatchObject({ id: 'wamid.1', from: '5491100000000', timestamp: '1700000000', type: 'text', text: 'hola' });
    });

    it.each(['image', 'document', 'audio', 'video'])('extracts %s media id', (type) => {
      const [msg] = extractMessages(wrap([{ ...base, type, [type]: { id: `media_${type}` } }]));
      expect(msg.type).toBe(type);
      expect(msg.mediaId).toBe(`media_${type}`);
      expect(msg.text).toBeUndefined();
    });

    it('marks unsupported types as unknown', () => {
      const [msg] = extractMessages(wrap([{ ...base, type: 'sticker' }]));
      expect(msg.type).toBe('unknown');
    });

    it('marks a text message without a body object as unknown', () => {
      const [msg] = extractMessages(wrap([{ ...base, type: 'text' }]));
      expect(msg.type).toBe('unknown');
    });

    it('flattens messages across several entries and changes', () => {
      const payload: WebhookPayload = {
        entry: [
          { id: 'a', changes: [{ value: { messages: [{ ...base, id: 'm1', type: 'text', text: { body: '1' } }] }, field: 'messages' }] },
          {
            id: 'b',
            changes: [
              { value: { messages: [{ ...base, id: 'm2', type: 'text', text: { body: '2' } }] }, field: 'messages' },
              { value: { messages: [{ ...base, id: 'm3', type: 'image', image: { id: 'i' } }] }, field: 'messages' },
            ],
          },
        ],
      };
      expect(extractMessages(payload).map((m) => m.id)).toEqual(['m1', 'm2', 'm3']);
    });
  });

  describe('webhookErrorHandler', () => {
    const run = (err: WebhookError) => {
      const res = { status: jest.fn().mockReturnThis(), json: jest.fn() } as unknown as Response;
      webhookErrorHandler(err, { path: '/x', method: 'POST', ip: '1.1.1.1' } as Request, res, jest.fn());
      return res;
    };

    it('uses the error statusCode when present', () => {
      const err: WebhookError = Object.assign(new Error('bad payload'), { statusCode: 422 });
      const res = run(err);
      expect(res.status).toHaveBeenCalledWith(422);
      expect(res.json).toHaveBeenCalledWith({ error: 'Webhook processing error', message: 'bad payload' });
    });

    it('defaults to 500', () => {
      expect(run(new Error('boom')).status).toHaveBeenCalledWith(500);
    });
  });
});
