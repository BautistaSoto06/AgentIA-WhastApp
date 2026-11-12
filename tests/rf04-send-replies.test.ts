import {
  WhatsAppSender,
  WhatsAppClient,
  WhatsAppSendError,
  ErrorRecord,
  buildChoiceMessage,
  buildPayload,
  classifyApiError,
} from '../src/services/whatsappSender';

// RF-04: no real WhatsApp API and no DB; client, window lookup and error store are injected.

const NOW = Date.parse('2026-01-10T12:00:00Z');
const HOUR = 3600 * 1000;
const TO = '5491100000000';

function setup(opts: { lastInboundAgoMs?: number | null; client?: Partial<WhatsAppClient>; maxAttempts?: number } = {}) {
  const sent: Record<string, any>[] = [];
  const errors: ErrorRecord[] = [];
  const sleeps: number[] = [];
  const client: WhatsAppClient = {
    send: jest.fn(async (p) => {
      sent.push(p);
      return { messageId: 'wamid.1' };
    }),
    ...opts.client,
  };
  const ago = opts.lastInboundAgoMs === undefined ? HOUR : opts.lastInboundAgoMs;
  const sender = new WhatsAppSender({
    client,
    getLastInboundAt: async () => (ago === null ? null : new Date(NOW - ago)),
    recordError: async (r) => {
      errors.push(r);
    },
    sleep: async (ms) => {
      sleeps.push(ms);
    },
    now: () => NOW,
    maxAttempts: opts.maxAttempts ?? 3,
    baseDelayMs: 100,
  });
  return { sender, client, sent, errors, sleeps };
}

const tpl = { kind: 'template' as const, name: 'order_followup', language: 'es_AR' };
const opts = (n: number) => Array.from({ length: n }, (_, i) => ({ id: `o${i}`, title: `Opcion ${i}` }));

describe('24h window', () => {
  it('sends free-form text inside the window', async () => {
    const { sender, sent } = setup({ lastInboundAgoMs: 23 * HOUR });
    expect(await sender.send(TO, { kind: 'text', body: 'hola' })).toBe('wamid.1');
    expect(sent[0]).toMatchObject({ type: 'text', to: TO, text: { body: 'hola' } });
  });

  it('rejects free-form outside the window with a clear error, not sent, and records it', async () => {
    const { sender, client, errors } = setup({ lastInboundAgoMs: 25 * HOUR });
    await expect(sender.send(TO, { kind: 'text', body: 'hola' })).rejects.toMatchObject({
      kind: 'outside_window',
      retryable: false,
    });
    expect(client.send).not.toHaveBeenCalled();
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ recipient: TO, messageType: 'text', retryable: false, errorCode: 'outside_window' });
  });

  it('rejects when the client never wrote (no inbound message)', async () => {
    const { sender, client } = setup({ lastInboundAgoMs: null });
    await expect(sender.send(TO, { kind: 'text', body: 'hola' })).rejects.toThrow(/24h/);
    expect(client.send).not.toHaveBeenCalled();
  });

  it('uses the fallback template outside the window instead of free-form', async () => {
    const { sender, sent } = setup({ lastInboundAgoMs: 30 * HOUR });
    await sender.send(TO, { kind: 'text', body: 'hola' }, { fallbackTemplate: tpl });
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({ type: 'template', template: { name: 'order_followup', language: { code: 'es_AR' } } });
  });

  it('templates are allowed outside the window', async () => {
    const { sender, sent } = setup({ lastInboundAgoMs: 100 * HOUR });
    await sender.send(TO, tpl);
    expect(sent[0].type).toBe('template');
  });
});

describe('message types', () => {
  it('reply buttons payload', async () => {
    const { sender, sent } = setup();
    await sender.send(TO, { kind: 'buttons', body: 'Confirmas?', buttons: [{ id: 'yes', title: 'Si' }, { id: 'no', title: 'No' }] });
    expect(sent[0].interactive).toEqual({
      type: 'button',
      body: { text: 'Confirmas?' },
      action: {
        buttons: [
          { type: 'reply', reply: { id: 'yes', title: 'Si' } },
          { type: 'reply', reply: { id: 'no', title: 'No' } },
        ],
      },
    });
  });

  it('list payload', async () => {
    const { sender, sent } = setup();
    await sender.send(TO, { kind: 'list', body: 'Talle?', buttonText: 'Elegir', options: opts(5) });
    const i = sent[0].interactive;
    expect(i.type).toBe('list');
    expect(i.action.button).toBe('Elegir');
    expect(i.action.sections[0].rows).toHaveLength(5);
    expect(i.action.sections[0].rows[0]).toEqual({ id: 'o0', title: 'Opcion 0' });
  });

  it('closed choices: <=3 -> buttons, 4..10 -> list, >10 -> error', () => {
    expect(buildChoiceMessage('x', opts(3)).kind).toBe('buttons');
    expect(buildChoiceMessage('x', opts(4)).kind).toBe('list');
    expect(buildChoiceMessage('x', opts(10)).kind).toBe('list');
    expect(() => buildChoiceMessage('x', opts(11))).toThrow(WhatsAppSendError);
  });

  it('enforces API limits', () => {
    const b = (buttons: any[]) => () => buildPayload(TO, { kind: 'buttons', body: 'b', buttons });
    expect(b(opts(4))).toThrow(/too many/);
    expect(b([{ id: 'a', title: 'x'.repeat(21) }])).toThrow(/20/);
    expect(b([{ id: 'x'.repeat(257), title: 't' }])).toThrow(/256/);
    expect(b([{ id: 'a', title: 't' }, { id: 'a', title: 'u' }])).toThrow(/duplicate/);
    expect(() => buildPayload(TO, { kind: 'list', body: 'b', buttonText: 'go', options: [{ id: 'a', title: 'x'.repeat(25) }] })).toThrow(/24/);
    expect(() => buildPayload(TO, { kind: 'list', body: 'b', buttonText: 'go', options: opts(11) })).toThrow(/too many/);
    expect(() => buildPayload(TO, { kind: 'text', body: 'x'.repeat(4097) })).toThrow(/4096/);
  });

  it('invalid payload is not sent, not retried, but recorded', async () => {
    const { sender, client, errors } = setup();
    await expect(sender.send(TO, { kind: 'buttons', body: 'b', buttons: opts(4) })).rejects.toMatchObject({ kind: 'invalid_payload' });
    expect(client.send).not.toHaveBeenCalled();
    expect(errors).toHaveLength(1);
    expect(errors[0].retryable).toBe(false);
  });
});

describe('retries and error recording', () => {
  it('retries transient failures with exponential backoff, recording each failed attempt', async () => {
    let calls = 0;
    const { sender, errors, sleeps } = setup({
      client: {
        send: async () => {
          if (++calls < 3) throw classifyApiError(503, { message: 'unavailable' });
          return { messageId: 'wamid.ok' };
        },
      },
    });
    expect(await sender.send(TO, { kind: 'text', body: 'hola' })).toBe('wamid.ok');
    expect(calls).toBe(3);
    expect(sleeps).toEqual([100, 200]);
    expect(errors.map((e) => e.attempt)).toEqual([1, 2]);
    expect(errors[0]).toMatchObject({ recipient: TO, messageType: 'text', retryable: true, errorMessage: 'unavailable' });
  });

  it('gives up after max attempts and records every attempt', async () => {
    const client = { send: jest.fn(async () => { throw new Error('ECONNRESET'); }) };
    const { sender, errors, sleeps } = setup({ client, maxAttempts: 3 });
    await expect(sender.send(TO, { kind: 'text', body: 'hola' })).rejects.toThrow('ECONNRESET');
    expect(client.send).toHaveBeenCalledTimes(3);
    expect(sleeps).toEqual([100, 200]);
    expect(errors.map((e) => e.attempt)).toEqual([1, 2, 3]);
    expect(errors[2].errorCode).toBe('network_error');
  });

  it('does not retry non-retryable API errors (400) but records them', async () => {
    const client = { send: jest.fn(async () => { throw classifyApiError(400, { code: 100, message: 'bad param' }); }) };
    const { sender, errors, sleeps } = setup({ client });
    await expect(sender.send(TO, { kind: 'text', body: 'hola' })).rejects.toThrow('bad param');
    expect(client.send).toHaveBeenCalledTimes(1);
    expect(sleeps).toEqual([]);
    expect(errors).toEqual([
      { recipient: TO, messageType: 'text', attempt: 1, retryable: false, errorCode: '100', errorMessage: 'bad param' },
    ]);
  });

  it('API "re-engagement required" (131047) is outside_window and not retried', () => {
    const e = classifyApiError(400, { code: 131047, message: 'Re-engagement message' });
    expect(e).toMatchObject({ kind: 'outside_window', retryable: false });
  });

  it('classifies 429 and 5xx as retryable', () => {
    expect(classifyApiError(429).retryable).toBe(true);
    expect(classifyApiError(500).retryable).toBe(true);
    expect(classifyApiError(401).retryable).toBe(false);
  });

  it('a failing error store does not mask the send error', async () => {
    const sender = new WhatsAppSender({
      client: { send: async () => { throw classifyApiError(400, { message: 'bad' }); } },
      getLastInboundAt: async () => new Date(NOW),
      recordError: async () => { throw new Error('db down'); },
      now: () => NOW,
      sleep: async () => {},
    });
    await expect(sender.send(TO, { kind: 'text', body: 'hi' })).rejects.toThrow('bad');
  });
});
