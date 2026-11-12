import { config } from '../config';
import { getPool } from '../database';
import logger from '../logger';

// RF-04: outbound messages through the WhatsApp Cloud API.
//
// - text / reply buttons / list messages (free-form, only inside the 24h window)
// - templates (allowed anywhere; the only thing allowed outside the window)
// - retries with exponential backoff for transient failures; every failed
//   attempt is recorded (outbound_message_errors + structured log)

// ---- Limits imposed by the Cloud API ----
export const LIMITS = {
  textBody: 4096,
  interactiveBody: 1024,
  buttons: 3,
  buttonTitle: 20,
  rows: 10,
  rowTitle: 24,
  rowDescription: 72,
  itemId: 256,
  listButton: 20,
  sectionTitle: 24,
} as const;

export interface ChoiceOption {
  id: string;
  title: string;
  description?: string; // only used when rendered as a list row
}

export type OutboundMessage =
  | { kind: 'text'; body: string }
  | { kind: 'buttons'; body: string; buttons: ChoiceOption[] }
  | { kind: 'list'; body: string; buttonText: string; options: ChoiceOption[]; sectionTitle?: string }
  | { kind: 'template'; name: string; language: string; components?: unknown[] };

export interface SendOptions {
  // Used instead of the free-form message when the 24h window is closed.
  fallbackTemplate?: Extract<OutboundMessage, { kind: 'template' }>;
}

export type ErrorKind = 'outside_window' | 'invalid_payload' | 'api_error' | 'network_error';

export class WhatsAppSendError extends Error {
  constructor(
    message: string,
    public readonly kind: ErrorKind,
    public readonly retryable: boolean,
    public readonly code?: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = 'WhatsAppSendError';
  }
}

// Injectable transport: POSTs a Cloud API payload and returns the wamid.
export interface WhatsAppClient {
  send(payload: Record<string, unknown>): Promise<{ messageId: string }>;
}

export interface ErrorRecord {
  recipient: string;
  messageType: string;
  attempt: number;
  retryable: boolean;
  errorCode?: string;
  errorMessage: string;
}

export interface SenderDeps {
  client: WhatsAppClient;
  getLastInboundAt: (recipient: string) => Promise<Date | null>;
  recordError: (rec: ErrorRecord) => Promise<void>;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
  maxAttempts?: number;
  baseDelayMs?: number;
  windowMs?: number;
}

// ---- Real transport ----
export class HttpWhatsAppClient implements WhatsAppClient {
  constructor(private readonly opts = config.whatsapp) {}

  async send(payload: Record<string, unknown>): Promise<{ messageId: string }> {
    const { accessToken, phoneNumberId, apiVersion } = this.opts;
    if (!accessToken || !phoneNumberId) {
      throw new WhatsAppSendError('WhatsApp credentials not configured', 'invalid_payload', false, 'no_credentials');
    }
    let res: Response;
    try {
      res = await fetch(`https://graph.facebook.com/${apiVersion}/${phoneNumberId}/messages`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
    } catch (err) {
      throw new WhatsAppSendError(err instanceof Error ? err.message : String(err), 'network_error', true);
    }
    const data: any = await res.json().catch(() => ({}));
    if (!res.ok) throw classifyApiError(res.status, data?.error);
    return { messageId: data?.messages?.[0]?.id ?? '' };
  }
}

// 131047 = re-engagement required (24h window closed). 429/5xx and the Meta
// rate-limit codes are transient; any other 4xx will fail the same way again.
export function classifyApiError(status: number, error?: { code?: number; message?: string }): WhatsAppSendError {
  const code = error?.code !== undefined ? String(error.code) : undefined;
  const message = error?.message || `WhatsApp API error (HTTP ${status})`;
  if (code === '131047' || code === '131051') {
    return new WhatsAppSendError(message, 'outside_window', false, code, status);
  }
  const transient = status === 429 || status >= 500 || ['4', '80007', '130429', '131056'].includes(code ?? '');
  return new WhatsAppSendError(message, 'api_error', transient, code, status);
}

// ---- Payload builders (validate API limits) ----
function invalid(msg: string): never {
  throw new WhatsAppSendError(msg, 'invalid_payload', false, 'invalid_payload');
}

function checkText(label: string, value: string, max: number): void {
  if (typeof value !== 'string' || value.trim().length === 0) invalid(`${label} is required`);
  if (value.length > max) invalid(`${label} exceeds ${max} characters (${value.length})`);
}

function checkOptions(options: ChoiceOption[], max: number, titleMax: number): void {
  if (!options.length) invalid('at least one option is required');
  if (options.length > max) invalid(`too many options: ${options.length} (max ${max})`);
  const ids = new Set<string>();
  for (const o of options) {
    checkText('option id', o.id, LIMITS.itemId);
    checkText(`option title "${o.id}"`, o.title, titleMax);
    if (ids.has(o.id)) invalid(`duplicate option id "${o.id}"`);
    ids.add(o.id);
  }
}

export function buildPayload(to: string, msg: OutboundMessage): Record<string, unknown> {
  checkText('recipient', to, 32);
  const base = { messaging_product: 'whatsapp', recipient_type: 'individual', to };

  switch (msg.kind) {
    case 'text':
      checkText('text body', msg.body, LIMITS.textBody);
      return { ...base, type: 'text', text: { body: msg.body } };

    case 'buttons':
      checkText('body', msg.body, LIMITS.interactiveBody);
      checkOptions(msg.buttons, LIMITS.buttons, LIMITS.buttonTitle);
      return {
        ...base,
        type: 'interactive',
        interactive: {
          type: 'button',
          body: { text: msg.body },
          action: { buttons: msg.buttons.map((b) => ({ type: 'reply', reply: { id: b.id, title: b.title } })) },
        },
      };

    case 'list':
      checkText('body', msg.body, LIMITS.interactiveBody);
      checkText('list button text', msg.buttonText, LIMITS.listButton);
      checkOptions(msg.options, LIMITS.rows, LIMITS.rowTitle);
      if (msg.sectionTitle) checkText('section title', msg.sectionTitle, LIMITS.sectionTitle);
      for (const o of msg.options) {
        if (o.description !== undefined && o.description.length > LIMITS.rowDescription) {
          invalid(`row description "${o.id}" exceeds ${LIMITS.rowDescription} characters`);
        }
      }
      return {
        ...base,
        type: 'interactive',
        interactive: {
          type: 'list',
          body: { text: msg.body },
          action: {
            button: msg.buttonText,
            sections: [
              {
                ...(msg.sectionTitle ? { title: msg.sectionTitle } : {}),
                rows: msg.options.map((o) => ({
                  id: o.id,
                  title: o.title,
                  ...(o.description ? { description: o.description } : {}),
                })),
              },
            ],
          },
        },
      };

    case 'template':
      checkText('template name', msg.name, 512);
      checkText('template language', msg.language, 20);
      return {
        ...base,
        type: 'template',
        template: {
          name: msg.name,
          language: { code: msg.language },
          ...(msg.components ? { components: msg.components } : {}),
        },
      };
  }
}

// Closed choices (confirm order, pick a size) are never sent as typed text:
// up to 3 options -> reply buttons, up to 10 -> list.
export function buildChoiceMessage(
  body: string,
  options: ChoiceOption[],
  opts: { listButtonText?: string; sectionTitle?: string } = {}
): OutboundMessage {
  if (options.length <= LIMITS.buttons) return { kind: 'buttons', body, buttons: options };
  if (options.length <= LIMITS.rows) {
    return { kind: 'list', body, buttonText: opts.listButtonText ?? 'Ver opciones', options, sectionTitle: opts.sectionTitle };
  }
  return invalid(`too many options for a closed choice: ${options.length} (max ${LIMITS.rows})`);
}

// ---- Sender ----
export class WhatsAppSender {
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly now: () => number;
  private readonly maxAttempts: number;
  private readonly baseDelayMs: number;
  private readonly windowMs: number;

  constructor(private readonly deps: SenderDeps) {
    this.sleep = deps.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
    this.now = deps.now ?? Date.now;
    this.maxAttempts = Math.max(1, deps.maxAttempts ?? config.whatsapp.sendMaxAttempts);
    this.baseDelayMs = deps.baseDelayMs ?? config.whatsapp.sendBaseDelayMs;
    this.windowMs = deps.windowMs ?? config.whatsapp.windowMs;
  }

  async isWithinWindow(recipient: string): Promise<boolean> {
    const last = await this.deps.getLastInboundAt(recipient);
    return last !== null && this.now() - last.getTime() < this.windowMs;
  }

  // Resolves to the wamid. Throws WhatsAppSendError when it ultimately fails
  // (every failed attempt has been recorded by then).
  async send(to: string, message: OutboundMessage, options: SendOptions = {}): Promise<string> {
    let effective = message;
    try {
      if (message.kind !== 'template' && !(await this.isWithinWindow(to))) {
        if (!options.fallbackTemplate) {
          throw new WhatsAppSendError(
            `Cannot send free-form "${message.kind}" to ${to}: outside the 24h customer service window; an approved template is required`,
            'outside_window',
            false,
            'outside_window'
          );
        }
        effective = options.fallbackTemplate;
      }
      const payload = buildPayload(to, effective);
      return await this.sendWithRetry(to, effective.kind, payload);
    } catch (err) {
      // Failures before any attempt (window / validation) are recorded here as attempt 0;
      // transport failures were already recorded per attempt by sendWithRetry.
      if (err instanceof WhatsAppSendError && (err.kind === 'outside_window' || err.kind === 'invalid_payload') && !this.recordedByRetry.has(err)) {
        await this.record(to, effective.kind, 0, err);
      }
      throw err;
    }
  }

  private readonly recordedByRetry = new WeakSet<Error>();

  private async sendWithRetry(to: string, type: string, payload: Record<string, unknown>): Promise<string> {
    for (let attempt = 1; ; attempt++) {
      try {
        const { messageId } = await this.deps.client.send(payload);
        logger.info('WhatsApp message sent', { to, type, attempt, messageId });
        return messageId;
      } catch (raw) {
        const err =
          raw instanceof WhatsAppSendError
            ? raw
            : new WhatsAppSendError(raw instanceof Error ? raw.message : String(raw), 'network_error', true);
        this.recordedByRetry.add(err);
        const willRetry = err.retryable && attempt < this.maxAttempts;
        await this.record(to, type, attempt, err);
        if (!willRetry) throw err;
        await this.sleep(this.baseDelayMs * 2 ** (attempt - 1));
      }
    }
  }

  private async record(to: string, type: string, attempt: number, err: WhatsAppSendError): Promise<void> {
    logger.error('WhatsApp send failed', {
      recipient: to,
      messageType: type,
      attempt,
      retryable: err.retryable,
      kind: err.kind,
      errorCode: err.code,
      error: err.message,
    });
    try {
      await this.deps.recordError({
        recipient: to,
        messageType: type,
        attempt,
        retryable: err.retryable,
        errorCode: err.code ?? err.kind,
        errorMessage: err.message,
      });
    } catch (dbErr) {
      // Never let error bookkeeping mask the original failure
      logger.error('Failed to persist outbound error', {
        error: dbErr instanceof Error ? dbErr.message : String(dbErr),
      });
    }
  }
}

// ---- Default wiring (DB-backed) ----
export async function getLastInboundAtFromDb(recipient: string): Promise<Date | null> {
  const r = await getPool().query('SELECT MAX(webhook_received_at) AS last FROM messages WHERE sender = $1', [recipient]);
  return r.rows[0]?.last ? new Date(r.rows[0].last) : null;
}

export async function recordErrorInDb(rec: ErrorRecord): Promise<void> {
  await getPool().query(
    `INSERT INTO outbound_message_errors (recipient, message_type, attempt, retryable, error_code, error_message)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [rec.recipient, rec.messageType, rec.attempt, rec.retryable, rec.errorCode ?? null, rec.errorMessage]
  );
}

let defaultSender: WhatsAppSender | undefined;

export function getWhatsAppSender(): WhatsAppSender {
  if (!defaultSender) {
    defaultSender = new WhatsAppSender({
      client: new HttpWhatsAppClient(),
      getLastInboundAt: getLastInboundAtFromDb,
      recordError: recordErrorInDb,
    });
  }
  return defaultSender;
}

// Tests / alternate wiring
export function setWhatsAppSender(sender: WhatsAppSender | undefined): void {
  defaultSender = sender;
}

export function isOutboundConfigured(): boolean {
  const { accessToken, phoneNumberId } = config.whatsapp;
  return Boolean(accessToken && phoneNumberId);
}
