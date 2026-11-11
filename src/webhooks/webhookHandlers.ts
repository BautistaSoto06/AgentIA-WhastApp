import { Request, Response } from 'express';
import logger from '../logger';
import { extractMessages, WebhookPayload } from './messageExtractor';

export function handleWebhookGet(req: Request, res: Response): void {
  const mode = req.query['hub.mode'] as string;
  const token = req.query['hub.verify_token'] as string;
  const challenge = req.query['hub.challenge'] as string;

  const verifyToken = process.env.WHATSAPP_VERIFY_TOKEN || '';

  if (mode !== 'subscribe') {
    logger.warn('Invalid webhook verification request', {
      mode,
      ip: req.ip,
    });
    res.status(403).json({ error: 'Invalid mode' });
    return;
  }

  if (token !== verifyToken) {
    logger.warn('Invalid verification token', {
      ip: req.ip,
      timestamp: new Date().toISOString(),
    });
    res.status(403).json({ error: 'Invalid token' });
    return;
  }

  logger.info('Webhook verified successfully', {
    ip: req.ip,
  });

  res.status(200).send(challenge);
}

export async function handleWebhookPost(req: Request, res: Response): Promise<void> {
  // Return 200 OK immediately to satisfy WhatsApp requirements
  res.status(200).json({ received: true });

  // Process webhook asynchronously
  try {
    const payload: WebhookPayload = req.body;

    if (!payload || !payload.entry) {
      logger.debug('Empty webhook payload received', {
        ip: req.ip,
      });
      return;
    }

    const messages = extractMessages(payload);

    if (messages.length > 0) {
      logger.info('Webhook messages received', {
        ip: req.ip,
        count: messages.length,
        types: messages.map((m) => m.type),
        from: messages.map((m) => m.from),
      });

      // Messages would be processed here (e.g., queued for processing)
      // For now, just log them
    } else {
      logger.debug('Webhook with no messages', {
        ip: req.ip,
      });
    }
  } catch (error) {
    logger.error('Error processing webhook', {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
      ip: req.ip,
    });
  }
}
