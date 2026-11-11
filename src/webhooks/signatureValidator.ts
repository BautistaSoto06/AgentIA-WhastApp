import { Request, Response, NextFunction } from 'express';
import crypto from 'crypto';
import logger from '../logger';

export interface WebhookRequest extends Request {
  rawBody?: string;
}

export function validateWhatsAppSignature(
  req: WebhookRequest,
  res: Response,
  next: NextFunction
): void {
  const signature = req.get('x-hub-signature-256');
  const appSecret = process.env.WHATSAPP_APP_SECRET || '';

  if (!signature) {
    logger.warn('Missing X-Hub-Signature-256 header', {
      ip: req.ip,
      path: req.path,
    });
    res.status(401).json({ error: 'Missing signature' });
    return;
  }

  const body = req.rawBody || JSON.stringify(req.body);

  const hash = crypto
    .createHmac('sha256', appSecret)
    .update(body)
    .digest('hex');

  const expectedSignature = `sha256=${hash}`;

  // Timing-safe comparison to prevent timing attacks
  const signatureBuffer = Buffer.from(signature);
  const expectedBuffer = Buffer.from(expectedSignature);

  let isValid = false;
  if (signatureBuffer.length === expectedBuffer.length) {
    try {
      isValid = crypto.timingSafeEqual(signatureBuffer, expectedBuffer).valueOf();
    } catch (error) {
      isValid = false;
    }
  }

  if (!isValid) {
    logger.warn('Invalid webhook signature', {
      ip: req.ip,
      signature,
      expected: expectedSignature,
      timestamp: new Date().toISOString(),
    });
    res.status(401).json({ error: 'Invalid signature' });
    return;
  }

  logger.debug('Webhook signature validated', {
    ip: req.ip,
  });
  next();
}
