import express, { Express, Request, Response } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import logger from './logger';
import { WebhookRequest, validateWhatsAppSignature } from './webhooks/signatureValidator';
import { handleWebhookGet, handleWebhookPost } from './webhooks/webhookHandlers';
import { webhookErrorHandler } from './webhooks/errorHandler';

export function createApp(): Express {
  const app = express();

  app.use(helmet());
  app.use(cors());

  // Capture raw body for webhook signature validation
  app.use(express.json({
    verify: (req: any, _res, buf) => {
      req.rawBody = buf.toString('utf8');
    },
  }));
  app.use(express.urlencoded({ extended: true }));

  app.use((req, _res, next) => {
    logger.info(`${req.method} ${req.path}`, {
      ip: req.ip,
      userAgent: req.get('user-agent'),
    });
    next();
  });

  app.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({
      status: 'ok',
      timestamp: new Date().toISOString(),
    });
  });

  // Webhook routes
  app.get('/webhooks/whatsapp', handleWebhookGet);
  app.post(
    '/webhooks/whatsapp',
    (req: WebhookRequest, res, next) => validateWhatsAppSignature(req, res, next),
    handleWebhookPost
  );

  app.use((req: Request, res: Response) => {
    res.status(404).json({
      error: 'Not Found',
      path: req.path,
    });
  });

  app.use(webhookErrorHandler);

  app.use((err: Error, _req: Request, res: Response) => {
    logger.error('Unhandled error', {
      error: err.message,
      stack: err.stack,
    });

    res.status(500).json({
      error: 'Internal Server Error',
    });
  });

  return app;
}
