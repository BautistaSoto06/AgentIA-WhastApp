import { Request, Response, NextFunction } from 'express';
import logger from '../logger';

export interface WebhookError extends Error {
  statusCode?: number;
}

export function webhookErrorHandler(
  err: WebhookError,
  req: Request,
  res: Response,
  _next: NextFunction
): void {
  const statusCode = err.statusCode || 500;

  logger.error('Webhook error', {
    error: err.message,
    stack: err.stack,
    statusCode,
    path: req.path,
    method: req.method,
    ip: req.ip,
  });

  res.status(statusCode).json({
    error: 'Webhook processing error',
    message: err.message,
  });
}
