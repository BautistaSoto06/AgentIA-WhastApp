import crypto from 'crypto';
import { createApp } from '../src/app';
import http from 'http';

describe('Webhook Endpoints Integration Tests', () => {
  jest.setTimeout(10000);

  describe('GET /webhooks/whatsapp - Verification', () => {
    it('should verify webhook with correct token', (done) => {
      process.env.WHATSAPP_VERIFY_TOKEN = 'test_verify_token';
      process.env.WHATSAPP_APP_SECRET = 'test_app_secret';
      const app = createApp();
      const challenge = 'test_challenge_string';
      const server = app.listen(0, () => {
        const port = (server.address() as any).port;
        const query = `?hub.mode=subscribe&hub.verify_token=test_verify_token&hub.challenge=${challenge}`;

        const req = http.get(`http://localhost:${port}/webhooks/whatsapp${query}`, (res) => {
          let data = '';
          res.on('data', (chunk) => (data += chunk));
          res.on('end', () => {
            expect(res.statusCode).toBe(200);
            expect(data).toBe(challenge);
            server.close(() => done());
          });
        });

        req.on('error', (err) => {
          server.close(() => done(err));
        });
      });
    });

    it('should reject webhook with incorrect token', (done) => {
      process.env.WHATSAPP_VERIFY_TOKEN = 'test_verify_token';
      const app = createApp();
      const server = app.listen(0, () => {
        const port = (server.address() as any).port;
        const query = '?hub.mode=subscribe&hub.verify_token=wrong_token&hub.challenge=test_challenge';

        const req = http.get(`http://localhost:${port}/webhooks/whatsapp${query}`, (res) => {
          let data = '';
          res.on('data', (chunk) => (data += chunk));
          res.on('end', () => {
            expect(res.statusCode).toBe(403);
            server.close(() => done());
          });
        });

        req.on('error', (err) => {
          server.close(() => done(err));
        });
      });
    });
  });

  describe('POST /webhooks/whatsapp - Message Handling', () => {
    beforeEach(() => {
      process.env.WHATSAPP_APP_SECRET = 'test_app_secret';
      process.env.WHATSAPP_VERIFY_TOKEN = 'test_verify_token';
    });

    it('should accept webhook with valid signature', (done) => {
      const app = createApp();
      const payload = {
        object: 'whatsapp_business_account',
        entry: [
          {
            id: '123456',
            changes: [
              {
                value: {
                  messages: [
                    {
                      from: '1234567890',
                      id: 'msg_id',
                      timestamp: '1234567890',
                      type: 'text',
                      text: { body: 'Hello' },
                    },
                  ],
                },
                field: 'messages',
              },
            ],
          },
        ],
      };

      const body = JSON.stringify(payload);
      const hash = crypto
        .createHmac('sha256', 'test_app_secret')
        .update(body)
        .digest('hex');
      const signature = `sha256=${hash}`;

      const server = app.listen(0, () => {
        const port = (server.address() as any).port;
        const req = http.request(
          {
            hostname: 'localhost',
            port,
            path: '/webhooks/whatsapp',
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Content-Length': Buffer.byteLength(body),
              'X-Hub-Signature-256': signature,
            },
          },
          (res) => {
            let data = '';
            res.on('data', (chunk) => (data += chunk));
            res.on('end', () => {
              expect(res.statusCode).toBe(200);
              const responseBody = JSON.parse(data);
              expect(responseBody).toHaveProperty('received', true);
              server.close(() => done());
            });
          }
        );

        req.on('error', (err) => {
          server.close(() => done(err));
        });

        req.write(body);
        req.end();
      });
    });

    it('should reject webhook with invalid signature', (done) => {
      const app = createApp();
      const payload = { object: 'whatsapp_business_account', entry: [] };
      const body = JSON.stringify(payload);

      const server = app.listen(0, () => {
        const port = (server.address() as any).port;
        const req = http.request(
          {
            hostname: 'localhost',
            port,
            path: '/webhooks/whatsapp',
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Content-Length': Buffer.byteLength(body),
              'X-Hub-Signature-256': 'sha256=invalidsignature',
            },
          },
          (res) => {
            let data = '';
            res.on('data', (chunk) => (data += chunk));
            res.on('end', () => {
              expect(res.statusCode).toBe(401);
              server.close(() => done());
            });
          }
        );

        req.on('error', (err) => {
          server.close(() => done(err));
        });

        req.write(body);
        req.end();
      });
    });

    it('should reject webhook without signature', (done) => {
      const app = createApp();
      const payload = { object: 'whatsapp_business_account' };
      const body = JSON.stringify(payload);

      const server = app.listen(0, () => {
        const port = (server.address() as any).port;
        const req = http.request(
          {
            hostname: 'localhost',
            port,
            path: '/webhooks/whatsapp',
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Content-Length': Buffer.byteLength(body),
            },
          },
          (res) => {
            let data = '';
            res.on('data', (chunk) => (data += chunk));
            res.on('end', () => {
              expect(res.statusCode).toBe(401);
              const responseBody = JSON.parse(data);
              expect(responseBody).toHaveProperty('error', 'Missing signature');
              server.close(() => done());
            });
          }
        );

        req.on('error', (err) => {
          server.close(() => done(err));
        });

        req.write(body);
        req.end();
      });
    });
  });
});
