import type { IncomingMessage, ServerResponse } from 'node:http';
import { describe, expect, it, vi } from 'vitest';

const middlewares = vi.fn((_req: IncomingMessage, _res: ServerResponse, next: (error?: unknown) => void) => {
  next();
});
const createServer = vi.fn(async () => ({ middlewares }));

vi.mock('vite', () => ({ createServer }));

describe('createDevMiddleware', () => {
  it('starts a middleware-mode vite server rooted at webRoot', async () => {
    const { createDevMiddleware } = await import('./dev');
    await createDevMiddleware('/some/web/root');

    expect(createServer).toHaveBeenCalledWith({
      root: '/some/web/root',
      server: { middlewareMode: true },
      appType: 'spa',
    });
  });

  it('resolves true once vite calls next() with no error', async () => {
    const { createDevMiddleware } = await import('./dev');
    const handler = await createDevMiddleware('/some/web/root');

    const req = {} as IncomingMessage;
    const res = {} as ServerResponse;
    await expect(handler(req, res, new URL('http://x/'))).resolves.toBe(true);
    expect(middlewares).toHaveBeenCalledWith(req, res, expect.any(Function));
  });

  it('rejects when vite passes an error to next()', async () => {
    middlewares.mockImplementationOnce((_req, _res, next: (error?: unknown) => void) => {
      next(new Error('vite blew up'));
    });
    const { createDevMiddleware } = await import('./dev');
    const handler = await createDevMiddleware('/some/web/root');

    await expect(handler({} as IncomingMessage, {} as ServerResponse, new URL('http://x/'))).rejects.toThrow(
      'vite blew up',
    );
  });
});
