import type { ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { createStudioServer, sendError, sendJson } from './create-server';
import { Router } from './router';

async function withServer<T>(
  server: ReturnType<typeof createStudioServer>,
  run: (baseUrl: string) => Promise<T>,
): Promise<T> {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('createStudioServer', () => {
  let router: Router;

  beforeEach(() => {
    router = new Router();
  });

  it('dispatches a matched route', async () => {
    router.get('/api/health', (_req, res) => sendJson(res, 200, { status: 'ok' }));
    const server = createStudioServer({ router });

    await withServer(server, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/health`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ status: 'ok' });
    });
  });

  it('passes route params through', async () => {
    router.get('/api/topics/:name', (_req, res, params) => sendJson(res, 200, { name: params.name }));
    const server = createStudioServer({ router });

    await withServer(server, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders`);
      await expect(res.json()).resolves.toEqual({ name: 'orders' });
    });
  });

  it('returns a json error envelope for an unmatched /api/* route', async () => {
    const server = createStudioServer({ router });

    await withServer(server, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/nope`);
      expect(res.status).toBe(404);
      const body = (await res.json()) as { error: { code: string } };
      expect(body.error.code).toBe('not_found');
    });
  });

  it('returns a plain 404 for an unmatched non-api route with no fallback', async () => {
    const server = createStudioServer({ router });

    await withServer(server, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/nope`);
      expect(res.status).toBe(404);
      expect(res.headers.get('content-type')).toContain('text/plain');
    });
  });

  it('falls through to the fallback handler when the router does not match', async () => {
    const fallback = vi.fn((_req, res: ServerResponse) => {
      res.writeHead(200, { 'content-type': 'text/html' });
      res.end('<html></html>');
      return true;
    });
    const server = createStudioServer({ router, fallback });

    await withServer(server, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/some/spa/route`);
      expect(res.status).toBe(200);
      expect(fallback).toHaveBeenCalledOnce();
    });
  });

  it('turns a synchronous handler throw into a 500 error envelope', async () => {
    router.get('/api/boom', () => {
      throw new Error('kaboom');
    });
    const server = createStudioServer({ router });

    await withServer(server, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/boom`);
      expect(res.status).toBe(500);
      const body = (await res.json()) as { error: { code: string; message: string } };
      expect(body.error).toEqual({ code: 'internal_error', message: 'kaboom' });
    });
  });

  it('turns a rejected async handler into a 500 error envelope', async () => {
    router.get('/api/boom-async', async () => {
      await Promise.resolve();
      throw new Error('async kaboom');
    });
    const server = createStudioServer({ router });

    await withServer(server, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/boom-async`);
      expect(res.status).toBe(500);
      const body = (await res.json()) as { error: { code: string } };
      expect(body.error.code).toBe('internal_error');
    });
  });
});

describe('sendError', () => {
  it('omits details when not given', () => {
    const write = vi.fn();
    const res = { writeHead: vi.fn(), end: write } as unknown as ServerResponse;
    sendError(res, 400, 'bad_request', 'nope');
    expect(write).toHaveBeenCalledWith(JSON.stringify({ error: { code: 'bad_request', message: 'nope' } }));
  });

  it('includes details when given', () => {
    const write = vi.fn();
    const res = { writeHead: vi.fn(), end: write } as unknown as ServerResponse;
    sendError(res, 400, 'bad_request', 'nope', { field: 'name' });
    expect(write).toHaveBeenCalledWith(
      JSON.stringify({ error: { code: 'bad_request', message: 'nope', details: { field: 'name' } } }),
    );
  });
});
