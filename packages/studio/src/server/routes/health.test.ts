import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { createStudioServer } from '../create-server';
import { Router } from '../router';
import { registerHealthRoutes } from './health';

async function withServer<T>(router: Router, run: (baseUrl: string) => Promise<T>): Promise<T> {
  const server = createStudioServer({ router });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('registerHealthRoutes', () => {
  const context = {
    version: '0.0.1',
    readOnly: true,
    host: '127.0.0.1',
    port: 5757,
    startedAt: new Date(Date.now() - 5000),
  };

  it('serves /api/health with version, readOnly, and uptime', async () => {
    const router = new Router();
    registerHealthRoutes(router, context);

    await withServer(router, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/health`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { status: string; version: string; readOnly: boolean; uptimeSeconds: number };
      expect(body.status).toBe('ok');
      expect(body.version).toBe('0.0.1');
      expect(body.readOnly).toBe(true);
      expect(body.uptimeSeconds).toBeGreaterThanOrEqual(0);
    });
  });

  it('serves /__studio_runtime.json with no-store and the bound address', async () => {
    const router = new Router();
    registerHealthRoutes(router, context);

    await withServer(router, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/__studio_runtime.json`);
      expect(res.status).toBe(200);
      expect(res.headers.get('cache-control')).toBe('no-store');
      await expect(res.json()).resolves.toEqual({ version: '0.0.1', readOnly: true, host: '127.0.0.1', port: 5757 });
    });
  });
});
