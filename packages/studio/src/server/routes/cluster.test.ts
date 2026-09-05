import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { createStudioServer } from '../create-server';
import { Router } from '../router';
import { registerClusterRoutes } from './cluster';

describe('registerClusterRoutes', () => {
  it('reports not connected', async () => {
    const router = new Router();
    registerClusterRoutes(router);
    const server = createStudioServer({ router });

    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as AddressInfo;
    try {
      const res = await fetch(`http://127.0.0.1:${String(port)}/api/cluster`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ connected: false });
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});
