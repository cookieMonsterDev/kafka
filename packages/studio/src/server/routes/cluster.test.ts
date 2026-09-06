import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { createStudioServer } from '../create-server';
import { AdminPool } from '../kafka/admin-pool';
import { createFakeAdmin, type FakeAdminOverrides } from '../kafka/create-fake-admin';
import { Router } from '../router';
import { registerClusterRoutes, type ClusterRouteContext } from './cluster';

function buildContext(overrides: FakeAdminOverrides = {}): ClusterRouteContext {
  const pool = new AdminPool(() => ({
    admin: () => createFakeAdmin({ connect: async () => {}, disconnect: async () => {}, ...overrides }),
  }));
  return { pool, getActiveProfile: () => null };
}

async function withServer<T>(context: ClusterRouteContext, run: (baseUrl: string) => Promise<T>): Promise<T> {
  const router = new Router();
  registerClusterRoutes(router, context);
  const server = createStudioServer({ router });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('registerClusterRoutes', () => {
  it('reports connected, with broker count and cluster ID, when the admin pool can reach the broker', async () => {
    const context = buildContext({
      describeCluster: async () => ({
        brokers: [
          { nodeId: 1, host: 'kafka', port: 9092 },
          { nodeId: 2, host: 'kafka', port: 9093 },
        ],
        controller: 1,
        clusterId: 'test-cluster',
      }),
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/cluster`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ connected: true, brokerCount: 2, clusterId: 'test-cluster' });
    });
  });

  it('reports not connected when the admin pool cannot connect', async () => {
    const context: ClusterRouteContext = {
      pool: new AdminPool(() => ({
        admin: () => {
          throw new Error('connection refused');
        },
      })),
      getActiveProfile: () => null,
    };

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/cluster`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ connected: false });
    });
  });

  it('reports not connected when describeCluster fails on an otherwise-connected admin', async () => {
    const context = buildContext({
      describeCluster: async () => {
        throw new Error('broker unavailable');
      },
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/cluster`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ connected: false });
    });
  });
});
