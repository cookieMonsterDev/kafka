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
    server.closeAllConnections();
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

  it('lists every log dir with its volume sizes, sorted by broker then path', async () => {
    const describeLogDirs = async (options: unknown) => {
      expect(options).toEqual({ topics: [] });
      return {
        brokers: [
          {
            brokerId: 2,
            logDirs: [{ errorCode: 0, logDir: '/data/b', topics: [] }],
          },
          {
            brokerId: 1,
            logDirs: [
              { errorCode: 0, logDir: '/data/z', topics: [], totalBytes: 1000n, usableBytes: 250n, isCordoned: true },
              { errorCode: 0, logDir: '/data/a', topics: [], totalBytes: 2000n, usableBytes: 2000n, isCordoned: false },
            ],
          },
        ],
      };
    };
    const context = buildContext({ describeLogDirs });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/cluster/log-dirs`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({
        logDirs: [
          { brokerId: 1, path: '/data/a', totalBytes: '2000', usableBytes: '2000', usedBytes: '0', isCordoned: false },
          { brokerId: 1, path: '/data/z', totalBytes: '1000', usableBytes: '250', usedBytes: '750', isCordoned: true },
          { brokerId: 2, path: '/data/b', totalBytes: null, usableBytes: null, usedBytes: null, isCordoned: null },
        ],
      });
    });
  });

  it('lists the API versions of every broker, sorted by node id', async () => {
    const describeApiVersions = async (options: unknown) => {
      expect(options).toBeUndefined();
      return {
        brokers: [
          { nodeId: 2, host: 'kafka-2', port: 9093, rack: 'b', apis: [] },
          {
            nodeId: 1,
            host: 'kafka-1',
            port: 9092,
            rack: null,
            apis: [
              {
                apiKey: 0,
                apiName: 'Produce',
                brokerVersions: { minVersion: 3, maxVersion: 13 },
                isImplemented: true,
                version: 13,
              },
              { apiKey: 1, apiName: 'Fetch', isImplemented: true },
              { apiKey: 999, brokerVersions: { minVersion: 0, maxVersion: 1 }, isImplemented: false },
            ],
          },
        ],
      };
    };
    const context = buildContext({ describeApiVersions });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/cluster/api-versions`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({
        brokers: [
          {
            nodeId: 1,
            host: 'kafka-1',
            port: 9092,
            rack: null,
            apis: [
              {
                apiKey: 0,
                apiName: 'Produce',
                brokerVersions: { minVersion: 3, maxVersion: 13 },
                isImplemented: true,
                version: 13,
              },
              {
                apiKey: 1,
                apiName: 'Fetch',
                brokerVersions: null,
                isImplemented: true,
                version: null,
              },
              {
                apiKey: 999,
                apiName: null,
                brokerVersions: { minVersion: 0, maxVersion: 1 },
                isImplemented: false,
                version: null,
              },
            ],
          },
          { nodeId: 2, host: 'kafka-2', port: 9093, rack: 'b', apis: [] },
        ],
      });
    });
  });

  it('reports a describeLogDirs failure as an error response', async () => {
    const context = buildContext({
      describeLogDirs: async () => {
        throw new Error('KAFKA_STORAGE_ERROR');
      },
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/cluster/log-dirs`);
      expect(res.status).toBeGreaterThanOrEqual(500);
    });
  });
});
