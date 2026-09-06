import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { createStudioServer } from '../create-server';
import { AdminPool } from '../kafka/admin-pool';
import { createFakeAdmin, type FakeAdminOverrides } from '../kafka/create-fake-admin';
import { Router } from '../router';
import { registerSettingsRoutes, type SettingsRouteContext } from './settings';

function buildContext(overrides: FakeAdminOverrides = {}): SettingsRouteContext {
  const pool = new AdminPool(() => ({
    admin: () => createFakeAdmin({ connect: async () => {}, disconnect: async () => {}, ...overrides }),
  }));
  return { pool, getActiveProfile: () => null };
}

async function withServer<T>(context: SettingsRouteContext, run: (baseUrl: string) => Promise<T>): Promise<T> {
  const router = new Router();
  registerSettingsRoutes(router, context);
  const server = createStudioServer({ router });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('registerSettingsRoutes', () => {
  it('GET /api/acls resolves resource/operation/permission codes to names', async () => {
    const context = buildContext({
      describeAcls: async (filter) => {
        expect(filter).toMatchObject({ resourceType: 1, resourcePatternType: 1, operation: 1, permissionType: 1 });
        return {
          resources: [
            {
              resourceType: 2,
              resourceName: 'orders',
              resourcePatternType: 3,
              acls: [{ principal: 'User:alice', host: '*', operation: 3, permissionType: 3 }],
            },
          ],
        };
      },
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/acls`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({
        acls: [
          {
            resourceType: 'TOPIC',
            resourceName: 'orders',
            resourcePatternType: 'LITERAL',
            principal: 'User:alice',
            host: '*',
            operation: 'READ',
            permissionType: 'ALLOW',
          },
        ],
      });
    });
  });

  it('GET /api/acls returns an empty list when the broker has no authorizer configured', async () => {
    const context = buildContext({
      describeAcls: async () => {
        throw Object.assign(new Error('Security features are disabled.'), { type: 'SECURITY_DISABLED' });
      },
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/acls`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ acls: [] });
    });
  });

  it('GET /api/quotas lists every entity and its overrides', async () => {
    const context = buildContext({
      describeClientQuotas: async () => ({
        entries: [
          {
            entity: [{ entityType: 'user', entityName: 'alice' }],
            values: [{ key: 'producer_byte_rate', value: 1_048_576 }],
          },
        ],
      }),
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/quotas`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({
        quotas: [
          {
            entity: [{ entityType: 'user', entityName: 'alice' }],
            values: [{ key: 'producer_byte_rate', value: 1_048_576 }],
          },
        ],
      });
    });
  });

  it('GET /api/transactions serializes producerId as a decimal string', async () => {
    const context = buildContext({
      listTransactions: async () => ({
        transactionStates: [{ transactionalId: 'txn-1', producerId: 42n, transactionState: 'Ongoing' }],
      }),
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/transactions`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({
        transactions: [{ transactionalId: 'txn-1', producerId: '42', transactionState: 'Ongoing' }],
      });
    });
  });
});
