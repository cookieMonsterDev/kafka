import type { AddressInfo } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import { createStudioServer } from '../create-server';
import { AdminPool } from '../kafka/admin-pool';
import { createFakeAdmin, type FakeAdminOverrides } from '../kafka/create-fake-admin';
import { Router } from '../router';
import { registerGroupRoutes, type GroupsRouteContext } from './groups';

function buildContext(overrides: FakeAdminOverrides = {}): GroupsRouteContext {
  const pool = new AdminPool(() => ({
    admin: () => createFakeAdmin({ connect: async () => {}, disconnect: async () => {}, ...overrides }),
  }));
  return { pool, getActiveProfile: () => null };
}

async function withServer<T>(context: GroupsRouteContext, run: (baseUrl: string) => Promise<T>): Promise<T> {
  const router = new Router();
  registerGroupRoutes(router, context);
  const server = createStudioServer({ router });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

const classicMember = {
  memberId: 'm1',
  clientId: 'c1',
  clientHost: 'localhost',
  memberMetadata: Buffer.alloc(0),
  memberAssignment: Buffer.alloc(0),
};

describe('registerGroupRoutes', () => {
  it('GET /api/groups lists only consumer-protocol groups', async () => {
    const context = buildContext({
      listGroups: async () => ({
        groups: [
          { groupId: 'checkout', protocolType: 'consumer' },
          { groupId: 'shares', protocolType: 'share' },
        ],
      }),
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/groups`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ groups: [{ groupId: 'checkout', protocolType: 'consumer' }] });
    });
  });

  it('GET /api/groups/:id returns members and computed per-partition lag', async () => {
    const context = buildContext({
      describeGroups: async () => ({
        groups: [
          {
            errorCode: 0,
            groupId: 'checkout',
            state: 'Stable',
            protocolType: 'consumer',
            protocol: 'range',
            members: [classicMember],
          },
        ],
      }),
      fetchOffsets: async () => [
        {
          topic: 'orders',
          partitions: [
            { partition: 0, offset: 5n, metadata: null },
            { partition: 1, offset: -1n, metadata: null },
          ],
        },
      ],
      fetchTopicOffsets: async () => [
        { partition: 0, offset: 10n, high: 10n, low: 0n },
        { partition: 1, offset: 3n, high: 3n, low: 0n },
      ],
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/groups/checkout`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        groupId: string;
        partitionLag: { partition: number; committedOffset: string | null; logEndOffset: string; lag: string | null }[];
      };
      expect(body.groupId).toBe('checkout');
      expect(body.partitionLag).toEqual([
        { topic: 'orders', partition: 0, committedOffset: '5', logEndOffset: '10', lag: '5' },
        { topic: 'orders', partition: 1, committedOffset: null, logEndOffset: '3', lag: null },
      ]);
    });
  });

  it('GET /api/groups/:id returns 404 when the group does not exist', async () => {
    const context = buildContext({
      describeGroups: async () => ({
        groups: [{ errorCode: 0, groupId: 'missing', state: 'Dead', protocolType: '', protocol: '', members: [] }],
      }),
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/groups/missing`);
      expect(res.status).toBe(404);
    });
  });

  it('POST /api/groups/:id/offsets/reset resolves earliest/latest/offset/timestamp targets before committing', async () => {
    const setOffsets = vi.fn(async () => {});
    const context = buildContext({
      setOffsets,
      fetchTopicOffsets: async () => [
        { partition: 0, offset: 10n, high: 10n, low: 0n },
        { partition: 1, offset: 10n, high: 10n, low: 0n },
        { partition: 2, offset: 10n, high: 10n, low: 0n },
      ],
      fetchTopicOffsetsByTimestamp: async () => [{ partition: 2, offset: 4n }],
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/groups/checkout/offsets/reset`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          topic: 'orders',
          partitions: [
            { partition: 0, to: 'earliest' },
            { partition: 1, to: 'offset', offset: '7' },
            { partition: 2, to: 'timestamp', timestamp: 1_700_000_000_000 },
          ],
        }),
      });
      expect(res.status).toBe(200);
    });

    expect(setOffsets).toHaveBeenCalledWith({
      groupId: 'checkout',
      topic: 'orders',
      partitions: [
        { partition: 0, offset: 0n },
        { partition: 1, offset: 7n },
        { partition: 2, offset: 4n },
      ],
    });
  });

  it('DELETE /api/groups/:id deletes the group and returns 204', async () => {
    const deleteGroups = vi.fn(async () => [{ groupId: 'checkout', errorCode: 0 }]);
    const context = buildContext({ deleteGroups });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/groups/checkout`, { method: 'DELETE' });
      expect(res.status).toBe(204);
    });

    expect(deleteGroups).toHaveBeenCalledWith(['checkout']);
  });

  it('DELETE /api/groups/:id returns 404 when the group does not exist', async () => {
    const context = buildContext({
      deleteGroups: async () => [{ groupId: 'missing', errorCode: 69 }],
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/groups/missing`, { method: 'DELETE' });
      expect(res.status).toBe(404);
      const body = (await res.json()) as { error: { code: string } };
      expect(body.error.code).toBe('unknown_group');
    });
  });

  it('POST /api/groups/:id/members/remove removes the given members', async () => {
    const removeMembersFromConsumerGroup = vi.fn(async () => ({
      members: [{ memberId: 'm1', groupInstanceId: null, errorCode: 0 }],
    }));
    const context = buildContext({ removeMembersFromConsumerGroup });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/groups/checkout/members/remove`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ members: [{ memberId: 'm1' }] }),
      });
      expect(res.status).toBe(200);
    });

    expect(removeMembersFromConsumerGroup).toHaveBeenCalledWith({
      groupId: 'checkout',
      members: [{ memberId: 'm1', groupInstanceId: undefined }],
    });
  });

  it('GET /api/share-groups lists only share-protocol groups', async () => {
    const context = buildContext({
      listGroups: async () => ({
        groups: [
          { groupId: 'checkout', protocolType: 'consumer' },
          { groupId: 'shares', protocolType: 'share' },
        ],
      }),
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/share-groups`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ groups: [{ groupId: 'shares', protocolType: 'share' }] });
    });
  });

  it('GET /api/share-groups/:id returns members and offsets', async () => {
    const context = buildContext({
      describeShareGroups: async () => ({
        groups: [
          {
            errorCode: 0,
            errorMessage: null,
            groupId: 'shares',
            groupState: 'Stable',
            groupEpoch: 1,
            assignmentEpoch: 1,
            assignorName: 'simple',
            members: [
              {
                memberId: 'm1',
                rackId: null,
                memberEpoch: 0,
                clientId: 'c1',
                clientHost: 'localhost',
                subscribedTopicNames: ['orders'],
                assignment: { topicPartitions: [{ topicId: Buffer.alloc(16), topicName: 'orders', partitions: [0] }] },
              },
            ],
            authorizedOperations: 0,
          },
        ],
      }),
      listShareGroupOffsets: async () => ({
        groups: [
          {
            groupId: 'shares',
            errorCode: 0,
            errorMessage: null,
            topics: [
              {
                topicName: 'orders',
                topicId: Buffer.alloc(16),
                partitions: [
                  { partitionIndex: 0, startOffset: 3n, leaderEpoch: 0, lag: 2n, errorCode: 0, errorMessage: null },
                ],
              },
            ],
          },
        ],
      }),
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/share-groups/shares`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        offsets: { topic: string; partitions: { partition: number; startOffset: string; lag: string }[] }[];
      };
      expect(body.offsets).toEqual([{ topic: 'orders', partitions: [{ partition: 0, startOffset: '3', lag: '2' }] }]);
    });
  });
});
