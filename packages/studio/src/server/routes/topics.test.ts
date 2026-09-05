import type { AddressInfo } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import { createStudioServer } from '../create-server';
import { AdminPool } from '../kafka/admin-pool';
import { createFakeAdmin, type FakeAdminOverrides } from '../kafka/create-fake-admin';
import { Router } from '../router';
import { registerTopicRoutes, type TopicsRouteContext } from './topics';

function buildContext(overrides: FakeAdminOverrides = {}): TopicsRouteContext {
  const pool = new AdminPool(() => ({
    admin: () => createFakeAdmin({ connect: async () => {}, disconnect: async () => {}, ...overrides }),
  }));
  return { pool, getActiveProfile: () => null };
}

async function withServer<T>(context: TopicsRouteContext, run: (baseUrl: string) => Promise<T>): Promise<T> {
  const router = new Router();
  registerTopicRoutes(router, context);
  const server = createStudioServer({ router });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

function unknownTopicError(): Error {
  return Object.assign(new Error('unknown topic'), { type: 'UNKNOWN_TOPIC_OR_PARTITION' });
}

describe('registerTopicRoutes', () => {
  it('GET /api/topics lists topics with partition count and replication factor', async () => {
    const context = buildContext({
      listTopics: async () => ['orders', 'empty-topic'],
      fetchTopicMetadata: async () => ({
        topics: [
          {
            name: 'orders',
            partitions: [
              { partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1, 2], isr: [1, 2], offlineReplicas: [] },
            ],
          },
          { name: 'empty-topic', partitions: [] },
        ],
      }),
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({
        topics: [
          { name: 'orders', partitionCount: 1, replicationFactor: 2 },
          { name: 'empty-topic', partitionCount: 0, replicationFactor: null },
        ],
      });
    });
  });

  it('GET /api/topics/:name returns partitions with offsets and configs', async () => {
    const context = buildContext({
      describeConfigs: async () => ({
        resources: [
          {
            errorCode: 0,
            errorMessage: null,
            resourceType: 2,
            resourceName: 'orders',
            configEntries: [
              {
                configName: 'retention.ms',
                configValue: '604800000',
                readOnly: false,
                isDefault: false,
                isSensitive: false,
                configSource: 1,
                configSynonyms: [],
              },
            ],
          },
        ],
      }),
      describeTopicPartitions: async () => ({
        topics: [
          {
            name: 'orders',
            topicId: Buffer.alloc(16),
            isInternal: false,
            topicAuthorizedOperations: 0,
            partitions: [
              {
                partitionIndex: 0,
                leader: 1,
                leaderEpoch: 0,
                replicas: [1, 2],
                isr: [1, 2],
                eligibleLeaderReplicas: null,
                lastKnownElr: null,
                offlineReplicas: [],
              },
            ],
          },
        ],
        nextCursor: null,
      }),
      fetchTopicOffsets: async () => [{ partition: 0, offset: 5n, high: 5n, low: 0n }],
      describeLogDirs: async () => ({
        brokers: [
          {
            brokerId: 1,
            logDirs: [
              {
                errorCode: 0,
                logDir: '/data',
                topics: [
                  { topic: 'orders', partitions: [{ partition: 0, size: 1024n, offsetLag: 0n, isFuture: false }] },
                ],
              },
            ],
          },
          {
            brokerId: 2,
            logDirs: [
              {
                errorCode: 0,
                logDir: '/data',
                topics: [
                  { topic: 'orders', partitions: [{ partition: 0, size: 900n, offsetLag: 0n, isFuture: false }] },
                ],
              },
            ],
          },
        ],
      }),
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
        name: string;
        partitions: { partitionIndex: number; earliestOffset: string; latestOffset: string; sizeBytes: string }[];
        configs: { name: string; value: string }[];
      };
      expect(body.name).toBe('orders');
      expect(body.partitions).toEqual([
        {
          partitionIndex: 0,
          leader: 1,
          replicas: [1, 2],
          isr: [1, 2],
          earliestOffset: '0',
          latestOffset: '5',
          sizeBytes: '1024',
        },
      ]);
      expect(body.configs).toEqual([
        { name: 'retention.ms', value: '604800000', isDefault: false, readOnly: false, isSensitive: false },
      ]);
    });
  });

  it('GET /api/topics/:name falls back to fetchTopicMetadata when describeTopicPartitions is unsupported', async () => {
    const context = buildContext({
      describeConfigs: async () => ({
        resources: [{ errorCode: 0, errorMessage: null, resourceType: 2, resourceName: 'orders', configEntries: [] }],
      }),
      describeTopicPartitions: async () => {
        throw Object.assign(new Error('unsupported'), { name: 'KafkaServerDoesNotSupportApiKey' });
      },
      fetchTopicMetadata: async () => ({
        topics: [
          {
            name: 'orders',
            partitions: [
              { partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1], offlineReplicas: [] },
            ],
          },
        ],
      }),
      fetchTopicOffsets: async () => [],
      describeLogDirs: async () => ({ brokers: [] }),
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { partitions: { partitionIndex: number }[] };
      expect(body.partitions).toEqual([
        {
          partitionIndex: 0,
          leader: 1,
          replicas: [1],
          isr: [1],
          earliestOffset: null,
          latestOffset: null,
          sizeBytes: null,
        },
      ]);
    });
  });

  it('GET /api/topics/:name returns 404 for an unknown topic', async () => {
    const context = buildContext({
      describeConfigs: async () => {
        throw unknownTopicError();
      },
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/missing`);
      expect(res.status).toBe(404);
      const body = (await res.json()) as { error: { code: string } };
      expect(body.error.code).toBe('unknown_topic');
    });
  });

  it('POST /api/topics creates a topic', async () => {
    const createTopics = vi.fn(async () => true);
    const context = buildContext({ createTopics });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          topic: 'orders',
          numPartitions: 3,
          replicationFactor: 1,
          configEntries: { 'retention.ms': '1000' },
        }),
      });
      expect(res.status).toBe(201);
      await expect(res.json()).resolves.toEqual({ topic: 'orders' });
    });

    expect(createTopics).toHaveBeenCalledWith({
      topics: [
        {
          topic: 'orders',
          numPartitions: 3,
          replicationFactor: 1,
          configEntries: [{ name: 'retention.ms', value: '1000' }],
        },
      ],
    });
  });

  it('POST /api/topics rejects an invalid body with 400', async () => {
    const context = buildContext();

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ topic: '' }),
      });
      expect(res.status).toBe(400);
      const body = (await res.json()) as { error: { code: string } };
      expect(body.error.code).toBe('bad_request');
    });
  });

  it('POST /api/topics returns 409 when the topic already exists', async () => {
    const context = buildContext({ createTopics: async () => false });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ topic: 'orders' }),
      });
      expect(res.status).toBe(409);
      const body = (await res.json()) as { error: { code: string } };
      expect(body.error.code).toBe('topic_already_exists');
    });
  });

  it('DELETE /api/topics/:name deletes a topic and returns 204', async () => {
    const deleteTopics = vi.fn(async () => {});
    const context = buildContext({ deleteTopics });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders`, { method: 'DELETE' });
      expect(res.status).toBe(204);
    });

    expect(deleteTopics).toHaveBeenCalledWith({ topics: ['orders'] });
  });

  it('DELETE /api/topics/:name returns 404 for an unknown topic', async () => {
    const context = buildContext({
      deleteTopics: async () => {
        throw unknownTopicError();
      },
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/missing`, { method: 'DELETE' });
      expect(res.status).toBe(404);
    });
  });

  it('POST /api/topics/:name/partitions raises the partition count', async () => {
    const createPartitions = vi.fn(async () => {});
    const context = buildContext({ createPartitions });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/partitions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ count: 6 }),
      });
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ topic: 'orders', count: 6 });
    });

    expect(createPartitions).toHaveBeenCalledWith({ topicPartitions: [{ topic: 'orders', count: 6 }] });
  });

  it('POST /api/topics/:name/partitions rejects a non-positive count with 400', async () => {
    const context = buildContext();

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/partitions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ count: 0 }),
      });
      expect(res.status).toBe(400);
    });
  });

  it('PATCH /api/topics/:name/configs sets and unsets entries in one call', async () => {
    const incrementalAlterConfigs = vi.fn(async () => ({ resources: [] }));
    const context = buildContext({ incrementalAlterConfigs });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/configs`, {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ set: { 'retention.ms': '2000' }, unset: ['cleanup.policy'] }),
      });
      expect(res.status).toBe(200);
    });

    expect(incrementalAlterConfigs).toHaveBeenCalledWith({
      resources: [
        {
          type: 2,
          name: 'orders',
          configs: [
            { name: 'retention.ms', value: '2000', operation: 0 },
            { name: 'cleanup.policy', value: null, operation: 1 },
          ],
        },
      ],
    });
  });

  it('PATCH /api/topics/:name/configs rejects an empty body with 400', async () => {
    const context = buildContext();

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/configs`, {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({}),
      });
      expect(res.status).toBe(400);
    });
  });
});
