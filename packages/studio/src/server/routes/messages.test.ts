import type { AddressInfo } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import { createStudioServer } from '../create-server';
import { AdminPool } from '../kafka/admin-pool';
import { createFakeAdmin, type FakeAdminOverrides } from '../kafka/create-fake-admin';
import {
  createFakeBatch,
  createFakeKafkaMessage,
  createFakeMessageConsumer,
} from '../kafka/create-fake-message-consumer';
import { StudioEventBus } from '../kafka/events';
import type { MessageConsumerFactory } from '../kafka/messages';
import { Router } from '../router';
import { registerMessageRoutes, type MessagesRouteContext } from './messages';

function buildContext(
  adminOverrides: FakeAdminOverrides = {},
  consumerFactory?: MessageConsumerFactory,
): MessagesRouteContext {
  const pool = new AdminPool(() => ({
    admin: () => createFakeAdmin({ connect: async () => {}, disconnect: async () => {}, ...adminOverrides }),
  }));
  return {
    pool,
    consumerFactory:
      consumerFactory ??
      (() => ({
        consumer: () =>
          createFakeMessageConsumer({
            connect: async () => {},
            disconnect: async () => {},
            assign: async () => {},
            seek: () => {},
            stream: async function* () {},
          }),
      })),
    maxTail: 100,
    events: new StudioEventBus(),
    getActiveProfile: () => null,
  };
}

async function withServer<T>(context: MessagesRouteContext, run: (baseUrl: string) => Promise<T>): Promise<T> {
  const router = new Router();
  registerMessageRoutes(router, context);
  const server = createStudioServer({ router });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('registerMessageRoutes', () => {
  it('GET /api/topics/:name/messages returns a bounded page', async () => {
    const context = buildContext(
      { fetchTopicOffsets: async () => [{ partition: 0, offset: 3n, high: 3n, low: 0n }] },
      () => ({
        consumer: () =>
          createFakeMessageConsumer({
            connect: async () => {},
            disconnect: async () => {},
            assign: async () => {},
            seek: () => {},
            stream: async function* () {
              yield createFakeBatch({
                topic: 'orders',
                partition: 0,
                highWatermark: 3n,
                fetchedOffset: 0n,
                messages: [0n, 1n, 2n].map((n) => createFakeKafkaMessage({ offset: n, value: Buffer.from('v') })),
              });
            },
          }),
      }),
    );

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/messages?limit=10`);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { messages: { offset: string }[]; ranges: unknown[] };
      expect(body.messages.map((m) => m.offset)).toEqual(['0', '1', '2']);
      expect(body.ranges).toEqual([{ partition: 0, low: '0', high: '3' }]);
    });
  });

  it('GET /api/topics/:name/messages rejects an invalid query with 400', async () => {
    const context = buildContext();

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/messages?from=not-a-real-mode`);
      expect(res.status).toBe(400);
    });
  });

  it('GET /api/topics/:name/tail streams message frames over SSE', async () => {
    const context = buildContext(
      { fetchTopicOffsets: async () => [{ partition: 0, offset: 0n, high: 0n, low: 0n }] },
      () => ({
        consumer: () =>
          createFakeMessageConsumer({
            connect: async () => {},
            disconnect: async () => {},
            assign: async () => {},
            seek: () => {},
            stream: async function* () {
              yield createFakeBatch({
                topic: 'orders',
                partition: 0,
                highWatermark: 1n,
                fetchedOffset: 0n,
                messages: [createFakeKafkaMessage({ offset: 0n, value: Buffer.from('hi') })],
              });
            },
          }),
      }),
    );

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/tail`);
      expect(res.status).toBe(200);
      expect(res.headers.get('content-type')).toBe('text/event-stream');
      const body = await res.text();
      expect(body).toContain('event: message');
      expect(body).toContain(Buffer.from('hi').toString('base64'));
    });
  });

  it('GET /api/topics/:name/tail publishes a consume activity event per delivered message', async () => {
    const context = buildContext(
      { fetchTopicOffsets: async () => [{ partition: 0, offset: 0n, high: 0n, low: 0n }] },
      () => ({
        consumer: () =>
          createFakeMessageConsumer({
            connect: async () => {},
            disconnect: async () => {},
            assign: async () => {},
            seek: () => {},
            stream: async function* () {
              yield createFakeBatch({
                topic: 'orders',
                partition: 0,
                highWatermark: 1n,
                fetchedOffset: 0n,
                messages: [createFakeKafkaMessage({ offset: 0n, value: Buffer.from('hi') })],
              });
            },
          }),
      }),
    );
    const received: unknown[] = [];
    context.events.subscribe((event) => received.push(event));

    await withServer(context, async (baseUrl) => {
      await fetch(`${baseUrl}/api/topics/orders/tail`);
    });

    expect(received).toEqual([expect.objectContaining({ kind: 'consume', topic: 'orders', partition: 0, count: 1 })]);
  });

  it('GET /api/topics/:name/tail sends an "error" event and closes when the tail fails', async () => {
    const context = buildContext({
      fetchTopicOffsets: async () => {
        throw new Error('broker unreachable');
      },
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/tail`);
      expect(res.status).toBe(200);
      const body = await res.text();
      expect(body).toContain('event: tail-error');
      expect(body).toContain('broker unreachable');
    });
  });

  it('POST /api/topics/:name/offsets/by-time maps a negative broker offset to null', async () => {
    const context = buildContext({
      fetchTopicOffsetsByTimestamp: async () => [
        { partition: 0, offset: 42n },
        { partition: 1, offset: -1n },
      ],
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/offsets/by-time`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ timestamp: 1_700_000_000_000 }),
      });
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({
        offsets: [
          { partition: 0, offset: '42' },
          { partition: 1, offset: null },
        ],
      });
    });
  });

  it('POST /api/topics/:name/offsets/by-time rejects an invalid body with 400', async () => {
    const context = buildContext();

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/offsets/by-time`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ timestamp: 1.5 }),
      });
      expect(res.status).toBe(400);
    });
  });

  it('POST /api/topics/:name/records/delete converts the decimal offset to a bigint', async () => {
    const deleteTopicRecords = vi.fn(async () => {});
    const context = buildContext({ deleteTopicRecords });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/records/delete`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ partitions: [{ partition: 0, beforeOffset: '100' }] }),
      });
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ topic: 'orders' });
    });

    expect(deleteTopicRecords).toHaveBeenCalledWith({
      topic: 'orders',
      partitions: [{ partition: 0, offset: 100n }],
    });
  });

  it('POST /api/topics/:name/records/delete rejects an empty partitions array with 400', async () => {
    const context = buildContext();

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/topics/orders/records/delete`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ partitions: [] }),
      });
      expect(res.status).toBe(400);
    });
  });
});
