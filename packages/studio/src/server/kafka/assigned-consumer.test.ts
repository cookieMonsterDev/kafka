import { describe, expect, it, vi } from 'vitest';
import { readAssignedMessages } from './assigned-consumer';
import { createFakeBatch, createFakeMessageConsumer } from './create-fake-message-consumer';

const batch = createFakeBatch({ topic: 'orders', partition: 0, highWatermark: 1n, fetchedOffset: 0n, messages: [] });
const cursors = [{ partition: 0, offset: 0n }];

describe('readAssignedMessages', () => {
  it('primes the stream before seeking and closes it before disconnecting on early break', async () => {
    const events: string[] = [];
    const consumer = createFakeMessageConsumer({
      connect: async () => {
        events.push('connect');
      },
      assign: async () => {
        events.push('assign');
      },
      seek: () => {
        events.push('seek');
      },
      stream: async function* () {
        events.push('start');
        try {
          yield batch;
        } finally {
          events.push('return');
        }
      },
      disconnect: async () => {
        events.push('disconnect');
      },
    });
    for await (const received of readAssignedMessages(consumer, 'orders', cursors)) {
      expect(received).toBe(batch);
      break;
    }
    expect(events).toEqual(['connect', 'assign', 'start', 'seek', 'return', 'disconnect']);
  });

  it.each(['connect', 'assign', 'seek'] as const)('disconnects after a %s failure', async (method) => {
    const disconnect = vi.fn(async () => {});
    const closed = vi.fn();
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      assign: async () => {},
      seek: () => {},
      disconnect,
      stream: async function* () {
        try {
          yield batch;
        } finally {
          closed();
        }
      },
      [method]: () => {
        throw new Error(`${method} failed`);
      },
    });
    await expect(readAssignedMessages(consumer, 'orders', cursors).next()).rejects.toThrow(`${method} failed`);
    expect(disconnect).toHaveBeenCalledOnce();
    expect(closed).toHaveBeenCalledTimes(method === 'seek' ? 1 : 0);
  });

  it('cancels a waiting stream and removes its consumer', async () => {
    const controller = new AbortController();
    let markStarted = () => {};
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const disconnect = vi.fn(async () => {});
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      assign: async () => {},
      seek: () => {},
      disconnect,
      stream: async function* (config) {
        markStarted();
        await new Promise<void>((resolve) =>
          config?.signal?.addEventListener('abort', () => resolve(), { once: true }),
        );
        yield* [];
      },
    });
    const pending = readAssignedMessages(consumer, 'orders', cursors, { signal: controller.signal }).next();
    await started;
    controller.abort();
    await expect(pending).resolves.toEqual({ done: true, value: undefined });
    expect(disconnect).toHaveBeenCalledOnce();
  });

  it('passes cancellation into connection setup before the connection settles', async () => {
    const controller = new AbortController();
    let markConnecting = () => {};
    const connecting = new Promise<void>((resolve) => {
      markConnecting = resolve;
    });
    const disconnect = vi.fn(async () => {});
    const consumer = createFakeMessageConsumer({
      connect: (options) =>
        new Promise<void>((_resolve, reject) => {
          options?.signal?.addEventListener(
            'abort',
            () => {
              const reason: unknown = options.signal?.reason;
              reject(reason instanceof Error ? reason : new Error('Connection cancelled'));
            },
            { once: true },
          );
          markConnecting();
        }),
      disconnect,
    });
    const pending = readAssignedMessages(consumer, 'orders', cursors, { signal: controller.signal }).next();
    await connecting;
    controller.abort(new Error('cancelled during connection'));
    await expect(pending).rejects.toThrow('cancelled during connection');
    expect(disconnect).toHaveBeenCalledOnce();
  });

  it('cleans up a connection that finishes after cancellation without starting assignment', async () => {
    const controller = new AbortController();
    let finishConnecting = () => {};
    const connecting = new Promise<void>((resolve) => {
      finishConnecting = resolve;
    });
    const assign = vi.fn(async () => {});
    const disconnect = vi.fn(async () => {});
    const consumer = createFakeMessageConsumer({ connect: () => connecting, assign, disconnect });
    const pending = readAssignedMessages(consumer, 'orders', cursors, { signal: controller.signal }).next();
    controller.abort();
    finishConnecting();
    await expect(pending).rejects.toThrow();
    expect(assign).not.toHaveBeenCalled();
    expect(disconnect).toHaveBeenCalledOnce();
  });
});
