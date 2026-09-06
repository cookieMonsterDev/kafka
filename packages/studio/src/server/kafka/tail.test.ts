import { describe, expect, it, vi } from 'vitest';
import type { TopicOffset } from '@cookiemonsterdev/kafka-core';
import type { MessageRecord } from '../../shared/contracts/message';
import { createFakeAdmin } from './create-fake-admin';
import { createFakeBatch, createFakeKafkaMessage, createFakeMessageConsumer } from './create-fake-message-consumer';
import { BoundedTailQueue, runTail } from './tail';

function offset(partition: number, low: bigint, high: bigint): TopicOffset {
  return { partition, offset: high, high, low };
}

function messageRecord(recordOffset: string): MessageRecord {
  return { partition: 0, offset: recordOffset, timestamp: '0', key: null, value: null, headers: {}, size: 0 };
}

describe('BoundedTailQueue', () => {
  it('delivers pushed messages in order when under capacity', () => {
    const queue = new BoundedTailQueue(10);
    queue.push(messageRecord('1'));
    queue.push(messageRecord('2'));

    expect(queue.shift()).toEqual({ message: messageRecord('1'), droppedBefore: 0 });
    expect(queue.shift()).toEqual({ message: messageRecord('2'), droppedBefore: 0 });
    expect(queue.shift()).toBeUndefined();
  });

  it('drops the oldest entries once past capacity and reports the count on the next shift', () => {
    const queue = new BoundedTailQueue(2);

    for (const n of ['1', '2', '3', '4', '5']) queue.push(messageRecord(n));

    const first = queue.shift();
    expect(first?.message.offset).toBe('4');
    expect(first?.droppedBefore).toBe(3);

    const second = queue.shift();
    expect(second?.message.offset).toBe('5');
    expect(second?.droppedBefore).toBe(0);

    expect(queue.shift()).toBeUndefined();
  });
});

describe('runTail', () => {
  it('seeks every target partition to its current high watermark before streaming', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 7n)] });
    const seek = vi.fn();
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect: async () => {},
      assign: async () => {},
      seek,
      stream: async function* () {},
    });
    const stream = { send: vi.fn() };

    await runTail(admin, consumer, stream, { topic: 'orders', maxBuffered: 10 }, new AbortController().signal);

    expect(seek).toHaveBeenCalledWith({ topic: 'orders', partition: 0, offset: 7n });
  });

  it('sends each new message as a "message" frame, in order', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 0n)] });
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect: async () => {},
      assign: async () => {},
      seek: () => {},
      stream: async function* () {
        yield createFakeBatch({
          topic: 'orders',
          partition: 0,
          highWatermark: 2n,
          fetchedOffset: 0n,
          messages: [0n, 1n].map((n) => createFakeKafkaMessage({ offset: n })),
        });
      },
    });
    const stream = { send: vi.fn() };

    await runTail(admin, consumer, stream, { topic: 'orders', maxBuffered: 10 }, new AbortController().signal);

    const messageSends = stream.send.mock.calls.filter(([event]) => event === 'message');
    expect(messageSends.map(([, payload]) => (payload as { offset: string }).offset)).toEqual(['0', '1']);
  });

  it('reports a "gap" when the buffer dropped messages the client fell behind on', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 0n)] });
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect: async () => {},
      assign: async () => {},
      seek: () => {},
      stream: async function* () {
        yield createFakeBatch({
          topic: 'orders',
          partition: 0,
          highWatermark: 5n,
          fetchedOffset: 0n,
          messages: [0n, 1n, 2n, 3n, 4n].map((n) => createFakeKafkaMessage({ offset: n })),
        });
      },
    });
    const stream = { send: vi.fn() };

    await runTail(admin, consumer, stream, { topic: 'orders', maxBuffered: 2 }, new AbortController().signal);

    const gaps = stream.send.mock.calls.filter(([event]) => event === 'gap');
    expect(gaps).toHaveLength(1);
    expect(gaps[0]?.[1]).toEqual({ dropped: 3 });
  });

  it('stops delivering once the signal aborts mid-drain', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 0n)] });
    const controller = new AbortController();
    const consumer = createFakeMessageConsumer({
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
          messages: [0n, 1n, 2n].map((n) => createFakeKafkaMessage({ offset: n })),
        });
      },
    });
    const stream = {
      send: vi.fn(() => controller.abort()),
    };

    await runTail(admin, consumer, stream, { topic: 'orders', maxBuffered: 10 }, controller.signal);

    expect(stream.send).toHaveBeenCalledTimes(1);
  });

  it('always disconnects the consumer, even when assigning partitions fails', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 0n)] });
    const disconnect = vi.fn(async () => {});
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect,
      assign: async () => {
        throw new Error('boom');
      },
    });

    await expect(
      runTail(admin, consumer, { send: vi.fn() }, { topic: 'orders', maxBuffered: 10 }, new AbortController().signal),
    ).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledOnce();
  });

  it('does nothing when the requested partition does not exist', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 0n)] });
    const consumer = createFakeMessageConsumer({}); // any call throws — proves none happen
    const stream = { send: vi.fn() };

    await runTail(
      admin,
      consumer,
      stream,
      { topic: 'orders', partition: 9, maxBuffered: 10 },
      new AbortController().signal,
    );

    expect(stream.send).not.toHaveBeenCalled();
  });
});
