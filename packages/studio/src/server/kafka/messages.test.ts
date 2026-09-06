import { describe, expect, it, vi } from 'vitest';
import type { TopicOffset } from '@cookiemonsterdev/kafka-core';
import { createFakeAdmin } from './create-fake-admin';
import { createFakeBatch, createFakeKafkaMessage, createFakeMessageConsumer } from './create-fake-message-consumer';
import { readMessagesPage, toMessageRecord } from './messages';

function offset(partition: number, low: bigint, high: bigint): TopicOffset {
  return { partition, offset: high, high, low };
}

describe('toMessageRecord', () => {
  it('base64-encodes the key and value and stringifies the offset/timestamp', () => {
    const message = createFakeKafkaMessage({
      offset: 42n,
      timestamp: 1_700_000_000_000n,
      key: Buffer.from('k1'),
      value: Buffer.from('v1'),
      byteSize: 17,
    });

    expect(toMessageRecord(0, message)).toEqual({
      partition: 0,
      offset: '42',
      timestamp: '1700000000000',
      key: Buffer.from('k1').toString('base64'),
      value: Buffer.from('v1').toString('base64'),
      headers: {},
      size: 17,
    });
  });

  it('renders a null key/value as null, not an empty string', () => {
    const message = createFakeKafkaMessage({ key: null, value: null });
    const record = toMessageRecord(0, message);
    expect(record.key).toBeNull();
    expect(record.value).toBeNull();
  });

  it('base64-encodes header values and keeps only the last of a duplicated key', () => {
    const message = createFakeKafkaMessage({
      headers: { 'content-type': Buffer.from('json'), retry: [Buffer.from('1'), Buffer.from('2')] },
    });
    expect(toMessageRecord(0, message).headers).toEqual({
      'content-type': Buffer.from('json').toString('base64'),
      retry: Buffer.from('2').toString('base64'),
    });
  });
});

describe('readMessagesPage', () => {
  it('reads the most recent `limit` messages per partition when no seek mode is given (tail read)', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 10n)] });
    const messages = [5n, 6n, 7n, 8n, 9n].map((n) => createFakeKafkaMessage({ offset: n }));
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect: async () => {},
      assign: async () => {},
      seek: () => {},
      stream: async function* () {
        yield createFakeBatch({ topic: 'orders', partition: 0, highWatermark: 10n, fetchedOffset: 5n, messages });
      },
    });

    const result = await readMessagesPage(admin, consumer, { topic: 'orders', query: { limit: 5 } });

    expect(result.ranges).toEqual([{ partition: 0, low: '0', high: '10' }]);
    expect(result.messages.map((m) => m.offset)).toEqual(['5', '6', '7', '8', '9']);
  });

  it('seeks to the low watermark for from: "earliest"', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 3n)] });
    const seek = vi.fn();
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect: async () => {},
      assign: async () => {},
      seek,
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

    const result = await readMessagesPage(admin, consumer, {
      topic: 'orders',
      query: { from: 'earliest', limit: 100 },
    });

    expect(seek).toHaveBeenCalledWith({ topic: 'orders', partition: 0, offset: 0n });
    expect(result.messages.map((m) => m.offset)).toEqual(['0', '1', '2']);
  });

  it('clamps an explicit "from" offset into the partition range', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 5n, 8n)] });
    const seek = vi.fn();
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect: async () => {},
      assign: async () => {},
      seek,
      stream: async function* () {
        yield createFakeBatch({
          topic: 'orders',
          partition: 0,
          highWatermark: 8n,
          fetchedOffset: 5n,
          messages: [5n, 6n, 7n].map((n) => createFakeKafkaMessage({ offset: n })),
        });
      },
    });

    // Requested "from" (2) is below the partition's low watermark (5) — this partition's data has
    // already aged out, so the effective start clamps up to what's still on disk.
    await readMessagesPage(admin, consumer, { topic: 'orders', query: { from: '2', limit: 100 } });

    expect(seek).toHaveBeenCalledWith({ topic: 'orders', partition: 0, offset: 5n });
  });

  it('seeks by timestamp, treating a negative seek result as "nothing to read"', async () => {
    const admin = createFakeAdmin({
      fetchTopicOffsets: async () => [offset(0, 0n, 10n)],
      fetchTopicOffsetsByTimestamp: async () => [{ partition: 0, offset: -1n }],
    });
    const seek = vi.fn();
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect: async () => {},
      assign: async () => {},
      seek,
      stream: async function* () {},
    });

    const result = await readMessagesPage(admin, consumer, {
      topic: 'orders',
      query: { timestamp: 1_700_000_000_000, limit: 100 },
    });

    expect(result.messages).toEqual([]);
  });

  it('returns every partition in "ranges" even when "partition" filters the read to one', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 0n), offset(1, 0n, 0n)] });
    const consumer = createFakeMessageConsumer({});

    const result = await readMessagesPage(admin, consumer, { topic: 'orders', query: { partition: 0, limit: 100 } });

    expect(result.ranges).toEqual([
      { partition: 0, low: '0', high: '0' },
      { partition: 1, low: '0', high: '0' },
    ]);
  });

  it('never touches the consumer when every target partition is already empty', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 0n)] });
    const consumer = createFakeMessageConsumer({}); // any call throws — this proves none happen

    const result = await readMessagesPage(admin, consumer, { topic: 'orders', query: { limit: 100 } });

    expect(result.messages).toEqual([]);
  });

  it('always disconnects the consumer, even when assigning partitions fails', async () => {
    const admin = createFakeAdmin({ fetchTopicOffsets: async () => [offset(0, 0n, 5n)] });
    const disconnect = vi.fn(async () => {});
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect,
      assign: async () => {
        throw new Error('boom');
      },
    });

    await expect(readMessagesPage(admin, consumer, { topic: 'orders', query: { limit: 100 } })).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledOnce();
  });
});
