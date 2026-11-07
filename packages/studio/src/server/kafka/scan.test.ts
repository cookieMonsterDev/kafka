import { describe, expect, it, vi } from 'vitest';
import { scanRequestSchema } from '../../shared/contracts/debug';
import { parseMessageQuery } from '../../shared/message-query';
import { createFakeAdmin } from './create-fake-admin';
import { createFakeBatch, createFakeKafkaMessage, createFakeMessageConsumer } from './create-fake-message-consumer';
import { scanMessages } from './scan';

function fixture(offsets: bigint[] = [0n, 1n, 2n]) {
  const disconnect = vi.fn(async () => {});
  const consumer = createFakeMessageConsumer({
    connect: async () => {},
    disconnect,
    assign: async () => {},
    seek: () => {},
    stream: async function* () {
      yield createFakeBatch({
        topic: 'orders',
        partition: 0,
        highWatermark: 3n,
        fetchedOffset: 0n,
        messages: offsets.map((offset) =>
          createFakeKafkaMessage({
            offset,
            timestamp: 100n,
            value: offset === 1n ? null : Buffer.from(`{"id":${String(offset)}}`),
            key: Buffer.from([0xff]),
            headers: { repeated: [Buffer.from('a'), null, Buffer.from('b')] },
            byteSize: 20,
          }),
        ),
      });
    },
  });
  const admin = createFakeAdmin({ fetchTopicOffsets: async () => [{ partition: 0, low: 0n, high: 3n, offset: 3n }] });
  return { admin, consumer, disconnect };
}
describe('bounded debugger scan', () => {
  it('reports complete coverage, preserves binary bytes and repeated/null headers', async () => {
    const { admin, consumer, disconnect } = fixture();
    const result = await scanMessages(
      admin,
      () => ({ consumer: () => consumer }),
      null,
      scanRequestSchema.parse({ topics: ['orders'], predicates: parseMessageQuery('value == null') }),
      new AbortController().signal,
    );
    expect(result.complete).toBe(true);
    expect(result.scanned).toBe(3);
    expect(result.records).toHaveLength(1);
    expect(result.records[0]).toMatchObject({
      offset: '1',
      value: null,
      key: '/w==',
      headerValues: { repeated: ['YQ==', null, 'Yg=='] },
    });
    expect(disconnect).toHaveBeenCalledOnce();
  });
  it('never labels a result-limited scan as full topic state', async () => {
    const { admin, consumer } = fixture();
    const result = await scanMessages(
      admin,
      () => ({ consumer: () => consumer }),
      null,
      scanRequestSchema.parse({ topics: ['orders'], maxResults: 1 }),
      new AbortController().signal,
    );
    expect(result.reason).toBe('result-limit');
    expect(result.complete).toBe(false);
    expect(result.coverage[0]?.next).toBe('1');
  });
  it('bounds records examined, not just matching results', async () => {
    const { admin, consumer } = fixture();
    const result = await scanMessages(
      admin,
      () => ({ consumer: () => consumer }),
      null,
      scanRequestSchema.parse({ topics: ['orders'], maxRecords: 1, predicates: parseMessageQuery('key == "absent"') }),
      new AbortController().signal,
    );
    expect(result.scanned).toBe(1);
    expect(result.reason).toBe('record-limit');
    expect(result.records).toEqual([]);
  });
  it('deduplicates offsets and excludes records past the snapshot end', async () => {
    const { admin, consumer } = fixture([0n, 0n, 2n, 3n]);
    const result = await scanMessages(
      admin,
      () => ({ consumer: () => consumer }),
      null,
      scanRequestSchema.parse({ topics: ['orders'] }),
      new AbortController().signal,
    );
    expect(result.records.map((record) => record.offset)).toEqual(['0', '2']);
    expect(result.complete).toBe(true);
  });
  it('checks the lower timestamp bound even after a timestamp seek', async () => {
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
          messages: [
            createFakeKafkaMessage({ offset: 0n, timestamp: 200n }),
            createFakeKafkaMessage({ offset: 1n, timestamp: 100n }),
          ],
        });
      },
    });
    const admin = createFakeAdmin({
      fetchTopicOffsets: async () => [{ partition: 0, offset: 2n, low: 0n, high: 2n }],
      fetchTopicOffsetsByTimestamp: async () => [{ partition: 0, offset: 0n }],
    });
    const result = await scanMessages(
      admin,
      () => ({ consumer: () => consumer }),
      null,
      scanRequestSchema.parse({ topics: ['orders'], startTime: 150 }),
      new AbortController().signal,
    );
    expect(result.records.map((entry) => entry.timestamp)).toEqual(['200']);
  });
});
