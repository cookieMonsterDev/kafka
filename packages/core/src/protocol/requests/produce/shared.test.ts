import { describe, expect, it, vi } from 'vitest';
import { bufferPoolStats, clearBufferPool, resetBufferPoolStats } from '../../buffer-pool';
import { Decoder } from '../../decoder';
import { COMPRESSION_TYPES, type CompressionType } from '../../compression/index';
import { Encoder } from '../../encoder';
import { decodeRecordBatch, encodeRecordBatch } from '../../records/batch';
import type * as BatchModule from '../../records/batch';
import {
  createProduceRequest,
  isUsableTopicId,
  readProducePartitionTaggedFields,
  readProduceResponseNodeEndpoints,
  resolveProduceTopicName,
  type ProduceMessage,
} from './shared';

vi.mock('../../records/batch', async (importOriginal) => {
  const actual = await importOriginal<typeof BatchModule>();
  return { ...actual, encodeRecordBatch: vi.fn(actual.encodeRecordBatch) };
});

async function encodeFirstRecordSet(messages: ProduceMessage[]): Promise<Buffer> {
  const encoded = await createProduceRequest(3, {
    acks: 1,
    timeout: 1000,
    topicData: [{ topic: 't', partitions: [{ partition: 0, messages }] }],
  }).encode();

  const decoder = new Decoder(encoded.buffer);
  decoder.readString(); // transactionalId
  decoder.readInt16(); // acks
  decoder.readInt32(); // timeout
  decoder.readInt32(); // topic count
  decoder.readString(); // topic
  decoder.readInt32(); // partition count
  decoder.readInt32(); // partition
  const recordSet = decoder.readBytes();
  if (recordSet === null) throw new Error('expected a record set');
  return recordSet;
}

async function decodeFirstBatch(messages: ProduceMessage[]) {
  return decodeRecordBatch(new Decoder(await encodeFirstRecordSet(messages)));
}

describe('protocol/requests/produce/shared', () => {
  it('sets firstTimestamp and maxTimestamp from a single min/max pass', async () => {
    const batch = await decodeFirstBatch([
      { value: 'a', timestamp: 500 },
      { value: 'b', timestamp: 100 },
      { value: 'c', timestamp: 300 },
    ]);
    expect(batch.firstTimestamp).toBe(100n);
    expect(batch.maxTimestamp).toBe(500n);
    expect(batch.records.map((r) => r.timestamp)).toEqual([500n, 100n, 300n]);
  });

  it('uses Date.now() when no message timestamps are set', async () => {
    const before = Date.now();
    const batch = await decodeFirstBatch([{ value: 'a' }, { value: 'b' }]);
    const after = Date.now();
    expect(batch.firstTimestamp).toBe(batch.maxTimestamp);
    expect(batch.firstTimestamp).toBeGreaterThanOrEqual(BigInt(before));
    expect(batch.firstTimestamp).toBeLessThanOrEqual(BigInt(after));
  });

  it('ignores missing timestamps when computing min/max', async () => {
    const batch = await decodeFirstBatch([
      { value: 'a', timestamp: 200 },
      { value: 'b' },
      { value: 'c', timestamp: 50 },
    ]);
    expect(batch.firstTimestamp).toBe(50n);
    expect(batch.maxTimestamp).toBe(200n);
  });

  it('sizes the record batch estimate to cover header bytes, so the encoder never grows', async () => {
    vi.mocked(encodeRecordBatch).mockClear();

    const recordSet = await encodeFirstRecordSet([
      { key: 'k', value: 'v', headers: { trace: 'x'.repeat(300), tags: ['a', 'b'], empty: null } },
      { value: 'v', headers: { trace: 'y'.repeat(300) } },
    ]);

    const estimatedBytes = vi.mocked(encodeRecordBatch).mock.calls[0]?.[0]?.estimatedBytes;
    expect(estimatedBytes).toBeGreaterThanOrEqual(recordSet.length);
  });

  describe('isUsableTopicId', () => {
    it('is true only for a non-zero 16-byte UUID', () => {
      expect(isUsableTopicId(undefined)).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(0))).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(8, 1))).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(16))).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(16, 1))).toBe(true);
    });
  });

  describe('resolveProduceTopicName', () => {
    const ordersId = Buffer.alloc(16, 1);
    const paymentsId = Buffer.alloc(16, 2);
    const topicData = [
      { topic: 'orders', topicId: ordersId, partitions: [] },
      { topic: 'payments', topicId: paymentsId, partitions: [] },
    ];

    it('resolves by topic UUID, then by index, then to an empty string', () => {
      expect(resolveProduceTopicName(paymentsId, 0, topicData)).toBe('payments');
      expect(resolveProduceTopicName(Buffer.alloc(16, 9), 1, topicData)).toBe('payments');
      expect(resolveProduceTopicName(Buffer.alloc(16, 9), 99, topicData)).toBe('');
      expect(resolveProduceTopicName(Buffer.alloc(16, 9), 0, [{ topic: 'anon', partitions: [] }])).toBe('anon');
    });
  });

  describe('createProduceRequest', () => {
    it('does not expect a response when acks is 0', () => {
      const request = createProduceRequest(3, {
        acks: 0,
        timeout: 1000,
        topicData: [{ topic: 't', partitions: [{ partition: 0, messages: [{ value: 'a' }] }] }],
      });
      expect(request.expectResponse?.()).toBe(false);
    });

    it('expects a response when acks is not 0', () => {
      const request = createProduceRequest(3, {
        acks: 1,
        timeout: 1000,
        topicData: [{ topic: 't', partitions: [{ partition: 0, messages: [{ value: 'a' }] }] }],
      });
      expect(request.expectResponse?.()).toBe(true);
    });

    it('encodes an uncompressed request synchronously and a compressed one asynchronously', async () => {
      const options = {
        acks: 1,
        timeout: 1000,
        topicData: [{ topic: 't', partitions: [{ partition: 0, messages: [{ value: 'a' }] }] }],
      };

      expect(createProduceRequest(3, options).encode()).toBeInstanceOf(Encoder);
      const compressed = createProduceRequest(3, { ...options, compression: COMPRESSION_TYPES.GZIP }).encode();
      expect(compressed).toBeInstanceOf(Promise);
      await expect(compressed).resolves.toBeInstanceOf(Encoder);
    });

    it('rejects a compressed request whose partition fails to encode, leaving no rejection unhandled', async () => {
      const unknownCodec = 7 as CompressionType;
      const encoding = createProduceRequest(3, {
        acks: 1,
        timeout: 1000,
        compression: unknownCodec,
        topicData: [
          {
            topic: 't',
            partitions: [
              { partition: 0, messages: [{ value: 'a' }] },
              { partition: 1, messages: [{ value: 42 as unknown as string }] },
            ],
          },
        ],
      }).encode();

      expect(encoding).toBeInstanceOf(Promise);
      await expect(encoding).rejects.toThrow();
      // Let the first partition's codec failure settle: an unobserved rejection would fail this run.
      await new Promise((resolve) => setImmediate(resolve));
    });

    it.each([
      ['uncompressed', COMPRESSION_TYPES.None],
      ['GZIP', COMPRESSION_TYPES.GZIP],
    ])('keeps every %s topic and partition in request order', async (_label, compression) => {
      const encoded = await createProduceRequest(3, {
        acks: 1,
        timeout: 1000,
        compression,
        topicData: [
          {
            topic: 'orders',
            partitions: [
              { partition: 2, messages: [{ value: 'o2' }] },
              { partition: 0, messages: [{ value: 'o0' }] },
            ],
          },
          { topic: 'payments', partitions: [{ partition: 1, messages: [{ value: 'p1' }] }] },
        ],
      }).encode();

      const decoder = new Decoder(encoded.buffer);
      decoder.readString(); // transactionalId
      decoder.readInt16(); // acks
      decoder.readInt32(); // timeout
      const topics = decoder.readArray((topicDecoder) => ({
        topic: topicDecoder.readString(),
        partitions: topicDecoder.readArray((partitionDecoder) => ({
          partition: partitionDecoder.readInt32(),
          recordSet: partitionDecoder.readBytes() ?? Buffer.alloc(0),
        })),
      }));
      const decoded = await Promise.all(
        topics.map(async ({ topic, partitions }) => ({
          topic,
          partitions: await Promise.all(
            partitions.map(async ({ partition, recordSet }) => {
              const batch = await decodeRecordBatch(new Decoder(recordSet));
              return { partition, values: batch.records.map((record) => record.value?.toString()) };
            }),
          ),
        })),
      );

      expect(decoded).toEqual([
        {
          topic: 'orders',
          partitions: [
            { partition: 2, values: ['o2'] },
            { partition: 0, values: ['o0'] },
          ],
        },
        { topic: 'payments', partitions: [{ partition: 1, values: ['p1'] }] },
      ]);
    });

    it('requires a usable topic UUID on Produce v13', () => {
      const request = createProduceRequest(13, {
        acks: 1,
        timeout: 1000,
        topicData: [{ topic: 'orders', partitions: [{ partition: 0, messages: [{ value: 'a' }] }] }],
      });
      expect(() => request.encode()).toThrow(/Produce v13 requires a 16-byte topicId/);
    });

    it('encodes Produce v13 with a usable topic UUID', async () => {
      const encoded = await createProduceRequest(13, {
        acks: 1,
        timeout: 1000,
        topicData: [
          {
            topic: 'orders',
            topicId: Buffer.alloc(16, 7),
            partitions: [{ partition: 0, messages: [{ value: 'a' }] }],
          },
        ],
      }).encode();
      expect(encoded.size()).toBeGreaterThan(16);
    });

    it('returns its record batch buffers to the pool once the request body is written', async () => {
      const value = Buffer.alloc(1024, 1);
      const request = createProduceRequest(3, {
        acks: 1,
        timeout: 1000,
        topicData: [
          {
            topic: 't',
            partitions: [0, 1, 2].map((partition) => ({
              partition,
              messages: Array.from({ length: 20 }, () => ({ value })),
            })),
          },
        ],
      });

      clearBufferPool();
      (await request.encode()).release();
      resetBufferPoolStats();
      (await request.encode()).release();

      // Every buffer the second encode needs, batches included, comes back from the first one.
      expect(bufferPoolStats().allocCount).toBe(0);
    });
  });

  describe('readProducePartitionTaggedFields', () => {
    it('returns null when there are no tagged fields', () => {
      const decoder = new Decoder(new Encoder().writeUVarInt(0).buffer);
      expect(readProducePartitionTaggedFields(decoder)).toBeNull();
    });

    it('reads CurrentLeader from tag 0 and skips unknown tags', () => {
      const decoder = new Decoder(
        new Encoder()
          .writeUVarInt(2)
          .writeUVarInt(99)
          .writeUVarInt(1)
          .writeInt8(0)
          .writeUVarInt(0)
          .writeUVarInt(8)
          .writeInt32(4)
          .writeInt32(11).buffer,
      );
      expect(readProducePartitionTaggedFields(decoder)).toEqual({ leaderId: 4, leaderEpoch: 11 });
    });
  });

  describe('readProduceResponseNodeEndpoints', () => {
    it('returns an empty list when there are no tagged fields', () => {
      const decoder = new Decoder(new Encoder().writeUVarInt(0).buffer);
      expect(readProduceResponseNodeEndpoints(decoder)).toEqual([]);
    });

    it('reads NodeEndpoints from tag 0', () => {
      const endpoints = new Encoder()
        .writeUVarInt(2)
        .writeInt32(3)
        .writeUVarIntString('broker-3')
        .writeInt32(9092)
        .writeUVarInt(0)
        .writeUVarInt(0);
      const decoder = new Decoder(
        new Encoder().writeUVarInt(1).writeUVarInt(0).writeUVarInt(endpoints.size()).writeEncoder(endpoints).buffer,
      );
      expect(readProduceResponseNodeEndpoints(decoder)).toEqual([
        { nodeId: 3, host: 'broker-3', port: 9092, rack: null },
      ]);
    });
  });
});
