import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { decodeRecordBatch } from '../../records/batch';
import {
  createProduceRequest,
  isUsableTopicId,
  readProducePartitionTaggedFields,
  readProduceResponseNodeEndpoints,
  resolveProduceTopicName,
} from './shared';

async function decodeFirstBatch(messages: { value: string; timestamp?: number }[]) {
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
  return decodeRecordBatch(new Decoder(recordSet));
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

    it('requires a usable topic UUID on Produce v13', async () => {
      await expect(
        createProduceRequest(13, {
          acks: 1,
          timeout: 1000,
          topicData: [{ topic: 'orders', partitions: [{ partition: 0, messages: [{ value: 'a' }] }] }],
        }).encode(),
      ).rejects.toThrow(/Produce v13 requires a 16-byte topicId/);
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
