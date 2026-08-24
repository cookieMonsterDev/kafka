import { describe, expect, it } from 'vitest';
import { KafkaOffsetOutOfRange, KafkaProtocolError } from '../../../errors';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { encodeMessageSet } from '../../message-set/index';
import {
  decodeCompactRecordSet,
  decodeRecordSet,
  fetchRequestHasUsableTopicIds,
  isUsableTopicId,
  parseFetchResponse,
  readFetchPartitionTaggedFields,
  readFetchResponseNodeEndpoints,
  readTopicName,
  resolveFetchTopicName,
  type FetchRequestOptions,
} from './shared';

function fetchOptions(overrides: Partial<FetchRequestOptions> = {}): FetchRequestOptions {
  return {
    replicaId: -1,
    maxWaitTime: 100,
    minBytes: 1,
    maxBytes: 1_048_576,
    topics: [],
    ...overrides,
  };
}

describe('protocol/requests/fetch/shared', () => {
  describe('isUsableTopicId', () => {
    it('is true only for a non-zero 16-byte UUID', () => {
      expect(isUsableTopicId(undefined)).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(16))).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(15, 1))).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(16, 1))).toBe(true);
    });
  });

  describe('fetchRequestHasUsableTopicIds', () => {
    const usable = Buffer.alloc(16, 1);
    const zero = Buffer.alloc(16);

    it('requires every topic and forgotten topic to carry a usable UUID', () => {
      expect(
        fetchRequestHasUsableTopicIds(fetchOptions({ topics: [{ topic: 'orders', topicId: usable, partitions: [] }] })),
      ).toBe(true);
      expect(fetchRequestHasUsableTopicIds(fetchOptions({ topics: [{ topic: 'orders', partitions: [] }] }))).toBe(
        false,
      );
      expect(
        fetchRequestHasUsableTopicIds(
          fetchOptions({
            topics: [{ topic: 'orders', topicId: usable, partitions: [] }],
            forgottenTopics: [{ topic: 'old', topicId: zero, partitions: [] }],
          }),
        ),
      ).toBe(false);
      expect(
        fetchRequestHasUsableTopicIds(
          fetchOptions({
            topics: [{ topic: 'orders', topicId: usable, partitions: [] }],
            forgottenTopics: [{ topic: 'old', topicId: Buffer.alloc(16, 2), partitions: [0] }],
          }),
        ),
      ).toBe(true);
    });
  });

  describe('resolveFetchTopicName', () => {
    const ordersId = Buffer.alloc(16, 1);
    const topics = [
      { topic: 'orders', topicId: ordersId, partitions: [] },
      { topic: 'payments', topicId: Buffer.alloc(16, 2), partitions: [] },
    ];

    it('resolves by topic UUID, then by index, then to an empty string', () => {
      expect(resolveFetchTopicName(ordersId, 1, topics)).toBe('orders');
      expect(resolveFetchTopicName(Buffer.alloc(16, 9), 1, topics)).toBe('payments');
      expect(resolveFetchTopicName(Buffer.alloc(16, 9), 99, topics)).toBe('');
    });
  });

  describe('readTopicName', () => {
    it('reads a non-null STRING', () => {
      const decoder = new Decoder(new Encoder().writeString('orders').buffer);
      expect(readTopicName(decoder)).toBe('orders');
    });

    it('throws when the broker encodes a null topic name', () => {
      const decoder = new Decoder(new Encoder().writeString(null).buffer);
      expect(() => readTopicName(decoder)).toThrow(RangeError);
    });
  });

  describe('readFetchPartitionTaggedFields', () => {
    it('returns null when there are no tagged fields', () => {
      const decoder = new Decoder(new Encoder().writeUVarInt(0).buffer);
      expect(readFetchPartitionTaggedFields(decoder)).toBeNull();
    });

    it('reads CurrentLeader from tag 1 and skips DivergingEpoch (tag 0)', () => {
      const decoder = new Decoder(
        new Encoder()
          .writeUVarInt(2)
          .writeUVarInt(0)
          .writeUVarInt(8)
          .writeInt32(1)
          .writeInt32(2)
          .writeUVarInt(1)
          .writeUVarInt(8)
          .writeInt32(5)
          .writeInt32(9).buffer,
      );
      expect(readFetchPartitionTaggedFields(decoder)).toEqual({ leaderId: 5, leaderEpoch: 9 });
    });
  });

  describe('readFetchResponseNodeEndpoints', () => {
    it('returns an empty list when there are no tagged fields', () => {
      const decoder = new Decoder(new Encoder().writeUVarInt(0).buffer);
      expect(readFetchResponseNodeEndpoints(decoder)).toEqual([]);
    });

    it('reads NodeEndpoints from tag 0', () => {
      const endpoints = new Encoder()
        .writeUVarInt(2)
        .writeInt32(4)
        .writeUVarIntString('broker-4')
        .writeInt32(9093)
        .writeUVarIntString('az-b')
        .writeUVarInt(0);
      const decoder = new Decoder(
        new Encoder().writeUVarInt(1).writeUVarInt(0).writeUVarInt(endpoints.size()).writeEncoder(endpoints).buffer,
      );
      expect(readFetchResponseNodeEndpoints(decoder)).toEqual([
        { nodeId: 4, host: 'broker-4', port: 9093, rack: 'az-b' },
      ]);
    });
  });

  describe('parseFetchResponse', () => {
    it('returns the body when every partition succeeded', async () => {
      const data = {
        responses: [{ topicName: 'orders', partitions: [{ errorCode: 0, partition: 0 }] }],
      };
      await expect(parseFetchResponse(data)).resolves.toBe(data);
    });

    it('throws KafkaOffsetOutOfRange with topic and partition extras', async () => {
      await expect(
        parseFetchResponse({
          responses: [{ topicName: 'orders', partitions: [{ errorCode: 1, partition: 3 }] }],
        }),
      ).rejects.toMatchObject({
        name: 'KafkaOffsetOutOfRange',
        topic: 'orders',
        partition: 3,
      });
      await expect(
        parseFetchResponse({
          responses: [{ topicName: 'orders', partitions: [{ errorCode: 1, partition: 3 }] }],
        }),
      ).rejects.toBeInstanceOf(KafkaOffsetOutOfRange);
    });

    it('throws a protocol error with CurrentLeader extras for a stale-leader code', async () => {
      const currentLeader = { leaderId: 2, leaderEpoch: 8 };
      const nodeEndpoints = [{ nodeId: 2, host: 'b2', port: 9092, rack: null }];
      const error = await parseFetchResponse({
        responses: [{ topicName: 'orders', partitions: [{ errorCode: 6, partition: 0, currentLeader }] }],
        nodeEndpoints,
      }).catch((e) => e);

      expect(error).toBeInstanceOf(KafkaProtocolError);
      expect(error.type).toBe('NOT_LEADER_OR_FOLLOWER');
      expect(error.currentLeader).toEqual(currentLeader);
      expect(error.nodeEndpoints).toEqual(nodeEndpoints);
    });
  });

  describe('decodeRecordSet', () => {
    it('returns an empty list for a zero, negative, or truncated size prefix', async () => {
      await expect(decodeRecordSet(new Decoder(new Encoder().writeInt32(0).buffer))).resolves.toEqual([]);
      await expect(decodeRecordSet(new Decoder(new Encoder().writeInt32(-1).buffer))).resolves.toEqual([]);
      await expect(decodeRecordSet(new Decoder(new Encoder().writeInt32(64).buffer))).resolves.toEqual([]);
    });

    it('returns an empty list when the payload is shorter than the magic-byte offset', async () => {
      const payload = Buffer.alloc(8);
      const decoder = new Decoder(new Encoder().writeInt32(payload.length).writeBuffer(payload).buffer);
      await expect(decodeRecordSet(decoder)).resolves.toEqual([]);
    });

    it('decodes a MessageSet payload (magic < 2)', async () => {
      const messageSet = encodeMessageSet({
        messageVersion: 0,
        entries: [{ key: 'k', value: 'v' }],
      });
      const decoder = new Decoder(new Encoder().writeInt32(messageSet.size()).writeEncoder(messageSet).buffer);
      const records = await decodeRecordSet(decoder);
      expect(records).toHaveLength(1);
      expect(records[0]?.key?.toString()).toBe('k');
      expect(records[0]?.value?.toString()).toBe('v');
    });
  });

  describe('decodeCompactRecordSet', () => {
    it('returns an empty list for a null compact records field', async () => {
      const decoder = new Decoder(new Encoder().writeUVarInt(0).buffer);
      await expect(decodeCompactRecordSet(decoder)).resolves.toEqual([]);
    });

    it('decodes a compact MessageSet payload', async () => {
      const messageSet = encodeMessageSet({
        messageVersion: 0,
        entries: [{ key: 'k', value: 'v' }],
      });
      const decoder = new Decoder(new Encoder().writeUVarInt(messageSet.size() + 1).writeEncoder(messageSet).buffer);
      const records = await decodeCompactRecordSet(decoder);
      expect(records[0]?.value?.toString()).toBe('v');
    });
  });
});
