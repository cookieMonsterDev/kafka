import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../errors';
import {
  AUTHORIZED_OPERATIONS_OMITTED,
  checkTopicMetadataErrors,
  metadataRequestTopicEntries,
  ZERO_TOPIC_ID,
} from './shared';

describe('protocol/requests/metadata/shared', () => {
  it('uses Integer.MIN_VALUE for omitted authorized operations and a zero UUID', () => {
    expect(AUTHORIZED_OPERATIONS_OMITTED).toBe(-2147483648);
    expect(ZERO_TOPIC_ID).toEqual(Buffer.alloc(16));
  });

  describe('metadataRequestTopicEntries', () => {
    it('encodes an empty query as a null array (all topics)', () => {
      expect(metadataRequestTopicEntries()).toBeNull();
      expect(metadataRequestTopicEntries([], [])).toBeNull();
    });

    it('pairs name queries with a zero UUID and id queries with a null name', () => {
      const topicId = Buffer.alloc(16, 9);
      expect(metadataRequestTopicEntries(['orders', 'payments'], [topicId])).toEqual([
        { name: 'orders', topicId: ZERO_TOPIC_ID },
        { name: 'payments', topicId: ZERO_TOPIC_ID },
        { name: null, topicId },
      ]);
    });
  });

  describe('checkTopicMetadataErrors', () => {
    it('returns when every topic and partition succeeded', () => {
      expect(() =>
        checkTopicMetadataErrors([
          { topicErrorCode: 0, topic: 'orders', partitionMetadata: [{ partitionErrorCode: 0, partitionId: 0 }] },
        ]),
      ).not.toThrow();
    });

    it('throws a topic-level error before inspecting partitions', () => {
      const failing = () =>
        checkTopicMetadataErrors([
          {
            topicErrorCode: 3,
            topic: 'missing',
            partitionMetadata: [{ partitionErrorCode: 5, partitionId: 0 }],
          },
        ]);
      expect(failing).toThrow(KafkaProtocolError);
      expect(failing).toThrow('topic: missing');
      expect(failing).not.toThrow(/partition:/);
    });

    it('throws a partition-level error with topic and partition extras', () => {
      const failing = () =>
        checkTopicMetadataErrors([
          {
            topicErrorCode: 0,
            topic: 'orders',
            partitionMetadata: [
              { partitionErrorCode: 0, partitionId: 0 },
              { partitionErrorCode: 6, partitionId: 2 },
            ],
          },
        ]);
      expect(failing).toThrow('topic: orders');
      expect(failing).toThrow('partition: 2');
    });

    it('omits topic extras when the topic name is null', () => {
      expect(() => checkTopicMetadataErrors([{ topicErrorCode: 3, topic: null, partitionMetadata: [] }])).not.toThrow(
        /topic:/,
      );
    });
  });
});
