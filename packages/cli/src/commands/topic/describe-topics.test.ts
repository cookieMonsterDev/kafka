import { describe, expect, it } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import {
  describeTopics,
  normalizeDescribeTopicPartitionsResult,
  normalizeFetchTopicMetadataResult,
} from './describe-topics';

describe('normalizeDescribeTopicPartitionsResult', () => {
  it('maps partitionIndex/leader/replicas/isr straight through', () => {
    const normalized = normalizeDescribeTopicPartitionsResult({
      nextCursor: null,
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
    });
    expect(normalized).toEqual([
      {
        name: 'orders',
        topicId: Buffer.alloc(16),
        partitions: [{ partitionIndex: 0, leader: 1, replicas: [1, 2], isr: [1, 2] }],
      },
    ]);
  });
});

describe('normalizeFetchTopicMetadataResult', () => {
  it('maps partitionId to partitionIndex', () => {
    const normalized = normalizeFetchTopicMetadataResult({
      topics: [
        {
          name: 'orders',
          partitions: [{ partitionId: 0, leader: 1, replicas: [1, 2], isr: [1, 2] }],
        },
      ],
    });
    expect(normalized).toEqual([
      {
        name: 'orders',
        topicId: undefined,
        partitions: [{ partitionIndex: 0, leader: 1, replicas: [1, 2], isr: [1, 2] }],
      },
    ]);
  });
});

describe('describeTopics', () => {
  it('uses describeTopicPartitions when the broker supports it', async () => {
    const admin = createFakeAdmin({
      describeTopicPartitions: async ({ topics }) => ({
        nextCursor: null,
        topics: topics.map((topic) => ({
          name: typeof topic === 'string' ? topic : topic.topic,
          topicId: Buffer.alloc(16),
          isInternal: false,
          topicAuthorizedOperations: 0,
          partitions: [],
        })),
      }),
      fetchTopicMetadata: () => {
        throw new Error('should not fall back');
      },
    });

    const result = await describeTopics(admin, ['orders']);
    expect(result).toEqual([{ name: 'orders', topicId: Buffer.alloc(16), partitions: [] }]);
  });

  it('falls back to fetchTopicMetadata when the broker does not support describeTopicPartitions', async () => {
    const unsupported = Object.assign(new Error('too old'), { name: 'KafkaServerDoesNotSupportApiKey' });
    const admin = createFakeAdmin({
      describeTopicPartitions: async () => {
        throw unsupported;
      },
      fetchTopicMetadata: async ({ topics = [] }: { topics?: string[] } = {}) => ({
        topics: topics.map((name) => ({
          name,
          partitions: [
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 1,
              leaderEpoch: 0,
              replicas: [1],
              isr: [1],
              offlineReplicas: [],
            },
          ],
        })),
      }),
    });

    const result = await describeTopics(admin, ['orders']);
    expect(result).toEqual([
      { name: 'orders', topicId: undefined, partitions: [{ partitionIndex: 0, leader: 1, replicas: [1], isr: [1] }] },
    ]);
  });

  it('re-throws any other error without falling back', async () => {
    const admin = createFakeAdmin({
      describeTopicPartitions: async () => {
        throw new Error('connection reset');
      },
    });

    await expect(describeTopics(admin, ['orders'])).rejects.toThrow('connection reset');
  });
});
