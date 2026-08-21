import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { ZERO_TOPIC_ID } from '../protocol/requests/metadata/shared';
import { createTopicsApi } from './topics';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const topicId = Buffer.from('0123456789abcdef');

function partitionBody(partitionIndex: number) {
  return {
    errorCode: 0,
    partitionIndex,
    leader: 1,
    leaderEpoch: 0,
    replicas: [1],
    isr: [1],
    eligibleLeaderReplicas: null,
    lastKnownElr: null,
    offlineReplicas: [],
  };
}

function fakeCluster(
  describeTopicPartitions = vi.fn().mockResolvedValue({
    topics: [
      {
        errorCode: 0,
        topic: 'orders',
        topicId,
        isInternal: false,
        partitions: [partitionBody(0)],
        topicAuthorizedOperations: -2147483648,
      },
    ],
    nextCursor: null,
  }),
) {
  const broker = { describeTopicPartitions };
  return {
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    broker,
  };
}

describe('admin/topics describeTopicPartitions', () => {
  it('sends name-only topics to the controller and returns one page plus nextCursor', async () => {
    const cluster = fakeCluster();
    const api = createTopicsApi(
      {
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      },
      { fetchTopicOffsets: vi.fn() },
    );

    await expect(api.describeTopicPartitions({ topics: ['orders'] })).resolves.toEqual({
      topics: [
        {
          name: 'orders',
          topicId,
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: [
            {
              partitionIndex: 0,
              leader: 1,
              leaderEpoch: 0,
              replicas: [1],
              isr: [1],
              eligibleLeaderReplicas: null,
              lastKnownElr: null,
              offlineReplicas: [],
            },
          ],
        },
      ],
      nextCursor: null,
    });

    expect(cluster.refreshMetadata).toHaveBeenCalled();
    expect(cluster.findControllerBroker).toHaveBeenCalled();
    expect(cluster.broker.describeTopicPartitions).toHaveBeenCalledWith({
      topics: [{ topic: 'orders' }],
      responsePartitionLimit: 2000,
      cursor: null,
    });
  });

  it('accepts optional topicId on input and still sends the topic name', async () => {
    const cluster = fakeCluster();
    const api = createTopicsApi(
      {
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      },
      { fetchTopicOffsets: vi.fn() },
    );

    await api.describeTopicPartitions({
      topics: [{ topic: 'orders', topicId: ZERO_TOPIC_ID }],
    });

    expect(cluster.broker.describeTopicPartitions).toHaveBeenCalledWith({
      topics: [{ topic: 'orders' }],
      responsePartitionLimit: 2000,
      cursor: null,
    });
  });

  it('passes a cursor and responsePartitionLimit through for pagination', async () => {
    const nextCursor = { topic: 'orders', partitionIndex: 2 };
    const cluster = fakeCluster(
      vi.fn().mockResolvedValue({
        topics: [
          {
            errorCode: 0,
            topic: 'orders',
            topicId,
            isInternal: false,
            partitions: [partitionBody(1)],
            topicAuthorizedOperations: -2147483648,
          },
        ],
        nextCursor,
      }),
    );
    const api = createTopicsApi(
      {
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      },
      { fetchTopicOffsets: vi.fn() },
    );

    const cursor = { topic: 'orders', partitionIndex: 1 };
    await expect(
      api.describeTopicPartitions({
        topics: [{ topic: 'orders' }],
        responsePartitionLimit: 1,
        cursor,
      }),
    ).resolves.toEqual(expect.objectContaining({ nextCursor }));

    expect(cluster.broker.describeTopicPartitions).toHaveBeenCalledWith({
      topics: [{ topic: 'orders' }],
      responsePartitionLimit: 1,
      cursor,
    });
  });

  it('rejects invalid topics, topicId, cursor, and partition limit', async () => {
    const cluster = fakeCluster();
    const api = createTopicsApi(
      {
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      },
      { fetchTopicOffsets: vi.fn() },
    );

    await expect(api.describeTopicPartitions({ topics: null as never })).rejects.toThrow(KafkaNonRetriableError);
    await expect(api.describeTopicPartitions({ topics: [''] })).rejects.toThrow(KafkaNonRetriableError);
    await expect(
      api.describeTopicPartitions({ topics: [{ topic: 'orders', topicId: Buffer.from([1, 2]) }] }),
    ).rejects.toThrow('Invalid topicId');
    await expect(api.describeTopicPartitions({ topics: ['orders'], responsePartitionLimit: 0 })).rejects.toThrow(
      'Invalid responsePartitionLimit',
    );
    await expect(
      api.describeTopicPartitions({ topics: ['orders'], cursor: { topic: '', partitionIndex: 0 } }),
    ).rejects.toThrow('Invalid cursor topic');
    expect(cluster.findControllerBroker).not.toHaveBeenCalled();
  });
});
