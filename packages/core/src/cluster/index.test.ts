import { describe, expect, it, vi } from 'vitest';
import { Broker } from '../broker/index';
import { KafkaTopicMetadataNotLoaded } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createDefaultSocketFactory } from '../network/socket-factory';
import type { MetadataResponseV6Body } from '../protocol/requests/metadata/v6/response';
import { Cluster } from './index';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function createCluster(overrides: Partial<ConstructorParameters<typeof Cluster>[0]> = {}): Cluster {
  return new Cluster({
    logger: silentLogger,
    socketFactory: createDefaultSocketFactory(),
    brokers: ['broker-1:9092'],
    clientId: 'test-client',
    connectionTimeout: 1000,
    ...overrides,
  });
}

function fakeMetadata(overrides: Partial<MetadataResponseV6Body> = {}): MetadataResponseV6Body {
  return {
    brokers: [],
    topicMetadata: [],
    throttleTime: 0,
    clusterId: null,
    controllerId: 0,
    clientSideThrottleTime: 0,
    ...overrides,
  };
}

describe('cluster/Cluster', () => {
  describe('findTopicPartitionMetadata', () => {
    it('throws KafkaTopicMetadataNotLoaded when metadata has never been fetched', () => {
      const cluster = createCluster();
      expect(() => cluster.findTopicPartitionMetadata('my-topic')).toThrow(KafkaTopicMetadataNotLoaded);
    });

    it('returns an empty array for a topic missing from metadata', () => {
      const cluster = createCluster();
      cluster.brokerPool.metadata = fakeMetadata({ topicMetadata: [] });
      expect(cluster.findTopicPartitionMetadata('missing-topic')).toEqual([]);
    });

    it('returns the partition metadata for a known topic', () => {
      const cluster = createCluster();
      const partitionMetadata = [
        { partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1], offlineReplicas: [] },
      ];
      cluster.brokerPool.metadata = fakeMetadata({
        topicMetadata: [{ topicErrorCode: 0, topic: 'my-topic', isInternal: false, partitionMetadata }],
      });

      expect(cluster.findTopicPartitionMetadata('my-topic')).toBe(partitionMetadata);
    });
  });

  describe('findLeaderForPartitions', () => {
    it('groups requested partitions by their leader nodeId', () => {
      const cluster = createCluster();
      cluster.brokerPool.metadata = fakeMetadata({
        topicMetadata: [
          {
            topicErrorCode: 0,
            topic: 'my-topic',
            isInternal: false,
            partitionMetadata: [
              { partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1], offlineReplicas: [] },
              { partitionErrorCode: 0, partitionId: 1, leader: 2, replicas: [2], isr: [2], offlineReplicas: [] },
              { partitionErrorCode: 0, partitionId: 2, leader: 1, replicas: [1], isr: [1], offlineReplicas: [] },
            ],
          },
        ],
      });

      expect(cluster.findLeaderForPartitions('my-topic', [0, 1, 2])).toEqual({ 1: [0, 2], 2: [1] });
    });

    it('silently skips partitions missing from metadata', () => {
      const cluster = createCluster();
      cluster.brokerPool.metadata = fakeMetadata({
        topicMetadata: [
          {
            topicErrorCode: 0,
            topic: 'my-topic',
            isInternal: false,
            partitionMetadata: [
              { partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1], offlineReplicas: [] },
            ],
          },
        ],
      });

      expect(cluster.findLeaderForPartitions('my-topic', [0, 99])).toEqual({ 1: [0] });
    });
  });

  describe('committedOffsets / markOffsetAsCommitted', () => {
    it('initializes an empty record for a new group and remembers committed offsets', () => {
      const cluster = createCluster();
      expect(cluster.committedOffsets({ groupId: 'g' })).toEqual({});

      cluster.markOffsetAsCommitted({ groupId: 'g', topic: 't', partition: 0, offset: 42n });
      expect(cluster.committedOffsets({ groupId: 'g' })).toEqual({ t: { 0: 42n } });
    });
  });

  describe('defaultOffset', () => {
    it('resolves to the earliest offset when fromBeginning is true', () => {
      const cluster = createCluster();
      expect(cluster.defaultOffset({ fromBeginning: true })).toBe(-2n);
    });

    it('resolves to the latest offset when fromBeginning is false', () => {
      const cluster = createCluster();
      expect(cluster.defaultOffset({ fromBeginning: false })).toBe(-1n);
    });
  });

  describe('addMultipleTargetTopics', () => {
    it('adds the topics and refreshes metadata when the set changes', async () => {
      const cluster = createCluster();
      const refreshSpy = vi.spyOn(cluster, 'refreshMetadata').mockResolvedValue(undefined);

      await cluster.addMultipleTargetTopics(['topic-a', 'topic-b']);

      expect(cluster.targetTopics).toEqual(new Set(['topic-a', 'topic-b']));
      expect(refreshSpy).toHaveBeenCalledOnce();
    });

    it('does not refresh metadata again when the topic set is unchanged and metadata already exists', async () => {
      const cluster = createCluster();
      cluster.brokerPool.metadata = fakeMetadata();
      const refreshSpy = vi.spyOn(cluster, 'refreshMetadata').mockResolvedValue(undefined);

      await cluster.addMultipleTargetTopics(['topic-a']);
      await cluster.addMultipleTargetTopics(['topic-a']);

      expect(refreshSpy).toHaveBeenCalledTimes(1);
    });

    it('reverts the target topic set when the refresh fails with an unknown-topic error', async () => {
      const cluster = createCluster();
      vi.spyOn(cluster, 'refreshMetadata').mockRejectedValue(
        Object.assign(new Error('nope'), { type: 'UNKNOWN_TOPIC_OR_PARTITION' }),
      );

      await expect(cluster.addMultipleTargetTopics(['topic-a'])).rejects.toThrow('nope');
      expect(cluster.targetTopics.size).toBe(0);
    });

    it('keeps the target topic set when the refresh fails with an unrelated error', async () => {
      const cluster = createCluster();
      vi.spyOn(cluster, 'refreshMetadata').mockRejectedValue(new Error('connection lost'));

      await expect(cluster.addMultipleTargetTopics(['topic-a'])).rejects.toThrow('connection lost');
      expect(cluster.targetTopics).toEqual(new Set(['topic-a']));
    });

    it('drops only the named topic when metadata refresh reports it unknown', async () => {
      const cluster = createCluster({ allowAutoTopicCreation: false });
      cluster.targetTopics = new Set(['keep', 'gone']);
      const refreshMetadata = vi
        .spyOn(cluster.brokerPool, 'refreshMetadata')
        .mockRejectedValueOnce(
          Object.assign(new Error('This server does not host this topic-partition'), {
            type: 'UNKNOWN_TOPIC_OR_PARTITION',
            topic: 'gone',
          }),
        )
        .mockResolvedValueOnce(undefined);

      await cluster.refreshMetadata();

      expect(cluster.targetTopics).toEqual(new Set(['keep']));
      expect(refreshMetadata).toHaveBeenCalledTimes(2);
    });
  });

  describe('findBroker', () => {
    it('refreshes metadata and rethrows when the broker pool reports a stale broker', async () => {
      const cluster = createCluster();
      vi.spyOn(cluster.brokerPool, 'findBroker').mockRejectedValue(
        Object.assign(new Error('not found'), { name: 'KafkaBrokerNotFound' }),
      );
      const refreshSpy = vi.spyOn(cluster, 'refreshMetadata').mockResolvedValue(undefined);

      await expect(cluster.findBroker({ nodeId: '1' })).rejects.toThrow('not found');
      expect(refreshSpy).toHaveBeenCalledOnce();
    });

    it('returns the broker pool result directly on success', async () => {
      const cluster = createCluster();
      const broker = new Broker({
        connectionPool: { host: 'x', port: 1, connectionTimeout: 1 } as never,
        logger: silentLogger,
      });
      vi.spyOn(cluster.brokerPool, 'findBroker').mockResolvedValue(broker);

      await expect(cluster.findBroker({ nodeId: '1' })).resolves.toBe(broker);
    });
  });
});
