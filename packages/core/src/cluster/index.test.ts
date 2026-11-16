import { afterEach, describe, expect, it, vi } from 'vitest';
import { Broker } from '../broker/index';
import { KafkaBrokerNotFound, KafkaConnectionClosedError, KafkaTopicMetadataNotLoaded } from '../errors';
import { ClientTelemetryReporter } from '../instrumentation/client-telemetry';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createDefaultSocketFactory } from '../network/socket-factory';
import type { ClusterMetadata } from '../protocol/requests/metadata/shared';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import { createErrorFromCode, ERROR_CODES } from '../protocol/error-codes';
import { Cluster } from './index';
import { ANY_BROKER, CONTROLLER, coordinatorTarget, leaderTarget, nodeTarget } from './run';

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

function fakeMetadata(overrides: Partial<ClusterMetadata> = {}): ClusterMetadata {
  return {
    brokers: [],
    topicMetadata: [],
    throttleTime: 0,
    clusterId: null,
    controllerId: 0,
    clientSideThrottleTime: 0,
    clusterAuthorizedOperations: -2147483648,
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

    it('looks up partitions by topic name when topicId is present', () => {
      const cluster = createCluster();
      const partitionMetadata = [
        { partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1], offlineReplicas: [] },
      ];
      const topicId = Buffer.from('0123456789abcdef');
      cluster.brokerPool.metadata = fakeMetadata({
        topicMetadata: [{ topicErrorCode: 0, topic: 'my-topic', topicId, isInternal: false, partitionMetadata }],
      });

      expect(cluster.findTopicPartitionMetadata('my-topic')).toBe(partitionMetadata);
    });
  });

  describe('findTopicId', () => {
    it('throws KafkaTopicMetadataNotLoaded when metadata has never been fetched', () => {
      const cluster = createCluster();
      expect(() => cluster.findTopicId('my-topic')).toThrow(KafkaTopicMetadataNotLoaded);
    });

    it('returns undefined when the topic has no usable topicId', () => {
      const cluster = createCluster();
      cluster.brokerPool.metadata = fakeMetadata({
        topicMetadata: [{ topicErrorCode: 0, topic: 'my-topic', isInternal: false, partitionMetadata: [] }],
      });
      expect(cluster.findTopicId('my-topic')).toBeUndefined();
    });

    it('returns the 16-byte topicId from metadata', () => {
      const cluster = createCluster();
      const topicId = Buffer.from('0123456789abcdef');
      cluster.brokerPool.metadata = fakeMetadata({
        topicMetadata: [{ topicErrorCode: 0, topic: 'my-topic', topicId, isInternal: false, partitionMetadata: [] }],
      });
      expect(cluster.findTopicId('my-topic')).toEqual(topicId);
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

  describe('committedOffset / markOffsetAsCommitted / forgetCommittedOffset', () => {
    it('remembers committed offsets per group until they are forgotten', () => {
      const cluster = createCluster();
      const partition = { topic: 't', partition: 0 };
      expect(cluster.committedOffset({ groupId: 'g', ...partition })).toBeUndefined();

      cluster.markOffsetAsCommitted({ groupId: 'g', ...partition, offset: 42n });
      expect(cluster.committedOffset({ groupId: 'g', ...partition })).toBe(42n);
      expect(cluster.committedOffset({ groupId: 'other', ...partition })).toBeUndefined();

      cluster.forgetCommittedOffset({ groupId: 'g', ...partition });
      expect(cluster.committedOffset({ groupId: 'g', ...partition })).toBeUndefined();
    });

    it('keeps a topic named __proto__ as an ordinary key', () => {
      const cluster = createCluster();
      cluster.markOffsetAsCommitted({ groupId: 'g', topic: '__proto__', partition: 1, offset: 7n });
      expect(cluster.committedOffset({ groupId: 'g', topic: '__proto__', partition: 1 })).toBe(7n);
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

    it('returns without locking when every topic is already targeted and metadata is loaded', async () => {
      const cluster = createCluster();
      cluster.brokerPool.metadata = fakeMetadata();
      cluster.targetTopics = new Set(['topic-a', 'topic-b']);
      const acquireSpy = vi.spyOn(cluster.mutatingTargetTopics, 'acquire');
      const refreshSpy = vi.spyOn(cluster, 'refreshMetadata').mockResolvedValue(undefined);

      await cluster.addMultipleTargetTopics(['topic-a', 'topic-b']);

      expect(acquireSpy).not.toHaveBeenCalled();
      expect(refreshSpy).not.toHaveBeenCalled();
      expect(cluster.targetTopics).toEqual(new Set(['topic-a', 'topic-b']));
    });

    it('locks when adding a topic that is not yet targeted', async () => {
      const cluster = createCluster();
      cluster.brokerPool.metadata = fakeMetadata();
      const acquireSpy = vi.spyOn(cluster.mutatingTargetTopics, 'acquire');
      vi.spyOn(cluster, 'refreshMetadata').mockResolvedValue(undefined);

      await cluster.addMultipleTargetTopics(['new-topic']);

      expect(acquireSpy).toHaveBeenCalledOnce();
      expect(cluster.targetTopics.has('new-topic')).toBe(true);
    });

    it('locks when topics are known but metadata has not been loaded', async () => {
      const cluster = createCluster();
      cluster.targetTopics = new Set(['topic-a']);
      const acquireSpy = vi.spyOn(cluster.mutatingTargetTopics, 'acquire');
      const refreshSpy = vi.spyOn(cluster, 'refreshMetadata').mockResolvedValue(undefined);

      await cluster.addMultipleTargetTopics(['topic-a']);

      expect(acquireSpy).toHaveBeenCalledOnce();
      expect(refreshSpy).toHaveBeenCalledOnce();
    });

    it('keeps a newly added topic when a concurrent add waits on the lock', async () => {
      const cluster = createCluster();
      let releaseRefresh!: () => void;
      const refreshStarted = new Promise<void>((resolve) => {
        vi.spyOn(cluster, 'refreshMetadata').mockImplementation(
          () =>
            new Promise<void>((release) => {
              resolve();
              releaseRefresh = () => {
                cluster.brokerPool.metadata = fakeMetadata();
                release();
              };
            }),
        );
      });
      const acquireSpy = vi.spyOn(cluster.mutatingTargetTopics, 'acquire');

      const first = cluster.addMultipleTargetTopics(['topic-a']);
      await refreshStarted;
      const second = cluster.addMultipleTargetTopics(['topic-a']);
      releaseRefresh();
      await Promise.all([first, second]);

      expect(cluster.targetTopics).toEqual(new Set(['topic-a']));
      expect(acquireSpy).toHaveBeenCalledTimes(2);
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

  describe('produce fast-path checks', () => {
    it('hasLoadedTargetTopics needs loaded metadata and every topic targeted', () => {
      const cluster = createCluster();
      cluster.targetTopics = new Set(['topic-a']);
      expect(cluster.hasLoadedTargetTopics(['topic-a'])).toBe(false);

      cluster.brokerPool.metadata = fakeMetadata();
      expect(cluster.hasLoadedTargetTopics(['topic-a'])).toBe(true);
      expect(cluster.hasLoadedTargetTopics(['topic-a', 'topic-b'])).toBe(false);
    });

    it('needsMetadataRefresh checks the target topics against the cached metadata', () => {
      const cluster = createCluster();
      cluster.brokerPool.metadata = fakeMetadata({
        topicMetadata: [{ topicErrorCode: 0, topic: 'topic-a', isInternal: false, partitionMetadata: [] }],
      });
      cluster.brokerPool.metadataExpireAt = Date.now() + 60_000;
      cluster.targetTopics = new Set(['topic-a']);
      expect(cluster.needsMetadataRefresh()).toBe(false);

      cluster.targetTopics.add('topic-b');
      expect(cluster.needsMetadataRefresh()).toBe(true);
    });
  });

  describe('resolveTarget', () => {
    const broker = { nodeId: 2 } as unknown as Broker;

    it('resolves the controller from metadata refreshed when needed', async () => {
      const cluster = createCluster();
      const refresh = vi.spyOn(cluster, 'refreshMetadataIfNecessary').mockResolvedValue(undefined);
      vi.spyOn(cluster, 'findControllerBroker').mockResolvedValue(broker);

      await expect(cluster.resolveTarget(CONTROLLER)).resolves.toBe(broker);
      expect(refresh).toHaveBeenCalledOnce();
    });

    it('looks the coordinator up by key and type', async () => {
      const cluster = createCluster();
      const find = vi.spyOn(cluster, 'findGroupCoordinator').mockResolvedValue(broker);

      await expect(cluster.resolveTarget(coordinatorTarget('tx', COORDINATOR_TYPES.TRANSACTION))).resolves.toBe(broker);
      expect(find).toHaveBeenCalledWith({ groupId: 'tx', coordinatorType: COORDINATOR_TYPES.TRANSACTION });
    });

    it("resolves a partition leader from the topic's metadata", async () => {
      const cluster = createCluster();
      const addTopic = vi.spyOn(cluster, 'addTargetTopic').mockResolvedValue(undefined);
      vi.spyOn(cluster, 'refreshMetadataIfNecessary').mockResolvedValue(undefined);
      const findBroker = vi.spyOn(cluster, 'findBroker').mockResolvedValue(broker);
      cluster.brokerPool.metadata = fakeMetadata({
        topicMetadata: [
          {
            topicErrorCode: 0,
            topic: 'orders',
            isInternal: false,
            partitionMetadata: [
              { partitionErrorCode: 0, partitionId: 0, leader: 2, replicas: [2], isr: [2], offlineReplicas: [] },
              { partitionErrorCode: 0, partitionId: 1, leader: -1, replicas: [2], isr: [], offlineReplicas: [] },
            ],
          },
        ],
      });

      await expect(cluster.resolveTarget(leaderTarget('orders', 0))).resolves.toBe(broker);
      expect(addTopic).toHaveBeenCalledWith('orders');
      expect(findBroker).toHaveBeenCalledWith({ nodeId: '2' });

      await expect(cluster.resolveTarget(leaderTarget('orders', 1))).rejects.toBeInstanceOf(KafkaBrokerNotFound);
      await expect(cluster.resolveTarget(leaderTarget('orders', 7))).rejects.toThrow(
        'Could not find leader for orders partition 7',
      );
    });

    it('resolves a node by id and any broker from the pool', async () => {
      const cluster = createCluster();
      const findBroker = vi.spyOn(cluster, 'findBroker').mockResolvedValue(broker);
      vi.spyOn(cluster.brokerPool, 'findConnectedBroker').mockResolvedValue(broker);

      await expect(cluster.resolveTarget(nodeTarget(2))).resolves.toBe(broker);
      expect(findBroker).toHaveBeenCalledWith({ nodeId: '2' });
      await expect(cluster.resolveTarget(ANY_BROKER)).resolves.toBe(broker);
    });
  });

  describe('recover', () => {
    function errorOfType(type: string) {
      return createErrorFromCode(ERROR_CODES.find((entry) => entry.type === type)!.code);
    }

    it('refreshes stale metadata through the update queue, sharing one refresh between callers', async () => {
      const cluster = createCluster();
      const expire = vi.spyOn(cluster.brokerPool, 'expireMetadata');
      const refresh = vi.spyOn(cluster.brokerPool, 'refreshMetadataIfNecessary').mockResolvedValue(undefined);
      const forcedRefresh = vi.spyOn(cluster.brokerPool, 'refreshMetadata');

      await Promise.all([
        cluster.recover(errorOfType('NOT_LEADER_OR_FOLLOWER')),
        cluster.recover(errorOfType('NOT_CONTROLLER')),
      ]);

      expect(expire).toHaveBeenCalledOnce();
      expect(refresh).toHaveBeenCalledOnce();
      expect(forcedRefresh).not.toHaveBeenCalled();
    });

    it('refreshes metadata for a moved coordinator', async () => {
      const cluster = createCluster();
      const refresh = vi.spyOn(cluster.brokerPool, 'refreshMetadataIfNecessary').mockResolvedValue(undefined);

      await cluster.recover(errorOfType('NOT_COORDINATOR_FOR_GROUP'));
      expect(refresh).toHaveBeenCalledOnce();
    });

    it('drops the broker of a closed connection', async () => {
      const cluster = createCluster();
      const remove = vi.spyOn(cluster.brokerPool, 'removeBroker');

      await cluster.recover(new KafkaConnectionClosedError('closed', { host: 'broker-1', port: 9092 }));
      expect(remove).toHaveBeenCalledWith({ host: 'broker-1', port: 9092 });
    });

    it('reconnects a disconnected cluster before refreshing metadata', async () => {
      const cluster = createCluster();
      vi.spyOn(cluster, 'isConnected').mockReturnValue(false);
      const connect = vi.spyOn(cluster, 'connect').mockResolvedValue(undefined);
      const refresh = vi.spyOn(cluster.brokerPool, 'refreshMetadataIfNecessary').mockResolvedValue(undefined);

      await cluster.recover(new KafkaConnectionClosedError('closed'));
      expect(connect).toHaveBeenCalledOnce();
      expect(refresh).toHaveBeenCalledOnce();
    });

    it('does nothing for an error it cannot recover from', async () => {
      const cluster = createCluster();
      const refresh = vi.spyOn(cluster.brokerPool, 'refreshMetadataIfNecessary');

      await cluster.recover(new Error('boom'));
      expect(refresh).not.toHaveBeenCalled();
    });
  });

  describe('onConnectionClosed', () => {
    it('drops the broker a closed connection names and reports it', () => {
      const cluster = createCluster();
      const remove = vi.spyOn(cluster.brokerPool, 'removeBroker');

      expect(cluster.onConnectionClosed(new KafkaConnectionClosedError('closed', { host: 'h', port: 1 }))).toBe(true);
      expect(remove).toHaveBeenCalledWith({ host: 'h', port: 1 });
    });

    it('ignores a closed connection without an address and any other error', () => {
      const cluster = createCluster();
      const remove = vi.spyOn(cluster.brokerPool, 'removeBroker');

      expect(cluster.onConnectionClosed(new KafkaConnectionClosedError('closed'))).toBe(false);
      expect(cluster.onConnectionClosed(new Error('boom'))).toBe(false);
      expect(remove).not.toHaveBeenCalled();
    });
  });

  describe('run', () => {
    it('recovers through the cluster and retries a request that reached a former controller', async () => {
      const cluster = createCluster({ retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 5 } });
      const broker = { nodeId: 1 } as unknown as Broker;
      const resolve = vi.spyOn(cluster, 'resolveTarget').mockResolvedValue(broker);
      const recover = vi.spyOn(cluster, 'recover').mockResolvedValue(undefined);
      const notController = createErrorFromCode(ERROR_CODES.find((entry) => entry.type === 'NOT_CONTROLLER')!.code);
      const work = vi.fn().mockRejectedValueOnce(notController).mockResolvedValue('done');

      await expect(cluster.run(CONTROLLER, work)).resolves.toBe('done');
      expect(resolve).toHaveBeenCalledTimes(2);
      expect(recover).toHaveBeenCalledExactlyOnceWith(notController);
      expect(work).toHaveBeenCalledWith(broker);
    });
  });

  describe('findGroupCoordinator', () => {
    function brokerAt(host: string, port: number): Broker {
      return { connectionPool: { host, port } } as unknown as Broker;
    }

    /** A cluster whose cached broker 1 is at `cached`, while FindCoordinator names node 1 at `found`. */
    function clusterWithCoordinator(cached: Broker, found: { host: string; port: number }) {
      const cluster = createCluster();
      cluster.brokerPool.brokers = { '1': cached };
      vi.spyOn(cluster, 'findGroupCoordinatorMetadata').mockResolvedValue({
        coordinator: { nodeId: 1, ...found },
      } as Awaited<ReturnType<Cluster['findGroupCoordinatorMetadata']>>);
      vi.spyOn(cluster.brokerPool, 'findBroker').mockImplementation(async ({ nodeId }) => {
        const broker = cluster.brokerPool.brokers[nodeId];
        if (!broker) throw new KafkaBrokerNotFound(`Broker ${nodeId} not found`);
        return broker;
      });
      return cluster;
    }

    it('refreshes metadata before returning a coordinator that moved to a new address under the same node id', async () => {
      const moved = brokerAt('new-host', 9093);
      const cluster = clusterWithCoordinator(brokerAt('old-host', 9092), { host: 'new-host', port: 9093 });
      const refresh = vi.spyOn(cluster.brokerPool, 'refreshMetadataIfNecessary').mockImplementation(async () => {
        cluster.brokerPool.brokers = { '1': moved };
      });

      await expect(cluster.findGroupCoordinator({ groupId: 'group' })).resolves.toBe(moved);
      expect(refresh).toHaveBeenCalledOnce();
    });

    it('returns the cached broker without a refresh when the coordinator address matches it', async () => {
      const cached = brokerAt('broker-1', 9092);
      const cluster = clusterWithCoordinator(cached, { host: 'broker-1', port: 9092 });
      const refresh = vi.spyOn(cluster.brokerPool, 'refreshMetadataIfNecessary');

      await expect(cluster.findGroupCoordinator({ groupId: 'group' })).resolves.toBe(cached);
      expect(refresh).not.toHaveBeenCalled();
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

    it('refreshes metadata when the broker pool reports a KafkaConnectionError subclass', async () => {
      const cluster = createCluster();
      vi.spyOn(cluster.brokerPool, 'findBroker').mockRejectedValue(
        new KafkaConnectionClosedError('closed', { host: 'broker-1', port: 9092 }),
      );
      const refreshSpy = vi.spyOn(cluster, 'refreshMetadata').mockResolvedValue(undefined);

      await expect(cluster.findBroker({ nodeId: '1' })).rejects.toThrow('closed');
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

  describe('listTopics / partitionsFor', () => {
    it('listTopics returns named topics from a full metadata fetch', async () => {
      const cluster = createCluster();
      vi.spyOn(cluster, 'connect').mockResolvedValue(undefined);
      vi.spyOn(cluster, 'metadata').mockResolvedValue(
        fakeMetadata({
          topicMetadata: [
            { topicErrorCode: 0, topic: 'orders', isInternal: false, partitionMetadata: [] },
            { topicErrorCode: 0, topic: null, isInternal: true, partitionMetadata: [] },
          ],
        }),
      );

      await expect(cluster.listTopics()).resolves.toEqual(['orders']);
    });

    it('partitionsFor maps cached partition metadata after adding the topic', async () => {
      const cluster = createCluster();
      vi.spyOn(cluster, 'connect').mockResolvedValue(undefined);
      vi.spyOn(cluster, 'addTargetTopic').mockResolvedValue(undefined);
      cluster.brokerPool.metadata = fakeMetadata({
        topicMetadata: [
          {
            topicErrorCode: 0,
            topic: 'orders',
            isInternal: false,
            partitionMetadata: [
              {
                partitionErrorCode: 0,
                partitionId: 1,
                leader: 2,
                replicas: [2, 3],
                isr: [2],
                offlineReplicas: [3],
              },
            ],
          },
        ],
      });

      await expect(cluster.partitionsFor('orders')).resolves.toEqual([
        {
          topic: 'orders',
          partitionId: 1,
          leader: 2,
          replicas: [2, 3],
          isr: [2],
          offlineReplicas: [3],
        },
      ]);
    });
  });

  describe('enableMetricsPush', () => {
    afterEach(() => {
      vi.restoreAllMocks();
    });

    it('does not start ClientTelemetryReporter when enableMetricsPush is false', async () => {
      const startSpy = vi.spyOn(ClientTelemetryReporter.prototype, 'start').mockImplementation(() => {});
      const cluster = createCluster({ enableMetricsPush: false });
      vi.spyOn(cluster.brokerPool, 'connect').mockResolvedValue(undefined);

      await cluster.connect();

      expect(startSpy).not.toHaveBeenCalled();
      expect(cluster.clientInstanceId()).toBeNull();
    });

    it('starts ClientTelemetryReporter once when enableMetricsPush is omitted', async () => {
      const startSpy = vi.spyOn(ClientTelemetryReporter.prototype, 'start').mockImplementation(() => {});
      const cluster = createCluster();
      vi.spyOn(cluster.brokerPool, 'connect').mockResolvedValue(undefined);

      await cluster.connect();

      expect(startSpy).toHaveBeenCalledOnce();
    });
  });
});
