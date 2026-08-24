import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaAggregateError, KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { API_KEYS } from '../protocol/requests/api-keys';
import { createTopicsApi } from './topics';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: Record<string, unknown>, retry?: { retries?: number }) {
  return createTopicsApi(
    {
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
      retry,
    },
    { fetchTopicOffsets: vi.fn().mockResolvedValue([{ partition: 0, offset: 10n, high: 10n, low: 0n }]) },
  );
}

describe('admin/topics', () => {
  describe('listTopics', () => {
    it('returns topic names and skips unnamed metadata entries', async () => {
      const cluster = {
        metadata: vi.fn().mockResolvedValue({
          topicMetadata: [{ topic: 'orders' }, { topic: null }, { topic: 'payments' }],
        }),
      };
      await expect(makeApi(cluster).listTopics()).resolves.toEqual(['orders', 'payments']);
    });
  });

  describe('createTopics', () => {
    it('rejects a non-array topics option', async () => {
      await expect(makeApi({}).createTopics({ topics: undefined as never })).rejects.toThrow(KafkaNonRetriableError);
    });

    it('rejects a non-string topic name', async () => {
      await expect(makeApi({}).createTopics({ topics: [{ topic: 1 as never }] })).rejects.toThrow(
        'the topic names have to be a valid string',
      );
    });

    it('rejects duplicate topic names', async () => {
      await expect(makeApi({}).createTopics({ topics: [{ topic: 'orders' }, { topic: 'orders' }] })).rejects.toThrow(
        'cannot have multiple entries for the same topic',
      );
    });

    it('rejects non-array configEntries', async () => {
      await expect(
        makeApi({}).createTopics({ topics: [{ topic: 'orders', configEntries: 'nope' as never }] }),
      ).rejects.toThrow('must be an array');
    });

    it('rejects a config entry missing name/value strings', async () => {
      await expect(
        makeApi({}).createTopics({
          topics: [{ topic: 'orders', configEntries: [{ name: 'retention.ms', value: 1 as never }] }],
        }),
      ).rejects.toThrow('must have a valid "value" property');
    });

    it('creates topics through the controller without waiting for leaders', async () => {
      const broker = { createTopics: vi.fn().mockResolvedValue(undefined), metadata: vi.fn() };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue(broker),
      };
      await expect(
        makeApi(cluster).createTopics({ topics: [{ topic: 'orders' }], waitForLeaders: false }),
      ).resolves.toBe(true);
      expect(broker.createTopics).toHaveBeenCalledWith({
        topics: [{ topic: 'orders' }],
        validateOnly: undefined,
        timeout: undefined,
      });
      expect(broker.metadata).not.toHaveBeenCalled();
    });

    it('returns false when every partition error is TOPIC_ALREADY_EXISTS', async () => {
      const error = new KafkaAggregateError('exists', [
        Object.assign(new Error('exists'), { type: 'TOPIC_ALREADY_EXISTS' }),
      ]);
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue({ createTopics: vi.fn().mockRejectedValue(error) }),
      };
      await expect(
        makeApi(cluster).createTopics({ topics: [{ topic: 'orders' }], waitForLeaders: false }),
      ).resolves.toBe(false);
    });

    it('retries on NOT_CONTROLLER', async () => {
      const error = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue({ createTopics: vi.fn().mockRejectedValue(error) }),
      };
      await expect(
        makeApi(cluster, { retries: 0 }).createTopics({ topics: [{ topic: 'orders' }], waitForLeaders: false }),
      ).rejects.toThrow();
    });
  });

  describe('createPartitions', () => {
    it('rejects a non-array, empty array, non-string names, and duplicates', async () => {
      const api = makeApi({});
      await expect(api.createPartitions({ topicPartitions: undefined as never })).rejects.toThrow(
        KafkaNonRetriableError,
      );
      await expect(api.createPartitions({ topicPartitions: [] })).rejects.toThrow('Empty topic partitions array');
      await expect(api.createPartitions({ topicPartitions: [{ topic: 1 as never, count: 2 }] })).rejects.toThrow(
        'the topic names have to be a valid string',
      );
      await expect(
        api.createPartitions({
          topicPartitions: [
            { topic: 'orders', count: 2 },
            { topic: 'orders', count: 3 },
          ],
        }),
      ).rejects.toThrow('cannot have multiple entries for the same topic');
    });

    it('forwards topicPartitions to the controller', async () => {
      const broker = { createPartitions: vi.fn().mockResolvedValue(undefined) };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue(broker),
      };
      const topicPartitions = [{ topic: 'orders', count: 4 }];
      await makeApi(cluster).createPartitions({ topicPartitions, validateOnly: true, timeout: 1000 });
      expect(broker.createPartitions).toHaveBeenCalledWith({ topicPartitions, validateOnly: true, timeout: 1000 });
    });
  });

  describe('deleteTopics', () => {
    it('rejects a non-array and non-string names', async () => {
      const api = makeApi({});
      await expect(api.deleteTopics({ topics: undefined as never })).rejects.toThrow(KafkaNonRetriableError);
      await expect(api.deleteTopics({ topics: [1 as never] })).rejects.toThrow('the names must be a valid string');
    });

    it('deletes topics, drops them from the target set, and refreshes metadata', async () => {
      const targetTopics = new Set(['orders', 'keep']);
      const broker = { deleteTopics: vi.fn().mockResolvedValue(undefined) };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue(broker),
        targetTopics,
      };
      await makeApi(cluster).deleteTopics({ topics: ['orders'], timeout: 5000 });
      expect(broker.deleteTopics).toHaveBeenCalledWith({ topics: ['orders'], timeout: 5000 });
      expect(targetTopics.has('orders')).toBe(false);
      expect(targetTopics.has('keep')).toBe(true);
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
    });
  });

  describe('fetchTopicMetadata', () => {
    it('rejects an empty or non-string topic filter', async () => {
      await expect(makeApi({}).fetchTopicMetadata({ topics: [''] })).rejects.toThrow('Invalid topic');
    });

    it('maps metadata, omitting unnamed topics and optional topicId', async () => {
      const topicId = Buffer.alloc(16, 7);
      const cluster = {
        metadata: vi.fn().mockResolvedValue({
          topicMetadata: [
            { topic: null, partitionMetadata: [] },
            { topic: 'orders', topicId, partitionMetadata: [{ partitionId: 0 }] },
            { topic: 'legacy', partitionMetadata: [] },
          ],
        }),
      };
      await expect(makeApi(cluster).fetchTopicMetadata({ topics: ['orders'] })).resolves.toEqual({
        topics: [
          { name: 'orders', topicId, partitions: [{ partitionId: 0 }] },
          { name: 'legacy', partitions: [] },
        ],
      });
    });
  });

  describe('describeCluster', () => {
    it('uses DescribeCluster when the broker advertises the API', async () => {
      const broker = {
        describeCluster: vi.fn().mockResolvedValue({
          brokers: [{ nodeId: 1, host: 'localhost', port: 9092 }],
          controllerId: 1,
          clusterId: 'cid',
        }),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue(broker),
        brokerPool: { versions: { [API_KEYS.DescribeCluster]: { min: 0, max: 1 } } },
      };
      await expect(makeApi(cluster).describeCluster()).resolves.toEqual({
        brokers: [{ nodeId: 1, host: 'localhost', port: 9092 }],
        controller: 1,
        clusterId: 'cid',
      });
    });

    it('falls back to metadata and maps a missing controller id to null', async () => {
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        brokerPool: { versions: {} },
        metadata: vi.fn().mockResolvedValue({
          brokers: [{ nodeId: 2, host: 'b', port: 9093 }],
          clusterId: 'cid',
          controllerId: -1,
        }),
      };
      await expect(makeApi(cluster).describeCluster()).resolves.toEqual({
        brokers: [{ nodeId: 2, host: 'b', port: 9093 }],
        controller: null,
        clusterId: 'cid',
      });
    });
  });

  describe('deleteTopicRecords', () => {
    it('rejects an invalid topic or empty partitions', async () => {
      const api = makeApi({});
      await expect(api.deleteTopicRecords({ topic: '', partitions: [{ partition: 0, offset: 0n }] })).rejects.toThrow(
        'Invalid topic',
      );
      await expect(api.deleteTopicRecords({ topic: 'orders', partitions: [] })).rejects.toThrow('Invalid partitions');
    });

    it('throws when a partition has no leader', async () => {
      const cluster = {
        findLeaderForPartitions: vi.fn().mockReturnValue({}),
      };
      await expect(
        makeApi(cluster).deleteTopicRecords({ topic: 'orders', partitions: [{ partition: 0, offset: 1n }] }),
      ).rejects.toThrow();
    });

    it('deletes records on the partition leader', async () => {
      const broker = { deleteRecords: vi.fn().mockResolvedValue(undefined) };
      const cluster = {
        findLeaderForPartitions: vi.fn().mockReturnValue({ '1': [0] }),
        findBroker: vi.fn().mockResolvedValue(broker),
      };
      await makeApi(cluster).deleteTopicRecords({ topic: 'orders', partitions: [{ partition: 0, offset: 5n }] });
      expect(broker.deleteRecords).toHaveBeenCalledWith({
        topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 5n }] }],
      });
    });
  });
});
