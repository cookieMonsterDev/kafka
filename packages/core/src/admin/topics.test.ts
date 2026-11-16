import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { STALE_METADATA_ERRORS } from '../cluster/error-classifier';
import { CONTROLLER } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';
import { KafkaAggregateError, KafkaDeleteTopicRecordsError, KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createErrorFromCode } from '../protocol/error-codes';
import { API_KEYS } from '../protocol/requests/api-keys';
import type { RetryOptions } from '../retry/index';
import { createTopicsApi } from './topics';
import { withSend } from '../broker/test-send';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: object, retry?: RetryOptions) {
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
      const cluster = withSend({
        metadata: vi.fn().mockResolvedValue({
          topicMetadata: [{ topic: 'orders' }, { topic: null }, { topic: 'payments' }],
        }),
      });
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
      const broker = withSend({ createTopics: vi.fn().mockResolvedValue(undefined), metadata: vi.fn() });
      const cluster = fakeRunCluster({ controller: broker });
      await expect(
        makeApi(cluster).createTopics({ topics: [{ topic: 'orders' }], waitForLeaders: false }),
      ).resolves.toBe(true);
      expect(cluster.refreshMetadata).toHaveBeenCalled();
      expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
      expect(broker.createTopics).toHaveBeenCalledWith({
        topics: [{ topic: 'orders' }],
        validateOnly: undefined,
        timeout: undefined,
      });
      expect(broker.metadata).not.toHaveBeenCalled();
    });

    it('does not wait for leaders on a validateOnly call, even with waitForLeaders left at its default', async () => {
      const broker = withSend({ createTopics: vi.fn().mockResolvedValue(undefined), metadata: vi.fn() });
      const cluster = fakeRunCluster({ controller: broker });
      await expect(makeApi(cluster).createTopics({ topics: [{ topic: 'orders' }], validateOnly: true })).resolves.toBe(
        true,
      );
      expect(broker.metadata).not.toHaveBeenCalled();
    });

    it('returns false when every partition error is TOPIC_ALREADY_EXISTS', async () => {
      const error = new KafkaAggregateError('exists', [
        Object.assign(new Error('exists'), { type: 'TOPIC_ALREADY_EXISTS' }),
      ]);
      const createTopics = vi.fn().mockRejectedValue(error);
      const cluster = fakeRunCluster({ controller: withSend({ createTopics }) });
      await expect(
        makeApi(cluster).createTopics({ topics: [{ topic: 'orders' }], waitForLeaders: false }),
      ).resolves.toBe(false);
      expect(createTopics).toHaveBeenCalledTimes(1);
      expect(cluster.recover).not.toHaveBeenCalled();
    });

    it('retries on NOT_CONTROLLER', async () => {
      const error = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const createTopics = vi.fn().mockRejectedValueOnce(error).mockResolvedValue(undefined);
      const cluster = fakeRunCluster({ controller: withSend({ createTopics }) });
      await expect(
        makeApi(cluster, { retries: 1, initialRetryTime: 1 }).createTopics({
          topics: [{ topic: 'orders' }],
          waitForLeaders: false,
        }),
      ).resolves.toBe(true);
      expect(createTopics).toHaveBeenCalledTimes(2);
      expect(cluster.recover).toHaveBeenCalledWith(error);
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
      const broker = withSend({ createPartitions: vi.fn().mockResolvedValue(undefined) });
      const cluster = fakeRunCluster({ controller: broker });
      const topicPartitions = [{ topic: 'orders', count: 4 }];
      await makeApi(cluster).createPartitions({ topicPartitions, validateOnly: true, timeout: 1000 });
      expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
      expect(broker.createPartitions).toHaveBeenCalledWith({ topicPartitions, validateOnly: true, timeout: 1000 });
    });

    it('retries on stale metadata from a topic the controller has not caught up on yet', async () => {
      const error = Object.assign(new Error('unknown topic'), { type: 'UNKNOWN_TOPIC_OR_PARTITION' });
      const createPartitions = vi.fn().mockRejectedValueOnce(error).mockResolvedValueOnce(undefined);
      const cluster = fakeRunCluster({ controller: withSend({ createPartitions }) });
      await makeApi(cluster, { retries: 1, initialRetryTime: 1 }).createPartitions({
        topicPartitions: [{ topic: 'orders', count: 2 }],
      });
      expect(createPartitions).toHaveBeenCalledTimes(2);
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
      expect(cluster.recover).toHaveBeenCalledWith(error);
    });

    it('bails immediately on a non-retriable error', async () => {
      const error = Object.assign(new Error('invalid partition count'), { type: 'INVALID_PARTITIONS' });
      const createPartitions = vi.fn().mockRejectedValue(error);
      const cluster = fakeRunCluster({ controller: withSend({ createPartitions }) });
      await expect(
        makeApi(cluster, { retries: 5, initialRetryTime: 1 }).createPartitions({
          topicPartitions: [{ topic: 'orders', count: 2 }],
        }),
      ).rejects.toThrow('invalid partition count');
      expect(createPartitions).toHaveBeenCalledTimes(1);
      expect(cluster.recover).not.toHaveBeenCalled();
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
      const broker = withSend({ deleteTopics: vi.fn().mockResolvedValue(undefined) });
      const cluster = fakeRunCluster({ controller: broker }, { targetTopics });
      await makeApi(cluster).deleteTopics({ topics: ['orders'], timeout: 5000 });
      expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
      expect(broker.deleteTopics).toHaveBeenCalledWith({ topics: ['orders'], timeout: 5000 });
      expect(targetTopics.has('orders')).toBe(false);
      expect(targetTopics.has('keep')).toBe(true);
      // Once before the attempt and once after the deletion.
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
    });

    it('retries the stale-metadata class', async () => {
      const error = Object.assign(new Error('unknown topic'), { type: 'UNKNOWN_TOPIC_OR_PARTITION' });
      const deleteTopics = vi.fn().mockRejectedValueOnce(error).mockResolvedValue(undefined);
      const cluster = fakeRunCluster({ controller: withSend({ deleteTopics }) }, { targetTopics: new Set<string>() });
      await makeApi(cluster, { retries: 1, initialRetryTime: 1 }).deleteTopics({ topics: ['orders'] });
      expect(deleteTopics).toHaveBeenCalledTimes(2);
      expect(cluster.recover).toHaveBeenCalledWith(error);
      expect(cluster.run).toHaveBeenCalledWith(
        CONTROLLER,
        expect.any(Function),
        expect.objectContaining({ refreshMetadata: true, retryOn: STALE_METADATA_ERRORS }),
      );
    });

    it('rejects REQUEST_TIMED_OUT without retrying', async () => {
      const error = Object.assign(new Error('timed out'), { type: 'REQUEST_TIMED_OUT' });
      const deleteTopics = vi.fn().mockRejectedValue(error);
      const targetTopics = new Set(['orders']);
      const cluster = fakeRunCluster({ controller: withSend({ deleteTopics }) }, { targetTopics });
      await expect(
        makeApi(cluster, { retries: 5, initialRetryTime: 1 }).deleteTopics({ topics: ['orders'] }),
      ).rejects.toBe(error);
      expect(deleteTopics).toHaveBeenCalledTimes(1);
      expect(cluster.recover).not.toHaveBeenCalled();
      expect(targetTopics.has('orders')).toBe(true);
    });
  });

  describe('fetchTopicMetadata', () => {
    it('rejects an empty or non-string topic filter', async () => {
      await expect(makeApi({}).fetchTopicMetadata({ topics: [''] })).rejects.toThrow('Invalid topic');
    });

    it('maps metadata, omitting unnamed topics and optional topicId', async () => {
      const topicId = Buffer.alloc(16, 7);
      const cluster = withSend({
        metadata: vi.fn().mockResolvedValue({
          topicMetadata: [
            { topic: null, partitionMetadata: [] },
            { topic: 'orders', topicId, partitionMetadata: [{ partitionId: 0 }] },
            { topic: 'legacy', partitionMetadata: [] },
          ],
        }),
      });
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
      const broker = withSend({
        describeCluster: vi.fn().mockResolvedValue({
          brokers: [{ nodeId: 1, host: 'localhost', port: 9092 }],
          controllerId: 1,
          clusterId: 'cid',
        }),
      });
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
      const cluster = withSend({
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        brokerPool: { versions: {} },
        metadata: vi.fn().mockResolvedValue({
          brokers: [{ nodeId: 2, host: 'b', port: 9093 }],
          clusterId: 'cid',
          controllerId: -1,
        }),
      });
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
      const cluster = fakeRunCluster({}, { findLeaderForPartitions: vi.fn().mockReturnValue({}) });
      await expect(
        makeApi(cluster).deleteTopicRecords({ topic: 'orders', partitions: [{ partition: 0, offset: 1n }] }),
      ).rejects.toThrow(KafkaDeleteTopicRecordsError);
      expect(cluster.run).not.toHaveBeenCalled();
    });

    it('deletes records on the partition leader', async () => {
      const broker = withSend({ deleteRecords: vi.fn().mockResolvedValue(undefined) });
      const cluster = fakeRunCluster(
        {},
        {
          findLeaderForPartitions: vi.fn().mockReturnValue({ '1': [0] }),
          findBroker: vi.fn().mockResolvedValue(broker),
        },
      );
      await makeApi(cluster).deleteTopicRecords({ topic: 'orders', partitions: [{ partition: 0, offset: 5n }] });
      expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: '1' });
      expect(broker.deleteRecords).toHaveBeenCalledWith({
        topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 5n }] }],
      });
      expect(cluster.resolveTarget).not.toHaveBeenCalled();
    });

    it('re-resolves the partition leader when retrying NOT_LEADER_OR_FOLLOWER', async () => {
      const oldLeader = withSend({
        deleteRecords: vi.fn().mockRejectedValue(
          new KafkaDeleteTopicRecordsError({
            partitions: [{ partition: 0, offset: 5n, error: createErrorFromCode(6) }],
          }),
        ),
      });
      const newLeader = withSend({ deleteRecords: vi.fn().mockResolvedValue(undefined) });
      const cluster = fakeRunCluster(
        {},
        {
          findLeaderForPartitions: vi
            .fn()
            .mockReturnValueOnce({ '1': [0] })
            .mockReturnValueOnce({ '1': [0] })
            .mockReturnValue({ '2': [0] }),
          findBroker: vi.fn(async ({ nodeId }: { nodeId: string }) => (nodeId === '1' ? oldLeader : newLeader)),
        },
      );

      await makeApi(cluster, {
        retries: 1,
        initialRetryTime: 1,
        maxRetryTime: 20,
        factor: 0,
        multiplier: 1,
      }).deleteTopicRecords({ topic: 'orders', partitions: [{ partition: 0, offset: 5n }] });

      // The stale-leader error goes to recovery (a metadata refresh) before the retry.
      expect(cluster.recover).toHaveBeenCalledTimes(1);
      expect(cluster.recover).toHaveBeenCalledWith(expect.any(KafkaDeleteTopicRecordsError));
      expect(oldLeader.deleteRecords).toHaveBeenCalledTimes(1);
      expect(newLeader.deleteRecords).toHaveBeenCalledWith({
        topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 5n }] }],
      });
    });
  });
});
