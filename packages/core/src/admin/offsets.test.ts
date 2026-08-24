import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createOffsetsApi } from './offsets';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: Record<string, unknown>, retry?: { retries?: number }) {
  return createOffsetsApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
    retry,
  });
}

describe('admin/offsets', () => {
  describe('fetchTopicOffsets', () => {
    it('rejects a missing or non-string topic', async () => {
      const api = makeApi({});
      await expect(api.fetchTopicOffsets('')).rejects.toThrow(KafkaNonRetriableError);
      await expect(api.fetchTopicOffsets(undefined as never)).rejects.toThrow('Invalid topic');
    });

    it('joins high and low watermarks per partition', async () => {
      const cluster = {
        addTargetTopic: vi.fn().mockResolvedValue(undefined),
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }, { partitionId: 1 }]),
        fetchTopicsOffset: vi
          .fn()
          .mockResolvedValueOnce([
            {
              topic: 'orders',
              partitions: [
                { partition: 0, offset: 10n },
                { partition: 1, offset: 20n },
              ],
            },
          ])
          .mockResolvedValueOnce([
            {
              topic: 'orders',
              partitions: [
                { partition: 0, offset: 0n },
                { partition: 1, offset: 5n },
              ],
            },
          ]),
      };

      await expect(makeApi(cluster).fetchTopicOffsets('orders')).resolves.toEqual([
        { partition: 0, offset: 10n, high: 10n, low: 0n },
        { partition: 1, offset: 20n, high: 20n, low: 5n },
      ]);

      expect(cluster.fetchTopicsOffset).toHaveBeenNthCalledWith(1, [
        { topic: 'orders', fromBeginning: false, partitions: [{ partition: 0 }, { partition: 1 }] },
      ]);
      expect(cluster.fetchTopicsOffset).toHaveBeenNthCalledWith(2, [
        { topic: 'orders', fromBeginning: true, partitions: [{ partition: 0 }, { partition: 1 }] },
      ]);
    });

    it('refreshes metadata and retries on UNKNOWN_TOPIC_OR_PARTITION', async () => {
      const error = Object.assign(new Error('unknown'), { type: 'UNKNOWN_TOPIC_OR_PARTITION' });
      const cluster = {
        addTargetTopic: vi.fn().mockResolvedValue(undefined),
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findTopicPartitionMetadata: vi.fn().mockImplementation(() => {
          throw error;
        }),
      };
      await expect(makeApi(cluster, { retries: 0 }).fetchTopicOffsets('orders')).rejects.toThrow();
      expect(cluster.refreshMetadata).toHaveBeenCalled();
    });
  });

  describe('fetchTopicOffsetsByTimestamp', () => {
    it('rejects a missing topic', async () => {
      await expect(makeApi({}).fetchTopicOffsetsByTimestamp('')).rejects.toThrow('Invalid topic');
    });

    it('uses the high watermark when the timestamp lookup returns a negative offset', async () => {
      const cluster = {
        addTargetTopic: vi.fn().mockResolvedValue(undefined),
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }]),
        fetchTopicsOffset: vi
          .fn()
          .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: 99n }] }])
          .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: -1n }] }]),
      };

      await expect(makeApi(cluster).fetchTopicOffsetsByTimestamp('orders', 1_700_000_000_000n)).resolves.toEqual([
        { partition: 0, offset: 99n },
      ]);
      expect(cluster.fetchTopicsOffset).toHaveBeenNthCalledWith(2, [
        { topic: 'orders', fromTimestamp: 1_700_000_000_000n, partitions: [{ partition: 0 }] },
      ]);
    });

    it('forwards a numeric timestamp through parseOffset', async () => {
      const cluster = {
        addTargetTopic: vi.fn().mockResolvedValue(undefined),
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }]),
        fetchTopicsOffset: vi
          .fn()
          .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: 5n }] }])
          .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: 3n }] }]),
      };

      await expect(makeApi(cluster).fetchTopicOffsetsByTimestamp('orders', '100')).resolves.toEqual([
        { partition: 0, offset: 3n },
      ]);
      expect(cluster.fetchTopicsOffset.mock.calls[1]![0][0].fromTimestamp).toBe(100n);
    });
  });

  describe('fetchOffsets', () => {
    it('rejects a missing groupId and a non-array topics option', async () => {
      const api = makeApi({});
      await expect(api.fetchOffsets({ groupId: '' })).rejects.toThrow('Invalid groupId');
      await expect(api.fetchOffsets({ groupId: 'g', topics: 'orders' as never })).rejects.toThrow(
        'Expected topics array to be set',
      );
    });

    it('fetches committed offsets from the group coordinator', async () => {
      const coordinator = {
        offsetFetch: vi.fn().mockResolvedValue({
          responses: [{ topic: 'orders', partitions: [{ partition: 0, offset: 7n, metadata: '' }] }],
        }),
      };
      const cluster = {
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
        addTargetTopic: vi.fn().mockResolvedValue(undefined),
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }]),
      };

      await expect(makeApi(cluster).fetchOffsets({ groupId: 'g1', topics: ['orders'] })).resolves.toEqual([
        { topic: 'orders', partitions: [{ partition: 0, offset: 7n, metadata: null }] },
      ]);
      expect(coordinator.offsetFetch).toHaveBeenCalledWith({
        groupId: 'g1',
        topics: [{ topic: 'orders', partitions: [{ partition: 0 }] }],
      });
    });
  });

  describe('setOffsets / resetOffsets', () => {
    it('rejects missing groupId, topic, or partitions', async () => {
      const api = makeApi({});
      await expect(
        api.setOffsets({ groupId: '', topic: 'orders', partitions: [{ partition: 0, offset: 1n }] }),
      ).rejects.toThrow('Invalid groupId');
      await expect(
        api.setOffsets({ groupId: 'g', topic: '', partitions: [{ partition: 0, offset: 1n }] }),
      ).rejects.toThrow('Invalid topic');
      await expect(api.setOffsets({ groupId: 'g', topic: 'orders', partitions: [] })).rejects.toThrow(
        'Invalid partitions',
      );
      await expect(api.resetOffsets({ groupId: '', topic: 'orders' })).rejects.toThrow('Invalid groupId');
      await expect(api.resetOffsets({ groupId: 'g', topic: '' })).rejects.toThrow('Invalid topic');
    });
  });
});
