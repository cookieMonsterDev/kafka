import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { OFFSET_SPECS } from '../protocol/enums/offset-specs';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createOffsetsApi } from './offsets';
import { withSend } from '../broker/test-send';
import { STALE_METADATA_ERRORS } from '../cluster/error-classifier';
import { fakeRunCluster } from '../cluster/test-run';
import { createErrorFromCode } from '../protocol/error-codes';
import { API_KEYS } from '../protocol/requests/api-keys';

const UNKNOWN_TOPIC_ID_CODE = 100;

/** The cluster calls that look up a single-partition `orders` topic, with the given topic id. */
function topicMetadata(topicId?: Buffer) {
  return {
    addMultipleTargetTopics: vi.fn().mockResolvedValue(undefined),
    refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
    findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }]),
    findTopicId: vi.fn().mockReturnValue(topicId),
  };
}

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: object, retry?: { retries?: number }) {
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
      const cluster = fakeRunCluster(
        {},
        {
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
        },
      );

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
      const cluster = fakeRunCluster(
        {},
        {
          addTargetTopic: vi.fn().mockResolvedValue(undefined),
          refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
          findTopicPartitionMetadata: vi.fn().mockImplementation(() => {
            throw error;
          }),
        },
      );
      await expect(makeApi(cluster, { retries: 0 }).fetchTopicOffsets('orders')).rejects.toThrow();
      expect(cluster.run).toHaveBeenCalledWith(
        expect.any(Function),
        expect.any(Function),
        expect.objectContaining({ retryOn: STALE_METADATA_ERRORS }),
      );
      expect(cluster.recover).toHaveBeenCalledWith(error);
    });

    it('looks up the offsetSpec first and returns it as offset next to the watermarks', async () => {
      const partitionOffsets = (offsets: readonly bigint[]) => [
        { topic: 'orders', partitions: offsets.map((offset, partition) => ({ partition, offset })) },
      ];
      const cluster = fakeRunCluster(
        {},
        {
          addTargetTopic: vi.fn().mockResolvedValue(undefined),
          refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
          findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }, { partitionId: 1 }]),
          fetchTopicsOffset: vi
            .fn()
            .mockResolvedValueOnce(partitionOffsets([7n, -1n]))
            .mockResolvedValueOnce(partitionOffsets([10n, 20n]))
            .mockResolvedValueOnce(partitionOffsets([0n, 5n])),
        },
      );

      await expect(
        makeApi(cluster).fetchTopicOffsets('orders', { offsetSpec: OFFSET_SPECS.LATEST_TIERED }),
      ).resolves.toEqual([
        { partition: 0, offset: 7n, high: 10n, low: 0n },
        { partition: 1, offset: -1n, high: 20n, low: 5n },
      ]);
      expect(cluster.fetchTopicsOffset).toHaveBeenNthCalledWith(1, [
        { topic: 'orders', fromTimestamp: -5n, partitions: [{ partition: 0 }, { partition: 1 }] },
      ]);
      expect(cluster.fetchTopicsOffset).toHaveBeenCalledTimes(3);
    });

    it('answers OFFSET_SPECS.EARLIEST from the low watermark lookup', async () => {
      const cluster = fakeRunCluster(
        {},
        {
          addTargetTopic: vi.fn().mockResolvedValue(undefined),
          refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
          findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }]),
          fetchTopicsOffset: vi
            .fn()
            .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: 10n }] }])
            .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: 3n }] }]),
        },
      );

      await expect(
        makeApi(cluster).fetchTopicOffsets('orders', { offsetSpec: OFFSET_SPECS.EARLIEST }),
      ).resolves.toEqual([{ partition: 0, offset: 3n, high: 10n, low: 3n }]);
      expect(cluster.fetchTopicsOffset).toHaveBeenCalledTimes(2);
    });

    it('rejects an offsetSpec that is not one of OFFSET_SPECS', async () => {
      const api = makeApi({});
      await expect(api.fetchTopicOffsets('orders', { offsetSpec: 1_735_689_600_000n as never })).rejects.toThrow(
        'Invalid offsetSpec 1735689600000',
      );
    });
  });

  describe('fetchTopicOffsetsByTimestamp', () => {
    it('rejects a missing topic', async () => {
      await expect(makeApi({}).fetchTopicOffsetsByTimestamp('')).rejects.toThrow('Invalid topic');
    });

    it('uses the high watermark when the timestamp lookup returns a negative offset', async () => {
      const cluster = fakeRunCluster(
        {},
        {
          addTargetTopic: vi.fn().mockResolvedValue(undefined),
          refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
          findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }]),
          fetchTopicsOffset: vi
            .fn()
            .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: 99n }] }])
            .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: -1n }] }]),
        },
      );

      await expect(makeApi(cluster).fetchTopicOffsetsByTimestamp('orders', 1_700_000_000_000n)).resolves.toEqual([
        { partition: 0, offset: 99n },
      ]);
      expect(cluster.fetchTopicsOffset).toHaveBeenNthCalledWith(2, [
        { topic: 'orders', fromTimestamp: 1_700_000_000_000n, partitions: [{ partition: 0 }] },
      ]);
    });

    it('forwards a numeric timestamp through parseOffset', async () => {
      const cluster = fakeRunCluster(
        {},
        {
          addTargetTopic: vi.fn().mockResolvedValue(undefined),
          refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
          findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }]),
          fetchTopicsOffset: vi
            .fn()
            .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: 5n }] }])
            .mockResolvedValueOnce([{ topic: 'orders', partitions: [{ partition: 0, offset: 3n }] }]),
        },
      );

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
      const coordinator = withSend({
        offsetFetch: vi.fn().mockResolvedValue({
          groups: [
            { groupId: 'g1', topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 7n, metadata: '' }] }] },
          ],
        }),
      });
      const cluster = fakeRunCluster({ coordinator }, topicMetadata());

      await expect(makeApi(cluster).fetchOffsets({ groupId: 'g1', topics: ['orders'] })).resolves.toEqual([
        { topic: 'orders', partitions: [{ partition: 0, offset: 7n, metadata: null }] },
      ]);
      expect(coordinator.offsetFetch).toHaveBeenCalledWith({
        groups: [{ groupId: 'g1', topics: [{ topic: 'orders', topicId: undefined, partitions: [{ partition: 0 }] }] }],
      });
    });
  });

  describe('listConsumerGroupOffsets', () => {
    it('rejects an empty groups array, a missing or duplicate groupId, and invalid topics', async () => {
      const api = makeApi({});
      await expect(api.listConsumerGroupOffsets({ groups: [] })).rejects.toThrow('Invalid groups array');
      await expect(api.listConsumerGroupOffsets({ groups: [{ groupId: '' }] })).rejects.toThrow(
        'Each group must have a non-empty groupId',
      );
      await expect(api.listConsumerGroupOffsets({ groups: [{ groupId: 'g' }, { groupId: 'g' }] })).rejects.toThrow(
        'Duplicate groupId g',
      );
      await expect(
        api.listConsumerGroupOffsets({ groups: [{ groupId: 'g', topics: 'orders' as never }] }),
      ).rejects.toThrow('Invalid topics array for group g');
    });

    it('sends one OffsetFetch per coordinator that supports v8, with topic ids, in groups order', async () => {
      const topicId = Buffer.alloc(16, 1);
      const offsetFetch = vi.fn(async ({ groups }: { groups: { groupId: string }[] }) => ({
        groups: groups.map(({ groupId }) => ({
          groupId,
          topics: [
            { topic: 'orders', partitions: [{ partition: 0, offset: groupId === 'g1' ? 1n : 3n, metadata: null }] },
          ],
        })),
      }));
      const coordinator = withSend({ versions: { [API_KEYS.OffsetFetch]: { maxVersion: 10 } }, offsetFetch });
      const cluster = fakeRunCluster({ coordinator }, topicMetadata(topicId));

      await expect(
        makeApi(cluster).listConsumerGroupOffsets({
          groups: [{ groupId: 'g3', topics: ['orders'] }, { groupId: 'g1' }],
        }),
      ).resolves.toEqual({
        groups: [
          { groupId: 'g3', topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 3n, metadata: null }] }] },
          { groupId: 'g1', topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 1n, metadata: null }] }] },
        ],
      });
      expect(offsetFetch).toHaveBeenCalledExactlyOnceWith({
        groups: [
          { groupId: 'g3', topics: [{ topic: 'orders', topicId, partitions: [{ partition: 0 }] }] },
          { groupId: 'g1', topics: [] },
        ],
      });
    });

    it('sends one OffsetFetch per group to a coordinator older than v8', async () => {
      const offsetFetch = vi.fn(async ({ groups }: { groups: { groupId: string }[] }) => ({
        groups: groups.map(({ groupId }) => ({ groupId, topics: [] })),
      }));
      const coordinator = withSend({ versions: { [API_KEYS.OffsetFetch]: { maxVersion: 7 } }, offsetFetch });
      const cluster = fakeRunCluster({ coordinator }, topicMetadata());

      await expect(
        makeApi(cluster).listConsumerGroupOffsets({ groups: [{ groupId: 'g1' }, { groupId: 'g2' }] }),
      ).resolves.toEqual({
        groups: [
          { groupId: 'g1', topics: [] },
          { groupId: 'g2', topics: [] },
        ],
      });
      expect(offsetFetch).toHaveBeenCalledTimes(2);
      expect(offsetFetch).toHaveBeenNthCalledWith(1, { groups: [{ groupId: 'g1', topics: [] }] });
      expect(offsetFetch).toHaveBeenNthCalledWith(2, { groups: [{ groupId: 'g2', topics: [] }] });
    });

    it('groups requests by coordinator', async () => {
      const first = withSend({
        versions: { [API_KEYS.OffsetFetch]: { maxVersion: 8 } },
        offsetFetch: vi.fn(async () => ({ groups: [{ groupId: 'g1', topics: [] }] })),
      });
      const second = withSend({
        versions: { [API_KEYS.OffsetFetch]: { maxVersion: 8 } },
        offsetFetch: vi.fn(async () => ({ groups: [{ groupId: 'g2', topics: [] }] })),
      });
      const cluster = fakeRunCluster({ coordinator: (key) => (key === 'g1' ? first : second) }, topicMetadata());

      await makeApi(cluster).listConsumerGroupOffsets({ groups: [{ groupId: 'g1' }, { groupId: 'g2' }] });

      expect(first.offsetFetch).toHaveBeenCalledWith({ groups: [{ groupId: 'g1', topics: [] }] });
      expect(second.offsetFetch).toHaveBeenCalledWith({ groups: [{ groupId: 'g2', topics: [] }] });
    });

    it('reads topic ids again after UNKNOWN_TOPIC_ID refreshes the metadata', async () => {
      const staleId = Buffer.alloc(16, 1);
      const freshId = Buffer.alloc(16, 2);
      const findTopicId = vi.fn().mockReturnValueOnce(staleId).mockReturnValue(freshId);
      const offsetFetch = vi
        .fn()
        .mockRejectedValueOnce(createErrorFromCode(UNKNOWN_TOPIC_ID_CODE))
        .mockResolvedValue({ groups: [{ groupId: 'g1', topics: [] }] });
      const coordinator = withSend({ versions: { [API_KEYS.OffsetFetch]: { maxVersion: 10 } }, offsetFetch });
      const cluster = fakeRunCluster({ coordinator }, { ...topicMetadata(), findTopicId });

      await makeApi(cluster).listConsumerGroupOffsets({ groups: [{ groupId: 'g1', topics: ['orders'] }] });

      expect(cluster.recover).toHaveBeenCalledWith(expect.objectContaining({ type: 'UNKNOWN_TOPIC_ID' }));
      expect(offsetFetch).toHaveBeenLastCalledWith({
        groups: [{ groupId: 'g1', topics: [{ topic: 'orders', topicId: freshId, partitions: [{ partition: 0 }] }] }],
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
