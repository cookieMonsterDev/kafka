import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaDeleteGroupsError, KafkaNumberOfRetriesExceeded } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createShareGroupsApi } from './share-groups';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const fastRetry = { retries: 1, initialRetryTime: 1, maxRetryTime: 20, factor: 0, multiplier: 1 };

function makeApi(cluster: Record<string, unknown>, retry?: { retries?: number } & Record<string, number>) {
  return createShareGroupsApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
    retry,
  });
}

describe('admin/share-groups', () => {
  describe('describeShareGroups', () => {
    it('returns immediately for an empty group id list', async () => {
      const findGroupCoordinator = vi.fn();
      await expect(makeApi({ findGroupCoordinator }).describeShareGroups([])).resolves.toEqual({ groups: [] });
      expect(findGroupCoordinator).not.toHaveBeenCalled();
    });

    it('rejects a non-array or empty-string group id', async () => {
      const api = makeApi({});
      await expect(api.describeShareGroups(null as never)).rejects.toThrow('Invalid groupIds array');
      await expect(api.describeShareGroups(['ok', ''])).rejects.toThrow('Group IDs must be non-empty strings');
      await expect(api.describeShareGroups([1 as never])).rejects.toThrow('Group IDs must be non-empty strings');
    });

    it('groups share group IDs by coordinator', async () => {
      const first = {
        nodeId: 1,
        shareGroupDescribe: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
          groups: groupIds.map((groupId) => ({ groupId, groupState: 'Stable' })),
        })),
      };
      const second = {
        nodeId: 2,
        shareGroupDescribe: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
          groups: groupIds.map((groupId) => ({ groupId, groupState: 'Empty' })),
        })),
      };
      const findGroupCoordinator = vi.fn(async ({ groupId }: { groupId: string }) =>
        groupId === 'share-c' ? second : first,
      );

      const result = await makeApi({ findGroupCoordinator }).describeShareGroups(['share-a', 'share-b', 'share-c']);

      expect(first.shareGroupDescribe).toHaveBeenCalledWith({ groupIds: ['share-a', 'share-b'] });
      expect(second.shareGroupDescribe).toHaveBeenCalledWith({ groupIds: ['share-c'] });
      expect(result.groups.map(({ groupId }) => groupId)).toEqual(['share-a', 'share-b', 'share-c']);
    });
  });

  describe('listShareGroupOffsets', () => {
    it('rejects a missing, non-array, or empty groups list', async () => {
      const api = makeApi({});
      await expect(api.listShareGroupOffsets({ groups: undefined as never })).rejects.toThrow('Invalid groups array');
      await expect(api.listShareGroupOffsets({ groups: [] })).rejects.toThrow('Invalid groups array');
    });

    it('rejects a group without a non-empty groupId', async () => {
      const api = makeApi({});
      await expect(api.listShareGroupOffsets({ groups: [{ groupId: '' }] })).rejects.toThrow(
        'Each group must have a non-empty groupId',
      );
      await expect(api.listShareGroupOffsets({ groups: [{ groupId: 1 as never }] })).rejects.toThrow(
        'Each group must have a non-empty groupId',
      );
    });

    it('forwards null topics and defaults omitted partitions to an empty list', async () => {
      const coordinator = {
        nodeId: 1,
        describeShareGroupOffsets: vi.fn(async () => ({ groups: [{ groupId: 'share-1', topics: [] }] })),
      };
      const cluster = {
        findGroupCoordinator: vi.fn(async () => coordinator),
      };

      const result = await makeApi(cluster).listShareGroupOffsets({
        groups: [
          { groupId: 'share-1', topics: null },
          { groupId: 'share-2', topics: [{ topicName: 'orders' }, { topicName: 'payments', partitions: [0, 1] }] },
        ],
      });

      expect(coordinator.describeShareGroupOffsets).toHaveBeenCalledWith({
        groups: [
          { groupId: 'share-1', topics: null },
          {
            groupId: 'share-2',
            topics: [
              { topicName: 'orders', partitions: [] },
              { topicName: 'payments', partitions: [0, 1] },
            ],
          },
        ],
      });
      expect(result.groups).toEqual([{ groupId: 'share-1', topics: [] }]);
    });

    it('groups offset requests by coordinator', async () => {
      const first = {
        nodeId: 1,
        describeShareGroupOffsets: vi.fn(async ({ groups }: { groups: { groupId: string }[] }) => ({
          groups: groups.map(({ groupId }) => ({ groupId, topics: [] })),
        })),
      };
      const second = {
        nodeId: 2,
        describeShareGroupOffsets: vi.fn(async ({ groups }: { groups: { groupId: string }[] }) => ({
          groups: groups.map(({ groupId }) => ({ groupId, topics: [] })),
        })),
      };
      const findGroupCoordinator = vi.fn(async ({ groupId }: { groupId: string }) =>
        groupId === 'share-b' ? second : first,
      );

      const result = await makeApi({ findGroupCoordinator }).listShareGroupOffsets({
        groups: [{ groupId: 'share-a' }, { groupId: 'share-b' }, { groupId: 'share-c' }],
      });

      expect(first.describeShareGroupOffsets).toHaveBeenCalledTimes(1);
      expect(second.describeShareGroupOffsets).toHaveBeenCalledTimes(1);
      expect(result.groups.map(({ groupId }) => groupId)).toEqual(['share-a', 'share-c', 'share-b']);
    });
  });

  describe('alterShareGroupOffsets', () => {
    const topics = [{ topicName: 'orders', partitions: [{ partitionIndex: 0, startOffset: 10n }] }];

    it('rejects an invalid groupId or empty topics list', async () => {
      const api = makeApi({});
      await expect(api.alterShareGroupOffsets({ groupId: '', topics })).rejects.toThrow('Invalid groupId');
      await expect(api.alterShareGroupOffsets({ groupId: 1 as never, topics })).rejects.toThrow('Invalid groupId');
      await expect(api.alterShareGroupOffsets({ groupId: 'share-1', topics: [] })).rejects.toThrow(
        'Invalid topics array',
      );
      await expect(api.alterShareGroupOffsets({ groupId: 'share-1', topics: undefined as never })).rejects.toThrow(
        'Invalid topics array',
      );
    });

    it('refreshes metadata and alters offsets on the group coordinator', async () => {
      const coordinator = {
        alterShareGroupOffsets: vi.fn(async () => ({ responses: [{ topicName: 'orders', partitions: [] }] })),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(makeApi(cluster).alterShareGroupOffsets({ groupId: 'share-1', topics })).resolves.toEqual({
        responses: [{ topicName: 'orders', partitions: [] }],
      });
      expect(cluster.refreshMetadata).toHaveBeenCalled();
      expect(coordinator.alterShareGroupOffsets).toHaveBeenCalledWith({ groupId: 'share-1', topics });
    });

    it('retries GROUP_COORDINATOR_NOT_AVAILABLE then succeeds', async () => {
      const coordinator = {
        alterShareGroupOffsets: vi
          .fn()
          .mockRejectedValueOnce(Object.assign(new Error('moving'), { type: 'GROUP_COORDINATOR_NOT_AVAILABLE' }))
          .mockResolvedValueOnce({ responses: [] }),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(makeApi(cluster, fastRetry).alterShareGroupOffsets({ groupId: 'share-1', topics })).resolves.toEqual(
        {
          responses: [],
        },
      );
      expect(coordinator.alterShareGroupOffsets).toHaveBeenCalledTimes(2);
    });

    it('does not retry a non-coordinator error', async () => {
      const coordinator = {
        alterShareGroupOffsets: vi
          .fn()
          .mockRejectedValue(Object.assign(new Error('denied'), { type: 'GROUP_AUTHORIZATION_FAILED' })),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(makeApi(cluster, fastRetry).alterShareGroupOffsets({ groupId: 'share-1', topics })).rejects.toThrow(
        'denied',
      );
      expect(coordinator.alterShareGroupOffsets).toHaveBeenCalledTimes(1);
    });
  });

  describe('deleteShareGroupOffsets', () => {
    const topics = ['orders'];

    it('rejects an invalid groupId or empty topics list', async () => {
      const api = makeApi({});
      await expect(api.deleteShareGroupOffsets({ groupId: '', topics })).rejects.toThrow('Invalid groupId');
      await expect(api.deleteShareGroupOffsets({ groupId: 'share-1', topics: [] })).rejects.toThrow(
        'Invalid topics array',
      );
    });

    it('deletes offsets through the group coordinator', async () => {
      const coordinator = {
        deleteShareGroupOffsets: vi.fn(async () => ({ responses: [{ topicName: 'orders', errorCode: 0 }] })),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(makeApi(cluster).deleteShareGroupOffsets({ groupId: 'share-1', topics })).resolves.toEqual({
        responses: [{ topicName: 'orders', errorCode: 0 }],
      });
    });

    it('retries NOT_COORDINATOR_FOR_GROUP then succeeds', async () => {
      const coordinator = {
        deleteShareGroupOffsets: vi
          .fn()
          .mockRejectedValueOnce(Object.assign(new Error('stale'), { type: 'NOT_COORDINATOR_FOR_GROUP' }))
          .mockResolvedValueOnce({ responses: [] }),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(
        makeApi(cluster, fastRetry).deleteShareGroupOffsets({ groupId: 'share-1', topics }),
      ).resolves.toEqual({
        responses: [],
      });
      expect(coordinator.deleteShareGroupOffsets).toHaveBeenCalledTimes(2);
    });
  });

  describe('deleteShareGroups', () => {
    it('rejects invalid group ids', async () => {
      const api = makeApi({});
      await expect(api.deleteShareGroups(undefined as never)).rejects.toThrow('Invalid groupIds array');
      await expect(api.deleteShareGroups([''])).rejects.toThrow('Group IDs must be non-empty strings');
    });

    it('returns an empty list without contacting brokers', async () => {
      const findGroupCoordinator = vi.fn();
      await expect(makeApi({ refreshMetadata: vi.fn(), findGroupCoordinator }).deleteShareGroups([])).resolves.toEqual(
        [],
      );
      expect(findGroupCoordinator).not.toHaveBeenCalled();
    });

    it('deletes groups grouped by coordinator node', async () => {
      const first = {
        nodeId: 1,
        deleteGroups: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
          results: groupIds.map((groupId) => ({ groupId, errorCode: 0 })),
        })),
      };
      const second = {
        nodeId: 2,
        deleteGroups: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
          results: groupIds.map((groupId) => ({ groupId, errorCode: 0 })),
        })),
      };
      const findGroupCoordinator = vi.fn(async ({ groupId }: { groupId: string }) =>
        groupId === 'share-c' ? second : first,
      );
      const cluster = { refreshMetadata: vi.fn().mockResolvedValue(undefined), findGroupCoordinator };

      const results = await makeApi(cluster).deleteShareGroups(['share-a', 'share-b', 'share-c']);

      expect(first.deleteGroups).toHaveBeenCalledWith({ groupIds: ['share-a', 'share-b'] });
      expect(second.deleteGroups).toHaveBeenCalledWith({ groupIds: ['share-c'] });
      expect(results.map(({ groupId }) => groupId)).toEqual(['share-a', 'share-b', 'share-c']);
    });

    it('aggregates per-group failures into KafkaDeleteGroupsError', async () => {
      const coordinator = {
        nodeId: 1,
        deleteGroups: vi.fn(async () => ({
          results: [
            { groupId: 'share-ok', errorCode: 0 },
            { groupId: 'share-bad', errorCode: 69, error: new Error('not empty') },
          ],
        })),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      const error = await makeApi(cluster, { retries: 0 })
        .deleteShareGroups(['share-ok', 'share-bad'])
        .catch((e) => e);
      expect(error).toBeInstanceOf(KafkaDeleteGroupsError);
      expect((error as KafkaDeleteGroupsError).groups).toEqual([
        { groupId: 'share-bad', errorCode: 69, error: expect.any(Error) },
      ]);
    });

    it('retries COORDINATOR_NOT_AVAILABLE then succeeds', async () => {
      const coordinator = {
        nodeId: 1,
        deleteGroups: vi
          .fn()
          .mockRejectedValueOnce(Object.assign(new Error('loading'), { type: 'COORDINATOR_NOT_AVAILABLE' }))
          .mockResolvedValueOnce({ results: [{ groupId: 'share-1', errorCode: 0 }] }),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(makeApi(cluster, fastRetry).deleteShareGroups(['share-1'])).resolves.toEqual([
        { groupId: 'share-1', errorCode: 0 },
      ]);
    });

    it('exhausts retries on a persistent NOT_CONTROLLER error', async () => {
      const coordinator = {
        nodeId: 1,
        deleteGroups: vi.fn().mockRejectedValue(Object.assign(new Error('moved'), { type: 'NOT_CONTROLLER' })),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(makeApi(cluster, { retries: 0 }).deleteShareGroups(['share-1'])).rejects.toBeInstanceOf(
        KafkaNumberOfRetriesExceeded,
      );
    });
  });
});
