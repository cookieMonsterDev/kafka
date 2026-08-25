import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaDeleteGroupsError, KafkaNumberOfRetriesExceeded } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createGroupsApi } from './groups';

const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const fastRetry = { retries: 1, initialRetryTime: 1, maxRetryTime: 20, factor: 0, multiplier: 1 };

function makeApi(cluster: Record<string, unknown>, retry?: typeof fastRetry | { retries: number }) {
  return createGroupsApi({
    cluster: cluster as unknown as Cluster,
    logger,
    rootLogger: logger,
    retry,
  });
}

describe('admin/groups', () => {
  it('groups consumer group IDs by group coordinator for describeConsumerGroups', async () => {
    const firstCoordinator = {
      nodeId: 1,
      consumerGroupDescribe: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
        groups: groupIds.map((groupId) => ({ groupId, groupState: 'Stable' })),
      })),
    };
    const secondCoordinator = {
      nodeId: 2,
      consumerGroupDescribe: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
        groups: groupIds.map((groupId) => ({ groupId, groupState: 'Stable' })),
      })),
    };
    const findGroupCoordinator = vi.fn(async ({ groupId }: { groupId: string }) =>
      groupId === 'g-c' ? secondCoordinator : firstCoordinator,
    );
    const cluster = { findGroupCoordinator } as unknown as Cluster;
    const api = createGroupsApi({ cluster, logger, rootLogger: logger });

    const result = await api.describeConsumerGroups(['g-a', 'g-b', 'g-c']);

    expect(findGroupCoordinator).toHaveBeenCalledTimes(3);
    expect(firstCoordinator.consumerGroupDescribe).toHaveBeenCalledWith({ groupIds: ['g-a', 'g-b'] });
    expect(secondCoordinator.consumerGroupDescribe).toHaveBeenCalledWith({ groupIds: ['g-c'] });
    expect(result.groups.map(({ groupId }) => groupId)).toEqual(['g-a', 'g-b', 'g-c']);
  });

  it('returns immediately for an empty describeConsumerGroups list', async () => {
    const findGroupCoordinator = vi.fn();
    const cluster = { findGroupCoordinator } as unknown as Cluster;
    const api = createGroupsApi({ cluster, logger, rootLogger: logger });

    await expect(api.describeConsumerGroups([])).resolves.toEqual({ groups: [] });
    expect(findGroupCoordinator).not.toHaveBeenCalled();
  });

  it('rejects invalid describeConsumerGroups group IDs', async () => {
    const cluster = {} as Cluster;
    const api = createGroupsApi({ cluster, logger, rootLogger: logger });

    await expect(api.describeConsumerGroups([''])).rejects.toThrow('Group IDs must be non-empty strings');
    await expect(api.describeConsumerGroups(null as never)).rejects.toThrow('Invalid groupIds array');
  });

  it('describeClassicGroups delegates to describeGroups', async () => {
    const coordinator = {
      nodeId: 1,
      describeGroups: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
        groups: groupIds.map((groupId) => ({ groupId, state: 'Stable', members: [] })),
      })),
    };
    const findGroupCoordinator = vi.fn(async () => coordinator);
    const cluster = { findGroupCoordinator } as unknown as Cluster;
    const api = createGroupsApi({ cluster, logger, rootLogger: logger });

    const classic = await api.describeClassicGroups(['g-1']);
    const legacy = await api.describeGroups(['g-1']);

    expect(classic).toEqual(legacy);
    expect(coordinator.describeGroups).toHaveBeenCalledTimes(2);
  });

  it('lists groups from every broker in the pool', async () => {
    const brokerA = { listGroups: vi.fn(async () => ({ groups: [{ groupId: 'g-a' }] })) };
    const brokerB = { listGroups: vi.fn(async () => ({ groups: [{ groupId: 'g-b' }] })) };
    const findBroker = vi.fn(async ({ nodeId }: { nodeId: string }) => (nodeId === '1' ? brokerA : brokerB));
    const cluster = {
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      brokerPool: { brokers: { 1: {}, 2: {} } },
      findBroker,
    };

    await expect(makeApi(cluster).listGroups()).resolves.toEqual({ groups: [{ groupId: 'g-a' }, { groupId: 'g-b' }] });
    expect(cluster.refreshMetadata).toHaveBeenCalled();
  });

  describe('deleteGroups', () => {
    it('rejects a missing or non-array groupIds list', async () => {
      const api = makeApi({});
      await expect(api.deleteGroups(undefined as never)).rejects.toThrow('Invalid groupIds array');
      await expect(api.deleteGroups('g-1' as never)).rejects.toThrow('Invalid groupIds array');
    });

    it('rejects a non-string group id', async () => {
      await expect(makeApi({}).deleteGroups([1 as never])).rejects.toThrow('Invalid groupId name');
    });

    it('returns an empty list without contacting brokers', async () => {
      const findGroupCoordinator = vi.fn();
      await expect(makeApi({ refreshMetadata: vi.fn(), findGroupCoordinator }).deleteGroups([])).resolves.toEqual([]);
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
        groupId === 'g-c' ? second : first,
      );

      const results = await makeApi({
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator,
      }).deleteGroups(['g-a', 'g-b', 'g-c']);

      expect(first.deleteGroups).toHaveBeenCalledWith({ groupIds: ['g-a', 'g-b'] });
      expect(second.deleteGroups).toHaveBeenCalledWith({ groupIds: ['g-c'] });
      expect(results.map(({ groupId }) => groupId)).toEqual(['g-a', 'g-b', 'g-c']);
    });

    it('aggregates per-group failures into KafkaDeleteGroupsError', async () => {
      const coordinator = {
        nodeId: 1,
        deleteGroups: vi.fn(async () => ({
          results: [
            { groupId: 'g-ok', errorCode: 0 },
            { groupId: 'g-bad', errorCode: 69, error: new Error('not empty') },
          ],
        })),
      };

      const error = await makeApi(
        {
          refreshMetadata: vi.fn().mockResolvedValue(undefined),
          findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
        },
        { retries: 0 },
      )
        .deleteGroups(['g-ok', 'g-bad'])
        .catch((e) => e);

      expect(error).toBeInstanceOf(KafkaDeleteGroupsError);
      expect((error as KafkaDeleteGroupsError).groups).toEqual([
        { groupId: 'g-bad', errorCode: 69, error: expect.any(Error) },
      ]);
    });
  });

  describe('deleteGroupOffsets', () => {
    it('rejects an invalid groupId, topics list, or partition index', async () => {
      const api = makeApi({});
      await expect(api.deleteGroupOffsets({ groupId: '', topics: [] })).rejects.toThrow('Invalid groupId');
      await expect(api.deleteGroupOffsets({ groupId: 'g-1', topics: undefined as never })).rejects.toThrow(
        'Invalid topics array',
      );
      await expect(
        api.deleteGroupOffsets({ groupId: 'g-1', topics: [{ topic: 1 as never, partitions: [] }] }),
      ).rejects.toThrow('the topic names have to be a valid string');
      await expect(
        api.deleteGroupOffsets({ groupId: 'g-1', topics: [{ topic: 'orders', partitions: undefined as never }] }),
      ).rejects.toThrow('Invalid partition array');
      await expect(
        api.deleteGroupOffsets({ groupId: 'g-1', topics: [{ topic: 'orders', partitions: [-1] }] }),
      ).rejects.toThrow('The partition indices have to be a valid number');
    });

    it('deletes offsets through the group coordinator, allowing partition 0', async () => {
      const coordinator = {
        offsetDelete: vi.fn(async () => ({ topics: [{ topic: 'orders', partitions: [] }] })),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(
        makeApi(cluster).deleteGroupOffsets({ groupId: 'g-1', topics: [{ topic: 'orders', partitions: [0, 2] }] }),
      ).resolves.toEqual({ topics: [{ topic: 'orders', partitions: [] }] });
      expect(coordinator.offsetDelete).toHaveBeenCalledWith({
        groupId: 'g-1',
        topics: [{ topic: 'orders', partitions: [0, 2] }],
      });
    });

    it('retries GROUP_COORDINATOR_NOT_AVAILABLE then succeeds', async () => {
      const coordinator = {
        offsetDelete: vi
          .fn()
          .mockRejectedValueOnce(Object.assign(new Error('moving'), { type: 'GROUP_COORDINATOR_NOT_AVAILABLE' }))
          .mockResolvedValueOnce({ topics: [] }),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(
        makeApi(cluster, fastRetry).deleteGroupOffsets({
          groupId: 'g-1',
          topics: [{ topic: 'orders', partitions: [0] }],
        }),
      ).resolves.toEqual({ topics: [] });
      expect(coordinator.offsetDelete).toHaveBeenCalledTimes(2);
    });
  });

  describe('removeMembersFromConsumerGroup', () => {
    it('rejects an invalid groupId, empty members list, or empty memberId', async () => {
      const api = makeApi({});
      await expect(api.removeMembersFromConsumerGroup({ groupId: '', members: [{ memberId: 'm' }] })).rejects.toThrow(
        'Invalid groupId',
      );
      await expect(api.removeMembersFromConsumerGroup({ groupId: 'g-1', members: [] })).rejects.toThrow(
        'Invalid members array',
      );
      await expect(api.removeMembersFromConsumerGroup({ groupId: 'g-1', members: [{ memberId: '' }] })).rejects.toThrow(
        'Each member must have a non-empty memberId',
      );
      await expect(
        api.removeMembersFromConsumerGroup({
          groupId: 'g-1',
          members: [{ memberId: 'm-1', groupInstanceId: 1 as never }],
        }),
      ).rejects.toThrow('Invalid groupInstanceId');
    });

    it('defaults omitted instance id and reason to null', async () => {
      const coordinator = {
        leaveGroupMembers: vi.fn(async () => ({
          members: [{ memberId: 'm-1', groupInstanceId: null, errorCode: 0 }],
        })),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(
        makeApi(cluster).removeMembersFromConsumerGroup({ groupId: 'g-1', members: [{ memberId: 'm-1' }] }),
      ).resolves.toEqual({ members: [{ memberId: 'm-1', groupInstanceId: null, errorCode: 0 }] });
      expect(coordinator.leaveGroupMembers).toHaveBeenCalledWith({
        groupId: 'g-1',
        members: [{ memberId: 'm-1', groupInstanceId: null, reason: null }],
      });
    });

    it('does not retry a non-coordinator error', async () => {
      const coordinator = {
        leaveGroupMembers: vi
          .fn()
          .mockRejectedValue(Object.assign(new Error('unknown'), { type: 'UNKNOWN_MEMBER_ID' })),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(
        makeApi(cluster, fastRetry).removeMembersFromConsumerGroup({
          groupId: 'g-1',
          members: [{ memberId: 'm-1' }],
        }),
      ).rejects.toThrow('unknown');
      expect(coordinator.leaveGroupMembers).toHaveBeenCalledTimes(1);
    });

    it('exhausts retries on a persistent NOT_COORDINATOR_FOR_GROUP error', async () => {
      const coordinator = {
        leaveGroupMembers: vi
          .fn()
          .mockRejectedValue(Object.assign(new Error('stale'), { type: 'NOT_COORDINATOR_FOR_GROUP' })),
      };
      const cluster = {
        refreshMetadata: vi.fn().mockResolvedValue(undefined),
        findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      };

      await expect(
        makeApi(cluster, { retries: 0 }).removeMembersFromConsumerGroup({
          groupId: 'g-1',
          members: [{ memberId: 'm-1' }],
        }),
      ).rejects.toBeInstanceOf(KafkaNumberOfRetriesExceeded);
    });
  });
});
