import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaDeleteGroupsError, KafkaNumberOfRetriesExceeded } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createErrorFromCode } from '../protocol/error-codes';
import { createGroupsApi } from './groups';
import { withSend } from '../broker/test-send';
import { COORDINATOR_LOADING_ERRORS } from '../cluster/error-classifier';
import { coordinatorTarget } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';

const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const fastRetry = { retries: 1, initialRetryTime: 1, maxRetryTime: 20, factor: 0, multiplier: 1 };

function makeApi(cluster: object, retry?: typeof fastRetry | { retries: number }) {
  return createGroupsApi({
    cluster: cluster as unknown as Cluster,
    logger,
    rootLogger: logger,
    retry,
  });
}

describe('admin/groups', () => {
  it('groups consumer group IDs by group coordinator for describeConsumerGroups', async () => {
    const firstCoordinator = withSend({
      nodeId: 1,
      consumerGroupDescribe: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
        groups: groupIds.map((groupId) => ({ groupId, groupState: 'Stable' })),
      })),
    });
    const secondCoordinator = withSend({
      nodeId: 2,
      consumerGroupDescribe: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
        groups: groupIds.map((groupId) => ({ groupId, groupState: 'Stable' })),
      })),
    });
    const cluster = fakeRunCluster({
      coordinator: (groupId) => (groupId === 'g-c' ? secondCoordinator : firstCoordinator),
    });

    const result = await makeApi(cluster).describeConsumerGroups(['g-a', 'g-b', 'g-c']);

    expect(cluster.resolveTarget).toHaveBeenCalledTimes(3);
    expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('g-a'));
    expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('g-b'));
    expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('g-c'));
    expect(firstCoordinator.consumerGroupDescribe).toHaveBeenCalledWith({ groupIds: ['g-a', 'g-b'] });
    expect(secondCoordinator.consumerGroupDescribe).toHaveBeenCalledWith({ groupIds: ['g-c'] });
    expect(result.groups.map(({ groupId }) => groupId)).toEqual(['g-a', 'g-b', 'g-c']);
  });

  it.each([
    ['describeGroups', 'describeGroups'],
    ['describeConsumerGroups', 'consumerGroupDescribe'],
  ] as const)('%s re-resolves the coordinator after NOT_COORDINATOR_FOR_GROUP', async (method, request) => {
    const staleCoordinator = withSend({ nodeId: 1, [request]: vi.fn().mockRejectedValue(createErrorFromCode(16)) });
    const currentCoordinator = withSend({
      nodeId: 2,
      [request]: vi.fn().mockResolvedValue({ groups: [{ groupId: 'g-a', groupState: 'Stable' }] }),
    });
    const cluster = fakeRunCluster({
      coordinator: vi.fn().mockReturnValueOnce(staleCoordinator).mockReturnValue(currentCoordinator),
    });

    const result = await makeApi(cluster, fastRetry)[method](['g-a']);

    expect(cluster.resolveTarget).toHaveBeenCalledTimes(2);
    expect(staleCoordinator[request]).toHaveBeenCalledTimes(1);
    expect(currentCoordinator[request]).toHaveBeenCalledWith({ groupIds: ['g-a'] });
    expect(result.groups).toEqual([{ groupId: 'g-a', groupState: 'Stable' }]);
  });

  it('returns immediately for an empty describeConsumerGroups list', async () => {
    const cluster = fakeRunCluster({});

    await expect(makeApi(cluster).describeConsumerGroups([])).resolves.toEqual({ groups: [] });
    expect(cluster.run).not.toHaveBeenCalled();
  });

  it('rejects invalid describeConsumerGroups group IDs', async () => {
    const cluster = {} as Cluster;
    const api = createGroupsApi({ cluster, logger, rootLogger: logger });

    await expect(api.describeConsumerGroups([''])).rejects.toThrow('Group IDs must be non-empty strings');
    await expect(api.describeConsumerGroups(null as never)).rejects.toThrow('Invalid groupIds array');
  });

  it('describeClassicGroups delegates to describeGroups', async () => {
    const coordinator = withSend({
      nodeId: 1,
      describeGroups: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
        groups: groupIds.map((groupId) => ({ groupId, state: 'Stable', members: [] })),
      })),
    });
    const api = makeApi(fakeRunCluster({ coordinator }));

    const classic = await api.describeClassicGroups(['g-1']);
    const legacy = await api.describeGroups(['g-1']);

    expect(classic).toEqual(legacy);
    expect(coordinator.describeGroups).toHaveBeenCalledTimes(2);
  });

  it('lists groups from every broker in the pool', async () => {
    const brokerA = withSend({ listGroups: vi.fn(async () => ({ groups: [{ groupId: 'g-a' }] })) });
    const brokerB = withSend({ listGroups: vi.fn(async () => ({ groups: [{ groupId: 'g-b' }] })) });
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
      const cluster = fakeRunCluster({});
      await expect(makeApi(cluster).deleteGroups([])).resolves.toEqual([]);
      expect(cluster.resolveTarget).not.toHaveBeenCalled();
    });

    it('deletes groups grouped by coordinator node', async () => {
      const first = withSend({
        nodeId: 1,
        deleteGroups: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
          results: groupIds.map((groupId) => ({ groupId, errorCode: 0 })),
        })),
      });
      const second = withSend({
        nodeId: 2,
        deleteGroups: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
          results: groupIds.map((groupId) => ({ groupId, errorCode: 0 })),
        })),
      });
      const cluster = fakeRunCluster({ coordinator: (groupId) => (groupId === 'g-c' ? second : first) });

      const results = await makeApi(cluster).deleteGroups(['g-a', 'g-b', 'g-c']);

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1);
      expect(cluster.run).toHaveBeenCalledWith(
        expect.any(Function),
        expect.any(Function),
        expect.objectContaining({ refreshMetadata: true, retryOn: COORDINATOR_LOADING_ERRORS }),
      );

      expect(first.deleteGroups).toHaveBeenCalledWith({ groupIds: ['g-a', 'g-b'] });
      expect(second.deleteGroups).toHaveBeenCalledWith({ groupIds: ['g-c'] });
      expect(results.map(({ groupId }) => groupId)).toEqual(['g-a', 'g-b', 'g-c']);
    });

    it('aggregates per-group failures into KafkaDeleteGroupsError', async () => {
      const coordinator = withSend({
        nodeId: 1,
        deleteGroups: vi.fn(async () => ({
          results: [
            { groupId: 'g-ok', errorCode: 0 },
            { groupId: 'g-bad', errorCode: 69, error: new Error('not empty') },
          ],
        })),
      });

      const error = await makeApi(fakeRunCluster({ coordinator }), { retries: 0 })
        .deleteGroups(['g-ok', 'g-bad'])
        .catch((e) => e);

      expect(error).toBeInstanceOf(KafkaDeleteGroupsError);
      expect((error as KafkaDeleteGroupsError).groups).toEqual([
        { groupId: 'g-bad', errorCode: 69, error: expect.any(Error) },
      ]);
    });

    it('retries only groups whose coordinator moved and keeps earlier successes', async () => {
      const coordinator = withSend({
        nodeId: 1,
        deleteGroups: vi
          .fn()
          .mockResolvedValueOnce({
            results: [
              { groupId: 'g-ok', errorCode: 0 },
              { groupId: 'g-moved', errorCode: 16, error: createErrorFromCode(16) },
            ],
          })
          .mockResolvedValueOnce({ results: [{ groupId: 'g-moved', errorCode: 0 }] }),
      });
      const cluster = fakeRunCluster({ coordinator });

      await expect(makeApi(cluster, fastRetry).deleteGroups(['g-ok', 'g-moved'])).resolves.toEqual([
        { groupId: 'g-ok', errorCode: 0 },
        { groupId: 'g-moved', errorCode: 0 },
      ]);
      expect(coordinator.deleteGroups).toHaveBeenNthCalledWith(2, { groupIds: ['g-moved'] });
      expect(cluster.resolveTarget).toHaveBeenLastCalledWith(coordinatorTarget('g-moved'));
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
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
      const coordinator = withSend({
        offsetDelete: vi.fn(async () => ({ topics: [{ topic: 'orders', partitions: [] }] })),
      });
      const cluster = fakeRunCluster({ coordinator });

      await expect(
        makeApi(cluster).deleteGroupOffsets({ groupId: 'g-1', topics: [{ topic: 'orders', partitions: [0, 2] }] }),
      ).resolves.toEqual({ topics: [{ topic: 'orders', partitions: [] }] });
      expect(coordinator.offsetDelete).toHaveBeenCalledWith({
        groupId: 'g-1',
        topics: [{ topic: 'orders', partitions: [0, 2] }],
      });
      expect(cluster.refreshMetadata).toHaveBeenCalled();
      expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('g-1'));
    });

    it('retries GROUP_COORDINATOR_NOT_AVAILABLE then succeeds', async () => {
      const moving = Object.assign(new Error('moving'), { type: 'GROUP_COORDINATOR_NOT_AVAILABLE' });
      const coordinator = withSend({
        offsetDelete: vi.fn().mockRejectedValueOnce(moving).mockResolvedValueOnce({ topics: [] }),
      });
      const cluster = fakeRunCluster({ coordinator });

      await expect(
        makeApi(cluster, fastRetry).deleteGroupOffsets({
          groupId: 'g-1',
          topics: [{ topic: 'orders', partitions: [0] }],
        }),
      ).resolves.toEqual({ topics: [] });
      expect(coordinator.offsetDelete).toHaveBeenCalledTimes(2);
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
      expect(cluster.recover).toHaveBeenCalledWith(moving);
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
      const coordinator = withSend({
        leaveGroupMembers: vi.fn(async () => ({
          members: [{ memberId: 'm-1', groupInstanceId: null, errorCode: 0 }],
        })),
      });
      const cluster = fakeRunCluster({ coordinator });

      await expect(
        makeApi(cluster).removeMembersFromConsumerGroup({ groupId: 'g-1', members: [{ memberId: 'm-1' }] }),
      ).resolves.toEqual({ members: [{ memberId: 'm-1', groupInstanceId: null, errorCode: 0 }] });
      expect(coordinator.leaveGroupMembers).toHaveBeenCalledWith({
        groupId: 'g-1',
        members: [{ memberId: 'm-1', groupInstanceId: null, reason: null }],
      });
      expect(cluster.refreshMetadata).toHaveBeenCalled();
      expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('g-1'));
    });

    it('does not retry a non-coordinator error', async () => {
      const coordinator = withSend({
        leaveGroupMembers: vi
          .fn()
          .mockRejectedValue(Object.assign(new Error('unknown'), { type: 'UNKNOWN_MEMBER_ID' })),
      });
      const cluster = fakeRunCluster({ coordinator });

      await expect(
        makeApi(cluster, fastRetry).removeMembersFromConsumerGroup({
          groupId: 'g-1',
          members: [{ memberId: 'm-1' }],
        }),
      ).rejects.toThrow('unknown');
      expect(coordinator.leaveGroupMembers).toHaveBeenCalledTimes(1);
    });

    it('exhausts retries on a persistent NOT_COORDINATOR_FOR_GROUP error', async () => {
      const coordinator = withSend({
        leaveGroupMembers: vi
          .fn()
          .mockRejectedValue(Object.assign(new Error('stale'), { type: 'NOT_COORDINATOR_FOR_GROUP' })),
      });
      const cluster = fakeRunCluster({ coordinator });

      await expect(
        makeApi(cluster, { retries: 0 }).removeMembersFromConsumerGroup({
          groupId: 'g-1',
          members: [{ memberId: 'm-1' }],
        }),
      ).rejects.toBeInstanceOf(KafkaNumberOfRetriesExceeded);
    });
  });
});
