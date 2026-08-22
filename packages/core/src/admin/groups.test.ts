import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createGroupsApi } from './groups';

const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

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
});
