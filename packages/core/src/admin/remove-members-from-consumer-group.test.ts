import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createGroupsApi } from './groups';

const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('admin/removeMembersFromConsumerGroup', () => {
  it('sends LeaveGroup with member identities to the group coordinator', async () => {
    const coordinator = {
      leaveGroupMembers: vi.fn(async () => ({
        members: [{ memberId: 'm-1', groupInstanceId: null, errorCode: 0 }],
      })),
    };
    const cluster = {
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      findGroupCoordinator: vi.fn(async () => coordinator),
    } as unknown as Cluster;
    const api = createGroupsApi({ cluster, logger, rootLogger: logger });

    const result = await api.removeMembersFromConsumerGroup({
      groupId: 'g-1',
      members: [{ memberId: 'm-1' }],
    });

    expect(coordinator.leaveGroupMembers).toHaveBeenCalledWith({
      groupId: 'g-1',
      members: [{ memberId: 'm-1', groupInstanceId: null, reason: null }],
    });
    expect(result.members).toEqual([{ memberId: 'm-1', groupInstanceId: null, errorCode: 0 }]);
  });

  it('rejects an empty members list', async () => {
    const cluster = {} as Cluster;
    const api = createGroupsApi({ cluster, logger, rootLogger: logger });

    await expect(api.removeMembersFromConsumerGroup({ groupId: 'g-1', members: [] })).rejects.toThrow(
      'Invalid members array',
    );
  });
});
