import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createErrorFromCode } from '../protocol/error-codes';
import { createStreamsGroupsApi } from './streams-groups';
import { withSend } from '../broker/test-send';
import { coordinatorTarget } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const fastRetry = { retries: 1, initialRetryTime: 1, maxRetryTime: 20, factor: 0, multiplier: 1 };

function makeApi(cluster: object, retry?: typeof fastRetry) {
  return createStreamsGroupsApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
    retry,
  });
}

function describingCoordinator(nodeId: number) {
  return withSend({
    nodeId,
    streamsGroupDescribe: vi.fn(async ({ groupIds }: { groupIds: string[] }) => ({
      groups: groupIds.map((groupId) => ({ groupId, groupState: 'Stable' })),
    })),
  });
}

describe('admin/streams-groups', () => {
  it('returns immediately for an empty group id list', async () => {
    const cluster = fakeRunCluster({});
    await expect(makeApi(cluster).describeStreamsGroups([])).resolves.toEqual({ groups: [] });
    expect(cluster.run).not.toHaveBeenCalled();
  });

  it('rejects invalid group ids and options', async () => {
    const api = makeApi({});
    await expect(api.describeStreamsGroups(null as never)).rejects.toThrow('Invalid groupIds array');
    await expect(api.describeStreamsGroups(['ok', ''])).rejects.toThrow('Group IDs must be non-empty strings');
    await expect(api.describeStreamsGroups(['ok'], { includeTopologyDescription: 'yes' as never })).rejects.toThrow(
      'Invalid includeTopologyDescription yes',
    );
  });

  it('batches group ids by group coordinator', async () => {
    const first = describingCoordinator(1);
    const second = describingCoordinator(2);
    const cluster = fakeRunCluster({ coordinator: (groupId) => (groupId === 'app-c' ? second : first) });

    const result = await makeApi(cluster).describeStreamsGroups(['app-a', 'app-b', 'app-c']);

    expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('app-a'));
    expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('app-c'));
    expect(first.streamsGroupDescribe).toHaveBeenCalledWith({
      groupIds: ['app-a', 'app-b'],
      includeTopologyDescription: false,
    });
    expect(second.streamsGroupDescribe).toHaveBeenCalledWith({
      groupIds: ['app-c'],
      includeTopologyDescription: false,
    });
    expect(result.groups.map(({ groupId }) => groupId)).toEqual(['app-a', 'app-b', 'app-c']);
  });

  it('forwards includeTopologyDescription', async () => {
    const coordinator = describingCoordinator(1);
    const cluster = fakeRunCluster({ coordinator: () => coordinator });

    await makeApi(cluster).describeStreamsGroups(['app'], { includeTopologyDescription: true });

    expect(coordinator.streamsGroupDescribe).toHaveBeenCalledWith({
      groupIds: ['app'],
      includeTopologyDescription: true,
    });
  });

  it('re-resolves the coordinator after NOT_COORDINATOR_FOR_GROUP', async () => {
    const stale = withSend({ nodeId: 1, streamsGroupDescribe: vi.fn().mockRejectedValue(createErrorFromCode(16)) });
    const current = describingCoordinator(2);
    const cluster = fakeRunCluster({ coordinator: vi.fn().mockReturnValueOnce(stale).mockReturnValue(current) });

    const result = await makeApi(cluster, fastRetry).describeStreamsGroups(['app']);

    expect(cluster.resolveTarget).toHaveBeenCalledTimes(2);
    expect(result.groups).toEqual([{ groupId: 'app', groupState: 'Stable' }]);
  });

  it('rejects GROUP_ID_NOT_FOUND without retrying', async () => {
    const coordinator = withSend({
      nodeId: 1,
      streamsGroupDescribe: vi.fn().mockRejectedValue(createErrorFromCode(69, { message: 'Group app not found.' })),
    });
    const cluster = fakeRunCluster({ coordinator: () => coordinator });

    await expect(makeApi(cluster, fastRetry).describeStreamsGroups(['app'])).rejects.toMatchObject({
      cause: { type: 'GROUP_ID_NOT_FOUND', message: 'Group app not found.' },
    });
    expect(coordinator.streamsGroupDescribe).toHaveBeenCalledTimes(1);
  });
});
