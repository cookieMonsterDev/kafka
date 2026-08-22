import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { SHARE_GROUP_JOIN_EPOCH } from '../protocol/requests/share-group-heartbeat/index';
import { ShareGroup } from './share-group';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('share-consumer/share-group', () => {
  it('subscribes to topic names', () => {
    const group = new ShareGroup({
      cluster: {} as Cluster,
      groupId: 'share-1',
      logger: silentLogger,
    });
    group.subscribe(['events', 'clicks']);
    expect(group.topicsSubscribed).toEqual(['events', 'clicks']);
  });

  it('joins through ShareGroupHeartbeat until the broker assigns an epoch', async () => {
    const topicId = Buffer.alloc(16, 1);
    const shareGroupHeartbeat = vi
      .fn()
      .mockResolvedValueOnce({
        errorCode: 0,
        memberId: 'member-1',
        memberEpoch: SHARE_GROUP_JOIN_EPOCH,
        heartbeatIntervalMs: 50,
        assignment: null,
      })
      .mockResolvedValueOnce({
        errorCode: 0,
        memberId: 'member-1',
        memberEpoch: 1,
        heartbeatIntervalMs: 50,
        assignment: { topicPartitions: [{ topicId, partitions: [0, 1] }] },
      });
    const coordinator = { shareGroupHeartbeat };
    const cluster = {
      connect: vi.fn(),
      disconnect: vi.fn(),
      findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
    } as unknown as Cluster;

    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger, retry: { retries: 0 } });
    group.subscribe(['events']);
    await group.joinAndSync();

    expect(group.joined).toBe(true);
    expect(group.memberId).toBe('member-1');
    expect(group.memberEpoch).toBe(1);
    expect(group.assigned()).toEqual([{ topic: 'events', partitions: [0, 1] }]);
    expect(shareGroupHeartbeat).toHaveBeenCalledTimes(2);
  });

  it('leaves with member epoch -1', async () => {
    const shareGroupHeartbeat = vi.fn().mockResolvedValue({
      errorCode: 0,
      memberId: 'member-1',
      memberEpoch: -1,
      heartbeatIntervalMs: 3000,
      assignment: null,
    });
    const coordinator = { shareGroupHeartbeat, nodeId: 1 };
    const group = new ShareGroup({
      cluster: { findGroupCoordinator: vi.fn().mockResolvedValue(coordinator) } as unknown as Cluster,
      groupId: 'share-1',
      logger: silentLogger,
    });
    group.coordinator = coordinator as never;
    group.memberId = 'member-1';
    group.joined = true;

    await group.leave();
    expect(shareGroupHeartbeat).toHaveBeenCalledWith(
      expect.objectContaining({ groupId: 'share-1', memberId: 'member-1', memberEpoch: -1 }),
    );
    expect(group.joined).toBe(false);
  });
});
