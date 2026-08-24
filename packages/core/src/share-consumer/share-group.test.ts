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

  it('connects and disconnects through the cluster, leaving when joined', async () => {
    const shareGroupHeartbeat = vi.fn().mockResolvedValue({
      errorCode: 0,
      memberId: 'member-1',
      memberEpoch: -1,
      heartbeatIntervalMs: 3000,
      assignment: null,
    });
    const connect = vi.fn();
    const disconnect = vi.fn();
    const cluster = {
      connect,
      disconnect,
      findGroupCoordinator: vi.fn(),
    } as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger });
    group.coordinator = { shareGroupHeartbeat } as never;
    group.memberId = 'member-1';
    group.joined = true;

    await group.connect();
    await group.disconnect();
    expect(connect).toHaveBeenCalled();
    expect(shareGroupHeartbeat).toHaveBeenCalled();
    expect(disconnect).toHaveBeenCalled();
    expect(group.joined).toBe(false);
  });

  it('leave is a no-op without a coordinator or member id', async () => {
    const group = new ShareGroup({ cluster: {} as Cluster, groupId: 'share-1', logger: silentLogger });
    await expect(group.leave()).resolves.toBeUndefined();
    group.coordinator = { shareGroupHeartbeat: vi.fn() } as never;
    await expect(group.leave()).resolves.toBeUndefined();
  });

  it('clears membership when leave heartbeat fails', async () => {
    const shareGroupHeartbeat = vi.fn().mockRejectedValue(new Error('offline'));
    const group = new ShareGroup({ cluster: {} as Cluster, groupId: 'share-1', logger: silentLogger });
    group.coordinator = { shareGroupHeartbeat } as never;
    group.memberId = 'member-1';
    group.joined = true;

    await group.leave();
    expect(group.joined).toBe(false);
    expect(group.memberEpoch).toBe(SHARE_GROUP_JOIN_EPOCH);
  });

  it('heartbeatDue is false without a coordinator and true after the interval elapses', () => {
    const group = new ShareGroup({ cluster: {} as Cluster, groupId: 'share-1', logger: silentLogger });
    expect(group.heartbeatDue(10)).toBe(false);
    group.coordinator = { shareGroupHeartbeat: vi.fn() } as never;
    group.lastHeartbeatAt = 0;
    group.heartbeatIntervalMs = 1;
    expect(group.heartbeatDue(10_000)).toBe(true);
    group.lastHeartbeatAt = Date.now();
    group.heartbeatIntervalMs = 60_000;
    expect(group.heartbeatDue(60_000)).toBe(false);
  });

  it('skips a heartbeat that is not due unless forced', async () => {
    const shareGroupHeartbeat = vi.fn().mockResolvedValue({
      errorCode: 0,
      memberId: 'member-1',
      memberEpoch: 1,
      heartbeatIntervalMs: 60_000,
      assignment: null,
    });
    const group = new ShareGroup({ cluster: {} as Cluster, groupId: 'share-1', logger: silentLogger });
    group.coordinator = { shareGroupHeartbeat } as never;
    group.memberId = 'member-1';
    group.memberEpoch = 1;
    group.lastHeartbeatAt = Date.now();
    group.heartbeatIntervalMs = 60_000;

    await group.heartbeat({ force: false });
    expect(shareGroupHeartbeat).not.toHaveBeenCalled();
    await group.heartbeat({ force: true });
    expect(shareGroupHeartbeat).toHaveBeenCalledTimes(1);
  });

  it('resolves assignment topic ids via ShareGroupDescribe when multiple topics are subscribed', async () => {
    const topicId = Buffer.alloc(16, 9);
    const shareGroupHeartbeat = vi.fn().mockResolvedValue({
      errorCode: 0,
      memberId: 'member-1',
      memberEpoch: 1,
      heartbeatIntervalMs: 50,
      assignment: { topicPartitions: [{ topicId, partitions: [2] }] },
    });
    const shareGroupDescribe = vi.fn().mockResolvedValue({
      groups: [
        {
          groupId: 'share-1',
          members: [{ assignment: { topicPartitions: [{ topicId, topicName: 'clicks', partitions: [2] }] } }],
        },
      ],
    });
    const coordinator = { shareGroupHeartbeat, shareGroupDescribe };
    const cluster = {
      connect: vi.fn(),
      findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
    } as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger, retry: { retries: 0 } });
    group.subscribe(['events', 'clicks']);
    await group.joinAndSync();

    expect(shareGroupDescribe).toHaveBeenCalled();
    expect(group.assigned()).toEqual([{ topic: 'clicks', partitions: [2] }]);
    expect(group.hasAssignment('clicks', 2)).toBe(true);
    expect(group.hasAssignment('events', 0)).toBe(false);
  });

  it('caches a resolved topic id so a later heartbeat does not re-describe', async () => {
    const topicId = Buffer.alloc(16, 3);
    const shareGroupHeartbeat = vi
      .fn()
      .mockResolvedValueOnce({
        errorCode: 0,
        memberId: 'member-1',
        memberEpoch: 1,
        heartbeatIntervalMs: 50,
        assignment: { topicPartitions: [{ topicId, partitions: [0] }] },
      })
      .mockResolvedValueOnce({
        errorCode: 0,
        memberId: 'member-1',
        memberEpoch: 1,
        heartbeatIntervalMs: 50,
        assignment: { topicPartitions: [{ topicId, partitions: [0, 1] }] },
      });
    const shareGroupDescribe = vi.fn().mockResolvedValue({
      groups: [
        {
          groupId: 'share-1',
          members: [{ assignment: { topicPartitions: [{ topicId, topicName: 'events', partitions: [0] }] } }],
        },
      ],
    });
    const coordinator = { shareGroupHeartbeat, shareGroupDescribe };
    const cluster = {
      findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
    } as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger, retry: { retries: 0 } });
    group.subscribe(['events', 'clicks']);
    await group.joinAndSync();
    await group.heartbeat({ force: true });
    expect(shareGroupDescribe).toHaveBeenCalledTimes(1);
    expect(group.assigned()).toEqual([{ topic: 'events', partitions: [0, 1] }]);
  });

  it('throws when a topic id cannot be resolved to a subscribed name', async () => {
    const topicId = Buffer.alloc(16, 4);
    const shareGroupHeartbeat = vi.fn().mockResolvedValue({
      errorCode: 0,
      memberId: 'member-1',
      memberEpoch: 1,
      heartbeatIntervalMs: 50,
      assignment: { topicPartitions: [{ topicId, partitions: [0] }] },
    });
    const shareGroupDescribe = vi.fn().mockResolvedValue({ groups: [] });
    const coordinator = { shareGroupHeartbeat, shareGroupDescribe };
    const cluster = {
      findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
    } as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger, retry: { retries: 0 } });
    group.subscribe(['events', 'clicks']);
    await expect(group.joinAndSync()).rejects.toThrow('Unable to resolve topic id');
  });

  it('filters assigned partitions by leader node', () => {
    const cluster = {
      findLeaderForPartitions: vi.fn((topic: string, partitions: number[]) => {
        if (topic === 'orders') return { 1: [0], 2: [1] };
        return { 1: partitions };
      }),
    } as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger });
    group.assignment = [
      { topic: 'orders', partitions: [0, 1] },
      { topic: 'payments', partitions: [0] },
    ];

    expect(group.getNodeIds().sort()).toEqual(['1', '2']);
    expect(group.filterPartitionsByNode('1', group.assignment)).toEqual([
      { topic: 'orders', partitions: [0] },
      { topic: 'payments', partitions: [0] },
    ]);
    expect(group.filterPartitionsByNode('2', group.assignment)).toEqual([{ topic: 'orders', partitions: [1] }]);
    expect(group.filterPartitionsByNode('9', group.assignment)).toEqual([]);
  });

  it('re-finds the coordinator on NOT_COORDINATOR_FOR_GROUP during fetch recovery', async () => {
    const coordinator = { shareGroupHeartbeat: vi.fn() };
    const findGroupCoordinator = vi.fn().mockResolvedValue(coordinator);
    const cluster = { findGroupCoordinator } as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger });

    await expect(
      group.recoverFromFetch(Object.assign(new Error('moved'), { type: 'NOT_COORDINATOR_FOR_GROUP' })),
    ).rejects.toThrow('moved');
    expect(findGroupCoordinator).toHaveBeenCalledWith({ groupId: 'share-1' });
    expect(group.coordinator).toBe(coordinator);
  });

  it('ignores unrelated fetch errors during recovery', async () => {
    const findGroupCoordinator = vi.fn();
    const cluster = { findGroupCoordinator } as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger });
    await expect(
      group.recoverFromFetch(Object.assign(new Error('range'), { type: 'OFFSET_OUT_OF_RANGE' })),
    ).resolves.toBeUndefined();
    expect(findGroupCoordinator).not.toHaveBeenCalled();
  });
});
