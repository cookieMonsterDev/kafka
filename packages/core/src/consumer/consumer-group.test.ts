import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { InstrumentationEventEmitter } from '../instrumentation/emitter';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { ISOLATION_LEVEL } from '../protocol/enums/isolation-level';
import { MemberAssignment } from './assigner-protocol';
import { ConsumerGroup, nextAdaptiveMaxBytes } from './consumer-group';
import { GROUP_JOIN } from './instrumentation-events';
import type { OffsetManager } from './offset-manager/index';
import type { Assigner } from './types';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function createGroup(): ConsumerGroup {
  return new ConsumerGroup({
    logger: silentLogger,
    topics: ['topic1'],
    topicConfigurations: {},
    cluster: {} as Cluster,
    groupId: 'group',
    assigners: [],
    sessionTimeout: 30_000,
    rebalanceTimeout: 60_000,
    maxBytesPerPartition: 1024,
    minBytes: 1,
    maxBytes: 1024,
    maxWaitTimeInMs: 100,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
    rackId: '',
    metadataMaxAge: 300_000,
    autoCommit: true,
    autoCommitInterval: null,
    autoCommitThreshold: null,
  });
}

describe('consumer/consumer-group', () => {
  it('delegates uncommittedOffsets to the offset manager', () => {
    const consumerGroup = createGroup();
    const mockOffsets = { topics: [] };
    const uncommittedOffsets = vi.fn(() => mockOffsets);
    consumerGroup.offsetManager = { uncommittedOffsets } as unknown as OffsetManager;

    expect(consumerGroup.uncommittedOffsets()).toStrictEqual(mockOffsets);
    expect(uncommittedOffsets).toHaveBeenCalled();
  });

  it('delegates commitOffsets to the offset manager', async () => {
    const consumerGroup = createGroup();
    const commitOffsets = vi.fn(async () => undefined);
    consumerGroup.offsetManager = { commitOffsets } as unknown as OffsetManager;

    const offsets = { topics: [{ topic: 'topic1', partitions: [{ offset: 0n, partition: 0 }] }] };
    await consumerGroup.commitOffsets(offsets);
    expect(commitOffsets).toHaveBeenCalledTimes(1);
    expect(commitOffsets).toHaveBeenCalledWith(offsets);
  });

  it('settles a cooperative revoke with a second join and sync generation', async () => {
    const firstAssignment = MemberAssignment.encode({
      version: 1,
      assignment: { topic1: [0] },
    });
    const settledAssignment = MemberAssignment.encode({
      version: 1,
      assignment: { topic1: [0, 2] },
    });
    const joinGroup = vi.fn(async () => ({
      generationId: joinGroup.mock.calls.length,
      leaderId: 'other-member',
      memberId: 'member-1',
      members: [],
      groupProtocol: 'cooperative',
    }));
    const syncGroup = vi
      .fn()
      .mockResolvedValueOnce({ memberAssignment: firstAssignment })
      .mockResolvedValueOnce({ memberAssignment: settledAssignment });
    const cluster = {
      findGroupCoordinator: vi.fn(async () => ({ joinGroup, syncGroup })),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0 }, { partitionId: 1 }, { partitionId: 2 }]),
      committedOffsets: vi.fn(() => ({})),
    } as unknown as Cluster;
    const onAssignment = vi.fn();
    const assigner: Assigner = {
      name: 'cooperative',
      version: 1,
      protocolType: 'cooperative',
      assign: vi.fn(async () => []),
      protocol: vi.fn(() => ({ name: 'cooperative', metadata: Buffer.alloc(0) })),
      onAssignment,
    };
    const consumerGroup = createGroup();
    consumerGroup.cluster = cluster;
    consumerGroup.assigners = [assigner];
    consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1] }]);
    const groupJoins: unknown[] = [];
    consumerGroup.instrumentationEmitter.addListener(GROUP_JOIN, (event) => groupJoins.push(event));

    await consumerGroup.joinAndSync();

    expect(joinGroup).toHaveBeenCalledTimes(2);
    expect(syncGroup).toHaveBeenCalledTimes(2);
    expect(onAssignment).toHaveBeenNthCalledWith(1, { topic1: [0] });
    expect(onAssignment).toHaveBeenNthCalledWith(2, { topic1: [0, 2] });
    expect(consumerGroup.assigned()).toEqual([{ topic: 'topic1', partitions: [0, 2] }]);
    expect(groupJoins).toHaveLength(1);
  });

  it('keeps eager assignment changes to one join and sync generation', async () => {
    const joinGroup = vi.fn(async () => ({
      generationId: 1,
      leaderId: 'other-member',
      memberId: 'member-1',
      members: [],
      groupProtocol: 'eager',
    }));
    const syncGroup = vi.fn(async () => ({
      memberAssignment: MemberAssignment.encode({
        version: 0,
        assignment: { topic1: [2] },
      }),
    }));
    const cluster = {
      findGroupCoordinator: vi.fn(async () => ({ joinGroup, syncGroup })),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0 }, { partitionId: 1 }, { partitionId: 2 }]),
      committedOffsets: vi.fn(() => ({})),
    } as unknown as Cluster;
    const assigner: Assigner = {
      name: 'eager',
      version: 0,
      protocolType: 'eager',
      assign: vi.fn(async () => []),
      protocol: vi.fn(() => ({ name: 'eager', metadata: Buffer.alloc(0) })),
    };
    const consumerGroup = createGroup();
    consumerGroup.cluster = cluster;
    consumerGroup.assigners = [assigner];
    consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1] }]);

    await consumerGroup.joinAndSync();

    expect(joinGroup).toHaveBeenCalledTimes(1);
    expect(syncGroup).toHaveBeenCalledTimes(1);
    expect(consumerGroup.assigned()).toEqual([{ topic: 'topic1', partitions: [2] }]);
  });

  it('uses JoinGroup when groupProtocol is omitted', async () => {
    const joinGroup = vi.fn(async () => ({
      generationId: 1,
      leaderId: 'other-member',
      memberId: 'member-1',
      members: [],
      groupProtocol: 'eager',
    }));
    const syncGroup = vi.fn(async () => ({
      memberAssignment: MemberAssignment.encode({
        version: 0,
        assignment: { topic1: [0] },
      }),
    }));
    const consumerGroupHeartbeat = vi.fn();
    const cluster = {
      findGroupCoordinator: vi.fn(async () => ({ joinGroup, syncGroup, consumerGroupHeartbeat })),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0 }]),
      committedOffsets: vi.fn(() => ({})),
    } as unknown as Cluster;
    const consumerGroup = createGroup();
    consumerGroup.cluster = cluster;
    consumerGroup.assigners = [
      {
        name: 'eager',
        version: 0,
        protocolType: 'eager',
        assign: vi.fn(async () => []),
        protocol: vi.fn(() => ({ name: 'eager', metadata: Buffer.alloc(0) })),
      },
    ];

    await consumerGroup.joinAndSync();

    expect(joinGroup).toHaveBeenCalled();
    expect(consumerGroupHeartbeat).not.toHaveBeenCalled();
  });

  it('joins with ConsumerGroupHeartbeat when groupProtocol is consumer', async () => {
    const topicId = Buffer.from('0123456789abcdef');
    const consumerGroupHeartbeat = vi
      .fn()
      .mockResolvedValueOnce({
        memberId: 'generated-member',
        memberEpoch: 1,
        heartbeatIntervalMs: 5_000,
        assignment: { topicPartitions: [{ topicId, partitions: [0, 1] }] },
      })
      .mockResolvedValueOnce({
        memberId: 'generated-member',
        memberEpoch: 1,
        heartbeatIntervalMs: 5_000,
        assignment: null,
      });
    const joinGroup = vi.fn();
    const cluster = {
      findGroupCoordinator: vi.fn(async () => ({ joinGroup, consumerGroupHeartbeat })),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0 }, { partitionId: 1 }]),
      committedOffsets: vi.fn(() => ({})),
      refreshMetadata: vi.fn(async () => undefined),
    } as unknown as Cluster;
    const consumerGroup = new ConsumerGroup({
      logger: silentLogger,
      topics: ['topic1'],
      topicConfigurations: {},
      cluster,
      groupId: 'group',
      assigners: [],
      sessionTimeout: 30_000,
      rebalanceTimeout: 60_000,
      maxBytesPerPartition: 1024,
      minBytes: 1,
      maxBytes: 1024,
      maxWaitTimeInMs: 100,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
      rackId: '',
      metadataMaxAge: 300_000,
      autoCommit: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      groupProtocol: 'consumer',
    });

    await consumerGroup.joinAndSync();

    expect(joinGroup).not.toHaveBeenCalled();
    expect(consumerGroupHeartbeat).toHaveBeenCalled();
    expect(consumerGroupHeartbeat.mock.calls[0]?.[0]).toMatchObject({
      groupId: 'group',
      memberEpoch: 0,
      rebalanceTimeoutMs: 60_000,
      subscribedTopicNames: ['topic1'],
      topicPartitions: [],
    });
    expect(consumerGroup.assigned()).toEqual([{ topic: 'topic1', partitions: [0, 1] }]);
    expect(consumerGroup.groupProtocol).toBe('consumer');
    expect(consumerGroupHeartbeat.mock.calls[1]?.[0]).toMatchObject({
      memberEpoch: 1,
      subscribedTopicNames: null,
    });
    expect(consumerGroupHeartbeat.mock.calls[1]?.[0].topicPartitions).toEqual([{ topicId, partitions: [0, 1] }]);
  });

  it('emits GROUP_JOIN when a later heartbeat installs a new assignment', async () => {
    const topicId = Buffer.from('0123456789abcdef');
    const consumerGroupHeartbeat = vi
      .fn()
      .mockResolvedValueOnce({
        memberId: 'generated-member',
        memberEpoch: 1,
        heartbeatIntervalMs: 5_000,
        assignment: { topicPartitions: [{ topicId, partitions: [0, 1] }] },
      })
      .mockResolvedValueOnce({
        memberId: 'generated-member',
        memberEpoch: 1,
        heartbeatIntervalMs: 5_000,
        assignment: null,
      })
      .mockResolvedValueOnce({
        memberId: 'generated-member',
        memberEpoch: 2,
        heartbeatIntervalMs: 5_000,
        assignment: { topicPartitions: [{ topicId, partitions: [0] }] },
      })
      .mockResolvedValueOnce({
        memberId: 'generated-member',
        memberEpoch: 2,
        heartbeatIntervalMs: 5_000,
        assignment: null,
      });
    const cluster = {
      findGroupCoordinator: vi.fn(async () => ({ joinGroup: vi.fn(), consumerGroupHeartbeat })),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0 }, { partitionId: 1 }]),
      committedOffsets: vi.fn(() => ({})),
      refreshMetadata: vi.fn(async () => undefined),
    } as unknown as Cluster;
    const instrumentationEmitter = new InstrumentationEventEmitter();
    const groupJoins: { payload: { memberAssignment: Record<string, number[]> } }[] = [];
    instrumentationEmitter.addListener(GROUP_JOIN, (event) =>
      groupJoins.push(event as { payload: { memberAssignment: Record<string, number[]> } }),
    );
    const consumerGroup = new ConsumerGroup({
      logger: silentLogger,
      topics: ['topic1'],
      topicConfigurations: {},
      cluster,
      groupId: 'group',
      assigners: [],
      sessionTimeout: 30_000,
      rebalanceTimeout: 60_000,
      maxBytesPerPartition: 1024,
      minBytes: 1,
      maxBytes: 1024,
      maxWaitTimeInMs: 100,
      instrumentationEmitter,
      isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
      rackId: '',
      metadataMaxAge: 300_000,
      autoCommit: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      groupProtocol: 'consumer',
    });

    await consumerGroup.joinAndSync();
    expect(groupJoins).toHaveLength(1);

    consumerGroup.lastRequest = 0;
    await consumerGroup.heartbeat({ interval: 0 });

    expect(groupJoins).toHaveLength(2);
    expect(groupJoins[1]?.payload.memberAssignment).toEqual({ topic1: [0] });
    expect(consumerGroup.assigned()).toEqual([{ topic: 'topic1', partitions: [0] }]);
  });

  it('caches getActiveTopicPartitions until pause, resume, or assign', () => {
    const consumerGroup = createGroup();
    consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1] }]);

    const first = consumerGroup.getActiveTopicPartitions();
    const second = consumerGroup.getActiveTopicPartitions();
    expect(first).toBe(second);
    expect([...(first.topic1 ?? [])]).toEqual([0, 1]);

    consumerGroup.pause([{ topic: 'topic1', partitions: [1] }]);
    const paused = consumerGroup.getActiveTopicPartitions();
    expect(paused).not.toBe(first);
    expect([...(paused.topic1 ?? [])]).toEqual([0]);

    const pausedAgain = consumerGroup.getActiveTopicPartitions();
    expect(pausedAgain).toBe(paused);

    consumerGroup.resume([{ topic: 'topic1', partitions: [1] }]);
    const resumed = consumerGroup.getActiveTopicPartitions();
    expect(resumed).not.toBe(paused);
    expect([...(resumed.topic1 ?? [])]).toEqual([0, 1]);
  });

  it('sleeps the short empty-node interval instead of maxWaitTime', async () => {
    vi.useFakeTimers();
    const consumerGroup = new ConsumerGroup({
      logger: silentLogger,
      topics: ['topic1'],
      topicConfigurations: {},
      cluster: {
        refreshMetadataIfNecessary: vi.fn(async () => undefined),
        findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1 }]),
        findTopicId: vi.fn(() => undefined),
      } as unknown as Cluster,
      groupId: 'group',
      assigners: [],
      sessionTimeout: 30_000,
      rebalanceTimeout: 60_000,
      maxBytesPerPartition: 1024,
      minBytes: 1,
      maxBytes: 1024,
      maxWaitTimeInMs: 5_000,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
      rackId: '',
      metadataMaxAge: 300_000,
      autoCommit: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
    });
    consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0] }]);
    consumerGroup.pause([{ topic: 'topic1' }]);
    consumerGroup.offsetManager = {
      committedOffsets: () => ({ topic1: { 0: 0n } }),
      nextOffset: () => 0n,
      seek: vi.fn(async () => undefined),
      resolveOffsets: vi.fn(async () => undefined),
    } as unknown as OffsetManager;

    const fetchPromise = consumerGroup.fetch('1');
    try {
      await vi.advanceTimersByTimeAsync(100);
      await expect(fetchPromise).resolves.toEqual([]);
    } finally {
      vi.useRealTimers();
    }
  });

  it('adapts fetch maxBytes from fill ratio', () => {
    expect(nextAdaptiveMaxBytes({ current: 1000, used: 1000, min: 100, max: 10_000 })).toBe(1500);
    expect(nextAdaptiveMaxBytes({ current: 1000, used: 200, min: 100, max: 10_000 })).toBe(500);
    expect(nextAdaptiveMaxBytes({ current: 1000, used: 500, min: 100, max: 10_000 })).toBe(1000);
    expect(nextAdaptiveMaxBytes({ current: 100, used: 0, min: 100, max: 10_000 })).toBe(100);
    expect(nextAdaptiveMaxBytes({ current: 8000, used: 8000, min: 100, max: 9000 })).toBe(9000);
  });
});
