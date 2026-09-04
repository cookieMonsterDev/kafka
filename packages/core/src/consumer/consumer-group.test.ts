import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaOffsetOutOfRange } from '../errors';
import { InstrumentationEventEmitter } from '../instrumentation/emitter';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createErrorFromCode, ERROR_CODES } from '../protocol/error-codes';
import { ISOLATION_LEVEL } from '../protocol/enums/isolation-level';
import { MemberAssignment } from './assigner-protocol';
import { ConsumerGroup, nextAdaptiveMaxBytes } from './consumer-group';
import { GROUP_JOIN } from './instrumentation-events';
import type { OffsetManager } from './offset-manager/index';
import type { TopicOffsetConfiguration } from './offset-reset';
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
    const onPartitionsRevoked = vi.fn();
    const onPartitionsAssigned = vi.fn();
    consumerGroup.onPartitionsRevoked = onPartitionsRevoked;
    consumerGroup.onPartitionsAssigned = onPartitionsAssigned;

    await consumerGroup.joinAndSync();

    expect(joinGroup).toHaveBeenCalledTimes(2);
    expect(syncGroup).toHaveBeenCalledTimes(2);
    expect(onAssignment).toHaveBeenNthCalledWith(1, { topic1: [0] });
    expect(onAssignment).toHaveBeenNthCalledWith(2, { topic1: [0, 2] });
    expect(consumerGroup.assigned()).toEqual([{ topic: 'topic1', partitions: [0, 2] }]);
    expect(groupJoins).toHaveLength(1);

    // Cooperative-sticky incremental rebalance: only the partition actually given up (1) is
    // reported revoked - partition 0, kept across both rounds, is never reported. Only the
    // partition actually gained (2) is reported assigned - it never fires for round 1, where
    // nothing was gained.
    expect(onPartitionsRevoked).toHaveBeenCalledTimes(1);
    expect(onPartitionsRevoked).toHaveBeenCalledWith([{ topic: 'topic1', partition: 1 }]);
    expect(onPartitionsAssigned).toHaveBeenCalledTimes(1);
    expect(onPartitionsAssigned).toHaveBeenCalledWith([{ topic: 'topic1', partition: 2 }]);
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
    const callOrder: string[] = [];
    const onPartitionsRevoked = vi.fn(() => {
      callOrder.push('revoked');
    });
    const onPartitionsAssigned = vi.fn(() => {
      callOrder.push('assigned');
    });
    consumerGroup.onPartitionsRevoked = onPartitionsRevoked;
    consumerGroup.onPartitionsAssigned = onPartitionsAssigned;

    await consumerGroup.joinAndSync();

    expect(joinGroup).toHaveBeenCalledTimes(1);
    expect(syncGroup).toHaveBeenCalledTimes(1);
    expect(consumerGroup.assigned()).toEqual([{ topic: 'topic1', partitions: [2] }]);

    // Classic (eager) full rebalance: the entire prior assignment is reported revoked, then the
    // entire new assignment is reported assigned, revoke strictly before assign.
    expect(onPartitionsRevoked).toHaveBeenCalledWith([
      { topic: 'topic1', partition: 0 },
      { topic: 'topic1', partition: 1 },
    ]);
    expect(onPartitionsAssigned).toHaveBeenCalledWith([{ topic: 'topic1', partition: 2 }]);
    expect(callOrder).toEqual(['revoked', 'assigned']);
  });

  it('does not call onPartitionsRevoked on the very first join, but reports the assignment as gained', async () => {
    const joinGroup = vi.fn(async () => ({
      generationId: 1,
      leaderId: 'other-member',
      memberId: 'member-1',
      members: [],
      groupProtocol: 'eager',
    }));
    const syncGroup = vi.fn(async () => ({
      memberAssignment: MemberAssignment.encode({ version: 0, assignment: { topic1: [0] } }),
    }));
    const cluster = {
      findGroupCoordinator: vi.fn(async () => ({ joinGroup, syncGroup })),
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
    const onPartitionsRevoked = vi.fn();
    const onPartitionsAssigned = vi.fn();
    consumerGroup.onPartitionsRevoked = onPartitionsRevoked;
    consumerGroup.onPartitionsAssigned = onPartitionsAssigned;

    await consumerGroup.joinAndSync();

    expect(onPartitionsRevoked).not.toHaveBeenCalled();
    expect(onPartitionsAssigned).toHaveBeenCalledWith([{ topic: 'topic1', partition: 0 }]);
  });

  it('logs and continues past a throwing onPartitionsRevoked/onPartitionsAssigned callback', async () => {
    const joinGroup = vi.fn(async () => ({
      generationId: 1,
      leaderId: 'other-member',
      memberId: 'member-1',
      members: [],
      groupProtocol: 'eager',
    }));
    const syncGroup = vi.fn(async () => ({
      memberAssignment: MemberAssignment.encode({ version: 0, assignment: { topic1: [2] } }),
    }));
    const cluster = {
      findGroupCoordinator: vi.fn(async () => ({ joinGroup, syncGroup })),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0 }, { partitionId: 1 }, { partitionId: 2 }]),
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
    consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1] }]);
    consumerGroup.onPartitionsRevoked = vi.fn(() => {
      throw new Error('boom from onPartitionsRevoked');
    });
    const onPartitionsAssigned = vi.fn();
    consumerGroup.onPartitionsAssigned = onPartitionsAssigned;

    // A throwing rebalance listener is logged and swallowed, not propagated: it must not abort a
    // rebalance that would otherwise succeed (same "ordered async, errors isolated" policy this
    // codebase already applies to other user-supplied hooks).
    await expect(consumerGroup.joinAndSync()).resolves.toBeUndefined();
    expect(consumerGroup.assigned()).toEqual([{ topic: 'topic1', partitions: [2] }]);
    expect(onPartitionsAssigned).toHaveBeenCalledWith([{ topic: 'topic1', partition: 2 }]);
  });

  it('fires onPartitionsLost (not onPartitionsRevoked) when the coordinator has already forgotten this member', async () => {
    const joinGroup = vi.fn(async () => {
      throw createErrorFromCode(ERROR_CODES.find((entry) => entry.type === 'UNKNOWN_MEMBER_ID')!.code);
    });
    const cluster = {
      findGroupCoordinator: vi.fn(async () => ({ joinGroup })),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0 }, { partitionId: 1 }]),
      committedOffsets: vi.fn(() => ({})),
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
      retry: { retries: 0 },
    });
    consumerGroup.cluster = cluster;
    consumerGroup.memberId = 'stale-member';
    consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1] }]);
    const onPartitionsRevoked = vi.fn();
    const onPartitionsLost = vi.fn();
    consumerGroup.onPartitionsRevoked = onPartitionsRevoked;
    consumerGroup.onPartitionsLost = onPartitionsLost;

    await expect(consumerGroup.joinAndSync()).rejects.toThrow();

    expect(onPartitionsRevoked).not.toHaveBeenCalled();
    expect(onPartitionsLost).toHaveBeenCalledTimes(1);
    expect(onPartitionsLost).toHaveBeenCalledWith([
      { topic: 'topic1', partition: 0 },
      { topic: 'topic1', partition: 1 },
    ]);
    // The lost assignment is cleared so a later successful rejoin doesn't also report it revoked.
    expect(consumerGroup.assigned()).toEqual([]);
  });

  it('notifyPartitionsLost is a no-op with nothing assigned', async () => {
    const consumerGroup = createGroup();
    const onPartitionsLost = vi.fn();
    consumerGroup.onPartitionsLost = onPartitionsLost;

    await consumerGroup.notifyPartitionsLost();

    expect(onPartitionsLost).not.toHaveBeenCalled();
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

  it('sends serverAssignor on ConsumerGroupHeartbeat when groupRemoteAssignor is set (KIP-848)', async () => {
    const consumerGroupHeartbeat = vi.fn().mockResolvedValueOnce({
      memberId: 'generated-member',
      memberEpoch: 1,
      heartbeatIntervalMs: 5_000,
      assignment: null,
    });
    const cluster = {
      findGroupCoordinator: vi.fn(async () => ({ joinGroup: vi.fn(), consumerGroupHeartbeat })),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0 }]),
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
      groupRemoteAssignor: 'uniform',
    });

    await consumerGroup.joinAndSync();

    expect(consumerGroup.groupRemoteAssignor).toBe('uniform');
    expect(consumerGroupHeartbeat.mock.calls[0]?.[0]).toMatchObject({ serverAssignor: 'uniform' });
  });

  it('ignores groupRemoteAssignor and subscribedTopicRegex when groupProtocol is classic', () => {
    const entries: { log: { message: string } }[] = [];
    const logger = createLogger({ level: LOG_LEVELS.DEBUG, logCreator: () => (entry) => entries.push(entry) });

    const consumerGroup = new ConsumerGroup({
      logger,
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
      groupProtocol: 'classic',
      groupRemoteAssignor: 'uniform',
      subscribedTopicRegex: 'foo.*',
    });

    expect(consumerGroup.groupRemoteAssignor).toBeNull();
    expect(consumerGroup.subscribedTopicRegex).toBeNull();
    expect(entries.some((entry) => entry.log.message.includes('groupRemoteAssignor'))).toBe(true);
    expect(entries.some((entry) => entry.log.message.includes('subscribedTopicRegex'))).toBe(true);
  });

  it('sends subscribedTopicRegex instead of topic names and applies its offset config to regex-matched topics', async () => {
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
    const consumerGroupDescribe = vi.fn(async () => ({
      groups: [
        {
          groupId: 'group',
          members: [
            {
              memberId: 'generated-member',
              assignment: { topicPartitions: [{ topicId, topicName: 'matched-topic' }] },
              targetAssignment: { topicPartitions: [] },
            },
          ],
        },
      ],
    }));
    const cluster = {
      findGroupCoordinator: vi.fn(async () => ({ joinGroup: vi.fn(), consumerGroupHeartbeat, consumerGroupDescribe })),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0 }, { partitionId: 1 }]),
      committedOffsets: vi.fn(() => ({})),
      refreshMetadata: vi.fn(async () => undefined),
    } as unknown as Cluster;
    const topicConfigurations: Record<string, TopicOffsetConfiguration> = {};
    const consumerGroup = new ConsumerGroup({
      logger: silentLogger,
      topics: [],
      topicConfigurations,
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
      subscribedTopicRegex: 'matched-.*',
      defaultTopicConfiguration: { autoOffsetReset: 'earliest' },
    });

    await consumerGroup.joinAndSync();

    expect(consumerGroupHeartbeat.mock.calls[0]?.[0]).toMatchObject({
      subscribedTopicNames: [],
      subscribedTopicRegex: 'matched-.*',
    });
    expect(consumerGroup.assigned()).toEqual([{ topic: 'matched-topic', partitions: [0, 1] }]);
    expect(topicConfigurations['matched-topic']).toEqual({ autoOffsetReset: 'earliest' });
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
    const onPartitionsRevoked = vi.fn();
    const onPartitionsAssigned = vi.fn();
    consumerGroup.onPartitionsRevoked = onPartitionsRevoked;
    consumerGroup.onPartitionsAssigned = onPartitionsAssigned;

    await consumerGroup.joinAndSync();
    expect(groupJoins).toHaveLength(1);
    // First join: nothing was previously held, so onPartitionsRevoked doesn't fire; the whole
    // initial assignment is reported gained.
    expect(onPartitionsRevoked).not.toHaveBeenCalled();
    expect(onPartitionsAssigned).toHaveBeenCalledWith([
      { topic: 'topic1', partition: 0 },
      { topic: 'topic1', partition: 1 },
    ]);
    onPartitionsAssigned.mockClear();

    consumerGroup.lastRequest = 0;
    await consumerGroup.heartbeat({ interval: 0 });

    expect(groupJoins).toHaveLength(2);
    expect(groupJoins[1]?.payload.memberAssignment).toEqual({ topic1: [0] });
    expect(consumerGroup.assigned()).toEqual([{ topic: 'topic1', partitions: [0] }]);

    // KIP-848 reconciliation is incremental at the wire level: the target assignment shrank from
    // {0,1} to {0}, so only partition 1 is reported revoked and nothing is reported gained.
    expect(onPartitionsRevoked).toHaveBeenCalledWith([{ topic: 'topic1', partition: 1 }]);
    expect(onPartitionsAssigned).not.toHaveBeenCalled();
  });

  it('recovers the leader from a CurrentLeader hint instead of a metadata refresh and rejoin (KIP-951)', async () => {
    const consumerGroup = createGroup();
    const applyLeaderUpdate = vi.fn().mockResolvedValue(true);
    const refreshMetadata = vi.fn().mockResolvedValue(undefined);
    consumerGroup.cluster = { applyLeaderUpdate, refreshMetadata } as unknown as Cluster;
    const joinAndSync = vi.fn().mockResolvedValue(undefined);
    consumerGroup.joinAndSync = joinAndSync;

    const code = ERROR_CODES.find((entry) => entry.type === 'NOT_LEADER_OR_FOLLOWER')!.code;
    const error = createErrorFromCode(code, {
      topic: 'topic1',
      partition: 0,
      currentLeader: { leaderId: 2, leaderEpoch: 5 },
      nodeEndpoints: [{ nodeId: 2, host: 'broker-2', port: 9093, rack: null }],
    });

    await consumerGroup.recoverFromFetch(error);

    expect(applyLeaderUpdate).toHaveBeenCalledWith({
      topic: 'topic1',
      partition: 0,
      currentLeader: { leaderId: 2, leaderEpoch: 5 },
      nodeEndpoints: [{ nodeId: 2, host: 'broker-2', port: 9093, rack: null }],
    });
    expect(refreshMetadata).not.toHaveBeenCalled();
    expect(joinAndSync).not.toHaveBeenCalled();
  });

  it('falls back to a full metadata refresh and rejoin when the leader patch misses (KIP-951)', async () => {
    const consumerGroup = createGroup();
    const applyLeaderUpdate = vi.fn().mockResolvedValue(false);
    const refreshMetadata = vi.fn().mockResolvedValue(undefined);
    consumerGroup.cluster = { applyLeaderUpdate, refreshMetadata } as unknown as Cluster;
    const joinAndSync = vi.fn().mockResolvedValue(undefined);
    consumerGroup.joinAndSync = joinAndSync;

    const code = ERROR_CODES.find((entry) => entry.type === 'NOT_LEADER_OR_FOLLOWER')!.code;
    const error = createErrorFromCode(code, {
      topic: 'topic1',
      partition: 0,
      currentLeader: { leaderId: 2, leaderEpoch: 5 },
    });

    await consumerGroup.recoverFromFetch(error);

    expect(applyLeaderUpdate).toHaveBeenCalled();
    expect(refreshMetadata).toHaveBeenCalled();
    expect(joinAndSync).toHaveBeenCalled();
  });

  it('sends the assigned currentLeaderEpoch on fetch and seeks past a truncated log via OffsetForLeaderEpoch (KIP-320)', async () => {
    const brokerFetch = vi.fn().mockResolvedValue({ responses: [] });
    const offsetForLeaderEpoch = vi.fn().mockResolvedValue({
      throttleTime: 0,
      topics: [{ topic: 'topic1', partitions: [{ errorCode: 0, partition: 0, leaderEpoch: 4, endOffset: 50n }] }],
    });
    const broker = { fetch: brokerFetch, offsetForLeaderEpoch };
    const cluster = {
      refreshMetadataIfNecessary: vi.fn(async () => undefined),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1, leaderEpoch: 4 }]),
      findTopicId: vi.fn(() => undefined),
      findBroker: vi.fn(async () => broker),
      applyLeaderUpdate: vi.fn().mockResolvedValue(true),
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
    });
    consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0] }]);
    const seek = vi.fn(async () => undefined);
    consumerGroup.offsetManager = {
      committedOffsets: () => ({ topic1: { 0: 100n } }),
      nextOffset: () => 100n,
      seek,
      resolveOffsets: vi.fn(async () => undefined),
    } as unknown as OffsetManager;

    await consumerGroup.fetch('1');

    expect(brokerFetch).toHaveBeenCalledWith(
      expect.objectContaining({
        topics: [
          expect.objectContaining({
            topic: 'topic1',
            partitions: [expect.objectContaining({ partition: 0, currentLeaderEpoch: 4 })],
          }),
        ],
      }),
    );

    const code = ERROR_CODES.find((entry) => entry.type === 'FENCED_LEADER_EPOCH')!.code;
    const error = createErrorFromCode(code, {
      topic: 'topic1',
      partition: 0,
      currentLeader: { leaderId: 1, leaderEpoch: 6 },
    });

    await consumerGroup.recoverFromFetch(error);

    expect(offsetForLeaderEpoch).toHaveBeenCalledWith({
      topics: [{ topic: 'topic1', partitions: [{ partition: 0, currentLeaderEpoch: 4, leaderEpoch: 4 }] }],
    });
    expect(seek).toHaveBeenCalledWith({ topic: 'topic1', partition: 0, offset: 50n });
  });

  it('passes checkCrcs to broker.fetch, defaulting to true when omitted', async () => {
    const brokerFetch = vi.fn().mockResolvedValue({ responses: [] });
    const cluster = {
      refreshMetadataIfNecessary: vi.fn(async () => undefined),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1, leaderEpoch: 4 }]),
      findTopicId: vi.fn(() => undefined),
      findBroker: vi.fn(async () => ({ fetch: brokerFetch })),
    } as unknown as Cluster;

    const consumerGroup = createGroup();
    consumerGroup.cluster = cluster;
    consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0] }]);
    consumerGroup.offsetManager = {
      committedOffsets: () => ({ topic1: { 0: 100n } }),
      nextOffset: () => 100n,
      resolveOffsets: vi.fn(async () => undefined),
    } as unknown as OffsetManager;

    await consumerGroup.fetch('1');

    expect(brokerFetch).toHaveBeenCalledWith(expect.objectContaining({ checkCrcs: true }));
  });

  it('passes checkCrcs: false to broker.fetch when the consumer group was configured with it', async () => {
    const brokerFetch = vi.fn().mockResolvedValue({ responses: [] });
    const cluster = {
      refreshMetadataIfNecessary: vi.fn(async () => undefined),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1, leaderEpoch: 4 }]),
      findTopicId: vi.fn(() => undefined),
      findBroker: vi.fn(async () => ({ fetch: brokerFetch })),
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
      checkCrcs: false,
      rackId: '',
      metadataMaxAge: 300_000,
      autoCommit: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
    });
    consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0] }]);
    consumerGroup.offsetManager = {
      committedOffsets: () => ({ topic1: { 0: 100n } }),
      nextOffset: () => 100n,
      resolveOffsets: vi.fn(async () => undefined),
    } as unknown as OffsetManager;

    await consumerGroup.fetch('1');

    expect(brokerFetch).toHaveBeenCalledWith(expect.objectContaining({ checkCrcs: false }));
  });

  it('does not seek when the epoch end offset is at or beyond the current fetch position (no truncation)', async () => {
    const cluster = {
      refreshMetadataIfNecessary: vi.fn(async () => undefined),
      findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1, leaderEpoch: 4 }]),
      findTopicId: vi.fn(() => undefined),
      findBroker: vi.fn(async () => ({
        fetch: vi.fn().mockResolvedValue({ responses: [] }),
        offsetForLeaderEpoch: vi.fn().mockResolvedValue({
          throttleTime: 0,
          topics: [{ topic: 'topic1', partitions: [{ errorCode: 0, partition: 0, leaderEpoch: 3, endOffset: 100n }] }],
        }),
      })),
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
    });
    consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0] }]);
    const seek = vi.fn(async () => undefined);
    consumerGroup.offsetManager = {
      committedOffsets: () => ({ topic1: { 0: 100n } }),
      nextOffset: () => 100n,
      seek,
      resolveOffsets: vi.fn(async () => undefined),
    } as unknown as OffsetManager;

    // Populates the tracked "last fetched epoch" for topic1/partition 0 (leaderEpoch: 4).
    await consumerGroup.fetch('1');

    const recovered = await consumerGroup.recoverFromTruncation({ topic: 'topic1', partition: 0 });

    expect(recovered).toBe(false);
    expect(seek).not.toHaveBeenCalled();
  });

  it('falls back to a full metadata refresh (no OffsetForLeaderEpoch attempt) without a previously fetched epoch', async () => {
    const consumerGroup = createGroup();
    const findBroker = vi.fn();
    consumerGroup.cluster = { findBroker } as unknown as Cluster;

    const recovered = await consumerGroup.recoverFromTruncation({ topic: 'topic1', partition: 0 });

    expect(recovered).toBe(false);
    expect(findBroker).not.toHaveBeenCalled();
  });

  it('resets to the default offset on OFFSET_OUT_OF_RANGE when there is no tracked epoch to validate', async () => {
    const consumerGroup = createGroup();
    const findBroker = vi.fn();
    consumerGroup.cluster = { findBroker } as unknown as Cluster;
    const setDefaultOffset = vi.fn(async () => undefined);
    consumerGroup.offsetManager = { setDefaultOffset } as unknown as OffsetManager;

    const code = ERROR_CODES.find((entry) => entry.type === 'OFFSET_OUT_OF_RANGE')!.code;
    const error = new KafkaOffsetOutOfRange(createErrorFromCode(code), { topic: 'topic1', partition: 0 });

    await consumerGroup.recoverFromFetch(error);

    expect(findBroker).not.toHaveBeenCalled();
    expect(setDefaultOffset).toHaveBeenCalledWith({ topic: 'topic1', partition: 0 });
  });

  describe('recoverFromOffsetOutOfRange', () => {
    it('retries from the leader without resetting offsets when the partition has a tracked preferred replica', async () => {
      const consumerGroup = createGroup();
      consumerGroup.preferredReadReplicasPerTopicPartition = {
        topic1: { 0: { nodeId: 5, expireAt: Date.now() + 60_000 } },
      };
      const setDefaultOffset = vi.fn(async () => undefined);
      consumerGroup.offsetManager = { setDefaultOffset } as unknown as OffsetManager;

      const error = Object.assign(new Error('offset out of range'), { topic: 'topic1', partition: 0 });
      await consumerGroup.recoverFromOffsetOutOfRange(error);

      expect(setDefaultOffset).not.toHaveBeenCalled();
      expect(consumerGroup.preferredReadReplicasPerTopicPartition.topic1?.[0]).toBeUndefined();
    });

    it('resets to the default offset when the partition has no tracked preferred replica', async () => {
      const consumerGroup = createGroup();
      const setDefaultOffset = vi.fn(async () => undefined);
      consumerGroup.offsetManager = { setDefaultOffset } as unknown as OffsetManager;

      const error = Object.assign(new Error('offset out of range'), { topic: 'topic1', partition: 0 });
      await consumerGroup.recoverFromOffsetOutOfRange(error);

      expect(setDefaultOffset).toHaveBeenCalledWith({ topic: 'topic1', partition: 0 });
    });
  });

  describe('findReadReplicaForPartitions', () => {
    it('falls back to the leader when the tracked preferred replica is in offlineReplicas', () => {
      const consumerGroup = createGroup();
      consumerGroup.cluster = {
        findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1, offlineReplicas: [5] }]),
      } as unknown as Cluster;
      consumerGroup.preferredReadReplicasPerTopicPartition = {
        topic1: { 0: { nodeId: 5, expireAt: Date.now() + 60_000 } },
      };

      const result = consumerGroup.findReadReplicaForPartitions('topic1', [0]);

      expect(result).toEqual({ '1': [0] });
    });

    it('keeps using the preferred replica when only the leader is in offlineReplicas', () => {
      const consumerGroup = createGroup();
      consumerGroup.cluster = {
        findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1, offlineReplicas: [1] }]),
      } as unknown as Cluster;
      consumerGroup.preferredReadReplicasPerTopicPartition = {
        topic1: { 0: { nodeId: 5, expireAt: Date.now() + 60_000 } },
      };

      const result = consumerGroup.findReadReplicaForPartitions('topic1', [0]);

      expect(result).toEqual({ '5': [0] });
    });
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

  describe('position', () => {
    it('returns the offset manager next offset for an assigned partition', () => {
      const consumerGroup = createGroup();
      consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0] }]);
      consumerGroup.offsetManager = {
        nextOffset: vi.fn(() => 42n),
      } as unknown as OffsetManager;

      expect(consumerGroup.position('topic1', 0)).toBe(42n);
    });

    it('returns null for a partition that is not currently assigned', () => {
      const consumerGroup = createGroup();
      consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0] }]);
      consumerGroup.offsetManager = {
        nextOffset: vi.fn(() => 42n),
      } as unknown as OffsetManager;

      expect(consumerGroup.position('topic1', 1)).toBeNull();
      expect(consumerGroup.position('other-topic', 0)).toBeNull();
    });
  });

  describe('highWatermark', () => {
    it('returns null before any fetch has happened for the partition', () => {
      const consumerGroup = createGroup();
      expect(consumerGroup.highWatermark('topic1', 0)).toBeNull();
    });

    it('records the high watermark from a fetch response and exposes it per topic/partition', async () => {
      const brokerFetch = vi.fn().mockResolvedValue({
        responses: [
          {
            topicName: 'topic1',
            partitions: [{ partition: 0, highWatermark: 10n, messages: [] }],
          },
        ],
        sessionId: 0,
      });
      const cluster = {
        refreshMetadataIfNecessary: vi.fn(async () => undefined),
        findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1 }]),
        findTopicId: vi.fn(() => undefined),
        findBroker: vi.fn(async () => ({ fetch: brokerFetch })),
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
      });
      consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0] }]);
      consumerGroup.offsetManager = {
        committedOffsets: () => ({ topic1: { 0: 0n } }),
        nextOffset: () => 0n,
        seek: vi.fn(async () => undefined),
        resolveOffsets: vi.fn(async () => undefined),
      } as unknown as OffsetManager;

      expect(consumerGroup.highWatermark('topic1', 0)).toBeNull();

      await consumerGroup.fetch('1');

      expect(consumerGroup.highWatermark('topic1', 0)).toBe(10n);
    });
  });

  describe('KIP-227 fetch sessions', () => {
    function createGroupWithBroker(brokerFetch: ReturnType<typeof vi.fn>): { consumerGroup: ConsumerGroup } {
      const cluster = {
        refreshMetadataIfNecessary: vi.fn(async () => undefined),
        findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1, leaderEpoch: 4 }]),
        findTopicId: vi.fn(() => undefined),
        findBroker: vi.fn(async () => ({ fetch: brokerFetch })),
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
      });
      consumerGroup.subscriptionState.assign([{ topic: 'topic1', partitions: [0] }]);
      consumerGroup.offsetManager = {
        committedOffsets: () => ({ topic1: { 0: 0n } }),
        nextOffset: () => 100n,
        seek: vi.fn(async () => undefined),
        resolveOffsets: vi.fn(async () => undefined),
      } as unknown as OffsetManager;

      return { consumerGroup };
    }

    it('opens a session with sessionId 0 and reuses the granted id on the next fetch', async () => {
      const brokerFetch = vi
        .fn()
        .mockResolvedValueOnce({ responses: [], sessionId: 77 })
        .mockResolvedValueOnce({ responses: [], sessionId: 77 });
      const { consumerGroup } = createGroupWithBroker(brokerFetch);

      await consumerGroup.fetch('1');
      expect(brokerFetch).toHaveBeenNthCalledWith(
        1,
        expect.objectContaining({ sessionId: 0, sessionEpoch: 0, forgottenTopics: [] }),
      );

      await consumerGroup.fetch('1');
      expect(brokerFetch).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ sessionId: 77, sessionEpoch: 1, topics: [], forgottenTopics: [] }),
      );

      // The full desired set must still reach the wire request even when the incremental
      // `topics` omits an unchanged partition - it's what the v13+ response decoder uses to
      // resolve topicId back to a name for data the broker returns outside of `topics`.
      const secondCallArgs = brokerFetch.mock.calls[1]?.[0] as { topicsForResponse: { topic: string }[] };
      expect(secondCallArgs.topicsForResponse).toEqual([expect.objectContaining({ topic: 'topic1' })]);
    });

    it('resets the session and sends a full fetch again after the broker rejects it', async () => {
      const code = ERROR_CODES.find((entry) => entry.type === 'FETCH_SESSION_ID_NOT_FOUND')!.code;
      const brokerFetch = vi
        .fn()
        .mockResolvedValueOnce({ responses: [], sessionId: 77 })
        .mockRejectedValueOnce(createErrorFromCode(code))
        .mockResolvedValueOnce({ responses: [], sessionId: 91 });
      const { consumerGroup } = createGroupWithBroker(brokerFetch);

      await consumerGroup.fetch('1');
      await consumerGroup.fetch('1');
      await consumerGroup.fetch('1');

      expect(brokerFetch).toHaveBeenNthCalledWith(3, expect.objectContaining({ sessionId: 0, sessionEpoch: 0 }));
      expect(brokerFetch.mock.calls[2]?.[0]?.topics).toHaveLength(1);
    });

    it('closes the open fetch session on leave()', async () => {
      const brokerFetch = vi.fn().mockResolvedValueOnce({ responses: [], sessionId: 55 }).mockResolvedValueOnce({
        responses: [],
        sessionId: 55,
      });
      const { consumerGroup } = createGroupWithBroker(brokerFetch);
      consumerGroup.memberId = null;
      consumerGroup.coordinator = null;

      await consumerGroup.fetch('1');
      await consumerGroup.leave();

      expect(brokerFetch).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ sessionId: 55, sessionEpoch: -1, topics: [], forgottenTopics: [] }),
      );
    });
  });

  describe('assign() mode', () => {
    function createAssignGroup(
      overrides: {
        groupId?: string | null;
        cluster?: Record<string, unknown>;
        findGroupCoordinator?: ReturnType<typeof vi.fn>;
      } = {},
    ): {
      consumerGroup: ConsumerGroup;
      findGroupCoordinator: ReturnType<typeof vi.fn>;
    } {
      const findGroupCoordinator = overrides.findGroupCoordinator ?? vi.fn();
      const cluster = {
        connect: vi.fn(async () => undefined),
        refreshMetadata: vi.fn(async () => undefined),
        refreshMetadataIfNecessary: vi.fn(async () => undefined),
        findGroupCoordinator,
        findTopicPartitionMetadata: vi.fn(() => [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 1 },
        ]),
        findTopicId: vi.fn(() => undefined),
        findBroker: vi.fn(async () => ({ fetch: vi.fn(async () => ({ responses: [], sessionId: 0 })) })),
        fetchTopicsOffset: vi.fn(async () => [{ topic: 'topic1', partitions: [{ partition: 0, offset: 0n }] }]),
        defaultOffset: vi.fn(() => 0n),
        ...overrides.cluster,
      } as unknown as Cluster;

      const consumerGroup = new ConsumerGroup({
        logger: silentLogger,
        topics: ['topic1'],
        topicConfigurations: {},
        cluster,
        groupId: overrides.groupId === undefined ? null : overrides.groupId,
        assignedPartitions: [{ topic: 'topic1', partitions: [0] }],
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
        autoCommit: false,
        autoCommitInterval: null,
        autoCommitThreshold: null,
      });

      return { consumerGroup, findGroupCoordinator };
    }

    it('installs the fixed assignment without any JoinGroup/SyncGroup RPC', async () => {
      const { consumerGroup, findGroupCoordinator } = createAssignGroup();

      await consumerGroup.joinAndSync();

      expect(consumerGroup.assigned()).toEqual([{ topic: 'topic1', partitions: [0] }]);
      expect(findGroupCoordinator).not.toHaveBeenCalled();
      expect(consumerGroup.memberId).toBeNull();
      expect(consumerGroup.generationId).toBeNull();
    });

    it('does not emit GROUP_JOIN', async () => {
      const { consumerGroup } = createAssignGroup();
      const groupJoins: unknown[] = [];
      consumerGroup.instrumentationEmitter.addListener(GROUP_JOIN, (event) => groupJoins.push(event));

      await consumerGroup.joinAndSync();

      expect(groupJoins).toHaveLength(0);
    });

    it('heartbeat() and heartbeatDue() are no-ops', async () => {
      const { consumerGroup } = createAssignGroup();
      await consumerGroup.joinAndSync();

      expect(consumerGroup.heartbeatDue(0)).toBe(false);
      await expect(consumerGroup.heartbeat({ interval: 0 })).resolves.toBeUndefined();
    });

    it('leave() does not perform a group leave RPC', async () => {
      const { consumerGroup, findGroupCoordinator } = createAssignGroup();
      await consumerGroup.joinAndSync();

      await consumerGroup.leave();

      expect(findGroupCoordinator).not.toHaveBeenCalled();
    });

    it('fetches the assigned partitions from the broker without a group coordinator lookup', async () => {
      const brokerFetch = vi.fn(async () => ({
        responses: [
          {
            topicName: 'topic1',
            partitions: [{ partition: 0, preferredReadReplica: -1, highWatermark: 10n, messages: [] }],
          },
        ],
        sessionId: 0,
      }));
      const { consumerGroup, findGroupCoordinator } = createAssignGroup({
        cluster: { findBroker: vi.fn(async () => ({ fetch: brokerFetch })) },
      });

      await consumerGroup.joinAndSync();
      const batches = await consumerGroup.fetch('1');

      expect(findGroupCoordinator).not.toHaveBeenCalled();
      expect(brokerFetch).toHaveBeenCalledWith(
        expect.objectContaining({
          topics: [expect.objectContaining({ topic: 'topic1' })],
        }),
      );
      expect(batches).toHaveLength(1);
      expect(batches[0]?.topic).toBe('topic1');
    });

    it('without a configured groupId, resolves the initial position from autoOffsetReset (default latest)', async () => {
      const fetchTopicsOffset = vi.fn(async () => [{ topic: 'topic1', partitions: [{ partition: 0, offset: 123n }] }]);
      const { consumerGroup } = createAssignGroup({ cluster: { fetchTopicsOffset } });
      await consumerGroup.joinAndSync();

      await consumerGroup.fetch('1');

      expect(fetchTopicsOffset).toHaveBeenCalledWith([
        { topic: 'topic1', partitions: [{ partition: 0 }], fromBeginning: false },
      ]);
    });

    it('pause/resume/seek work exactly like subscribe-mode', async () => {
      const { consumerGroup } = createAssignGroup();
      await consumerGroup.joinAndSync();

      consumerGroup.pause([{ topic: 'topic1', partitions: [0] }]);
      expect(consumerGroup.isPaused('topic1', 0)).toBe(true);
      expect(consumerGroup.paused()).toEqual([{ topic: 'topic1', partitions: [0] }]);

      consumerGroup.resume([{ topic: 'topic1', partitions: [0] }]);
      expect(consumerGroup.isPaused('topic1', 0)).toBe(false);

      consumerGroup.seek({ topic: 'topic1', partition: 0, offset: 500n });
      expect(consumerGroup.hasSeekOffset({ topic: 'topic1', partition: 0 })).toBe(true);
    });

    it('seek overrides the fetch offset on the next fetch', async () => {
      const brokerFetch = vi.fn(async () => ({ responses: [], sessionId: 0 }));
      const { consumerGroup } = createAssignGroup({
        cluster: { findBroker: vi.fn(async () => ({ fetch: brokerFetch })) },
      });
      await consumerGroup.joinAndSync();

      consumerGroup.seek({ topic: 'topic1', partition: 0, offset: 777n });
      await consumerGroup.fetch('1');

      expect(brokerFetch).toHaveBeenCalledWith(
        expect.objectContaining({
          topics: [expect.objectContaining({ partitions: [expect.objectContaining({ fetchOffset: 777n })] })],
        }),
      );
    });

    it('commitOffsets throws a clear error without a configured groupId', async () => {
      const { consumerGroup } = createAssignGroup({ groupId: null });
      await consumerGroup.joinAndSync();
      consumerGroup.resolveOffset({ topic: 'topic1', partition: 0, offset: 5n });

      await expect(consumerGroup.commitOffsets()).rejects.toThrow(/without a configured groupId/);
    });

    it('commitOffsets works as a standalone consumer when a groupId is configured', async () => {
      const offsetCommit = vi.fn(async () => undefined);
      const findGroupCoordinator = vi.fn(async () => ({ offsetCommit }));
      const { consumerGroup } = createAssignGroup({
        groupId: 'my-group',
        cluster: { findGroupCoordinator },
      });
      await consumerGroup.joinAndSync();
      consumerGroup.resolveOffset({ topic: 'topic1', partition: 0, offset: 5n });

      await consumerGroup.commitOffsets();

      expect(offsetCommit).toHaveBeenCalledWith(
        expect.objectContaining({ groupId: 'my-group', memberId: '', groupGenerationId: -1 }),
      );
    });
  });
});
