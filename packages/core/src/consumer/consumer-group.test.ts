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
});
