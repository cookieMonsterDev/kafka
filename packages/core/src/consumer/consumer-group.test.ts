import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { InstrumentationEventEmitter } from '../instrumentation/emitter';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { ISOLATION_LEVEL } from '../protocol/enums/isolation-level';
import { MemberAssignment } from './assigner-protocol';
import { ConsumerGroup } from './consumer-group';
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
});
