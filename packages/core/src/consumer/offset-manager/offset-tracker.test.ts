import { describe, expect, it, vi } from 'vitest';
import type { Broker } from '../../broker/index';
import type { Cluster } from '../../cluster/index';
import { KafkaNonRetriableError, KafkaProtocolError } from '../../errors';
import { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { createErrorFromCode } from '../../protocol/error-codes';
import type { TopicOffsetConfiguration } from '../offset-reset';
import type { ConsumerHooks, MemberAssignment } from '../types';
import { AssignModeCommitTarget, GroupMemberCommitTarget, type CommitTarget } from './commit-target';
import { OffsetTracker } from './offset-tracker';
import { withSend } from '../../broker/test-send';

const NOT_COORDINATOR_FOR_GROUP_CODE = 16;
const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

type TargetKind = 'group member' | 'assign mode';

interface TrackerOptions {
  target?: TargetKind;
  assignment?: MemberAssignment;
  /** Assign mode only; a group member always has one. */
  groupId?: string | null;
  cluster?: Record<string, unknown>;
  coordinator?: Record<string, unknown>;
  autoCommit?: boolean;
  autoCommitInterval?: number | null;
  autoCommitThreshold?: number | null;
  topicConfigurations?: Record<string, TopicOffsetConfiguration>;
  hooks?: ConsumerHooks;
}

interface Harness {
  tracker: OffsetTracker;
  target: CommitTarget;
  cluster: Cluster;
  membership: { coordinator: Broker | null; memberId: string | null; generationId: number | null };
  offsetCommit: ReturnType<typeof vi.fn>;
  offsetFetch: ReturnType<typeof vi.fn>;
}

/** The cluster's committed-offset calls, backed by one in-memory record. */
function committedOffsetCalls(): Pick<Cluster, 'committedOffset' | 'markOffsetAsCommitted' | 'forgetCommittedOffset'> {
  const offsets = new Map<string, bigint>();
  const key = ({ groupId, topic, partition }: { groupId: string; topic: string; partition: number }): string =>
    `${groupId}/${topic}/${partition}`;
  return {
    committedOffset: (partition) => offsets.get(key(partition)),
    markOffsetAsCommitted: ({ offset, ...partition }) => {
      offsets.set(key(partition), offset);
    },
    forgetCommittedOffset: (partition) => {
      offsets.delete(key(partition));
    },
  };
}

function createTracker(options: TrackerOptions = {}): Harness {
  const offsetCommit = vi.fn(async () => undefined);
  const offsetFetch = vi.fn(async () => ({ responses: [] }));
  const coordinator = withSend({
    nodeId: 1,
    isConnected: vi.fn(() => true),
    offsetCommit,
    offsetFetch,
    ...options.coordinator,
  }) as unknown as Broker;
  const cluster = withSend({
    ...committedOffsetCalls(),
    findGroupCoordinator: vi.fn(async () => coordinator),
    findBroker: vi.fn(async () => coordinator),
    recover: vi.fn(async () => undefined),
    defaultOffset: vi.fn(() => -1n),
    fetchTopicsOffset: vi.fn(async () => []),
    ...options.cluster,
  }) as unknown as Cluster;
  const membership: Harness['membership'] = { coordinator, memberId: 'memberId', generationId: 1 };
  const target =
    options.target === 'assign mode'
      ? new AssignModeCommitTarget(cluster, options.groupId === undefined ? 'groupId' : options.groupId)
      : new GroupMemberCommitTarget(cluster, 'groupId', membership);

  const tracker = new OffsetTracker({
    cluster,
    target,
    topicConfigurations: options.topicConfigurations ?? {},
    instrumentationEmitter: new InstrumentationEventEmitter(),
    autoCommit: options.autoCommit ?? true,
    autoCommitInterval: options.autoCommitInterval ?? null,
    autoCommitThreshold: options.autoCommitThreshold ?? null,
    logger: silentLogger,
    hooks: options.hooks,
  });
  tracker.updateAssignment(options.assignment ?? { topic1: [0, 1, 2, 3], topic2: [0, 1, 2, 3, 4, 5] });

  return withSend({
    tracker,
    target,
    cluster,
    membership,
    offsetCommit: offsetCommit,
    offsetFetch: offsetFetch,
  });
}

const IDENTITY: Record<TargetKind, { memberId: string; groupGenerationId: number }> = {
  'group member': { memberId: 'memberId', groupGenerationId: 1 },
  'assign mode': { memberId: '', groupGenerationId: -1 },
};

describe('consumer/offset-manager/offset-tracker', () => {
  describe.each<TargetKind>(['group member', 'assign mode'])('with a %s commit target', (kind) => {
    const create = (options: TrackerOptions = {}): Harness => createTracker({ target: kind, ...options });

    describe('countResolvedOffsets', () => {
      it('counts every resolved offset of partitions without a valid committed offset', () => {
        const { tracker, target } = create();
        target.setCommittedOffset('topic1', 0, -1n);

        for (let i = 0; i < 10; i++) tracker.resolveOffset({ topic: 'topic1', partition: 0, offset: BigInt(i) });
        tracker.resolveOffset({ topic: 'topic1', partition: 1, offset: 0n });
        for (let i = 0; i < 3; i++) tracker.resolveOffset({ topic: 'topic1', partition: 2, offset: BigInt(i) });
        for (let i = 0; i < 6; i++) tracker.resolveOffset({ topic: 'topic2', partition: 5, offset: BigInt(i) });

        expect(tracker.countResolvedOffsets()).toEqual(20n);
      });

      it('counts only the offsets resolved past the committed ones', () => {
        const { tracker, target } = create();
        target.setCommittedOffset('topic1', 0, 10n);
        target.setCommittedOffset('topic1', 1, 1n);
        target.setCommittedOffset('topic1', 2, 3n);
        target.setCommittedOffset('topic2', 5, 5n);

        for (let i = 0; i < 10; i++) tracker.resolveOffset({ topic: 'topic1', partition: 0, offset: BigInt(i) });
        tracker.resolveOffset({ topic: 'topic1', partition: 1, offset: 0n });
        for (let i = 0; i < 3; i++) tracker.resolveOffset({ topic: 'topic1', partition: 2, offset: BigInt(i) });
        for (let i = 0; i < 6; i++) tracker.resolveOffset({ topic: 'topic2', partition: 5, offset: BigInt(i) });

        expect(tracker.countResolvedOffsets()).toEqual(1n);
      });
    });

    describe('resolveOffset and nextOffset', () => {
      it('makes the offset after the last resolved one the next offset', () => {
        const { tracker } = create();
        tracker.resolveOffset({ topic: 'topic1', partition: 0, offset: 4n });
        tracker.resolveOffset({ topic: 'topic1', partition: 0, offset: 5n });
        tracker.resolveOffset({ topic: 'topic2', partition: 3, offset: 0n });

        expect(tracker.nextOffset('topic1', 0)).toBe(6n);
        expect(tracker.nextOffset('topic2', 3)).toBe(1n);
      });

      it('resets a resolved offset back to the committed offset', () => {
        const { tracker, target } = create();
        target.setCommittedOffset('topic1', 0, 2n);

        tracker.resolveOffset({ topic: 'topic1', partition: 0, offset: 9n });
        tracker.resetOffset({ topic: 'topic1', partition: 0 });

        expect(tracker.nextOffset('topic1', 0)).toBe(2n);
      });

      it('does not cache a fallback 0n before resolveOffsets runs', async () => {
        const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 42n }] }]);
        const offsetFetch = vi.fn(async () => ({
          responses: [{ topic: 'events', partitions: [{ partition: 0, offset: -1n }] }],
        }));
        const { tracker } = create({
          assignment: { events: [0] },
          cluster: { fetchTopicsOffset },
          coordinator: withSend({ offsetFetch }),
        });

        expect(tracker.nextOffset('events', 0)).toBe(0n);
        expect(tracker.hasCommittedOffset('events', 0)).toBe(false);

        await tracker.resolveOffsets();

        expect(tracker.nextOffset('events', 0)).toBe(42n);
        expect(tracker.hasCommittedOffset('events', 0)).toBe(true);
      });
    });

    describe('uncommittedOffsets', () => {
      it('returns the resolved offsets that differ from the committed ones', () => {
        const { tracker, target } = create({ assignment: { 'topic-1': [0, 1], 'topic-2': [0, 1, 2, 3] } });

        for (const topic of ['topic-1', 'topic-2']) {
          for (const partition of [0, 1, 2, 3]) {
            if (topic === 'topic-1' && partition > 1) continue;
            tracker.resolveOffset({ topic, partition, offset: 2n });
          }
        }
        target.setCommittedOffset('topic-2', 0, 3n);
        target.setCommittedOffset('topic-2', 1, 2n);

        expect(tracker.uncommittedOffsets()).toEqual({
          topics: [
            {
              topic: 'topic-1',
              partitions: [
                { partition: 0, offset: 3n },
                { partition: 1, offset: 3n },
              ],
            },
            {
              topic: 'topic-2',
              partitions: [
                { partition: 1, offset: 3n },
                { partition: 2, offset: 3n },
                { partition: 3, offset: 3n },
              ],
            },
          ],
        });
      });
    });

    describe('updateAssignment', () => {
      it('keeps the offsets of retained partitions and forgets revoked ones', async () => {
        const offsetFetch = vi.fn(async () => ({
          responses: [{ topic: 'events', partitions: [{ partition: 2, offset: 7n }] }],
        }));
        const { tracker, target } = create({ assignment: { events: [0, 1] }, coordinator: withSend({ offsetFetch }) });
        target.setCommittedOffset('events', 0, 10n);
        target.setCommittedOffset('events', 1, 20n);
        tracker.resolveOffset({ topic: 'events', partition: 0, offset: 14n });
        tracker.resolveOffset({ topic: 'events', partition: 1, offset: 24n });

        tracker.updateAssignment({ events: [0, 2] });

        expect(tracker.nextOffset('events', 0)).toBe(15n);
        expect(tracker.hasCommittedOffset('events', 1)).toBe(false);
        expect(tracker.uncommittedOffsets()).toEqual({
          topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 15n }] }],
        });

        await tracker.resolveOffsets();
        expect(offsetFetch).toHaveBeenCalledWith({
          groupId: 'groupId',
          topics: [{ topic: 'events', partitions: [{ partition: 2 }] }],
        });
      });

      it('looks the committed offset of a gained partition up again', async () => {
        const offsetFetch = vi.fn(async () => ({
          responses: [{ topic: 'events', partitions: [{ partition: 1, offset: 30n }] }],
        }));
        const { tracker, target } = create({ assignment: { events: [0] }, coordinator: withSend({ offsetFetch }) });
        // Left over from an earlier owner or an earlier run on the same client.
        target.setCommittedOffset('events', 1, 12n);

        tracker.updateAssignment({ events: [0, 1] });
        expect(tracker.hasCommittedOffset('events', 1)).toBe(false);

        await tracker.resolveOffsets();
        expect(tracker.nextOffset('events', 1)).toBe(30n);
      });

      it('drops resolved offsets of a partition that left the assignment', () => {
        const { tracker } = create({ assignment: { events: [0] } });
        tracker.updateAssignment({ events: [0] });
        tracker.resolveOffset({ topic: 'events', partition: 1, offset: 5n });

        tracker.updateAssignment({ events: [0] });

        expect(tracker.uncommittedOffsets()).toEqual({ topics: [] });
      });

      it('forgets every offset when the assignment is lost', () => {
        const { tracker, target } = create({ assignment: { events: [0] } });
        target.setCommittedOffset('events', 0, 3n);
        tracker.resolveOffset({ topic: 'events', partition: 0, offset: 8n });

        tracker.updateAssignment({});

        expect(target.committedOffset('events', 0)).toBeUndefined();
        expect(tracker.uncommittedOffsets()).toEqual({ topics: [] });
      });
    });

    describe('seek', () => {
      it('ignores a seek outside the assignment', async () => {
        const { tracker, offsetCommit } = create();

        await tracker.seek({ topic: 'topic3', partition: 0, offset: 100n });
        await tracker.seek({ topic: 'topic1', partition: 4, offset: 101n });

        expect(offsetCommit).not.toHaveBeenCalled();
        expect(tracker.nextOffset('topic1', 4)).toBe(0n);
      });
    });

    describe('commitOffsets', () => {
      it('commits every uncommitted resolved offset and records it as committed', async () => {
        const { tracker, target, offsetCommit } = create({ assignment: { 'topic-1': [0, 1] } });
        tracker.resolveOffset({ topic: 'topic-1', partition: 0, offset: 2n });
        tracker.resolveOffset({ topic: 'topic-1', partition: 1, offset: 2n });
        target.setCommittedOffset('topic-1', 1, 3n);

        await tracker.commitOffsets();

        expect(offsetCommit).toHaveBeenCalledWith({
          groupId: 'groupId',
          ...IDENTITY[kind],
          topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 3n }] }],
        });
        expect(target.committedOffset('topic-1', 0)).toBe(3n);
        expect(tracker.uncommittedOffsets()).toEqual({ topics: [] });
      });

      it('commits only the given offsets', async () => {
        const { tracker, offsetCommit } = create({ assignment: { 'topic-1': [0, 1] } });
        tracker.resolveOffset({ topic: 'topic-1', partition: 1, offset: 8n });
        const offsets = { topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 42n }] }] };

        await tracker.commitOffsets(offsets);

        expect(offsetCommit).toHaveBeenCalledTimes(1);
        expect(offsetCommit).toHaveBeenCalledWith({ groupId: 'groupId', ...IDENTITY[kind], topics: offsets.topics });
      });

      it('sends nothing when there is nothing uncommitted', async () => {
        const { tracker, offsetCommit } = create();

        await tracker.commitOffsets();

        expect(offsetCommit).not.toHaveBeenCalled();
      });

      it('fires onCommit hooks in order after a successful commit', async () => {
        const order: string[] = [];
        const first = vi.fn((_event: { error?: unknown }) => {
          order.push('first');
        });
        const second = vi.fn(() => {
          order.push('second');
        });
        const { tracker } = create({ assignment: { 'topic-1': [0] }, hooks: { onCommit: [first, second] } });

        const offsets = { topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 42n }] }] };
        await tracker.commitOffsets(offsets);

        expect(order).toEqual(['first', 'second']);
        expect(first).toHaveBeenCalledWith(
          expect.objectContaining({ groupId: 'groupId', ...IDENTITY[kind], topics: offsets.topics }),
        );
        expect(first.mock.calls[0]?.[0].error).toBeUndefined();
      });

      it('fires onCommit hooks with the error when the commit fails, without altering the rejection', async () => {
        const commitError = new Error('commit failed');
        const onCommit = vi.fn();
        const { tracker } = create({
          assignment: { 'topic-1': [0] },
          coordinator: withSend({
            offsetCommit: vi.fn(() => {
              throw commitError;
            }),
          }),
          hooks: { onCommit: [onCommit] },
        });

        const offsets = { topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 42n }] }] };
        await expect(tracker.commitOffsets(offsets)).rejects.toBe(commitError);

        expect(onCommit).toHaveBeenCalledWith(expect.objectContaining({ topics: offsets.topics, error: commitError }));
      });

      it('does not fail the commit when an onCommit hook throws', async () => {
        const throwingHook = vi.fn(() => {
          throw new Error('hook boom');
        });
        const nextHook = vi.fn();
        const { tracker, offsetCommit } = create({
          assignment: { 'topic-1': [0] },
          hooks: { onCommit: [throwingHook, nextHook] },
        });

        const offsets = { topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 42n }] }] };
        await expect(tracker.commitOffsets(offsets)).resolves.toBeUndefined();

        expect(offsetCommit).toHaveBeenCalledTimes(1);
        expect(nextHook).toHaveBeenCalledTimes(1);
      });

      it('recovers from a NOT_COORDINATOR_FOR_GROUP error through the cluster', async () => {
        const recover = vi.fn(async () => undefined);
        const { tracker } = create({
          assignment: { 'topic-1': [0] },
          cluster: { recover },
          coordinator: withSend({
            offsetCommit: vi.fn(() => {
              throw createErrorFromCode(NOT_COORDINATOR_FOR_GROUP_CODE);
            }),
          }),
        });

        const offsets = { topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 1n }] }] };
        await expect(tracker.commitOffsets(offsets)).rejects.toThrow(KafkaProtocolError);
        expect(recover).toHaveBeenCalledWith(expect.objectContaining({ type: 'NOT_COORDINATOR_FOR_GROUP' }));
      });
    });

    describe('commitOffsetsIfNecessary', () => {
      it('commits on every call when neither interval nor threshold is set', async () => {
        const { tracker, offsetCommit } = create({ assignment: { events: [0] } });
        tracker.resolveOffset({ topic: 'events', partition: 0, offset: 1n });
        await tracker.commitOffsetsIfNecessary();
        tracker.resolveOffset({ topic: 'events', partition: 0, offset: 2n });
        await tracker.commitOffsetsIfNecessary();

        expect(offsetCommit).toHaveBeenCalledTimes(2);
      });

      it('commits once the interval has elapsed', async () => {
        const now = vi.spyOn(Date, 'now').mockReturnValue(1_000);
        try {
          const { tracker, offsetCommit } = create({ assignment: { events: [0] }, autoCommitInterval: 30 });
          tracker.resolveOffset({ topic: 'events', partition: 0, offset: 1n });

          await tracker.commitOffsetsIfNecessary();
          expect(offsetCommit).not.toHaveBeenCalled();

          now.mockReturnValue(1_030);
          await tracker.commitOffsetsIfNecessary();
          await tracker.commitOffsetsIfNecessary();
          expect(offsetCommit).toHaveBeenCalledTimes(1);
        } finally {
          now.mockRestore();
        }
      });

      it('commits once the threshold of resolved offsets is reached', async () => {
        const { tracker, target, offsetCommit } = create({ assignment: { events: [0] }, autoCommitThreshold: 3 });
        target.setCommittedOffset('events', 0, 0n);

        tracker.resolveOffset({ topic: 'events', partition: 0, offset: 1n });
        await tracker.commitOffsetsIfNecessary();
        expect(offsetCommit).not.toHaveBeenCalled();

        tracker.resolveOffset({ topic: 'events', partition: 0, offset: 2n });
        await tracker.commitOffsetsIfNecessary();
        await tracker.commitOffsetsIfNecessary();
        expect(offsetCommit).toHaveBeenCalledTimes(1);
      });
    });

    describe('resolveOffsets', () => {
      it('throws without ListOffsets when reset is none and there is no committed offset', async () => {
        const fetchTopicsOffset = vi.fn();
        const offsetFetch = vi.fn(async () => ({
          responses: [{ topic: 'events', partitions: [{ partition: 0, offset: -1n }] }],
        }));
        const { tracker } = create({
          assignment: { events: [0] },
          topicConfigurations: { events: { autoOffsetReset: 'none' } },
          cluster: { fetchTopicsOffset },
          coordinator: withSend({ offsetFetch }),
        });

        await expect(tracker.resolveOffsets()).rejects.toThrow(
          new KafkaNonRetriableError('Offset reset policy is none; no committed offset for topic events partition 0'),
        );
        expect(fetchTopicsOffset).not.toHaveBeenCalled();
      });

      it('keeps the group committed offset without ListOffsets', async () => {
        const fetchTopicsOffset = vi.fn();
        const offsetFetch = vi.fn(async () => ({
          responses: [{ topic: 'events', partitions: [{ partition: 0, offset: 14n }] }],
        }));
        const { tracker } = create({
          assignment: { events: [0] },
          topicConfigurations: { events: { autoOffsetReset: 'none' } },
          cluster: { fetchTopicsOffset },
          coordinator: withSend({ offsetFetch }),
        });

        await tracker.resolveOffsets();

        expect(offsetFetch).toHaveBeenCalledWith({
          groupId: 'groupId',
          topics: [{ topic: 'events', partitions: [{ partition: 0 }] }],
        });
        expect(fetchTopicsOffset).not.toHaveBeenCalled();
        expect(tracker.nextOffset('events', 0)).toBe(14n);
      });

      it('falls back to the earliest offset when the group has none and autoOffsetReset is earliest', async () => {
        const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 5n }] }]);
        const offsetFetch = vi.fn(async () => ({
          responses: [{ topic: 'events', partitions: [{ partition: 0, offset: -1n }] }],
        }));
        const { tracker } = create({
          assignment: { events: [0] },
          topicConfigurations: { events: { autoOffsetReset: 'earliest' } },
          cluster: { fetchTopicsOffset },
          coordinator: withSend({ offsetFetch }),
        });

        await tracker.resolveOffsets();

        expect(fetchTopicsOffset).toHaveBeenCalledWith([
          { topic: 'events', partitions: [{ partition: 0 }], fromBeginning: true },
        ]);
        expect(tracker.nextOffset('events', 0)).toBe(5n);
      });

      it('looks up offsets by timestamp when autoOffsetReset is by_duration', async () => {
        const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 3n }] }]);
        const offsetFetch = vi.fn(async () => ({
          responses: [{ topic: 'events', partitions: [{ partition: 0, offset: -1n }] }],
        }));
        const { tracker } = create({
          assignment: { events: [0] },
          topicConfigurations: { events: { autoOffsetReset: 'by_duration:PT1H' } },
          cluster: { fetchTopicsOffset },
          coordinator: withSend({ offsetFetch }),
        });

        await tracker.resolveOffsets();

        const [queries] = fetchTopicsOffset.mock.calls[0] as unknown as [
          { topic: string; partitions: { partition: number }[]; fromTimestamp: bigint }[],
        ];
        expect(queries).toHaveLength(1);
        expect(queries[0]).toMatchObject({ topic: 'events', partitions: [{ partition: 0 }] });
        expect(queries[0]?.fromTimestamp).toBeLessThan(BigInt(Date.now()));
        expect(tracker.nextOffset('events', 0)).toBe(3n);
      });

      it('asks for a partition only until its committed offset is known', async () => {
        const offsetFetch = vi.fn(async () => ({
          responses: [{ topic: 'events', partitions: [{ partition: 0, offset: 7n }] }],
        }));
        const { tracker } = create({ assignment: { events: [0] }, coordinator: withSend({ offsetFetch }) });

        await tracker.resolveOffsets();
        await tracker.resolveOffsets();

        expect(offsetFetch).toHaveBeenCalledTimes(1);
      });
    });

    describe('setDefaultOffset', () => {
      it('throws when reset is none', async () => {
        const { tracker, offsetCommit } = create({
          assignment: { events: [0] },
          topicConfigurations: { events: { autoOffsetReset: 'none' } },
        });

        await expect(tracker.setDefaultOffset({ topic: 'events', partition: 0 })).rejects.toThrow(
          new KafkaNonRetriableError('Offset reset policy is none; no committed offset for topic events partition 0'),
        );
        expect(offsetCommit).not.toHaveBeenCalled();
      });

      it('commits the default offset and asks the group for it again on the next resolve', async () => {
        const defaultOffset = vi.fn(() => 0n);
        const { tracker, target, offsetCommit } = create({ assignment: { events: [0] }, cluster: { defaultOffset } });
        target.setCommittedOffset('events', 0, 50n);

        await tracker.setDefaultOffset({ topic: 'events', partition: 0 });

        expect(offsetCommit).toHaveBeenCalledWith({
          groupId: 'groupId',
          ...IDENTITY[kind],
          topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 0n }] }],
        });
        expect(tracker.hasCommittedOffset('events', 0)).toBe(false);
      });

      it('commits the ListOffsets timestamp result when autoOffsetReset is by_duration', async () => {
        const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 9n }] }]);
        const { tracker, offsetCommit } = create({
          assignment: { events: [0] },
          topicConfigurations: { events: { autoOffsetReset: 'by_duration:PT1H' } },
          cluster: { fetchTopicsOffset },
        });

        await tracker.setDefaultOffset({ topic: 'events', partition: 0 });

        expect(fetchTopicsOffset).toHaveBeenCalledWith([
          expect.objectContaining({ topic: 'events', partitions: [{ partition: 0 }] }),
        ]);
        expect(offsetCommit).toHaveBeenCalledWith(
          expect.objectContaining({ topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 9n }] }] }),
        );
      });
    });
  });

  describe('with a group member commit target', () => {
    it('reads the member identity when each commit is sent', async () => {
      const { tracker, membership, offsetCommit } = createTracker({ assignment: { events: [0] } });
      membership.memberId = 'rejoined-member';
      membership.generationId = 7;

      await tracker.commitOffsets({ topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 1n }] }] });

      expect(offsetCommit).toHaveBeenCalledWith(
        expect.objectContaining({ memberId: 'rejoined-member', groupGenerationId: 7 }),
      );
    });

    it('rejects a commit with a retriable error while the member has not joined', async () => {
      const { tracker, membership, offsetCommit } = createTracker({ assignment: { events: [0] } });
      membership.generationId = null;

      const commit = tracker.commitOffsets({
        topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 1n }] }],
      });

      await expect(commit).rejects.toMatchObject({ message: 'Consumer group has not joined', retriable: true });
      expect(offsetCommit).not.toHaveBeenCalled();
    });

    it('finds the coordinator broker again once the joined one is disconnected', async () => {
      const offsetCommit = vi.fn(async () => undefined);
      const reconnected = withSend({ offsetCommit });
      const findBroker = vi.fn(async () => reconnected);
      const { tracker } = createTracker({
        assignment: { events: [0] },
        cluster: { findBroker },
        coordinator: { isConnected: () => false },
      });

      await tracker.commitOffsets({ topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 1n }] }] });

      expect(findBroker).toHaveBeenCalledWith({ nodeId: '1' });
      expect(offsetCommit).toHaveBeenCalledTimes(1);
    });

    it('sees an offset a transactional producer of the same client committed for the group', () => {
      const { tracker, cluster } = createTracker({ assignment: { events: [0] } });
      tracker.resolveOffset({ topic: 'events', partition: 0, offset: 4n });

      cluster.markOffsetAsCommitted({ groupId: 'groupId', topic: 'events', partition: 0, offset: 5n });

      expect(tracker.uncommittedOffsets()).toEqual({ topics: [] });
    });

    it('commits a seek at once with auto-commit, then asks the group for the offset again', async () => {
      const { tracker, offsetCommit } = createTracker({ assignment: { events: [0] }, autoCommit: true });
      tracker.resolveOffset({ topic: 'events', partition: 0, offset: 30n });

      await tracker.seek({ topic: 'events', partition: 0, offset: 5n });

      expect(offsetCommit).toHaveBeenCalledWith({
        groupId: 'groupId',
        memberId: 'memberId',
        groupGenerationId: 1,
        topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 5n }] }],
      });
      expect(tracker.hasCommittedOffset('events', 0)).toBe(false);
      expect(tracker.uncommittedOffsets()).toEqual({ topics: [] });
    });

    it('commits nothing for an empty list', async () => {
      const { tracker, offsetCommit } = createTracker({ assignment: { events: [0] } });
      tracker.resolveOffset({ topic: 'events', partition: 0, offset: 8n });

      await tracker.commitOffsets({ topics: [] });

      expect(offsetCommit).not.toHaveBeenCalled();
    });

    it('leaves a seek uncommitted without auto-commit', async () => {
      const { tracker, offsetCommit } = createTracker({ assignment: { events: [0] }, autoCommit: false });

      await tracker.seek({ topic: 'events', partition: 0, offset: 5n });

      expect(offsetCommit).not.toHaveBeenCalled();
      expect(tracker.nextOffset('events', 0)).toBe(5n);
      expect(tracker.uncommittedOffsets()).toEqual({
        topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 5n }] }],
      });
    });
  });

  describe('with an assign mode commit target', () => {
    it('moves the local position on seek without any broker round trip', async () => {
      const findGroupCoordinator = vi.fn();
      const { tracker, offsetCommit } = createTracker({
        target: 'assign mode',
        assignment: { events: [0] },
        cluster: withSend({ findGroupCoordinator }),
      });
      tracker.resolveOffset({ topic: 'events', partition: 0, offset: 30n });

      await tracker.seek({ topic: 'events', partition: 0, offset: 55n });

      expect(offsetCommit).not.toHaveBeenCalled();
      expect(findGroupCoordinator).not.toHaveBeenCalled();
      expect(tracker.nextOffset('events', 0)).toBe(55n);
      expect(tracker.uncommittedOffsets()).toEqual({ topics: [] });
    });

    it('commits every uncommitted offset for an empty list, since it commits only when asked to', async () => {
      const { tracker, offsetCommit } = createTracker({ target: 'assign mode', assignment: { events: [0] } });
      tracker.resolveOffset({ topic: 'events', partition: 0, offset: 8n });

      await tracker.commitOffsets({ topics: [] });

      expect(offsetCommit).toHaveBeenCalledWith(
        expect.objectContaining({ topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 9n }] }] }),
      );
    });

    describe('without a group id', () => {
      const createWithoutGroup = (options: TrackerOptions = {}): Harness =>
        createTracker({ target: 'assign mode', groupId: null, assignment: { events: [0] }, ...options });

      it('resolves from autoOffsetReset (default latest) without OffsetFetch', async () => {
        const findGroupCoordinator = vi.fn();
        const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 99n }] }]);
        const { tracker, offsetFetch } = createWithoutGroup({
          cluster: withSend({ fetchTopicsOffset, findGroupCoordinator }),
        });

        await tracker.resolveOffsets();

        expect(findGroupCoordinator).not.toHaveBeenCalled();
        expect(offsetFetch).not.toHaveBeenCalled();
        expect(fetchTopicsOffset).toHaveBeenCalledWith([
          { topic: 'events', partitions: [{ partition: 0 }], fromBeginning: false },
        ]);
        expect(tracker.nextOffset('events', 0)).toBe(99n);
      });

      it('throws instead of resolving when autoOffsetReset is none', async () => {
        const fetchTopicsOffset = vi.fn();
        const { tracker } = createWithoutGroup({
          topicConfigurations: { events: { autoOffsetReset: 'none' } },
          cluster: { fetchTopicsOffset },
        });

        await expect(tracker.resolveOffsets()).rejects.toThrow(
          'Offset reset policy is none; no committed offset for topic events partition 0',
        );
        expect(fetchTopicsOffset).not.toHaveBeenCalled();
      });

      it('rejects a commit with a clear error, even with nothing to commit', async () => {
        const { tracker } = createWithoutGroup();

        await expect(tracker.commitOffsets()).rejects.toThrow(KafkaNonRetriableError);
        await expect(tracker.commitOffsets()).rejects.toThrow(/without a configured groupId/);
      });

      it('resets the local position without committing on setDefaultOffset', async () => {
        const { tracker, target, offsetCommit } = createWithoutGroup();
        target.setCommittedOffset('events', 0, 40n);

        await tracker.setDefaultOffset({ topic: 'events', partition: 0 });

        expect(offsetCommit).not.toHaveBeenCalled();
        expect(tracker.hasCommittedOffset('events', 0)).toBe(false);
      });
    });
  });
});
