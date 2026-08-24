import { describe, expect, it, vi } from 'vitest';
import type { Broker } from '../../broker/index';
import type { Cluster } from '../../cluster/index';
import { KafkaNonRetriableError, KafkaProtocolError } from '../../errors';
import { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { createErrorFromCode } from '../../protocol/error-codes';
import { sleep } from '../../utils/wait';
import type { ConsumerHooks } from '../types';
import { OffsetManager } from './index';

const NOT_COORDINATOR_FOR_GROUP_CODE = 16;
const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function createOffsetManager(
  overrides: {
    memberAssignment?: Record<string, number[]>;
    cluster?: Partial<Cluster>;
    coordinator?: {
      isConnected?: () => boolean;
      offsetCommit?: (...args: never[]) => unknown;
      offsetFetch?: (...args: never[]) => unknown;
    };
    autoCommit?: boolean;
    autoCommitInterval?: number | null;
    autoCommitThreshold?: number | null;
    groupId?: string;
    topicConfigurations?: OffsetManager['topicConfigurations'];
    hooks?: ConsumerHooks;
  } = {},
): OffsetManager {
  const memberAssignment = overrides.memberAssignment ?? { topic1: [0, 1, 2, 3], topic2: [0, 1, 2, 3, 4, 5] };
  const coordinator = {
    isConnected: vi.fn(() => true),
    offsetCommit: vi.fn(),
    offsetFetch: vi.fn(),
    ...overrides.coordinator,
  };

  return new OffsetManager({
    cluster: {
      committedOffsets: vi.fn(() => ({})),
      refreshMetadata: vi.fn(),
      defaultOffset: vi.fn(() => -1n),
      fetchTopicsOffset: vi.fn(),
      findBroker: vi.fn(),
      ...overrides.cluster,
    } as unknown as Cluster,
    coordinator: coordinator as unknown as Broker,
    memberAssignment,
    autoCommit: overrides.autoCommit ?? true,
    autoCommitInterval: overrides.autoCommitInterval ?? null,
    autoCommitThreshold: overrides.autoCommitThreshold ?? null,
    topicConfigurations: overrides.topicConfigurations ?? {},
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: overrides.groupId ?? 'groupId',
    generationId: 1,
    memberId: 'memberId',
    logger: silentLogger,
    hooks: overrides.hooks,
  });
}

describe('consumer/offset-manager', () => {
  describe('countResolvedOffsets', () => {
    it('counts the number of resolved offsets for all topics', () => {
      const offsetManager = createOffsetManager();
      offsetManager.committedOffsets()['topic1']![0] = -1n;
      offsetManager.committedOffsets()['topic1']![1] = -1n;
      offsetManager.committedOffsets()['topic1']![2] = -1n;
      offsetManager.committedOffsets()['topic2']![5] = -1n;

      for (let i = 0; i < 10; i++) offsetManager.resolveOffset({ topic: 'topic1', partition: 0, offset: BigInt(i) });
      offsetManager.resolveOffset({ topic: 'topic1', partition: 1, offset: 0n });
      for (let i = 0; i < 3; i++) offsetManager.resolveOffset({ topic: 'topic1', partition: 2, offset: BigInt(i) });
      for (let i = 0; i < 6; i++) offsetManager.resolveOffset({ topic: 'topic2', partition: 5, offset: BigInt(i) });

      expect(offsetManager.countResolvedOffsets()).toEqual(20n);
    });

    it('takes the committed offsets into consideration', () => {
      const offsetManager = createOffsetManager();
      offsetManager.committedOffsets()['topic1']![0] = 10n;
      offsetManager.committedOffsets()['topic1']![1] = 1n;
      offsetManager.committedOffsets()['topic1']![2] = 3n;
      offsetManager.committedOffsets()['topic2']![5] = 5n;

      for (let i = 0; i < 10; i++) offsetManager.resolveOffset({ topic: 'topic1', partition: 0, offset: BigInt(i) });
      offsetManager.resolveOffset({ topic: 'topic1', partition: 1, offset: 0n });
      for (let i = 0; i < 3; i++) offsetManager.resolveOffset({ topic: 'topic1', partition: 2, offset: BigInt(i) });
      for (let i = 0; i < 6; i++) offsetManager.resolveOffset({ topic: 'topic2', partition: 5, offset: BigInt(i) });

      expect(offsetManager.countResolvedOffsets()).toEqual(1n);
    });
  });

  describe('uncommittedOffsets', () => {
    it('returns all resolved offsets which have not been committed', () => {
      const topic1 = 'topic-1';
      const topic2 = 'topic-2';
      const memberAssignment: Record<string, number[]> = { [topic1]: [0, 1], [topic2]: [0, 1, 2, 3] };
      const offsetManager = createOffsetManager({ memberAssignment, groupId: '' });

      for (const topic of Object.keys(memberAssignment)) {
        for (const partition of memberAssignment[topic] ?? []) {
          offsetManager.resolveOffset({ topic, partition, offset: 2n });
        }
      }

      offsetManager.committedOffsets()[topic2]![0] = 3n;
      offsetManager.committedOffsets()[topic2]![1] = 2n;
      offsetManager.committedOffsets()[topic2]![2] = 4n;

      expect(offsetManager.uncommittedOffsets()).toEqual({
        topics: [
          {
            topic: topic1,
            partitions: [
              { partition: 0, offset: 3n },
              { partition: 1, offset: 3n },
            ],
          },
          {
            topic: topic2,
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

  describe('seek', () => {
    it('ignores the seek when the consumer is not assigned to the topic', async () => {
      const offsetCommit = vi.fn();
      const offsetManager = createOffsetManager({ coordinator: { offsetCommit } });
      await offsetManager.seek({ topic: 'topic3', partition: 0, offset: 100n });
      expect(offsetCommit).not.toHaveBeenCalled();
    });

    it('ignores the seek when the consumer is not assigned to the partition', async () => {
      const offsetCommit = vi.fn();
      const offsetManager = createOffsetManager({ coordinator: { offsetCommit } });
      await offsetManager.seek({ topic: 'topic1', partition: 4, offset: 101n });
      expect(offsetCommit).not.toHaveBeenCalled();
    });
  });

  describe('commitOffsets', () => {
    it('commits all the resolved offsets that have not already been committed', async () => {
      const topic1 = 'topic-1';
      const topic2 = 'topic-2';
      const memberAssignment = { [topic1]: [0, 1], [topic2]: [0, 1, 2, 3] };
      const offsetCommit = vi.fn();
      const offsetManager = createOffsetManager({ memberAssignment, coordinator: { offsetCommit } });

      for (const topic of Object.keys(memberAssignment) as Array<typeof topic1 | typeof topic2>) {
        for (const partition of memberAssignment[topic]) {
          offsetManager.resolveOffset({ topic, partition, offset: 2n });
        }
      }

      offsetManager.committedOffsets()[topic2]![0] = 3n;

      await offsetManager.commitOffsets();

      expect(offsetCommit).toHaveBeenCalledWith({
        groupId: 'groupId',
        memberId: 'memberId',
        groupGenerationId: 1,
        topics: [
          {
            topic: topic1,
            partitions: [
              { partition: 0, offset: 3n },
              { partition: 1, offset: 3n },
            ],
          },
          {
            topic: topic2,
            partitions: [
              { partition: 1, offset: 3n },
              { partition: 2, offset: 3n },
              { partition: 3, offset: 3n },
            ],
          },
        ],
      });

      expect(offsetManager.committedOffsets()).toEqual({
        [topic1]: { 0: 3n, 1: 3n },
        [topic2]: { 0: 3n, 1: 3n, 2: 3n, 3: 3n },
      });
    });

    it('commits any provided offsets', async () => {
      const offsetCommit = vi.fn();
      const offsetManager = createOffsetManager({
        memberAssignment: { 'topic-1': [0, 1] },
        coordinator: { offsetCommit },
      });
      const offsets = { topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 42n }] }] };
      await offsetManager.commitOffsets(offsets);

      expect(offsetCommit).toHaveBeenCalledWith({
        groupId: 'groupId',
        memberId: 'memberId',
        groupGenerationId: 1,
        topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 42n }] }],
      });
    });

    it('fires onCommit hooks in order with the committed topics after a successful commit', async () => {
      const offsetCommit = vi.fn();
      const order: string[] = [];
      const first = vi.fn((_event: { error?: unknown }) => {
        order.push('first');
      });
      const second = vi.fn((_event: { error?: unknown }) => {
        order.push('second');
      });
      const offsetManager = createOffsetManager({
        memberAssignment: { 'topic-1': [0] },
        coordinator: { offsetCommit },
        hooks: { onCommit: [first, second] },
      });

      const offsets = { topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 42n }] }] };
      await offsetManager.commitOffsets(offsets);

      expect(order).toEqual(['first', 'second']);
      expect(first).toHaveBeenCalledWith(
        expect.objectContaining({
          groupId: 'groupId',
          memberId: 'memberId',
          groupGenerationId: 1,
          topics: offsets.topics,
        }),
      );
      const event = first.mock.calls[0]?.[0];
      expect(event?.error).toBeUndefined();
    });

    it('fires onCommit hooks with the error when the commit fails, without altering the rejection', async () => {
      const commitError = new Error('commit failed');
      const offsetCommit = vi.fn(() => {
        throw commitError;
      });
      const onCommit = vi.fn();
      const offsetManager = createOffsetManager({
        memberAssignment: { 'topic-1': [0] },
        coordinator: { offsetCommit },
        hooks: { onCommit: [onCommit] },
      });

      const offsets = { topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 42n }] }] };
      await expect(offsetManager.commitOffsets(offsets)).rejects.toBe(commitError);

      expect(onCommit).toHaveBeenCalledWith(expect.objectContaining({ topics: offsets.topics, error: commitError }));
    });

    it('does not fail the commit when an onCommit hook throws', async () => {
      const offsetCommit = vi.fn();
      const throwingHook = vi.fn(() => {
        throw new Error('hook boom');
      });
      const nextHook = vi.fn();
      const offsetManager = createOffsetManager({
        memberAssignment: { 'topic-1': [0] },
        coordinator: { offsetCommit },
        hooks: { onCommit: [throwingHook, nextHook] },
      });

      const offsets = { topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 42n }] }] };
      await expect(offsetManager.commitOffsets(offsets)).resolves.toBeUndefined();

      expect(offsetCommit).toHaveBeenCalledTimes(1);
      expect(throwingHook).toHaveBeenCalledTimes(1);
      expect(nextHook).toHaveBeenCalledTimes(1);
    });

    it('refreshes metadata on NOT_COORDINATOR_FOR_GROUP protocol error', async () => {
      const refreshMetadata = vi.fn();
      const offsetCommit = vi.fn(() => {
        throw createErrorFromCode(NOT_COORDINATOR_FOR_GROUP_CODE);
      });
      const offsetManager = createOffsetManager({
        memberAssignment: { 'topic-1': [0] },
        cluster: { refreshMetadata },
        coordinator: { offsetCommit },
      });

      const offsets = { topics: [{ topic: 'topic-1', partitions: [{ partition: 0, offset: 1n }] }] };
      await expect(offsetManager.commitOffsets(offsets)).rejects.toThrow(KafkaProtocolError);
      expect(refreshMetadata).toHaveBeenCalled();
    });
  });

  describe('commitOffsetsIfNecessary', () => {
    it('commits offsets when interval and threshold are unset', async () => {
      const offsetManager = createOffsetManager({ autoCommitInterval: null, autoCommitThreshold: null });
      const commitOffsets = vi.spyOn(offsetManager, 'commitOffsets');
      await offsetManager.commitOffsetsIfNecessary();
      expect(commitOffsets).toHaveBeenCalledTimes(1);
    });

    it('commits the offsets whenever the interval is reached', async () => {
      const offsetManager = createOffsetManager({ autoCommitInterval: 30, autoCommitThreshold: null });
      const commitOffsets = vi.spyOn(offsetManager, 'commitOffsets').mockImplementation(async () => {
        offsetManager.lastCommit = Date.now();
      });

      await offsetManager.commitOffsetsIfNecessary();
      await sleep(50);
      await offsetManager.commitOffsetsIfNecessary();
      await offsetManager.commitOffsetsIfNecessary();

      expect(commitOffsets).toHaveBeenCalledTimes(1);
    });

    it('commits the offsets whenever the threshold is reached', async () => {
      const offsetManager = createOffsetManager({ autoCommitInterval: null, autoCommitThreshold: 3 });
      const commitOffsets = vi.spyOn(offsetManager, 'commitOffsets').mockImplementation(async () => {
        const committed = offsetManager.committedOffsets();
        for (const topic of Object.keys(offsetManager.resolvedOffsets)) {
          committed[topic] = { ...offsetManager.resolvedOffsets[topic] };
        }
        offsetManager.lastCommit = Date.now();
      });

      await offsetManager.commitOffsetsIfNecessary();
      offsetManager.resolveOffset({ topic: 'topic1', partition: 0, offset: 3n });
      await offsetManager.commitOffsetsIfNecessary();
      await offsetManager.commitOffsetsIfNecessary();

      expect(commitOffsets).toHaveBeenCalledTimes(1);
    });
  });

  describe('resolveOffsets', () => {
    it('skips ListOffsets and throws when reset is none and there is no committed offset', async () => {
      const fetchTopicsOffset = vi.fn();
      const offsetFetch = vi.fn(async () => ({
        responses: [{ topic: 'events', partitions: [{ partition: 0, offset: -1n }] }],
      }));
      const offsetManager = createOffsetManager({
        memberAssignment: { events: [0] },
        topicConfigurations: { events: { autoOffsetReset: 'none' } },
        cluster: { fetchTopicsOffset },
        coordinator: { offsetFetch },
      });

      await expect(offsetManager.resolveOffsets()).rejects.toThrow(
        new KafkaNonRetriableError('Offset reset policy is none; no committed offset for topic events partition 0'),
      );
      expect(fetchTopicsOffset).not.toHaveBeenCalled();
    });

    it('skips ListOffsets and keeps a valid committed offset when reset is none', async () => {
      const fetchTopicsOffset = vi.fn();
      const offsetFetch = vi.fn(async () => ({
        responses: [{ topic: 'events', partitions: [{ partition: 0, offset: 14n }] }],
      }));
      const offsetManager = createOffsetManager({
        memberAssignment: { events: [0] },
        topicConfigurations: { events: { autoOffsetReset: 'none' } },
        cluster: { fetchTopicsOffset },
        coordinator: { offsetFetch },
      });

      await offsetManager.resolveOffsets();

      expect(fetchTopicsOffset).not.toHaveBeenCalled();
      expect(offsetManager.committedOffsets()['events']![0]).toBe(14n);
    });

    it('fetches earliest offsets when autoOffsetReset is earliest', async () => {
      const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 5n }] }]);
      const offsetFetch = vi.fn(async () => ({
        responses: [{ topic: 'events', partitions: [{ partition: 0, offset: -1n }] }],
      }));
      const offsetManager = createOffsetManager({
        memberAssignment: { events: [0] },
        topicConfigurations: { events: { autoOffsetReset: 'earliest' } },
        cluster: { fetchTopicsOffset },
        coordinator: { offsetFetch },
      });

      await offsetManager.resolveOffsets();

      expect(fetchTopicsOffset).toHaveBeenCalledWith([
        { topic: 'events', partitions: [{ partition: 0 }], fromBeginning: true },
      ]);
      expect(offsetManager.committedOffsets()['events']![0]).toBe(5n);
    });
  });

  describe('setDefaultOffset', () => {
    it('throws when reset is none', async () => {
      const offsetCommit = vi.fn();
      const offsetManager = createOffsetManager({
        memberAssignment: { events: [0] },
        topicConfigurations: { events: { autoOffsetReset: 'none' } },
        coordinator: { offsetCommit },
      });

      await expect(offsetManager.setDefaultOffset({ topic: 'events', partition: 0 })).rejects.toThrow(
        new KafkaNonRetriableError('Offset reset policy is none; no committed offset for topic events partition 0'),
      );
      expect(offsetCommit).not.toHaveBeenCalled();
    });
  });
});
