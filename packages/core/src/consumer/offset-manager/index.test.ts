import { describe, expect, it, vi } from 'vitest';
import type { Broker } from '../../broker/index.js';
import type { Cluster } from '../../cluster/index.js';
import { KafkaJSProtocolError } from '../../errors.js';
import { InstrumentationEventEmitter } from '../../instrumentation/emitter.js';
import { createErrorFromCode } from '../../protocol/error-codes.js';
import { sleep } from '../../utils/wait.js';
import { OffsetManager } from './index.js';

const NOT_COORDINATOR_FOR_GROUP_CODE = 16;

function createOffsetManager(
  overrides: {
    memberAssignment?: Record<string, number[]>;
    cluster?: Partial<Cluster>;
    coordinator?: Partial<Broker>;
    autoCommit?: boolean;
    autoCommitInterval?: number | null;
    autoCommitThreshold?: number | null;
    groupId?: string;
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
    topicConfigurations: {},
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: overrides.groupId ?? 'groupId',
    generationId: 1,
    memberId: 'memberId',
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
      await expect(offsetManager.commitOffsets(offsets)).rejects.toThrow(KafkaJSProtocolError);
      expect(refreshMetadata).toHaveBeenCalled();
    });
  });

  describe('commitOffsetsIfNecessary', () => {
    it('does not commit offsets when interval and threshold are unset', async () => {
      const offsetManager = createOffsetManager({ autoCommitInterval: null, autoCommitThreshold: null });
      const commitOffsets = vi.spyOn(offsetManager, 'commitOffsets');
      await offsetManager.commitOffsetsIfNecessary();
      expect(commitOffsets).not.toHaveBeenCalled();
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
});
