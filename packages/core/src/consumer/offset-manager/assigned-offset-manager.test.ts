import { describe, expect, it, vi } from 'vitest';
import type { Broker } from '../../broker/index';
import type { Cluster } from '../../cluster/index';
import { KafkaNonRetriableError } from '../../errors';
import { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import { AssignedOffsetManager } from './assigned-offset-manager';

function createManager(
  overrides: {
    assignment?: Record<string, number[]>;
    groupId?: string | null;
    cluster?: Partial<Cluster>;
    coordinator?: {
      isConnected?: () => boolean;
      offsetCommit?: (...args: never[]) => unknown;
      offsetFetch?: (...args: never[]) => unknown;
    };
    topicConfigurations?: AssignedOffsetManager['topicConfigurations'];
  } = {},
): AssignedOffsetManager {
  const assignment = overrides.assignment ?? { events: [0] };
  const coordinator = {
    isConnected: vi.fn(() => true),
    offsetCommit: vi.fn(async () => undefined),
    offsetFetch: vi.fn(async () => ({ responses: [] })),
    ...overrides.coordinator,
  };

  return new AssignedOffsetManager({
    cluster: {
      findGroupCoordinator: vi.fn(async () => coordinator as unknown as Broker),
      defaultOffset: vi.fn(() => -1n),
      fetchTopicsOffset: vi.fn(async () => []),
      refreshMetadata: vi.fn(async () => undefined),
      ...overrides.cluster,
    } as unknown as Cluster,
    groupId: overrides.groupId === undefined ? null : overrides.groupId,
    assignment,
    topicConfigurations: overrides.topicConfigurations ?? {},
    instrumentationEmitter: new InstrumentationEventEmitter(),
  });
}

describe('consumer/offset-manager/assigned-offset-manager', () => {
  describe('resolveOffsets', () => {
    it('without a groupId, skips OffsetFetch entirely and resolves via autoOffsetReset', async () => {
      const offsetFetch = vi.fn();
      const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 42n }] }]);
      const findGroupCoordinator = vi.fn();
      const manager = createManager({
        groupId: null,
        topicConfigurations: { events: { autoOffsetReset: 'earliest' } },
        cluster: { fetchTopicsOffset, findGroupCoordinator },
        coordinator: { offsetFetch },
      });

      await manager.resolveOffsets();

      expect(findGroupCoordinator).not.toHaveBeenCalled();
      expect(offsetFetch).not.toHaveBeenCalled();
      expect(fetchTopicsOffset).toHaveBeenCalledWith([
        { topic: 'events', partitions: [{ partition: 0 }], fromBeginning: true },
      ]);
      expect(manager.nextOffset('events', 0)).toBe(42n);
    });

    it('without a groupId, defaults to latest when autoOffsetReset is unset', async () => {
      const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 99n }] }]);
      const manager = createManager({ groupId: null, cluster: { fetchTopicsOffset } });

      await manager.resolveOffsets();

      expect(fetchTopicsOffset).toHaveBeenCalledWith([
        { topic: 'events', partitions: [{ partition: 0 }], fromBeginning: false },
      ]);
      expect(manager.nextOffset('events', 0)).toBe(99n);
    });

    it('without a groupId, throws instead of resolving when autoOffsetReset is none', async () => {
      const fetchTopicsOffset = vi.fn();
      const manager = createManager({
        groupId: null,
        topicConfigurations: { events: { autoOffsetReset: 'none' } },
        cluster: { fetchTopicsOffset },
      });

      await expect(manager.resolveOffsets()).rejects.toThrow(
        'Offset reset policy is none; no committed offset for topic events partition 0',
      );
      expect(fetchTopicsOffset).not.toHaveBeenCalled();
    });

    it('with a groupId, honors an existing committed offset via OffsetFetch without ListOffsets', async () => {
      const fetchTopicsOffset = vi.fn();
      const offsetFetch = vi.fn(async () => ({
        responses: [{ topic: 'events', partitions: [{ partition: 0, offset: 14n }] }],
      }));
      const manager = createManager({
        groupId: 'my-group',
        cluster: { fetchTopicsOffset },
        coordinator: { offsetFetch },
      });

      await manager.resolveOffsets();

      expect(offsetFetch).toHaveBeenCalledWith({
        groupId: 'my-group',
        topics: [{ topic: 'events', partitions: [{ partition: 0 }] }],
      });
      expect(fetchTopicsOffset).not.toHaveBeenCalled();
      expect(manager.nextOffset('events', 0)).toBe(14n);
    });

    it('with a groupId but no committed offset, falls back to autoOffsetReset', async () => {
      const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 7n }] }]);
      const offsetFetch = vi.fn(async () => ({
        responses: [{ topic: 'events', partitions: [{ partition: 0, offset: -1n }] }],
      }));
      const manager = createManager({
        groupId: 'my-group',
        topicConfigurations: { events: { autoOffsetReset: 'earliest' } },
        cluster: { fetchTopicsOffset },
        coordinator: { offsetFetch },
      });

      await manager.resolveOffsets();

      expect(offsetFetch).toHaveBeenCalled();
      expect(fetchTopicsOffset).toHaveBeenCalledWith([
        { topic: 'events', partitions: [{ partition: 0 }], fromBeginning: true },
      ]);
      expect(manager.nextOffset('events', 0)).toBe(7n);
    });

    it('does not cache a fallback 0n before resolveOffsets runs', async () => {
      const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 99n }] }]);
      const manager = createManager({ groupId: null, cluster: { fetchTopicsOffset } });

      expect(manager.nextOffset('events', 0)).toBe(0n);

      await manager.resolveOffsets();

      expect(manager.nextOffset('events', 0)).toBe(99n);
    });

    it('fetches ListOffsets by timestamp when autoOffsetReset is by_duration', async () => {
      const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 4n }] }]);
      const manager = createManager({
        groupId: null,
        topicConfigurations: { events: { autoOffsetReset: 'by_duration:PT30M' } },
        cluster: { fetchTopicsOffset },
      });

      await manager.resolveOffsets();

      const [queries] = fetchTopicsOffset.mock.calls[0] as unknown as [
        { topic: string; partitions: { partition: number }[]; fromTimestamp: bigint }[],
      ];
      expect(queries[0]).toMatchObject({ topic: 'events', partitions: [{ partition: 0 }] });
      expect(typeof queries[0]?.fromTimestamp).toBe('bigint');
      expect(manager.nextOffset('events', 0)).toBe(4n);
    });

    it('caches the resolved position so a second call skips both RPCs', async () => {
      const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 7n }] }]);
      const manager = createManager({ groupId: null, cluster: { fetchTopicsOffset } });

      await manager.resolveOffsets();
      await manager.resolveOffsets();

      expect(fetchTopicsOffset).toHaveBeenCalledTimes(1);
    });
  });

  describe('seek', () => {
    it('overrides the local position directly, without any broker round-trip', async () => {
      const offsetCommit = vi.fn();
      const findGroupCoordinator = vi.fn();
      const manager = createManager({
        groupId: 'my-group',
        cluster: { findGroupCoordinator },
        coordinator: { offsetCommit },
      });

      await manager.seek({ topic: 'events', partition: 0, offset: 55n });

      expect(offsetCommit).not.toHaveBeenCalled();
      expect(findGroupCoordinator).not.toHaveBeenCalled();
      expect(manager.nextOffset('events', 0)).toBe(55n);
    });

    it('ignores a seek for a partition outside the fixed assignment', async () => {
      const manager = createManager({ assignment: { events: [0] } });

      await manager.seek({ topic: 'events', partition: 1, offset: 55n });

      expect(manager.nextOffset('events', 1)).toBe(0n);
    });
  });

  describe('commitOffsets', () => {
    it('throws a clear error when no groupId is configured', async () => {
      const manager = createManager({ groupId: null });
      manager.resolveOffset({ topic: 'events', partition: 0, offset: 9n });

      await expect(manager.commitOffsets()).rejects.toThrow(KafkaNonRetriableError);
      await expect(manager.commitOffsets()).rejects.toThrow(/without a configured groupId/);
    });

    it('commits as a standalone consumer when a groupId is configured', async () => {
      const offsetCommit = vi.fn(async () => undefined);
      const manager = createManager({ groupId: 'my-group', coordinator: { offsetCommit } });
      manager.resolveOffset({ topic: 'events', partition: 0, offset: 9n });

      await manager.commitOffsets();

      expect(offsetCommit).toHaveBeenCalledWith({
        groupId: 'my-group',
        memberId: '',
        groupGenerationId: -1,
        topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 10n }] }],
      });
    });

    it('is a no-op when there is nothing uncommitted', async () => {
      const offsetCommit = vi.fn(async () => undefined);
      const manager = createManager({ groupId: 'my-group', coordinator: { offsetCommit } });

      await manager.commitOffsets();

      expect(offsetCommit).not.toHaveBeenCalled();
    });

    it('commitOffsetsIfNecessary always commits immediately (assign mode never auto-commits on its own)', async () => {
      const offsetCommit = vi.fn(async () => undefined);
      const manager = createManager({ groupId: 'my-group', coordinator: { offsetCommit } });
      manager.resolveOffset({ topic: 'events', partition: 0, offset: 4n });

      await manager.commitOffsetsIfNecessary();

      expect(offsetCommit).toHaveBeenCalledTimes(1);
    });
  });

  describe('updateAssignment', () => {
    it('preserves already-resolved positions for partitions that remain assigned', async () => {
      const manager = createManager({ assignment: { events: [0] } });
      manager.resolveOffset({ topic: 'events', partition: 0, offset: 20n });

      manager.updateAssignment({ events: [0, 1] });

      expect(manager.nextOffset('events', 0)).toBe(21n);
      expect(manager.nextOffset('events', 1)).toBe(0n);
    });
  });

  describe('setDefaultOffset', () => {
    it('throws when the reset policy is none', async () => {
      const manager = createManager({ topicConfigurations: { events: { autoOffsetReset: 'none' } } });

      await expect(manager.setDefaultOffset({ topic: 'events', partition: 0 })).rejects.toThrow(
        'Offset reset policy is none; no committed offset for topic events partition 0',
      );
    });

    it('without a groupId, resets the local position without committing to a group', async () => {
      const offsetCommit = vi.fn();
      const manager = createManager({ groupId: null, coordinator: { offsetCommit } });
      manager.resolveOffset({ topic: 'events', partition: 0, offset: 5n });

      await manager.setDefaultOffset({ topic: 'events', partition: 0 });

      expect(offsetCommit).not.toHaveBeenCalled();
    });

    it('with a groupId, commits the default offset to the group', async () => {
      const offsetCommit = vi.fn(async () => undefined);
      const defaultOffset = vi.fn(() => 0n);
      const manager = createManager({ groupId: 'my-group', cluster: { defaultOffset }, coordinator: { offsetCommit } });

      await manager.setDefaultOffset({ topic: 'events', partition: 0 });

      expect(offsetCommit).toHaveBeenCalledWith({
        groupId: 'my-group',
        memberId: '',
        groupGenerationId: -1,
        topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 0n }] }],
      });
    });

    it('with a groupId, commits the ListOffsets timestamp result when autoOffsetReset is by_duration', async () => {
      const fetchTopicsOffset = vi.fn(async () => [{ topic: 'events', partitions: [{ partition: 0, offset: 6n }] }]);
      const offsetCommit = vi.fn(async () => undefined);
      const manager = createManager({
        groupId: 'my-group',
        topicConfigurations: { events: { autoOffsetReset: 'by_duration:PT30M' } },
        cluster: { fetchTopicsOffset },
        coordinator: { offsetCommit },
      });

      await manager.setDefaultOffset({ topic: 'events', partition: 0 });

      expect(fetchTopicsOffset).toHaveBeenCalledWith([
        expect.objectContaining({ topic: 'events', partitions: [{ partition: 0 }] }),
      ]);
      expect(offsetCommit).toHaveBeenCalledWith(
        expect.objectContaining({
          topics: [{ topic: 'events', partitions: [{ partition: 0, offset: 6n }] }],
        }),
      );
    });
  });
});
