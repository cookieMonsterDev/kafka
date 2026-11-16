import { describe, expect, it, vi } from 'vitest';
import { Batch } from './batch';
import { PartitionPipeline, type PartitionState } from './partition-pipeline';
import type { KafkaMessage } from './types';

const topic = 'topic1';

function record(offset: bigint): KafkaMessage {
  return { offset, isControlRecord: false, byteSize: 10 } as KafkaMessage;
}

function createPipeline(partitions: number[] = [0]): {
  pipeline: PartitionPipeline;
  setPosition: (offset: bigint) => void;
  stateOf: (partition?: number) => PartitionState;
} {
  let position = 0n;
  const pipeline = new PartitionPipeline({ readPosition: () => position });
  pipeline.assign([{ topic, partitions }]);
  return {
    pipeline,
    setPosition: (offset) => {
      position = offset;
    },
    stateOf: (partition = 0) => {
      const state = pipeline.state(topic, partition);
      if (!state) throw new Error(`expected partition ${partition} to have a state`);
      return state;
    },
  };
}

/** A tracked batch of `count` records from `fetchedOffset`, as `ConsumerGroup.fetch` returns it. */
function fetchBatch(pipeline: PartitionPipeline, state: PartitionState, fetchedOffset: bigint, count = 2): Batch {
  const messages = Array.from({ length: count }, (_, index) => record(fetchedOffset + BigInt(index)));
  const batch = new Batch(topic, fetchedOffset, { partition: state.partition, highWatermark: 1_000n, messages }, state);
  pipeline.track(batch);
  return batch;
}

function fetchOffsets(pipeline: PartitionPipeline, partitions: number[] = [0]): bigint[] {
  const { topics } = pipeline.planFetch([{ topic, partitions }], () => true);
  return topics.flatMap(({ partitions: planned }) => planned.map(({ fetchOffset }) => fetchOffset));
}

describe('consumer/partition-pipeline', () => {
  describe('pause and resume', () => {
    it('updates every assigned partition of a topic when the whole topic is paused', () => {
      const { pipeline, stateOf } = createPipeline([0, 1]);

      pipeline.pause([{ topic }]);
      expect([stateOf(0).isPaused, stateOf(1).isPaused]).toEqual([true, true]);

      pipeline.resume([{ topic, partitions: [1] }]);
      expect([stateOf(0).isPaused, stateOf(1).isPaused]).toEqual([true, false]);
    });

    it('applies a pause made before the assignment to the partitions assigned later', () => {
      const pipeline = new PartitionPipeline({ readPosition: () => 0n });
      pipeline.pause([{ topic, partitions: [1] }]);

      pipeline.assign([{ topic, partitions: [0, 1] }]);

      expect(pipeline.state(topic, 0)?.isPaused).toBe(false);
      expect(pipeline.state(topic, 1)?.isPaused).toBe(true);
      expect(pipeline.paused()).toEqual([{ topic, partitions: [1] }]);
    });

    it('wakes a fetch waiting for a fetchable partition on resume', async () => {
      const { pipeline } = createPipeline();
      pipeline.pause([{ topic }]);
      const wakeCount = pipeline.wakeCount;

      const waiting = pipeline.waitForFetchablePartition(60_000);
      pipeline.resume([{ topic }]);

      await expect(waiting).resolves.toBeUndefined();
      expect(pipeline.wakeCount).toBe(wakeCount + 1);
    });
  });

  describe('assign', () => {
    it("keeps a retained partition's fetch state and fetches a gained one from its position", () => {
      const { pipeline, stateOf } = createPipeline([0]);
      const first = fetchBatch(pipeline, stateOf(0), 0n);

      pipeline.assign([{ topic, partitions: [0, 1] }]);

      expect(stateOf(0).unsettledBatches).toBe(1);
      expect(pipeline.isBatchCurrent(first)).toBe(true);
      expect(fetchOffsets(pipeline, [0, 1])).toEqual([2n, 0n]);
    });

    it('drops the state of a partition outside the assignment unless it holds a pending seek', () => {
      const { pipeline, stateOf } = createPipeline([0, 1]);
      const dropped = stateOf(0);
      pipeline.seek({ topic, partition: 1, offset: 5n });

      pipeline.assign([]);

      expect(pipeline.state(topic, 0)).toBeUndefined();
      expect(dropped.isAssigned).toBe(false);
      expect(pipeline.state(topic, 1)).toMatchObject({ isAssigned: false, pendingSeek: 5n });
    });

    it('keeps the state of a partition a worker is handling, so a seek after it is assigned again waits', async () => {
      const { pipeline, stateOf } = createPipeline([0]);
      const batch = fetchBatch(pipeline, stateOf(), 0n);
      pipeline.beginProcessing(batch);

      pipeline.assign([]);
      pipeline.assign([{ topic, partitions: [0] }]);
      pipeline.seek({ topic, partition: 0, offset: 50n });
      const applySeek = vi.fn(async () => undefined);
      await pipeline.applyPendingSeeks([{ topic, partitions: [0] }], applySeek);

      expect(stateOf()).toBe(batch.partitionState);
      expect(applySeek).not.toHaveBeenCalled();
    });

    it('keeps a pause of a retained partition', () => {
      const { pipeline, stateOf } = createPipeline([0, 1]);
      pipeline.pause([{ topic, partitions: [0] }]);

      pipeline.assign([{ topic, partitions: [0] }]);

      expect(stateOf(0).isPaused).toBe(true);
      expect(pipeline.paused()).toEqual([{ topic, partitions: [0] }]);
    });
  });

  describe('planFetch', () => {
    it('leaves out paused partitions and those without a committed offset', () => {
      const { pipeline } = createPipeline([0, 1, 2]);
      pipeline.pause([{ topic, partitions: [1] }]);

      const plan = pipeline.planFetch([{ topic, partitions: [0, 1, 2] }], (_topic, partition) => partition !== 2);

      expect(plan).toEqual({
        topics: [{ topic, partitions: [{ partition: 0, fetchOffset: 0n }] }],
        hasHeldBackPartition: false,
      });
    });

    it('starts the next fetch after the last fetched batch and holds back a partition with two unsettled batches', () => {
      const { pipeline, stateOf } = createPipeline();

      fetchBatch(pipeline, stateOf(), 0n);
      expect(fetchOffsets(pipeline)).toEqual([2n]);
      fetchBatch(pipeline, stateOf(), 2n);

      expect(pipeline.planFetch([{ topic, partitions: [0] }], () => true)).toEqual({
        topics: [],
        hasHeldBackPartition: true,
      });
    });
  });

  describe('isBatchCurrent', () => {
    it('is current while assigned, not paused, without a pending seek, and starting at the position', () => {
      const { pipeline, stateOf } = createPipeline();
      const batch = fetchBatch(pipeline, stateOf(), 0n);

      expect(pipeline.isBatchCurrent(batch)).toBe(true);
    });

    it('drops a batch once its partition is paused, sought, or revoked', () => {
      const paused = createPipeline();
      const pausedBatch = fetchBatch(paused.pipeline, paused.stateOf(), 0n);
      paused.pipeline.pause([{ topic, partitions: [0] }]);
      expect(paused.pipeline.isBatchCurrent(pausedBatch)).toBe(false);

      const sought = createPipeline();
      const soughtBatch = fetchBatch(sought.pipeline, sought.stateOf(), 0n);
      sought.pipeline.seek({ topic, partition: 0, offset: 10n });
      expect(sought.pipeline.isBatchCurrent(soughtBatch)).toBe(false);

      const revoked = createPipeline();
      const revokedBatch = fetchBatch(revoked.pipeline, revoked.stateOf(), 0n);
      revoked.pipeline.assign([]);
      expect(revoked.pipeline.isBatchCurrent(revokedBatch)).toBe(false);
    });

    it('drops a prefetched batch once the batch before it settles without being fully consumed', () => {
      const { pipeline, setPosition, stateOf } = createPipeline();
      const first = fetchBatch(pipeline, stateOf(), 0n);
      const second = fetchBatch(pipeline, stateOf(), 2n);

      setPosition(1n);
      pipeline.settle(first);

      expect(pipeline.isBatchCurrent(second)).toBe(false);
      expect(fetchOffsets(pipeline)).toEqual([1n]);
    });

    it('treats a batch without a partition state as current', () => {
      const { pipeline } = createPipeline();
      const batch = new Batch(topic, 5n, { partition: 0, highWatermark: 10n, messages: [record(5n)] });

      expect(pipeline.isBatchCurrent(batch)).toBe(true);
    });
  });

  describe('settle', () => {
    it('keeps fetching ahead while batches are consumed in order', () => {
      const { pipeline, setPosition, stateOf } = createPipeline();
      const first = fetchBatch(pipeline, stateOf(), 0n);
      const second = fetchBatch(pipeline, stateOf(), 2n);

      setPosition(2n);
      pipeline.settle(first);

      expect(pipeline.isBatchCurrent(second)).toBe(true);
      expect(fetchOffsets(pipeline)).toEqual([4n]);
    });

    it('fetches from the consumed position once the newest batch settles', () => {
      const { pipeline, setPosition, stateOf } = createPipeline();
      const first = fetchBatch(pipeline, stateOf(), 0n);

      setPosition(7n);
      pipeline.settle(first);

      expect(stateOf().unsettledBatches).toBe(0);
      expect(fetchOffsets(pipeline)).toEqual([7n]);
    });

    it('does not move the fetch offset for an empty batch or one fetched behind the position', () => {
      const { pipeline, setPosition, stateOf } = createPipeline();
      const empty = fetchBatch(pipeline, stateOf(), 0n, 0);
      expect(fetchOffsets(pipeline)).toEqual([0n]);
      pipeline.settle(empty);

      setPosition(5n);
      fetchBatch(pipeline, stateOf(), 0n);
      expect(fetchOffsets(pipeline)).toEqual([5n]);
    });

    it('stops counting unsettled batches of a regained partition without going negative when they settle', () => {
      const { pipeline, stateOf } = createPipeline();
      const first = fetchBatch(pipeline, stateOf(), 0n);
      fetchBatch(pipeline, stateOf(), 2n);

      pipeline.assign([]);
      pipeline.assign([{ topic, partitions: [0] }]);
      expect(fetchOffsets(pipeline)).toEqual([0n]);
      pipeline.settle(first);

      expect(stateOf().unsettledBatches).toBe(0);
    });

    it('wakes a fetch waiting for a held-back partition', async () => {
      vi.useFakeTimers();
      try {
        const { pipeline, stateOf } = createPipeline();
        const first = fetchBatch(pipeline, stateOf(), 0n);
        let isDone = false;
        const waiting = pipeline.waitForFetchablePartition(100).then(() => {
          isDone = true;
        });

        await vi.advanceTimersByTimeAsync(10);
        expect(isDone).toBe(false);
        pipeline.settle(first);
        await waiting;

        expect(isDone).toBe(true);
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe('applyPendingSeeks', () => {
    it('keeps a seek pending while its partition is being processed', async () => {
      const { pipeline, stateOf } = createPipeline();
      const batch = fetchBatch(pipeline, stateOf(), 0n);
      const applySeek = vi.fn(async () => undefined);
      pipeline.seek({ topic, partition: 0, offset: 50n });

      pipeline.beginProcessing(batch);
      await pipeline.applyPendingSeeks([{ topic, partitions: [0] }], applySeek);
      expect(applySeek).not.toHaveBeenCalled();
      expect(stateOf().pendingSeek).toBe(50n);

      pipeline.endProcessing(batch);
      await pipeline.applyPendingSeeks([{ topic, partitions: [0] }], applySeek);
      expect(applySeek).toHaveBeenCalledWith({ topic, partition: 0, offset: 50n });
      expect(stateOf().pendingSeek).toBeNull();
    });

    it('fetches from the sought position once a seek is applied', async () => {
      const { pipeline, setPosition, stateOf } = createPipeline();
      fetchBatch(pipeline, stateOf(), 0n);
      pipeline.seek({ topic, partition: 0, offset: 50n });

      await pipeline.applyPendingSeeks([{ topic, partitions: [0] }], async ({ offset }) => setPosition(offset));

      expect(fetchOffsets(pipeline)).toEqual([50n]);
    });

    it('keeps a newer seek made while an earlier one is applied', async () => {
      const { pipeline, stateOf } = createPipeline();
      pipeline.seek({ topic, partition: 0, offset: 50n });

      await pipeline.applyPendingSeeks([{ topic, partitions: [0] }], async () => {
        pipeline.seek({ topic, partition: 0, offset: 60n });
      });

      expect(stateOf().pendingSeek).toBe(60n);
    });

    it('applies a seek made before the partition was assigned once it is', async () => {
      const pipeline = new PartitionPipeline({ readPosition: () => 0n });
      const applySeek = vi.fn(async () => undefined);
      pipeline.seek({ topic, partition: 0, offset: 50n });
      expect(pipeline.isAssigned(topic, 0)).toBe(false);

      pipeline.assign([{ topic, partitions: [0] }]);
      await pipeline.applyPendingSeeks([{ topic, partitions: [0] }], applySeek);

      expect(applySeek).toHaveBeenCalledWith({ topic, partition: 0, offset: 50n });
    });
  });

  it('fetches every partition from its position after resetPrefetches', () => {
    const { pipeline, setPosition, stateOf } = createPipeline();
    fetchBatch(pipeline, stateOf(), 0n);

    setPosition(1n);
    pipeline.resetPrefetches();

    expect(fetchOffsets(pipeline)).toEqual([1n]);
  });
});
