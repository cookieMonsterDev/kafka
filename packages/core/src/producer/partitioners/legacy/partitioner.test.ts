import { describe, expect, it } from 'vitest';
import type { PartitionMetadata } from '../../../cluster/index.js';
import { createPartitionerFactory } from './partitioner.js';

function fakePartitionMetadata(partitionId: number, leader: number): PartitionMetadata {
  return { partitionErrorCode: 0, partitionId, leader, replicas: [leader], isr: [leader], offlineReplicas: [] };
}

describe('producer/partitioners/legacy/partitioner', () => {
  const topic = 'test-topic-1';
  // Intentionally not in partition order, to exercise the round-robin-over-available-partitions path.
  const partitionMetadata = [fakePartitionMetadata(1, 1), fakePartitionMetadata(2, 2), fakePartitionMetadata(0, 0)];

  it('routes the same key to the same partition', () => {
    const partitioner = createPartitionerFactory(() => 42)();
    const a = partitioner({ topic, partitionMetadata, message: { value: null, key: 'test-key' } });
    const b = partitioner({ topic, partitionMetadata, message: { value: null, key: 'test-key' } });
    expect(a).toBe(b);
  });

  it('round-robins evenly across available partitions, skipping unavailable ones', () => {
    const unavailable = [{ ...partitionMetadata[0]!, leader: -1 }, partitionMetadata[1]!, partitionMetadata[2]!];
    const partitioner = createPartitionerFactory(() => 0)();

    let countForPartition0 = 0;
    let countForPartition2 = 0;
    for (let i = 0; i < 100; i++) {
      const partition = partitioner({ topic, partitionMetadata: unavailable, message: { value: null } });
      expect([0, 2]).toContain(partition);
      if (partition === 0) countForPartition0++;
      else countForPartition2++;
    }

    expect(countForPartition0).toBe(countForPartition2);
  });

  it('round-robins evenly across all partitions when all are available', () => {
    const partitioner = createPartitionerFactory(() => 0)();
    const counts: Record<number, number> = {};

    for (let i = 0; i < 30; i++) {
      const partition = partitioner({ topic, partitionMetadata, message: { value: null } });
      counts[partition] = (counts[partition] ?? 0) + 1;
    }

    expect(counts[0]).toBe(10);
    expect(counts[1]).toBe(10);
    expect(counts[2]).toBe(10);
  });

  it('keeps a separate round-robin counter per topic', () => {
    const twoPartitions = [fakePartitionMetadata(1, 1), fakePartitionMetadata(2, 2)];
    const partitioner = createPartitionerFactory(() => 0)();
    const countsByTopic: Record<string, Record<number, number>> = { 'topic-a': {}, 'topic-b': {} };

    for (let i = 0; i < 30; i++) {
      for (const t of ['topic-a', 'topic-b']) {
        const partition = partitioner({ topic: t, partitionMetadata: twoPartitions, message: { value: null } });
        countsByTopic[t]![partition] = (countsByTopic[t]![partition] ?? 0) + 1;
      }
    }

    expect(countsByTopic['topic-a']).toEqual({ 1: 15, 2: 15 });
    expect(countsByTopic['topic-b']).toEqual({ 1: 15, 2: 15 });
  });

  it('returns the configured partition if one is set, including partition 0', () => {
    const partitioner = createPartitionerFactory(() => 0)();

    expect(partitioner({ topic, partitionMetadata, message: { value: null, key: '1', partition: 99 } })).toBe(99);
    expect(partitioner({ topic, partitionMetadata, message: { value: null, key: '1', partition: 0 } })).toBe(0);
  });
});
