import { describe, expect, it } from 'vitest';
import type { PartitionMetadata } from '../../cluster/index';
import { createAvailablePartitionCache } from './available-partitions';

function metadata(partitionId: number, leader: number): PartitionMetadata {
  return { partitionErrorCode: 0, partitionId, leader, replicas: [leader], isr: [leader], offlineReplicas: [] };
}

describe('producer/partitioners/available-partitions', () => {
  it('returns the same filtered array until metadata identity or leaders change', () => {
    const cache = createAvailablePartitionCache();
    const first = [metadata(0, 1), metadata(1, -1), metadata(2, 2)];

    const available = cache.available(first);
    expect(available.map((p) => p.partitionId)).toEqual([0, 2]);
    expect(cache.available(first)).toBe(available);

    const sameLeaders = [metadata(0, 1), metadata(1, -1), metadata(2, 2)];
    expect(cache.available(sameLeaders)).toBe(available);

    const refreshed = [metadata(0, 1), metadata(1, 1), metadata(2, 2)];
    const next = cache.available(refreshed);
    expect(next).not.toBe(available);
    expect(next.map((p) => p.partitionId)).toEqual([0, 1, 2]);
  });

  it('recomputes after invalidate even when the metadata array is reused', () => {
    const cache = createAvailablePartitionCache();
    const partitions = [metadata(0, 1), metadata(1, -1)];

    const first = cache.available(partitions);
    cache.invalidate();
    const second = cache.available(partitions);

    expect(second).not.toBe(first);
    expect(second.map((p) => p.partitionId)).toEqual([0]);
  });
});
