import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { PartitionMetadata } from '../../../cluster/index';

const { randomBytes } = vi.hoisted(() => ({ randomBytes: vi.fn() }));

vi.mock('../legacy/random-bytes', () => ({ randomBytes }));

import { StickyPartitioner } from './index';

function metadata(partitionId: number, leader = 0): PartitionMetadata {
  return { partitionErrorCode: 0, partitionId, leader, replicas: [0], isr: [0], offlineReplicas: [] };
}

function useRandomValues(...values: number[]): void {
  let index = 0;
  randomBytes.mockImplementation(() => {
    const buffer = Buffer.alloc(4);
    buffer.writeUInt32BE(values[index++] ?? 0);
    return buffer;
  });
}

describe('StickyPartitioner', () => {
  beforeEach(() => {
    randomBytes.mockReset();
    useRandomValues(0);
  });

  it('keeps unkeyed records together within a batch and rotates between batches', () => {
    useRandomValues(0, 0);
    const partitioner = StickyPartitioner();
    const partitionMetadata = [metadata(0), metadata(1), metadata(2)];
    const args = { topic: 'events', partitionMetadata, message: { value: 'value' } };

    partitioner.onNewBatch?.({ topic: 'events', partitionMetadata });
    expect(partitioner(args)).toBe(0);
    expect(partitioner(args)).toBe(0);

    partitioner.onNewBatch?.({ topic: 'events', partitionMetadata });
    expect(partitioner(args)).toBe(2);
    expect(partitioner(args)).toBe(2);
  });

  it('selects each non-previous partition uniformly from the random range', () => {
    useRandomValues(1, 0, 1);
    const partitioner = StickyPartitioner();
    const partitionMetadata = [metadata(0), metadata(1), metadata(2)];
    const args = { topic: 'events', partitionMetadata, message: { value: 'value' } };

    partitioner.onNewBatch?.({ topic: 'events', partitionMetadata });
    expect(partitioner(args)).toBe(1);

    partitioner.onNewBatch?.({ topic: 'events', partitionMetadata });
    expect(partitioner(args)).toBe(0);

    partitioner.onNewBatch?.({ topic: 'events', partitionMetadata });
    expect(partitioner(args)).toBe(1);
  });

  it('preserves explicit partitions and murmur2 routing for keyed records', () => {
    const partitioner = StickyPartitioner();
    const partitionMetadata = [metadata(0), metadata(1), metadata(2)];

    expect(partitioner({ topic: 'events', partitionMetadata, message: { partition: 2, value: 'value' } })).toBe(2);

    const keyed = { topic: 'events', partitionMetadata, message: { key: 'same-key', value: 'value' } };
    expect(partitioner(keyed)).toBe(partitioner(keyed));
  });

  it('sticks only to available partitions when any are available', () => {
    const partitioner = StickyPartitioner();
    const partitionMetadata = [metadata(0, -1), metadata(1), metadata(2, -1)];
    const args = { topic: 'events', partitionMetadata, message: { value: 'value' } };

    partitioner.onNewBatch?.({ topic: 'events', partitionMetadata });
    expect(partitioner(args)).toBe(1);
    partitioner.onNewBatch?.({ topic: 'events', partitionMetadata });
    expect(partitioner(args)).toBe(1);
  });

  it('picks up newly available partitions after onNewBatch with refreshed metadata', () => {
    const partitioner = StickyPartitioner();
    const unavailable = [metadata(0, -1), metadata(1), metadata(2, -1)];
    partitioner.onNewBatch?.({ topic: 'events', partitionMetadata: unavailable });
    expect(partitioner({ topic: 'events', partitionMetadata: unavailable, message: { value: 'value' } })).toBe(1);

    useRandomValues(0);
    const available = [metadata(0), metadata(1), metadata(2)];
    partitioner.onNewBatch?.({ topic: 'events', partitionMetadata: available });
    expect(partitioner({ topic: 'events', partitionMetadata: available, message: { value: 'value' } })).toBe(0);
  });
});
