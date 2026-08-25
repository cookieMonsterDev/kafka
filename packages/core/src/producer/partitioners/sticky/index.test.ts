import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { PartitionMetadata } from '../../../cluster/index';
import type { NodeLatencyReader } from '../../node-latency-tracker';

const { randomBytes } = vi.hoisted(() => ({ randomBytes: vi.fn() }));

vi.mock('../legacy/random-bytes', () => ({ randomBytes }));

import { StickyPartitioner } from './index';

function metadata(partitionId: number, leader = 0): PartitionMetadata {
  return { partitionErrorCode: 0, partitionId, leader, replicas: [0], isr: [0], offlineReplicas: [] };
}

function latencyReader(byNodeId: Record<number, number>): NodeLatencyReader {
  return { latencyFor: (nodeId) => byNodeId[nodeId] };
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

  describe('adaptive latency bias', () => {
    it('is on by default and biases toward the lower-latency leader on a low random draw', () => {
      useRandomValues(0);
      const partitioner = StickyPartitioner();
      const partitionMetadata = [metadata(0, 10), metadata(1, 20)];
      const nodeLatency = latencyReader({ 10: 1, 20: 3 }); // weights: 0.5 vs 0.25

      const args = { topic: 'events', partitionMetadata, message: { value: 'value' }, nodeLatency };
      partitioner.onNewBatch?.({ topic: 'events', partitionMetadata, nodeLatency });
      expect(partitioner(args)).toBe(0); // leader 10's partition - lower latency, most of the weight
    });

    it('picks the higher-latency leader on a high enough random draw', () => {
      useRandomValues(0xffffffff);
      const partitioner = StickyPartitioner();
      const partitionMetadata = [metadata(0, 10), metadata(1, 20)];
      const nodeLatency = latencyReader({ 10: 1, 20: 3 });

      const args = { topic: 'events', partitionMetadata, message: { value: 'value' }, nodeLatency };
      partitioner.onNewBatch?.({ topic: 'events', partitionMetadata, nodeLatency });
      expect(partitioner(args)).toBe(1); // leader 20's partition - the tail of the weighted range
    });

    it('excludes the previous partition from the weighted pool when rotating', () => {
      useRandomValues(0, 0);
      const partitioner = StickyPartitioner();
      const partitionMetadata = [metadata(0, 10), metadata(1, 20), metadata(2, 30)];
      const nodeLatency = latencyReader({ 10: 1, 20: 1, 30: 100 }); // 20 and 30 tie/lose against 10 once excluded

      const args = { topic: 'events', partitionMetadata, message: { value: 'value' }, nodeLatency };
      partitioner.onNewBatch?.({ topic: 'events', partitionMetadata, nodeLatency });
      expect(partitioner(args)).toBe(0); // first pick: full pool, leader 10 wins on latency

      partitioner.onNewBatch?.({ topic: 'events', partitionMetadata, nodeLatency });
      expect(partitioner(args)).toBe(1); // rotates: leader 10 excluded, leader 20 wins the remaining pool
    });

    it('falls back to the plain uniform choice when no candidate has recorded latency yet', () => {
      useRandomValues(1, 0, 1);
      const partitioner = StickyPartitioner();
      const partitionMetadata = [metadata(0), metadata(1), metadata(2)];
      const nodeLatency = latencyReader({}); // no data for any leader
      const args = { topic: 'events', partitionMetadata, message: { value: 'value' }, nodeLatency };

      // Same random sequence and expected partitions as the plain uniform-selection test above.
      partitioner.onNewBatch?.({ topic: 'events', partitionMetadata, nodeLatency });
      expect(partitioner(args)).toBe(1);
      partitioner.onNewBatch?.({ topic: 'events', partitionMetadata, nodeLatency });
      expect(partitioner(args)).toBe(0);
      partitioner.onNewBatch?.({ topic: 'events', partitionMetadata, nodeLatency });
      expect(partitioner(args)).toBe(1);
    });

    it('ignores latency data and uses the plain uniform choice when adaptive is false', () => {
      useRandomValues(1);
      const partitioner = StickyPartitioner({ adaptive: false });
      const partitionMetadata = [metadata(0, 10), metadata(1, 20)];
      // Latency data heavily favors leader 10 - if this were honored, partition 0 would win.
      const nodeLatency = latencyReader({ 10: 1, 20: 99 });

      const args = { topic: 'events', partitionMetadata, message: { value: 'value' }, nodeLatency };
      partitioner.onNewBatch?.({ topic: 'events', partitionMetadata, nodeLatency });
      expect(partitioner(args)).toBe(1); // plain uniform: randomIndex(2) = 1 % 2 = 1
    });
  });
});
