import { describe, expect, it } from 'vitest';
import type { PartitionMetadata } from '../../../cluster/index';
import { DefaultPartitioner } from './index';

describe('producer/partitioners/default', () => {
  it('hashes a key deterministically into one of the topic partitions', () => {
    const partitionMetadata: PartitionMetadata[] = [0, 1, 2].map((partitionId) => ({
      partitionErrorCode: 0,
      partitionId,
      leader: partitionId,
      replicas: [partitionId],
      isr: [partitionId],
      offlineReplicas: [],
    }));

    const partitioner = DefaultPartitioner();
    const partition = partitioner({ topic: 'test-topic', partitionMetadata, message: { value: null, key: 'a-key' } });

    expect(partition).toBeGreaterThanOrEqual(0);
    expect(partition).toBeLessThan(3);
    // Same key, same partition, every time.
    expect(partitioner({ topic: 'test-topic', partitionMetadata, message: { value: null, key: 'a-key' } })).toBe(
      partition,
    );
  });
});
