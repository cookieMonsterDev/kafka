import { describe, expect, it } from 'vitest';
import type { PartitionMetadata } from '../cluster/index.js';
import { groupMessagesPerPartition } from './group-messages-per-partition.js';
import type { Message } from './types.js';

function fakePartitionMetadata(partitionId: number): PartitionMetadata {
  return { partitionErrorCode: 0, partitionId, leader: 0, replicas: [0], isr: [0], offlineReplicas: [] };
}

describe('producer/groupMessagesPerPartition', () => {
  it('returns an empty map when there is no partition metadata', () => {
    const result = groupMessagesPerPartition({
      topic: 'topic',
      partitionMetadata: [],
      messages: [{ value: 'x' }],
      partitioner: () => 0,
    });

    expect(result.size).toBe(0);
  });

  it('groups messages by the partition the partitioner assigns', () => {
    const messages: Message[] = [{ value: 'a' }, { value: 'b' }, { value: 'c' }];
    const partitionMetadata = [fakePartitionMetadata(0), fakePartitionMetadata(1)];
    let call = 0;
    const partitioner = () => {
      const partition = call % 2;
      call++;
      return partition;
    };

    const result = groupMessagesPerPartition({ topic: 'topic', partitionMetadata, messages, partitioner });

    expect(result.get(0)).toEqual([messages[0], messages[2]]);
    expect(result.get(1)).toEqual([messages[1]]);
  });
});
