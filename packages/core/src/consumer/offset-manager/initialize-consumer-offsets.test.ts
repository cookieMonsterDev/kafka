import { describe, expect, it } from 'vitest';
import { initializeConsumerOffsets } from './initialize-consumer-offsets.js';

describe('consumer/offset-manager/initialize-consumer-offsets', () => {
  it('replaces consumer offsets assigned to -1 with topic offsets', () => {
    const consumerOffsets = [
      {
        topic: 'topic-name1',
        partitions: [
          { partition: 0, offset: -1n },
          { partition: 1, offset: -1n },
          { partition: 2, offset: 14n },
          { partition: 3, offset: -1n },
        ],
      },
      {
        topic: 'topic-name2',
        partitions: [
          { partition: 0, offset: -1n },
          { partition: 1, offset: 2n },
        ],
      },
    ];
    const topicOffsets = [
      {
        topic: 'topic-name1',
        partitions: [
          { partition: 0, offset: -1n },
          { partition: 1, offset: 3n },
          { partition: 2, offset: 16n },
          { partition: 3, offset: 8n },
        ],
      },
      {
        topic: 'topic-name2',
        partitions: [
          { partition: 0, offset: 1n },
          { partition: 1, offset: 2n },
        ],
      },
    ];

    expect(initializeConsumerOffsets(consumerOffsets, topicOffsets)).toEqual([
      {
        topic: 'topic-name1',
        partitions: [
          { partition: 0, offset: -1n },
          { partition: 1, offset: 3n },
          { partition: 2, offset: 14n },
          { partition: 3, offset: 8n },
        ],
      },
      {
        topic: 'topic-name2',
        partitions: [
          { partition: 0, offset: 1n },
          { partition: 1, offset: 2n },
        ],
      },
    ]);
  });
});
