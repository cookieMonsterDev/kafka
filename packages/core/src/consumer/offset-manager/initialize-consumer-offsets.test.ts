import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../errors';
import { initializeConsumerOffsets } from './initialize-consumer-offsets';

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

  it('throws when reset is none and the committed offset is invalid', () => {
    const consumerOffsets = [
      {
        topic: 'events',
        partitions: [{ partition: 2, offset: -1n }],
      },
    ];

    expect(() => initializeConsumerOffsets(consumerOffsets, [], { events: 'none' })).toThrow(
      new KafkaNonRetriableError('Offset reset policy is none; no committed offset for topic events partition 2'),
    );
  });

  it('keeps a valid committed offset when reset is none', () => {
    const consumerOffsets = [
      {
        topic: 'events',
        partitions: [
          { partition: 0, offset: 14n },
          { partition: 1, offset: 0n },
        ],
      },
    ];

    expect(initializeConsumerOffsets(consumerOffsets, [], { events: 'none' })).toEqual([
      {
        topic: 'events',
        partitions: [
          { partition: 0, offset: 14n },
          { partition: 1, offset: 0n },
        ],
      },
    ]);
  });

  it('still substitutes invalid offsets when reset is earliest', () => {
    const consumerOffsets = [
      {
        topic: 'events',
        partitions: [
          { partition: 0, offset: -1n },
          { partition: 1, offset: 9n },
        ],
      },
    ];
    const topicOffsets = [
      {
        topic: 'events',
        partitions: [
          { partition: 0, offset: 5n },
          { partition: 1, offset: 100n },
        ],
      },
    ];

    expect(initializeConsumerOffsets(consumerOffsets, topicOffsets, { events: 'earliest' })).toEqual([
      {
        topic: 'events',
        partitions: [
          { partition: 0, offset: 5n },
          { partition: 1, offset: 9n },
        ],
      },
    ]);
  });
});
