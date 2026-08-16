import { describe, expect, it } from 'vitest';
import { createTopicData } from './create-topic-data.js';

describe('producer/createTopicData', () => {
  it('formats data by topic and partition', () => {
    const messagesPerPartition = new Map([
      [1, [{ value: 'v1', key: '1' }]],
      [2, [{ value: 'v2', key: '2' }]],
      [3, [{ value: 'v3', key: '3' }, { value: 'v4', key: '4' }]],
    ]);

    const result = createTopicData([{ topic: 'test-topic', partitions: [1, 2, 3], messagesPerPartition }]);

    expect(result).toEqual([
      {
        topic: 'test-topic',
        partitions: [
          { partition: 1, messages: [{ key: '1', value: 'v1', timestamp: undefined, headers: undefined }] },
          { partition: 2, messages: [{ key: '2', value: 'v2', timestamp: undefined, headers: undefined }] },
          {
            partition: 3,
            messages: [
              { key: '3', value: 'v3', timestamp: undefined, headers: undefined },
              { key: '4', value: 'v4', timestamp: undefined, headers: undefined },
            ],
          },
        ],
      },
    ]);
  });

  it('produces an empty messages array for a partition missing from the map', () => {
    const result = createTopicData([{ topic: 'test-topic', partitions: [0], messagesPerPartition: new Map() }]);
    expect(result).toEqual([{ topic: 'test-topic', partitions: [{ partition: 0, messages: [] }] }]);
  });
});
