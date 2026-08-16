import { describe, expect, it } from 'vitest';
import { responseSerializer } from './response-serializer.js';

describe('producer/responseSerializer', () => {
  it('flattens topics/partitions into one record per partition', () => {
    const response = {
      throttleTime: 0,
      clientSideThrottleTime: 0,
      topics: [
        {
          topicName: 'topic-a',
          partitions: [
            { partition: 0, errorCode: 0, baseOffset: 0n, logAppendTime: -1n, logStartOffset: 0n },
            { partition: 1, errorCode: 0, baseOffset: 5n, logAppendTime: -1n, logStartOffset: 0n },
          ],
        },
        {
          topicName: 'topic-b',
          partitions: [{ partition: 0, errorCode: 0, baseOffset: 10n, logAppendTime: -1n, logStartOffset: 0n }],
        },
      ],
    };

    expect(responseSerializer(response)).toEqual([
      { topicName: 'topic-a', partition: 0, errorCode: 0, baseOffset: 0n, logAppendTime: -1n, logStartOffset: 0n },
      { topicName: 'topic-a', partition: 1, errorCode: 0, baseOffset: 5n, logAppendTime: -1n, logStartOffset: 0n },
      { topicName: 'topic-b', partition: 0, errorCode: 0, baseOffset: 10n, logAppendTime: -1n, logStartOffset: 0n },
    ]);
  });
});
