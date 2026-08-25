import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../errors';
import { checkOffsetFetchPartitionErrors } from './shared';

describe('protocol/requests/offset-fetch/shared', () => {
  it('returns when every partition succeeded', () => {
    expect(() =>
      checkOffsetFetchPartitionErrors({
        responses: [{ partitions: [{ errorCode: 0 }, { errorCode: 0 }] }],
      }),
    ).not.toThrow();
  });

  it('throws the first partition-level protocol error', () => {
    expect(() =>
      checkOffsetFetchPartitionErrors({
        responses: [{ partitions: [{ errorCode: 0 }] }, { partitions: [{ errorCode: 16 }] }],
      }),
    ).toThrow(KafkaProtocolError);
    expect(() =>
      checkOffsetFetchPartitionErrors({
        responses: [{ partitions: [{ errorCode: 0 }] }, { partitions: [{ errorCode: 16 }] }],
      }),
    ).toThrow('This is not the correct coordinator for this group');
  });
});
