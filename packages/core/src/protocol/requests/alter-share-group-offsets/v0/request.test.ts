import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { alterShareGroupOffsetsRequestV0, requestSchema } from './request';

describe('protocol/requests/alter-share-group-offsets/v0/request', () => {
  it('round-trips a flexible v0 request', async () => {
    const value = {
      groupId: 'my-group',
      topics: [
        {
          topicName: 'events',
          partitions: [{ partitionIndex: 0, startOffset: 42n }],
        },
      ],
    };

    const encoder = await alterShareGroupOffsetsRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
