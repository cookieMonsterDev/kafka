import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeShareGroupOffsetsRequestV0, requestSchema } from './request';

describe('protocol/requests/describe-share-group-offsets/v0/request', () => {
  it('round-trips a flexible v0 request', async () => {
    const value = {
      groups: [
        {
          groupId: 'my-group',
          topics: [{ topicName: 'events', partitions: [0, 1] }],
        },
      ],
    };

    const encoder = await describeShareGroupOffsetsRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });

  it('encodes null topics for all topic-partitions', async () => {
    const value = {
      groups: [{ groupId: 'my-group', topics: null }],
    };

    const encoder = await describeShareGroupOffsetsRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
