import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeShareGroupOffsetsRequestV1, requestSchema } from './request';

describe('protocol/requests/describe-share-group-offsets/v1/request', () => {
  it('round-trips a flexible v1 request', async () => {
    const value = {
      groups: [{ groupId: 'my-group', topics: [{ topicName: 'events', partitions: [0] }] }],
    };

    const encoder = await describeShareGroupOffsetsRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
