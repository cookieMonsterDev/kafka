import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { deleteShareGroupOffsetsRequestV0, requestSchema } from './request';

describe('protocol/requests/delete-share-group-offsets/v0/request', () => {
  it('round-trips a flexible v0 request', async () => {
    const value = {
      groupId: 'my-group',
      topics: [{ topicName: 'events' }, { topicName: 'orders' }],
    };

    const encoder = await deleteShareGroupOffsetsRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
