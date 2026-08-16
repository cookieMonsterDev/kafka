import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { offsetDeleteRequestV1, requestSchema } from './request';

describe('protocol/requests/offset-delete/v1/request', () => {
  it('round-trips a flexible v1 request', async () => {
    const value = {
      groupId: 'my-group',
      topics: [{ name: 'orders', partitions: [{ partitionIndex: 0 }] }],
    };

    const encoder = await offsetDeleteRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
