import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { offsetDeleteRequestV0, requestSchema } from './request';

describe('protocol/requests/offset-delete/v0/request', () => {
  it('round-trips a v0 request', async () => {
    const value = {
      groupId: 'my-group',
      topics: [
        { name: 'orders', partitions: [{ partitionIndex: 0 }, { partitionIndex: 1 }] },
        { name: 'payments', partitions: [{ partitionIndex: 0 }] },
      ],
    };

    const encoder = await offsetDeleteRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
