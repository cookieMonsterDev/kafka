import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { offsetFetchRequestV4 } from '../v4/request';
import { offsetFetchRequestV5, requestSchema } from './request';

const payload = {
  groupId: 'g1',
  topics: [{ topic: 'orders', partitions: [{ partition: 0 }] }],
};

describe('protocol/requests/offset-fetch/v5/request', () => {
  it('encodes identically to v4, wire-for-wire', async () => {
    const definition = offsetFetchRequestV5(payload);
    expect(definition.apiVersion).toBe(5);

    const encoder = await definition.encode();
    const expected = new Encoder().writeString('g1').writeInt32(1).writeString('orders').writeInt32(1).writeInt32(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);

    const v4 = await offsetFetchRequestV4(payload).encode();
    expect(encoder.buffer).toEqual(v4.buffer);
  });

  it('collapses an empty topics array to wire length -1 ("all topics")', async () => {
    const encoder = await offsetFetchRequestV5({ groupId: 'g', topics: [] }).encode();
    const expected = new Encoder().writeString('g').writeInt32(-1);
    expect(encoder.buffer).toEqual(expected.buffer);
  });
});
