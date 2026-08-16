import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { offsetFetchRequestV5 } from '../v5/request';
import { offsetFetchRequestV6, requestSchema } from './request';

const payload = {
  groupId: 'g1',
  topics: [{ topic: 'orders', partitions: [{ partition: 0 }] }],
};

describe('protocol/requests/offset-fetch/v6/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = offsetFetchRequestV6(payload);
    expect(definition.apiVersion).toBe(6);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('g1')
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('collapses an empty topics array to compact null ("all topics")', async () => {
    const encoder = await offsetFetchRequestV6({ groupId: 'g', topics: [] }).encode();
    const expected = new Encoder().writeUVarIntString('g').writeUVarInt(0).writeUVarInt(0);
    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({ groupId: 'g', topics: [] });
  });

  it('is not the non-flexible v5 encoding', async () => {
    const v6 = await offsetFetchRequestV6(payload).encode();
    const v5 = await offsetFetchRequestV5(payload).encode();
    expect(v6.buffer).not.toEqual(v5.buffer);
  });
});
