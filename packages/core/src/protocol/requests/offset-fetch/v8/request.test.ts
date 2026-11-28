import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { offsetFetchRequestV8, requestSchema } from './request';

const payload = {
  groups: [
    { groupId: 'g1', topics: [{ topic: 'orders', partitions: [{ partition: 0 }] }] },
    { groupId: 'g2', topics: [] },
  ],
  requireStable: true,
};

describe('protocol/requests/offset-fetch/v8/request', () => {
  it('encodes several groups, writing an empty topics list as compact null', async () => {
    const definition = offsetFetchRequestV8(payload);
    expect(definition.apiVersion).toBe(8);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeUVarIntString('g1')
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarIntString('g2')
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeBoolean(true)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
