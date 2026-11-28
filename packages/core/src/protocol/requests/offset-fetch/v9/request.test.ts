import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { offsetFetchRequestV9, requestSchema } from './request';

const payload = {
  groups: [
    { groupId: 'g1', memberId: 'm1', memberEpoch: 7, topics: [{ topic: 'orders', partitions: [{ partition: 0 }] }] },
  ],
  requireStable: false,
};

describe('protocol/requests/offset-fetch/v9/request', () => {
  it('encodes each group member id and epoch before its topics', async () => {
    const definition = offsetFetchRequestV9(payload);
    expect(definition.apiVersion).toBe(9);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('g1')
      .writeUVarIntString('m1')
      .writeInt32(7)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes a null member id', async () => {
    const encoder = await offsetFetchRequestV9({
      groups: [{ groupId: 'g', memberId: null, memberEpoch: -1, topics: [] }],
      requireStable: false,
    }).encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('g')
      .writeUVarIntString(null)
      .writeInt32(-1)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
  });
});
