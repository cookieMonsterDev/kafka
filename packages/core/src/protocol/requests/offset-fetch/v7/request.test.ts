import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { offsetFetchRequestV6 } from '../v6/request';
import { offsetFetchRequestV7, requestSchema } from './request';

const payload = {
  groupId: 'g1',
  topics: [{ topic: 'orders', partitions: [{ partition: 0 }] }],
  requireStable: true,
};

describe('protocol/requests/offset-fetch/v7/request', () => {
  it('encodes requireStable after topics', async () => {
    const definition = offsetFetchRequestV7(payload);
    expect(definition.apiVersion).toBe(7);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('g1')
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeUVarInt(0)
      .writeBoolean(true)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes requireStable false', async () => {
    const encoder = await offsetFetchRequestV7({ ...payload, requireStable: false }).encode();
    const expected = new Encoder()
      .writeUVarIntString('g1')
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeUVarInt(0)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('collapses an empty topics array to compact null ("all topics")', async () => {
    const encoder = await offsetFetchRequestV7({ groupId: 'g', topics: [], requireStable: false }).encode();
    const expected = new Encoder().writeUVarIntString('g').writeUVarInt(0).writeBoolean(false).writeUVarInt(0);
    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('is not the v6 encoding', async () => {
    const v7 = await offsetFetchRequestV7(payload).encode();
    const v6 = await offsetFetchRequestV6({ groupId: payload.groupId, topics: payload.topics }).encode();
    expect(v7.buffer).not.toEqual(v6.buffer);
  });
});
