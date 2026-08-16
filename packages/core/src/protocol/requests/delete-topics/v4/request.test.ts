import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { deleteTopicsRequestV1 } from '../v1/request';
import { deleteTopicsRequestV4, requestSchema } from './request';

const payload = {
  topics: ['orders', 'payments'],
  timeout: 5000,
};

describe('protocol/requests/delete-topics/v4/request', () => {
  it('encodes compact strings/arrays and a trailing TAG_BUFFER', async () => {
    const definition = deleteTopicsRequestV4(payload);
    expect(definition.apiVersion).toBe(4);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeUVarIntString('orders')
      .writeUVarIntString('payments')
      .writeInt32(5000)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const v4 = await deleteTopicsRequestV4(payload).encode();
    const v1 = await deleteTopicsRequestV1(payload).encode();
    expect(v4.buffer).not.toEqual(v1.buffer);
  });
});
