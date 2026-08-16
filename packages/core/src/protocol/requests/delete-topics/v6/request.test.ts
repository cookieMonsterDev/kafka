import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { deleteTopicsRequestV4 } from '../v4/request';
import { deleteTopicsRequestV6, requestSchema } from './request';

const ZERO_UUID = Buffer.alloc(16);
const payload = {
  topics: ['orders', 'payments'],
  timeout: 5000,
};

describe('protocol/requests/delete-topics/v6/request', () => {
  it('encodes name-only deletes as { name, zero topicId } structs', async () => {
    const definition = deleteTopicsRequestV6(payload);
    expect(definition.apiVersion).toBe(6);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeUVarIntString('orders')
      .writeBuffer(ZERO_UUID)
      .writeUVarInt(0)
      .writeUVarIntString('payments')
      .writeBuffer(ZERO_UUID)
      .writeUVarInt(0)
      .writeInt32(5000)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({
      topics: [
        { name: 'orders', topicId: ZERO_UUID },
        { name: 'payments', topicId: ZERO_UUID },
      ],
      timeout: 5000,
    });
  });

  it('is not the v4 compact-name-array encoding', async () => {
    const v6 = await deleteTopicsRequestV6(payload).encode();
    const v4 = await deleteTopicsRequestV4(payload).encode();
    expect(v6.buffer).not.toEqual(v4.buffer);
  });
});
