import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { deleteRecordsRequestV1 } from '../v1/request';
import { deleteRecordsRequestV2, requestSchema } from './request';

const payload = {
  topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 7n }] }],
  timeout: 5000,
};

describe('protocol/requests/delete-records/v2/request', () => {
  it('encodes compact topics/partitions and a TAG_BUFFER on every struct', async () => {
    const definition = deleteRecordsRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt64(7n)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeInt32(5000)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const v2 = await deleteRecordsRequestV2(payload).encode();
    const v1 = await deleteRecordsRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
