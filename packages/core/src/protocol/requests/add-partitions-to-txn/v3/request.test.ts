import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { addPartitionsToTxnRequestV2 } from '../v2/request';
import { addPartitionsToTxnRequestV3, requestSchema } from './request';

const payload = {
  transactionalId: 'txn-1',
  producerId: 1001n,
  producerEpoch: 0,
  topics: [{ topic: 'orders', partitions: [0, 1] }],
};

describe('protocol/requests/add-partitions-to-txn/v3/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = addPartitionsToTxnRequestV3(payload);
    expect(definition.apiVersion).toBe(3);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('txn-1')
      .writeInt64(1001n)
      .writeInt16(0)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(3)
      .writeInt32(0)
      .writeInt32(1)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v2 encoding', async () => {
    const v3 = await addPartitionsToTxnRequestV3(payload).encode();
    const v2 = await addPartitionsToTxnRequestV2(payload).encode();
    expect(v3.buffer).not.toEqual(v2.buffer);
  });
});
