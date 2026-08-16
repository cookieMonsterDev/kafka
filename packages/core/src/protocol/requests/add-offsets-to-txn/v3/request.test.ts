import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { addOffsetsToTxnRequestV2 } from '../v2/request';
import { addOffsetsToTxnRequestV3, requestSchema } from './request';

const payload = {
  transactionalId: 'txn-1',
  producerId: 1001n,
  producerEpoch: 0,
  groupId: 'g1',
};

describe('protocol/requests/add-offsets-to-txn/v3/request', () => {
  it('encodes compact strings and a trailing TAG_BUFFER', async () => {
    const definition = addOffsetsToTxnRequestV3(payload);
    expect(definition.apiVersion).toBe(3);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('txn-1')
      .writeInt64(1001n)
      .writeInt16(0)
      .writeUVarIntString('g1')
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v2 encoding', async () => {
    const v3 = await addOffsetsToTxnRequestV3(payload).encode();
    const v2 = await addOffsetsToTxnRequestV2(payload).encode();
    expect(v3.buffer).not.toEqual(v2.buffer);
  });
});
