import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { endTxnRequestV2 } from '../v2/request';
import { endTxnRequestV3, requestSchema } from './request';

const payload = {
  transactionalId: 'txn-1',
  producerId: 1001n,
  producerEpoch: 0,
  transactionResult: true,
};

describe('protocol/requests/end-txn/v3/request', () => {
  it('encodes compact strings and a trailing TAG_BUFFER', async () => {
    const definition = endTxnRequestV3(payload);
    expect(definition.apiVersion).toBe(3);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('txn-1')
      .writeInt64(1001n)
      .writeInt16(0)
      .writeBoolean(true)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v2 encoding', async () => {
    const v3 = await endTxnRequestV3(payload).encode();
    const v2 = await endTxnRequestV2(payload).encode();
    expect(v3.buffer).not.toEqual(v2.buffer);
  });
});
