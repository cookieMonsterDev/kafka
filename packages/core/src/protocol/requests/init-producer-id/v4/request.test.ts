import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { initProducerIdRequestV4 } from './request';

describe('protocol/requests/init-producer-id/v4/request', () => {
  it('uses api version 4 with the same compact body as v3', async () => {
    const request = initProducerIdRequestV4({
      transactionalId: 'txn-id',
      transactionTimeout: 30_000,
      producerId: 1006n,
      producerEpoch: 3,
    });
    expect(request.apiVersion).toBe(4);

    const encoder = await request.encode();
    const expected = new Encoder()
      .writeUVarIntString('txn-id')
      .writeInt32(30_000)
      .writeInt64(1006n)
      .writeInt16(3)
      .writeUVarInt(0);
    expect(encoder.buffer).toEqual(expected.buffer);
  });
});
