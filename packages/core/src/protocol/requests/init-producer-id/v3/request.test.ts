import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { initProducerIdRequestV3 } from './request';

describe('protocol/requests/init-producer-id/v3/request', () => {
  it('encodes a compact nullable transactional_id and a trailing TAG_BUFFER', async () => {
    const producerId = 1006n;
    const encoder = await initProducerIdRequestV3({
      transactionalId: 'txn-id',
      transactionTimeout: 30_000,
      producerId,
      producerEpoch: 3,
    }).encode();

    const expected = new Encoder()
      .writeUVarIntString('txn-id')
      .writeInt32(30_000)
      .writeInt64(producerId)
      .writeInt16(3)
      .writeUVarInt(0);
    expect(encoder.buffer).toEqual(expected.buffer);

    const decoder = new Decoder(encoder.buffer);
    expect(decoder.readUVarIntString()).toBe('txn-id');
    expect(decoder.readInt32()).toBe(30_000);
    expect(decoder.readInt64()).toBe(producerId);
    expect(decoder.readInt16()).toBe(3);
    expect(decoder.readTaggedFields()).toBeNull();
  });

  it('encodes a null transactional_id as compact null', async () => {
    const encoder = await initProducerIdRequestV3({
      transactionalId: null,
      transactionTimeout: 30_000,
      producerId: -1n,
      producerEpoch: -1,
    }).encode();

    const expected = new Encoder()
      .writeUVarIntString(null)
      .writeInt32(30_000)
      .writeInt64(-1n)
      .writeInt16(-1)
      .writeUVarInt(0);
    expect(encoder.buffer).toEqual(expected.buffer);
  });
});
