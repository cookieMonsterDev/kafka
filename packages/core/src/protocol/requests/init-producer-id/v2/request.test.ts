import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { initProducerIdRequestV2 } from './request';

describe('protocol/requests/init-producer-id/v2/request', () => {
  it('encodes producer_id as int64 and producer_epoch as int16', async () => {
    const producerId = 9007199254740993n;
    const encoder = await initProducerIdRequestV2({
      transactionalId: 'txn-id',
      transactionTimeout: 30_000,
      producerId,
      producerEpoch: 3,
    }).encode();

    const expected = new Encoder().writeString('txn-id').writeInt32(30_000).writeInt64(producerId).writeInt16(3);
    expect(encoder.buffer).toEqual(expected.buffer);

    const decoder = new Decoder(encoder.buffer);
    expect(decoder.readString()).toBe('txn-id');
    expect(decoder.readInt32()).toBe(30_000);
    expect(decoder.readInt64()).toBe(producerId);
    expect(decoder.readInt16()).toBe(3);
  });

  it('encodes the allocate-new sentinels for producer_id and producer_epoch', async () => {
    const encoder = await initProducerIdRequestV2({
      transactionalId: null,
      transactionTimeout: 30_000,
      producerId: -1n,
      producerEpoch: -1,
    }).encode();

    const expected = new Encoder().writeString(null).writeInt32(30_000).writeInt64(-1n).writeInt16(-1);
    expect(encoder.buffer).toEqual(expected.buffer);
  });
});
