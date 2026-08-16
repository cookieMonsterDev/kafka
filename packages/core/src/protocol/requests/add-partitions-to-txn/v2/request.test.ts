import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { addPartitionsToTxnRequestV1 } from '../v1/request';
import { addPartitionsToTxnRequestV2 } from './request';

const payload = {
  transactionalId: 'txn-1',
  producerId: 1001n,
  producerEpoch: 0,
  topics: [{ topic: 'orders', partitions: [0, 1] }],
};

describe('protocol/requests/add-partitions-to-txn/v2/request', () => {
  it('round-trips the same wire as v1 with apiVersion 2', async () => {
    const definition = addPartitionsToTxnRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const v1 = await addPartitionsToTxnRequestV1(payload).encode();
    expect(encoder.buffer).toEqual(v1.buffer);

    const decoder = new Decoder(encoder.buffer);
    expect(decoder.readString()).toBe('txn-1');
    expect(decoder.readInt64()).toBe(1001n);
    expect(decoder.readInt16()).toBe(0);
  });
});
