import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { writeTxnMarkersRequestV2, requestSchema } from './request';

describe('protocol/requests/write-txn-markers/v2/request', () => {
  it('encodes transactionVersion on each marker', async () => {
    const value = {
      markers: [
        {
          producerId: 7n,
          producerEpoch: 0,
          transactionResult: false,
          coordinatorEpoch: 12,
          transactionVersion: 2,
          topics: [{ topic: 'orders', partitions: [0] }],
        },
      ],
    };
    const encoder = await writeTxnMarkersRequestV2(value).encode();

    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
