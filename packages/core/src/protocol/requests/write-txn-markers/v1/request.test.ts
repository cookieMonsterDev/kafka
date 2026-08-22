import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { requestSchema, writeTxnMarkersRequestV1 } from './request';

describe('protocol/requests/write-txn-markers/v1/request', () => {
  it('round-trips an abort marker', async () => {
    const value = {
      markers: [
        {
          producerId: 42n,
          producerEpoch: 1,
          transactionResult: false,
          topics: [{ topic: 'orders', partitions: [0] }],
          coordinatorEpoch: 0,
        },
      ],
    };

    const encoder = await writeTxnMarkersRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
