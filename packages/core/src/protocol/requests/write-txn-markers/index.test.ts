import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { WriteTxnMarkers } from './index';
import { requestSchema as requestSchemaV1 } from './v1/request';
import { requestSchema as requestSchemaV2 } from './v2/request';

describe('protocol/requests/write-txn-markers', () => {
  it('implements versions 1 and 2', () => {
    expect(WriteTxnMarkers.versions).toEqual([1, 2]);
  });

  it('creates a version 1 request without transactionVersion', async () => {
    const options = {
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
    const { request } = WriteTxnMarkers.protocol({ version: 1 })(options);
    expect(request).toMatchObject({ apiKey: 27, apiVersion: 1, apiName: 'WriteTxnMarkers' });

    const encoder = await request.encode();
    expect(requestSchemaV1.read(new Decoder(encoder.buffer))).toEqual({
      markers: [
        {
          producerId: 7n,
          producerEpoch: 0,
          transactionResult: false,
          coordinatorEpoch: 12,
          topics: [{ topic: 'orders', partitions: [0] }],
        },
      ],
    });
  });

  it('creates a version 2 request with transactionVersion', async () => {
    const options = {
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
    const { request } = WriteTxnMarkers.protocol({ version: 2 })(options);
    expect(request).toMatchObject({ apiKey: 27, apiVersion: 2, apiName: 'WriteTxnMarkers' });

    const encoder = await request.encode();
    expect(requestSchemaV2.read(new Decoder(encoder.buffer))).toEqual({
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
    });
  });
});
