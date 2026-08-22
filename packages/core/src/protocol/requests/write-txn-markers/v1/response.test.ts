import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { throwOnWriteTxnMarkersPartitionErrors, writeTxnMarkersResponseV1, responseSchema } from './response';

describe('protocol/requests/write-txn-markers/v1/response', () => {
  it('decodes and parses a successful marker response', async () => {
    const value = {
      markers: [
        {
          producerId: 7n,
          topics: [{ topic: 'orders', partitions: [{ partition: 0, errorCode: 0 }] }],
        },
      ],
    };
    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await writeTxnMarkersResponseV1.decode(encoder.buffer);

    expect(data).toEqual(value);
    await expect(writeTxnMarkersResponseV1.parse(data)).resolves.toEqual(value);
  });

  it('throws on partition errors', async () => {
    const value = {
      markers: [
        {
          producerId: 7n,
          topics: [{ topic: 'orders', partitions: [{ partition: 0, errorCode: 6 }] }],
        },
      ],
    };

    expect(() => throwOnWriteTxnMarkersPartitionErrors(value.markers)).toThrow();
  });
});
