import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { alterReplicaLogDirsRequestV2, requestSchema } from './request';

describe('protocol/requests/alter-replica-log-dirs/v2/request', () => {
  it('round-trips a flexible v2 request', async () => {
    const value = {
      dirs: [
        {
          path: '/var/kafka/data-1',
          topics: [
            { topic: 'orders', partitions: [0, 1] },
            { topic: 'payments', partitions: [0] },
          ],
        },
      ],
    };

    const encoder = await alterReplicaLogDirsRequestV2(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
