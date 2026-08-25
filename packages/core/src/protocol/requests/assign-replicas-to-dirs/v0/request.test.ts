import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { assignReplicasToDirsRequestV0, requestSchema } from './request';

describe('protocol/requests/assign-replicas-to-dirs/v0/request', () => {
  it('round-trips a flexible v0 request', async () => {
    const value = {
      brokerId: 2,
      brokerEpoch: 7n,
      directories: [
        {
          id: Buffer.alloc(16, 1),
          topics: [
            {
              topicId: Buffer.alloc(16, 2),
              partitions: [{ partitionIndex: 0 }, { partitionIndex: 1 }],
            },
          ],
        },
      ],
    };

    const encoder = await assignReplicasToDirsRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
