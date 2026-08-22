import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeQuorumResponseV1, responseSchema } from './response';

describe('protocol/requests/describe-quorum/v1/response', () => {
  it('round-trips a flexible v1 response with replica timestamps', async () => {
    const value = {
      errorCode: 0,
      topics: [
        {
          topicName: '__cluster_metadata',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              leaderId: 1,
              leaderEpoch: 2,
              highWatermark: 42n,
              currentVoters: [
                {
                  replicaId: 1,
                  logEndOffset: 42n,
                  lastFetchTimestamp: -1n,
                  lastCaughtUpTimestamp: 100n,
                },
              ],
              observers: [],
            },
          ],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeQuorumResponseV1.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(describeQuorumResponseV1.parse(data)).resolves.toEqual(data);
  });
});
