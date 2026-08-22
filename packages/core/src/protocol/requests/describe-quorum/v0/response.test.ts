import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeQuorumResponseV0, responseSchema } from './response';

describe('protocol/requests/describe-quorum/v0/response', () => {
  it('round-trips a flexible v0 response', async () => {
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
              currentVoters: [{ replicaId: 1, logEndOffset: 42n }],
              observers: [{ replicaId: 2, logEndOffset: 40n }],
            },
          ],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeQuorumResponseV0.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(describeQuorumResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws on a top-level error code', async () => {
    await expect(
      describeQuorumResponseV0.parse({
        errorCode: 31,
        topics: [],
      }),
    ).rejects.toMatchObject({ type: 'CLUSTER_AUTHORIZATION_FAILED' });
  });
});
