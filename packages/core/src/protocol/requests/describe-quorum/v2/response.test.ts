import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeQuorumResponseV2, responseSchema } from './response';

const DIRECTORY_ID = Buffer.alloc(16, 7);

describe('protocol/requests/describe-quorum/v2/response', () => {
  it('round-trips a flexible v2 response with nodes and directory ids', async () => {
    const value = {
      errorCode: 0,
      errorMessage: null as string | null,
      topics: [
        {
          topicName: '__cluster_metadata',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null as string | null,
              leaderId: 1,
              leaderEpoch: 2,
              highWatermark: 42n,
              currentVoters: [
                {
                  replicaId: 1,
                  replicaDirectoryId: DIRECTORY_ID,
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
      nodes: [
        {
          nodeId: 1,
          listeners: [{ name: 'CONTROLLER', host: 'localhost', port: 9093 }],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeQuorumResponseV2.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(describeQuorumResponseV2.parse(data)).resolves.toEqual(data);
  });

  it('throws on a top-level error code', async () => {
    await expect(
      describeQuorumResponseV2.parse({
        errorCode: 31,
        errorMessage: 'denied',
        topics: [],
        nodes: [],
      }),
    ).rejects.toMatchObject({ type: 'CLUSTER_AUTHORIZATION_FAILED' });
  });
});
