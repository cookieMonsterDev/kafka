import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeShareGroupOffsetsResponseV0, responseSchema } from './response';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/describe-share-group-offsets/v0/response', () => {
  it('decodes offsets without lag', async () => {
    const value = {
      throttleTime: 5,
      groups: [
        {
          groupId: 'g',
          topics: [
            {
              topicName: 'events',
              topicId,
              partitions: [
                {
                  partitionIndex: 0,
                  startOffset: 42n,
                  leaderEpoch: 3,
                  errorCode: 0,
                  errorMessage: null,
                },
              ],
            },
          ],
          errorCode: 0,
          errorMessage: null,
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeShareGroupOffsetsResponseV0.decode(encoder.buffer);
    expect(data).toEqual({ ...value, throttleTime: 0, clientSideThrottleTime: 5 });
    await expect(describeShareGroupOffsetsResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('rejects a partition-level error', async () => {
    const value = {
      throttleTime: 0,
      groups: [
        {
          groupId: 'g',
          topics: [
            {
              topicName: 'events',
              topicId,
              partitions: [{ partitionIndex: 0, startOffset: 0n, leaderEpoch: 0, errorCode: 3, errorMessage: null }],
            },
          ],
          errorCode: 0,
          errorMessage: null,
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeShareGroupOffsetsResponseV0.decode(encoder.buffer);
    await expect(describeShareGroupOffsetsResponseV0.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    });
  });
});
