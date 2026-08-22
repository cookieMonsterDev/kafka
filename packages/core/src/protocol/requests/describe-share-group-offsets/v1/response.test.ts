import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeShareGroupOffsetsResponseV1, responseSchema } from './response';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/describe-share-group-offsets/v1/response', () => {
  it('decodes offsets with lag', async () => {
    const value = {
      throttleTime: 0,
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
                  lag: 10n,
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
    const data = await describeShareGroupOffsetsResponseV1.decode(encoder.buffer);
    expect(data).toEqual({ ...value, throttleTime: 0, clientSideThrottleTime: 0 });
    await expect(describeShareGroupOffsetsResponseV1.parse(data)).resolves.toEqual(data);
  });
});
