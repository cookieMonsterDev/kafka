import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { alterShareGroupOffsetsResponseV0, responseSchema } from './response';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/alter-share-group-offsets/v0/response', () => {
  it('decodes a successful response with KIP-219 throttle handling', async () => {
    const value = {
      throttleTime: 8,
      errorCode: 0,
      errorMessage: null,
      responses: [
        {
          topicName: 'events',
          topicId,
          partitions: [{ partitionIndex: 0, errorCode: 0, errorMessage: null }],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await alterShareGroupOffsetsResponseV0.decode(encoder.buffer);
    expect(data).toEqual({ ...value, throttleTime: 0, clientSideThrottleTime: 8 });
    await expect(alterShareGroupOffsetsResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('rejects a partition-level error', async () => {
    const value = {
      throttleTime: 0,
      errorCode: 0,
      errorMessage: null,
      responses: [
        {
          topicName: 'events',
          topicId,
          partitions: [{ partitionIndex: 0, errorCode: 3, errorMessage: null }],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await alterShareGroupOffsetsResponseV0.decode(encoder.buffer);
    await expect(alterShareGroupOffsetsResponseV0.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    });
  });
});
