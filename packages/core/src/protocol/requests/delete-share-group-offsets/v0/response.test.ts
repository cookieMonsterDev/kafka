import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { deleteShareGroupOffsetsResponseV0, responseSchema } from './response';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/delete-share-group-offsets/v0/response', () => {
  it('decodes a successful response with KIP-219 throttle handling', async () => {
    const value = {
      throttleTime: 4,
      errorCode: 0,
      errorMessage: null,
      responses: [{ topicName: 'events', topicId, errorCode: 0, errorMessage: null }],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await deleteShareGroupOffsetsResponseV0.decode(encoder.buffer);
    expect(data).toEqual({ ...value, throttleTime: 0, clientSideThrottleTime: 4 });
    await expect(deleteShareGroupOffsetsResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('rejects a topic-level error', async () => {
    const value = {
      throttleTime: 0,
      errorCode: 0,
      errorMessage: null,
      responses: [{ topicName: 'events', topicId, errorCode: 3, errorMessage: null }],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await deleteShareGroupOffsetsResponseV0.decode(encoder.buffer);
    await expect(deleteShareGroupOffsetsResponseV0.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    });
  });
});
