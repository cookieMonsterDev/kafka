import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { offsetDeleteResponseV1, responseSchema } from './response';

describe('protocol/requests/offset-delete/v1/response', () => {
  it('round-trips a flexible v1 response', async () => {
    const value = {
      errorCode: 0,
      throttleTime: 0,
      topics: [{ name: 'orders', partitions: [{ partitionIndex: 0, errorCode: 0 }] }],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await offsetDeleteResponseV1.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(offsetDeleteResponseV1.parse(data)).resolves.toEqual(data);
  });
});
