import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { offsetDeleteResponseV0, responseSchema } from './response';

describe('protocol/requests/offset-delete/v0/response', () => {
  it('round-trips a successful v0 response', async () => {
    const value = {
      errorCode: 0,
      throttleTime: 0,
      topics: [
        {
          name: 'orders',
          partitions: [{ partitionIndex: 0, errorCode: 0 }],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await offsetDeleteResponseV0.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(offsetDeleteResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws on a top-level error', async () => {
    const value = { errorCode: 69, throttleTime: 0, topics: [] };
    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await offsetDeleteResponseV0.decode(encoder.buffer);
    await expect(offsetDeleteResponseV0.parse(data)).rejects.toBeInstanceOf(KafkaProtocolError);
  });

  it('throws on a partition error', async () => {
    const value = {
      errorCode: 0,
      throttleTime: 0,
      topics: [{ name: 'orders', partitions: [{ partitionIndex: 0, errorCode: 3 }] }],
    };
    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await offsetDeleteResponseV0.decode(encoder.buffer);
    await expect(offsetDeleteResponseV0.parse(data)).rejects.toBeInstanceOf(KafkaProtocolError);
  });
});
