import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { electLeadersResponseV0, responseSchema } from './response';

describe('protocol/requests/elect-leaders/v0/response', () => {
  it('round-trips a successful v0 response and synthesizes errorCode 0', async () => {
    const wire = {
      throttleTime: 0,
      results: [
        {
          topic: 'orders',
          partitions: [{ partition: 0, errorCode: 0, errorMessage: null }],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, wire);
    const data = await electLeadersResponseV0.decode(encoder.buffer);
    expect(data).toEqual({ ...wire, errorCode: 0 });
    await expect(electLeadersResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws the first partition error from parse', async () => {
    const wire = {
      throttleTime: 0,
      results: [
        {
          topic: 'orders',
          partitions: [{ partition: 0, errorCode: 5, errorMessage: 'Not leader' }],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, wire);
    const data = await electLeadersResponseV0.decode(encoder.buffer);
    await expect(electLeadersResponseV0.parse(data)).rejects.toBeInstanceOf(KafkaProtocolError);
  });
});
