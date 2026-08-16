import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { electLeadersRequestV1, requestSchema } from './request';

describe('protocol/requests/elect-leaders/v1/request', () => {
  it('round-trips a v1 request including electionType', async () => {
    const value = {
      electionType: 1,
      timeout: 5000,
      topicPartitions: [{ topic: 'orders', partitions: [0] }],
    };

    const encoder = await electLeadersRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
