import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { electLeadersRequestV2, requestSchema } from './request';

describe('protocol/requests/elect-leaders/v2/request', () => {
  it('round-trips a flexible v2 request, preserving a null topicPartitions list', async () => {
    const value = { electionType: 0, timeout: 5000, topicPartitions: null };

    const encoder = await electLeadersRequestV2(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
