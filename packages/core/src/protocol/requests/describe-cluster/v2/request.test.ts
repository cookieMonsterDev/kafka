import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeClusterRequestV2, requestSchema } from './request';

describe('protocol/requests/describe-cluster/v2/request', () => {
  it('round-trips a flexible v2 request', async () => {
    const value = {
      includeClusterAuthorizedOperations: true,
      endpointType: 1,
      includeFencedBrokers: true,
    };

    const encoder = await describeClusterRequestV2(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
