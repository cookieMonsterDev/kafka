import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeClusterRequestV1, requestSchema } from './request';

describe('protocol/requests/describe-cluster/v1/request', () => {
  it('round-trips a flexible v1 request', async () => {
    const value = { includeClusterAuthorizedOperations: false, endpointType: 2 };

    const encoder = await describeClusterRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
