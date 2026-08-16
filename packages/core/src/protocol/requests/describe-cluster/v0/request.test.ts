import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeClusterRequestV0, requestSchema } from './request';

describe('protocol/requests/describe-cluster/v0/request', () => {
  it('round-trips a flexible v0 request', async () => {
    const value = { includeClusterAuthorizedOperations: true };

    const encoder = await describeClusterRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
