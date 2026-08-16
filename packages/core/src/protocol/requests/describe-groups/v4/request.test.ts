import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeGroupsRequestV3, requestSchema } from '../v3/request';
import { describeGroupsRequestV4 } from './request';

const payload = { groupIds: ['g1'], includeAuthorizedOperations: false };

describe('protocol/requests/describe-groups/v4/request', () => {
  it('round-trips the same wire as v3 with apiVersion 4', async () => {
    const definition = describeGroupsRequestV4(payload);
    expect(definition.apiVersion).toBe(4);

    const encoder = await definition.encode();
    const v3 = await describeGroupsRequestV3(payload).encode();
    expect(encoder.buffer).toEqual(v3.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
