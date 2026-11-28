import { describe, expect, it } from 'vitest';
import { describeGroupsRequestV5 } from '../v5/request';
import { describeGroupsRequestV6 } from './request';

const payload = { groupIds: ['g1', 'g2'], includeAuthorizedOperations: true };

describe('protocol/requests/describe-groups/v6/request', () => {
  it('encodes the same body as v5 under api version 6', async () => {
    const definition = describeGroupsRequestV6(payload);
    expect(definition.apiVersion).toBe(6);

    const v6 = await definition.encode();
    const v5 = await describeGroupsRequestV5(payload).encode();
    expect(v6.buffer).toEqual(v5.buffer);
  });
});
