import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { deleteAclsRequestV0 } from './request';

describe('protocol/requests/delete-acls/v0/request', () => {
  it('encodes a real fixture without resourcePatternType', async () => {
    const encoder = await deleteAclsRequestV0({
      filters: [
        {
          resourceName: 'test-topic-78d599e9d78a4da685ae-21381-e8f39f07-7d19-4677-aecb-bd0f731f1e28',
          resourceType: 2,
          principal: 'User:bob-cd8856cf4f23fe19899c-21381-c20b6340-b95c-431d-9237-2f15e310fba7',
          host: '*',
          permissionType: 3,
          operation: 1,
        },
      ],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
