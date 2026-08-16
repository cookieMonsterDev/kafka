import { describe, expect, it } from 'vitest';
import { createAclsRequestV2 } from '../v2/request';
import { createAclsRequestV3 } from './request';

const payload = {
  creations: [
    {
      resourceType: 2,
      resourceName: 'orders',
      resourcePatternType: 3,
      principal: 'User:alice',
      host: '*',
      operation: 2,
      permissionType: 3,
    },
  ],
};

describe('protocol/requests/create-acls/v3/request', () => {
  it('uses api version 3 with the same compact body as v2', async () => {
    const request = createAclsRequestV3(payload);
    expect(request.apiVersion).toBe(3);

    const v3 = await request.encode();
    const v2 = await createAclsRequestV2(payload).encode();
    expect(v3.buffer).toEqual(v2.buffer);
  });
});
