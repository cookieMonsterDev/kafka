import { describe, expect, it } from 'vitest';
import { deleteAclsRequestV2 } from '../v2/request';
import { deleteAclsRequestV3 } from './request';

const payload = {
  filters: [
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

describe('protocol/requests/delete-acls/v3/request', () => {
  it('uses api version 3 with the same compact body as v2', async () => {
    const request = deleteAclsRequestV3(payload);
    expect(request.apiVersion).toBe(3);

    const v3 = await request.encode();
    const v2 = await deleteAclsRequestV2(payload).encode();
    expect(v3.buffer).toEqual(v2.buffer);
  });
});
