import { describe, expect, it } from 'vitest';
import { describeAclsRequestV2 } from '../v2/request';
import { describeAclsRequestV3 } from './request';

const payload = {
  resourceType: 2,
  resourceName: 'orders',
  resourcePatternType: 3,
  principal: null,
  host: '*',
  operation: 2,
  permissionType: 3,
};

describe('protocol/requests/describe-acls/v3/request', () => {
  it('uses api version 3 with the same compact body as v2', async () => {
    const request = describeAclsRequestV3(payload);
    expect(request.apiVersion).toBe(3);

    const v3 = await request.encode();
    const v2 = await describeAclsRequestV2(payload).encode();
    expect(v3.buffer).toEqual(v2.buffer);
  });
});
