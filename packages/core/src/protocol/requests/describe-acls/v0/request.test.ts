import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { describeAclsRequestV0 } from './request';

describe('protocol/requests/describe-acls/v0/request', () => {
  it('encodes a real fixture without resourcePatternType', async () => {
    const encoder = await describeAclsRequestV0({
      resourceType: 2,
      resourceName: 'test-topic-064a1bcf62c877843e3c-18742-da400056-f741-4b3e-a725-b758d8104afa',
      principal: null,
      host: '*',
      operation: 2,
      permissionType: 3,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
