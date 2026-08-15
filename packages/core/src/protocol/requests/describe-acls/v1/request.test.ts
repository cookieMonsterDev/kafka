import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { describeAclsRequestV1 } from './request.js';

describe('protocol/requests/describe-acls/v1/request', () => {
  it('encodes a real fixture', async () => {
    const encoder = await describeAclsRequestV1({
      resourceType: 2,
      resourceName: 'test-topic-3091e37cb34e1e916cfa-18029-1b277b41-4f40-4740-9274-51f556f212c9',
      resourcePatternType: 3,
      principal: null,
      host: '*',
      operation: 2,
      permissionType: 3,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
