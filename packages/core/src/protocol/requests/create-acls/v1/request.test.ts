import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { createAclsRequestV1 } from './request';

describe('protocol/requests/create-acls/v1/request', () => {
  it('encodes a real fixture', async () => {
    const encoder = await createAclsRequestV1({
      creations: [
        {
          resourceType: 2,
          resourceName: 'test-topic-392850dd6d7a5d5b19ce-14472-4a2169c1-4784-4717-b2d1-9189bdfb8322',
          resourcePatternType: 3,
          principal: 'User:bob-4330407946585067d2b2-14472-2904446a-488c-4e40-8d24-cd7f758de713',
          host: '*',
          operation: 2,
          permissionType: 3,
        },
      ],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
