import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { createAclsRequestV0 } from './request';

describe('protocol/requests/create-acls/v0/request', () => {
  it('encodes a real fixture without resourcePatternType', async () => {
    const encoder = await createAclsRequestV0({
      creations: [
        {
          resourceType: 2,
          resourceName: 'test-topic-119fe09ddb8092d6113d-15436-9fdcf583-7b77-4489-ac86-8af4a76ef420',
          principal: 'User:bob-575703bfac1e8c129332-15436-137b3edd-b741-4bb6-a266-318ac292beb8',
          host: '*',
          operation: 2,
          permissionType: 3,
        },
      ],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
