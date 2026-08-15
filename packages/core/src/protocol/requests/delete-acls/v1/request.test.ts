import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { deleteAclsRequestV1 } from './request.js';

describe('protocol/requests/delete-acls/v1/request', () => {
  it('encodes a real fixture', async () => {
    const encoder = await deleteAclsRequestV1({
      filters: [
        {
          resourceName: 'test-topic-000b0fa9008f920bc684-20826-6bcf579e-e882-47b8-9586-e778588f9e78',
          resourceType: 2,
          resourcePatternType: 3,
          principal: 'User:bob-51fe15d9fc1c5a3be5f2-20826-fcf12830-b5a1-477a-8ac9-866a4088273a',
          host: '*',
          permissionType: 3,
          operation: 1,
        },
      ],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
