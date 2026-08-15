import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { deleteAclsResponseV1 } from './response.js';

describe('protocol/requests/delete-acls/v1/response', () => {
  it('decodes a real fixture, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await deleteAclsResponseV1.decode(Buffer.from(v1ResponseFixture.data));

    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      filterResponses: [
        {
          errorCode: 0,
          errorMessage: null,
          matchingAcls: [
            {
              errorCode: 0,
              errorMessage: null,
              resourceType: 2,
              resourceName: 'test-topic-000b0fa9008f920bc684-20826-6bcf579e-e882-47b8-9586-e778588f9e78',
              resourcePatternType: 3,
              principal: 'User:bob-51fe15d9fc1c5a3be5f2-20826-fcf12830-b5a1-477a-8ac9-866a4088273a',
              host: '*',
              operation: 2,
              permissionType: 3,
            },
          ],
        },
      ],
    });

    await expect(deleteAclsResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
