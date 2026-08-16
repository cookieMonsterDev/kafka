import { describe, expect, it } from 'vitest';
import { RESOURCE_PATTERN_TYPES } from '../../../enums/resource-pattern-types';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { deleteAclsResponseV0 } from './response';

describe('protocol/requests/delete-acls/v0/response', () => {
  it('decodes a real fixture and defaults resourcePatternType to LITERAL', async () => {
    const data = await deleteAclsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
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
              resourceName: 'test-topic-78d599e9d78a4da685ae-21381-e8f39f07-7d19-4677-aecb-bd0f731f1e28',
              resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
              principal: 'User:bob-cd8856cf4f23fe19899c-21381-c20b6340-b95c-431d-9237-2f15e310fba7',
              host: '*',
              operation: 2,
              permissionType: 3,
            },
          ],
        },
      ],
    });

    await expect(deleteAclsResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
