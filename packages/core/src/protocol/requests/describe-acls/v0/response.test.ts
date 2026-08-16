import { describe, expect, it } from 'vitest';
import { RESOURCE_PATTERN_TYPES } from '../../../enums/resource-pattern-types';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { describeAclsResponseV0 } from './response';

describe('protocol/requests/describe-acls/v0/response', () => {
  it('decodes a real fixture and defaults resourcePatternType to LITERAL', async () => {
    const data = await describeAclsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      errorCode: 0,
      errorMessage: null,
      resources: [
        {
          resourceType: 2,
          resourceName: 'test-topic-064a1bcf62c877843e3c-18742-da400056-f741-4b3e-a725-b758d8104afa',
          resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
          acls: [
            {
              principal: 'User:bob-eef72cddd1a7bb3f1252-18742-bcac65c3-bec4-42ac-8f30-00314f6d428e',
              host: '*',
              operation: 2,
              permissionType: 3,
            },
          ],
        },
      ],
    });

    await expect(describeAclsResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
