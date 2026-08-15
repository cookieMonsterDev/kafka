import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { describeAclsResponseV1 } from './response.js';

describe('protocol/requests/describe-acls/v1/response', () => {
  it('decodes a real fixture, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await describeAclsResponseV1.decode(Buffer.from(v1ResponseFixture.data));

    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      errorCode: 0,
      errorMessage: null,
      resources: [
        {
          resourceType: 2,
          resourceName: 'test-topic-3091e37cb34e1e916cfa-18029-1b277b41-4f40-4740-9274-51f556f212c9',
          resourcePatternType: 3,
          acls: [
            {
              principal: 'User:bob-bbc9e8f21ca0d1e60eba-18029-e0cee136-6f05-43fc-a235-26a779e72413',
              host: '*',
              operation: 2,
              permissionType: 3,
            },
          ],
        },
      ],
    });

    await expect(describeAclsResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
