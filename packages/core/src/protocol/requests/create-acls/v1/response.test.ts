import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { createAclsResponseV1 } from './response.js';

describe('protocol/requests/create-acls/v1/response', () => {
  it('decodes a real fixture, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await createAclsResponseV1.decode(Buffer.from(v1ResponseFixture.data));

    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      creationResponses: [{ errorCode: 0, errorMessage: null }],
    });

    await expect(createAclsResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
