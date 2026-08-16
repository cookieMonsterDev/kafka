import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { createAclsResponseV0 } from './response';

describe('protocol/requests/create-acls/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await createAclsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      creationResponses: [{ errorCode: 0, errorMessage: null }],
    });

    await expect(createAclsResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
