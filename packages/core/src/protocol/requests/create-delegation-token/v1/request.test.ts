import { describe, expect, it } from 'vitest';
import { createDelegationTokenRequestV0 } from '../v0/request';
import { createDelegationTokenRequestV1 } from './request';

const payload = {
  renewers: [{ principalType: 'User', name: 'alice' }],
  maxLifetimeMs: -1n,
};

describe('protocol/requests/create-delegation-token/v1/request', () => {
  it('encodes the same body as v0 with apiVersion 1', async () => {
    const definition = createDelegationTokenRequestV1(payload);
    expect(definition.apiVersion).toBe(1);
    expect(definition.apiKey).toBe(38);

    const v1 = await definition.encode();
    const v0 = await createDelegationTokenRequestV0(payload).encode();
    expect(v1.buffer).toEqual(v0.buffer);
  });
});
