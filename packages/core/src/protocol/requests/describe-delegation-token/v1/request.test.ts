import { describe, expect, it } from 'vitest';
import { describeDelegationTokenRequestV0 } from '../v0/request';
import { describeDelegationTokenRequestV1 } from './request';

const payload = { owners: [{ principalType: 'User', name: 'alice' }] };

describe('protocol/requests/describe-delegation-token/v1/request', () => {
  it('encodes the same body as v0 with apiVersion 1', async () => {
    const definition = describeDelegationTokenRequestV1(payload);
    expect(definition.apiVersion).toBe(1);
    const v1 = await definition.encode();
    const v0 = await describeDelegationTokenRequestV0(payload).encode();
    expect(v1.buffer).toEqual(v0.buffer);
  });
});
