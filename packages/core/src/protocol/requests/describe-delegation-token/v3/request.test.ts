import { describe, expect, it } from 'vitest';
import { describeDelegationTokenRequestV2 } from '../v2/request';
import { describeDelegationTokenRequestV3 } from './request';

const payload = { owners: [{ principalType: 'User', name: 'alice' }] };

describe('protocol/requests/describe-delegation-token/v3/request', () => {
  it('encodes the same body as v2 with apiVersion 3', async () => {
    const definition = describeDelegationTokenRequestV3(payload);
    expect(definition.apiVersion).toBe(3);
    const v3 = await definition.encode();
    const v2 = await describeDelegationTokenRequestV2(payload).encode();
    expect(v3.buffer).toEqual(v2.buffer);
  });
});
