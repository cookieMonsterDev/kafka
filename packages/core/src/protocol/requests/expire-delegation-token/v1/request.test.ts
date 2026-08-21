import { describe, expect, it } from 'vitest';
import { expireDelegationTokenRequestV0 } from '../v0/request';
import { expireDelegationTokenRequestV1 } from './request';

const payload = { hmac: Buffer.from([1]), expiryTimePeriodMs: -1n };

describe('protocol/requests/expire-delegation-token/v1/request', () => {
  it('encodes the same body as v0 with apiVersion 1', async () => {
    const definition = expireDelegationTokenRequestV1(payload);
    expect(definition.apiVersion).toBe(1);
    const v1 = await definition.encode();
    const v0 = await expireDelegationTokenRequestV0(payload).encode();
    expect(v1.buffer).toEqual(v0.buffer);
  });
});
