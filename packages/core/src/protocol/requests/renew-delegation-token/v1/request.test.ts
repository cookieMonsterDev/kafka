import { describe, expect, it } from 'vitest';
import { renewDelegationTokenRequestV0 } from '../v0/request';
import { renewDelegationTokenRequestV1 } from './request';

const payload = { hmac: Buffer.from([1]), renewPeriodMs: 60_000n };

describe('protocol/requests/renew-delegation-token/v1/request', () => {
  it('encodes the same body as v0 with apiVersion 1', async () => {
    const definition = renewDelegationTokenRequestV1(payload);
    expect(definition.apiVersion).toBe(1);
    const v1 = await definition.encode();
    const v0 = await renewDelegationTokenRequestV0(payload).encode();
    expect(v1.buffer).toEqual(v0.buffer);
  });
});
