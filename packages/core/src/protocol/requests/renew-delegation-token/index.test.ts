import { describe, expect, it } from 'vitest';
import { RenewDelegationToken } from './index';

describe('protocol/requests/renew-delegation-token', () => {
  it('implements versions 0 through 2', () => {
    expect(RenewDelegationToken.versions).toEqual([0, 1, 2]);
  });

  it('creates a version 2 request', () => {
    const { request } = RenewDelegationToken.protocol({ version: 2 })({ hmac: Buffer.from([1]) });
    expect(request).toMatchObject({ apiKey: 39, apiVersion: 2, apiName: 'RenewDelegationToken' });
  });
});
