import { describe, expect, it } from 'vitest';
import { ExpireDelegationToken } from './index';

describe('protocol/requests/expire-delegation-token', () => {
  it('implements versions 0 through 2', () => {
    expect(ExpireDelegationToken.versions).toEqual([0, 1, 2]);
  });

  it('creates a version 2 request', () => {
    const { request } = ExpireDelegationToken.protocol({ version: 2 })({ hmac: Buffer.from([1]) });
    expect(request).toMatchObject({ apiKey: 40, apiVersion: 2, apiName: 'ExpireDelegationToken' });
  });
});
