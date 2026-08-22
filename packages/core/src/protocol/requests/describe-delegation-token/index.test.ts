import { describe, expect, it } from 'vitest';
import { DescribeDelegationToken } from './index';

describe('protocol/requests/describe-delegation-token', () => {
  it('implements versions 0 through 3', () => {
    expect(DescribeDelegationToken.versions).toEqual([0, 1, 2, 3]);
  });

  it('creates a version 3 request', () => {
    const { request } = DescribeDelegationToken.protocol({ version: 3 })({
      owners: [{ principalType: 'User', name: 'alice' }],
    });
    expect(request).toMatchObject({ apiKey: 41, apiVersion: 3, apiName: 'DescribeDelegationToken' });
  });
});
