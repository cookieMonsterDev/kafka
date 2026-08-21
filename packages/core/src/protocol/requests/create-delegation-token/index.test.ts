import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../../errors';
import { CreateDelegationToken } from './index';

describe('protocol/requests/create-delegation-token', () => {
  it('implements versions 0 through 3', () => {
    expect(CreateDelegationToken.versions).toEqual([0, 1, 2, 3]);
  });

  it('creates a version 3 request with a nullable owner', () => {
    const { request } = CreateDelegationToken.protocol({ version: 3 })({
      renewers: [{ principalType: 'User', name: 'alice' }],
      owner: { principalType: 'User', name: 'bob' },
    });
    expect(request).toMatchObject({ apiKey: 38, apiVersion: 3, apiName: 'CreateDelegationToken' });
  });

  it('rejects an owner principal on versions before 3', () => {
    expect(() =>
      CreateDelegationToken.protocol({ version: 2 })({
        owner: { principalType: 'User', name: 'bob' },
      }),
    ).toThrow(KafkaNonRetriableError);
  });
});
