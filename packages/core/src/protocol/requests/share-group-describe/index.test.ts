import { describe, expect, it } from 'vitest';
import { ShareGroupDescribe } from './index';

describe('protocol/requests/share-group-describe', () => {
  it('implements version 1', () => {
    expect(ShareGroupDescribe.versions).toEqual([1]);
  });

  it('builds a version 1 request', () => {
    const { request } = ShareGroupDescribe.protocol({ version: 1 })({
      groupIds: ['g1'],
      includeAuthorizedOperations: true,
    });
    expect(request).toMatchObject({ apiKey: 77, apiVersion: 1, apiName: 'ShareGroupDescribe' });
  });
});
