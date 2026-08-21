import { describe, expect, it } from 'vitest';
import { ConsumerGroupDescribe } from './index';

describe('protocol/requests/consumer-group-describe', () => {
  it('implements versions 0 and 1', () => {
    expect(ConsumerGroupDescribe.versions).toEqual([0, 1]);
  });

  it('creates version 0 and 1 requests with the same body', () => {
    const options = { groupIds: ['g'], includeAuthorizedOperations: true };
    const v0 = ConsumerGroupDescribe.protocol({ version: 0 })(options);
    const v1 = ConsumerGroupDescribe.protocol({ version: 1 })(options);
    expect(v0.request).toMatchObject({ apiKey: 69, apiVersion: 0, apiName: 'ConsumerGroupDescribe' });
    expect(v1.request).toMatchObject({ apiKey: 69, apiVersion: 1, apiName: 'ConsumerGroupDescribe' });
  });
});
