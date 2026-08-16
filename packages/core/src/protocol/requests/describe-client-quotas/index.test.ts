import { describe, expect, it } from 'vitest';
import { DescribeClientQuotas } from './index';

describe('protocol/requests/describe-client-quotas', () => {
  it('implements versions 0 through 1', () => {
    expect(DescribeClientQuotas.versions).toEqual([0, 1]);
  });

  it('builds a request for the requested version', () => {
    const options = {
      components: [{ entityType: 'client-id', matchType: 0, match: 'orders-producer' }],
    };
    const { request: v0 } = DescribeClientQuotas.protocol({ version: 0 })(options);
    const { request: v1 } = DescribeClientQuotas.protocol({ version: 1 })(options);
    expect(v0.apiVersion).toBe(0);
    expect(v1.apiVersion).toBe(1);
  });
});
