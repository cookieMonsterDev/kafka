import { describe, expect, it } from 'vitest';
import { DescribeTransactions } from './index';

describe('protocol/requests/describe-transactions', () => {
  it('implements version 0', () => {
    expect(DescribeTransactions.versions).toEqual([0]);
  });

  it('creates a version 0 request', () => {
    const { request } = DescribeTransactions.protocol({ version: 0 })({ transactionalIds: ['tx-a'] });
    expect(request).toMatchObject({ apiKey: 65, apiVersion: 0, apiName: 'DescribeTransactions' });
  });
});
