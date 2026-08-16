import { describe, expect, it } from 'vitest';
import { DescribeUserScramCredentials } from './index';

describe('protocol/requests/describe-user-scram-credentials', () => {
  it('implements version 0', () => {
    expect(DescribeUserScramCredentials.versions).toEqual([0]);
  });

  it('maps an empty users list to wire null', async () => {
    const { request } = DescribeUserScramCredentials.protocol({ version: 0 })({ users: [] });
    const encoder = await request.encode();
    expect(encoder.buffer).toEqual(Buffer.from([0, 0]));
  });
});
