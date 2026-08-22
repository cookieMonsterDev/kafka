import { describe, expect, it } from 'vitest';
import { DescribeQuorum } from './index';

describe('protocol/requests/describe-quorum', () => {
  it('registers version 0', () => {
    expect(DescribeQuorum.versions).toEqual([0]);
  });

  it('builds a v0 request with an empty topics array by default', () => {
    const { request } = DescribeQuorum.protocol({ version: 0 })({});
    expect(request.apiVersion).toBe(0);
  });
});
