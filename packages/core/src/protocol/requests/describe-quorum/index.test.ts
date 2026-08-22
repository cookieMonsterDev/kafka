import { describe, expect, it } from 'vitest';
import { DescribeQuorum } from './index';

describe('protocol/requests/describe-quorum', () => {
  it('registers versions 0-2', () => {
    expect(DescribeQuorum.versions).toEqual([0, 1, 2]);
  });

  it('builds a v0 request with an empty topics array by default', () => {
    const { request } = DescribeQuorum.protocol({ version: 0 })({});
    expect(request.apiVersion).toBe(0);
  });

  it('builds v1 and v2 requests with the same body as v0', () => {
    expect(DescribeQuorum.protocol({ version: 1 })({}).request.apiVersion).toBe(1);
    expect(DescribeQuorum.protocol({ version: 2 })({}).request.apiVersion).toBe(2);
  });
});
