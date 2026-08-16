import { describe, expect, it } from 'vitest';
import { DescribeLogDirs } from './index';

describe('protocol/requests/describe-log-dirs', () => {
  it('implements versions 0 through 2', () => {
    expect(DescribeLogDirs.versions).toEqual([0, 1, 2]);
  });

  it('builds a request for the requested version', () => {
    const options = { topics: [{ topic: 'orders', partitions: [0] }] };
    expect(DescribeLogDirs.protocol({ version: 0 })(options).request.apiVersion).toBe(0);
    expect(DescribeLogDirs.protocol({ version: 1 })(options).request.apiVersion).toBe(1);
    expect(DescribeLogDirs.protocol({ version: 2 })(options).request.apiVersion).toBe(2);
  });
});
