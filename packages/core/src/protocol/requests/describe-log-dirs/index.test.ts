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

  it.each([0, 1, 2])('v%i encodes an empty topics list as empty, not null (all)', async (version) => {
    const encode = (options: Parameters<ReturnType<typeof DescribeLogDirs.protocol>>[0]) =>
      DescribeLogDirs.protocol({ version })(options).request.encode();
    const empty = (await encode({ topics: [] })).buffer;
    const all = (await encode({})).buffer;
    expect((await encode({ topics: null })).buffer).toEqual(all);
    expect(empty).not.toEqual(all);
  });
});
