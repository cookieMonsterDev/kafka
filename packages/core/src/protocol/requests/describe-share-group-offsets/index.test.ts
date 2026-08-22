import { describe, expect, it } from 'vitest';
import { DescribeShareGroupOffsets } from './index';

describe('protocol/requests/describe-share-group-offsets', () => {
  it('implements versions 0 through 1', () => {
    expect(DescribeShareGroupOffsets.versions).toEqual([0, 1]);
  });

  it('builds a request for the requested version', () => {
    const options = {
      groups: [{ groupId: 'g', topics: [{ topicName: 'events', partitions: [0] }] }],
    };
    expect(DescribeShareGroupOffsets.protocol({ version: 0 })(options).request.apiVersion).toBe(0);
    expect(DescribeShareGroupOffsets.protocol({ version: 1 })(options).request.apiVersion).toBe(1);
  });
});
