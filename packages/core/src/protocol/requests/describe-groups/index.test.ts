import { describe, expect, it } from 'vitest';
import { DescribeGroups } from './index';

describe('protocol/requests/describe-groups', () => {
  it('implements versions 0 through 5', () => {
    expect(DescribeGroups.versions).toEqual([0, 1, 2, 3, 4, 5]);
  });
});
