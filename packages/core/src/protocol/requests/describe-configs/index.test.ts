import { describe, expect, it } from 'vitest';
import { DescribeConfigs } from './index';

describe('protocol/requests/describe-configs', () => {
  it('implements versions 1 through 2 (v0 is below the real Kafka 4.0.0 floor)', () => {
    expect(DescribeConfigs.versions).toEqual([1, 2]);
  });
});
