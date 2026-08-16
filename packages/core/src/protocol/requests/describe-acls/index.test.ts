import { describe, expect, it } from 'vitest';
import { DescribeAcls } from './index';

describe('protocol/requests/describe-acls', () => {
  it('implements only version 1 — the real Kafka 4.0.0 floor for this API', () => {
    expect(DescribeAcls.versions).toEqual([1]);
  });
});
