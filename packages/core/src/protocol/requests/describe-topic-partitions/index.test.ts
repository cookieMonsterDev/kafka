import { describe, expect, it } from 'vitest';
import { DescribeTopicPartitions } from './index';

describe('protocol/requests/describe-topic-partitions', () => {
  it('implements version 0', () => {
    expect(DescribeTopicPartitions.versions).toEqual([0]);
  });

  it('creates a version 0 request', () => {
    const { request } = DescribeTopicPartitions.protocol({ version: 0 })({ topics: [{ topic: 'orders' }] });
    expect(request).toMatchObject({ apiKey: 75, apiVersion: 0, apiName: 'DescribeTopicPartitions' });
  });
});
