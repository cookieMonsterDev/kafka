import { describe, expect, it } from 'vitest';
import { alterTopicConfigsRequestSchema, createPartitionsRequestSchema, createTopicRequestSchema } from './topic';

describe('createTopicRequestSchema', () => {
  it('accepts a minimal topic name', () => {
    const result = createTopicRequestSchema.safeParse({ topic: 'orders' });
    expect(result.success).toBe(true);
  });

  it('accepts partitions, replication factor, and config entries', () => {
    const result = createTopicRequestSchema.safeParse({
      topic: 'orders',
      numPartitions: 3,
      replicationFactor: 1,
      configEntries: { 'retention.ms': '1000' },
    });
    expect(result.success).toBe(true);
  });

  it('rejects an empty topic name', () => {
    expect(createTopicRequestSchema.safeParse({ topic: '' }).success).toBe(false);
  });

  it('rejects a topic name with disallowed characters', () => {
    expect(createTopicRequestSchema.safeParse({ topic: 'orders/v2' }).success).toBe(false);
  });

  it('rejects a non-positive partition count', () => {
    expect(createTopicRequestSchema.safeParse({ topic: 'orders', numPartitions: 0 }).success).toBe(false);
  });
});

describe('createPartitionsRequestSchema', () => {
  it('accepts a positive count', () => {
    expect(createPartitionsRequestSchema.safeParse({ count: 6 }).success).toBe(true);
  });

  it('rejects zero and negative counts', () => {
    expect(createPartitionsRequestSchema.safeParse({ count: 0 }).success).toBe(false);
    expect(createPartitionsRequestSchema.safeParse({ count: -1 }).success).toBe(false);
  });
});

describe('alterTopicConfigsRequestSchema', () => {
  it('accepts a "set" entry', () => {
    expect(alterTopicConfigsRequestSchema.safeParse({ set: { 'retention.ms': '1000' } }).success).toBe(true);
  });

  it('accepts an "unset" entry', () => {
    expect(alterTopicConfigsRequestSchema.safeParse({ unset: ['cleanup.policy'] }).success).toBe(true);
  });

  it('rejects a body with neither "set" nor "unset"', () => {
    expect(alterTopicConfigsRequestSchema.safeParse({}).success).toBe(false);
  });

  it('rejects an empty "set" object with no "unset"', () => {
    expect(alterTopicConfigsRequestSchema.safeParse({ set: {} }).success).toBe(false);
  });
});
