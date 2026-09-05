import { describe, expect, it } from 'vitest';
import {
  deleteGroupOffsetsRequestSchema,
  removeGroupMembersRequestSchema,
  resetGroupOffsetsRequestSchema,
} from './group';

describe('resetGroupOffsetsRequestSchema', () => {
  it('accepts one target of each kind', () => {
    const result = resetGroupOffsetsRequestSchema.safeParse({
      topic: 'orders',
      partitions: [
        { partition: 0, to: 'earliest' },
        { partition: 1, to: 'latest' },
        { partition: 2, to: 'offset', offset: '42' },
        { partition: 3, to: 'timestamp', timestamp: 1_700_000_000_000 },
      ],
    });
    expect(result.success).toBe(true);
  });

  it('rejects an offset target with a non-numeric offset', () => {
    const result = resetGroupOffsetsRequestSchema.safeParse({
      topic: 'orders',
      partitions: [{ partition: 0, to: 'offset', offset: 'not-a-number' }],
    });
    expect(result.success).toBe(false);
  });

  it('rejects an unknown "to" kind', () => {
    const result = resetGroupOffsetsRequestSchema.safeParse({
      topic: 'orders',
      partitions: [{ partition: 0, to: 'middle' }],
    });
    expect(result.success).toBe(false);
  });

  it('rejects an empty partitions array', () => {
    const result = resetGroupOffsetsRequestSchema.safeParse({ topic: 'orders', partitions: [] });
    expect(result.success).toBe(false);
  });
});

describe('deleteGroupOffsetsRequestSchema', () => {
  it('accepts a topic with at least one partition', () => {
    const result = deleteGroupOffsetsRequestSchema.safeParse({ topics: [{ topic: 'orders', partitions: [0, 1] }] });
    expect(result.success).toBe(true);
  });

  it('rejects a topic with an empty partitions array', () => {
    const result = deleteGroupOffsetsRequestSchema.safeParse({ topics: [{ topic: 'orders', partitions: [] }] });
    expect(result.success).toBe(false);
  });
});

describe('removeGroupMembersRequestSchema', () => {
  it('accepts a member id with an optional group instance id', () => {
    const result = removeGroupMembersRequestSchema.safeParse({
      members: [{ memberId: 'm1' }, { memberId: 'm2', groupInstanceId: 'i2' }],
    });
    expect(result.success).toBe(true);
  });

  it('rejects an empty members array', () => {
    const result = removeGroupMembersRequestSchema.safeParse({ members: [] });
    expect(result.success).toBe(false);
  });
});
