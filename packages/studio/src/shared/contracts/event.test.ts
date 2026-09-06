import { describe, expect, it } from 'vitest';
import { studioEventSchema } from './event';

describe('studioEventSchema', () => {
  it('accepts a produce event', () => {
    const result = studioEventSchema.safeParse({
      id: 1,
      kind: 'produce',
      topic: 'orders',
      partition: 0,
      count: 1,
      bytes: 12,
      timestamp: 1_700_000_000_000,
    });
    expect(result.success).toBe(true);
  });

  it('accepts a null partition', () => {
    const result = studioEventSchema.safeParse({
      id: 1,
      kind: 'consume',
      topic: 'orders',
      partition: null,
      count: 1,
      bytes: 12,
      timestamp: 1_700_000_000_000,
    });
    expect(result.success).toBe(true);
  });

  it('rejects an unknown kind', () => {
    const result = studioEventSchema.safeParse({
      id: 1,
      kind: 'delete',
      topic: 'orders',
      partition: null,
      count: 1,
      bytes: 12,
      timestamp: 1_700_000_000_000,
    });
    expect(result.success).toBe(false);
  });

  it('rejects a zero count', () => {
    const result = studioEventSchema.safeParse({
      id: 1,
      kind: 'produce',
      topic: 'orders',
      partition: null,
      count: 0,
      bytes: 12,
      timestamp: 1_700_000_000_000,
    });
    expect(result.success).toBe(false);
  });
});
