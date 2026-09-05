import { describe, expect, it } from 'vitest';
import {
  DEFAULT_MESSAGES_LIMIT,
  deleteRecordsRequestSchema,
  messagesQuerySchema,
  seekByTimeRequestSchema,
  tailQuerySchema,
} from './message';

describe('messagesQuerySchema', () => {
  it('defaults the limit when omitted', () => {
    const result = messagesQuerySchema.safeParse({});
    expect(result.success).toBe(true);
    expect(result.success && result.data.limit).toBe(DEFAULT_MESSAGES_LIMIT);
  });

  it('coerces numeric query params from strings', () => {
    const result = messagesQuerySchema.safeParse({ partition: '2', limit: '50' });
    expect(result.success).toBe(true);
    expect(result.success && result.data).toEqual({ partition: 2, limit: 50 });
  });

  it('accepts "earliest", "latest", and a decimal offset for "from"', () => {
    expect(messagesQuerySchema.safeParse({ from: 'earliest' }).success).toBe(true);
    expect(messagesQuerySchema.safeParse({ from: 'latest' }).success).toBe(true);
    expect(messagesQuerySchema.safeParse({ from: '42' }).success).toBe(true);
  });

  it('rejects a non-decimal "from"', () => {
    expect(messagesQuerySchema.safeParse({ from: 'nope' }).success).toBe(false);
  });

  it('rejects "from" and "timestamp" together', () => {
    expect(messagesQuerySchema.safeParse({ from: 'earliest', timestamp: 1700000000000 }).success).toBe(false);
  });

  it('rejects a limit above the cap', () => {
    expect(messagesQuerySchema.safeParse({ limit: '5000' }).success).toBe(false);
  });
});

describe('tailQuerySchema', () => {
  it('accepts an empty query', () => {
    expect(tailQuerySchema.safeParse({}).success).toBe(true);
  });

  it('coerces the partition from a string', () => {
    const result = tailQuerySchema.safeParse({ partition: '3' });
    expect(result.success).toBe(true);
    expect(result.success && result.data.partition).toBe(3);
  });

  it('rejects a negative partition', () => {
    expect(tailQuerySchema.safeParse({ partition: '-1' }).success).toBe(false);
  });
});

describe('seekByTimeRequestSchema', () => {
  it('accepts a timestamp with no partition', () => {
    expect(seekByTimeRequestSchema.safeParse({ timestamp: 1700000000000 }).success).toBe(true);
  });

  it('rejects a non-integer timestamp', () => {
    expect(seekByTimeRequestSchema.safeParse({ timestamp: 1.5 }).success).toBe(false);
  });
});

describe('deleteRecordsRequestSchema', () => {
  it('accepts one or more partitions with a decimal beforeOffset', () => {
    expect(deleteRecordsRequestSchema.safeParse({ partitions: [{ partition: 0, beforeOffset: '100' }] }).success).toBe(
      true,
    );
  });

  it('rejects an empty partitions array', () => {
    expect(deleteRecordsRequestSchema.safeParse({ partitions: [] }).success).toBe(false);
  });

  it('rejects a non-decimal beforeOffset', () => {
    expect(deleteRecordsRequestSchema.safeParse({ partitions: [{ partition: 0, beforeOffset: '-1' }] }).success).toBe(
      false,
    );
  });
});
