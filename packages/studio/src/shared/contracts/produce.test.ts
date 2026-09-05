import { describe, expect, it } from 'vitest';
import { burstRequestSchema, produceMessageSchema, produceRequestSchema } from './produce';

describe('produceMessageSchema', () => {
  it('accepts a value-only message', () => {
    expect(produceMessageSchema.safeParse({ value: 'hello' }).success).toBe(true);
  });

  it('accepts a null value as a tombstone', () => {
    expect(produceMessageSchema.safeParse({ value: null }).success).toBe(true);
  });

  it('rejects a missing value', () => {
    expect(produceMessageSchema.safeParse({ key: 'k' }).success).toBe(false);
  });

  it('rejects a negative partition', () => {
    expect(produceMessageSchema.safeParse({ value: 'v', partition: -1 }).success).toBe(false);
  });

  it('accepts string-valued headers', () => {
    expect(
      produceMessageSchema.safeParse({ value: 'v', headers: { 'content-type': 'application/json' } }).success,
    ).toBe(true);
  });
});

describe('produceRequestSchema', () => {
  it('accepts a topic with one message', () => {
    expect(produceRequestSchema.safeParse({ topic: 'orders', messages: [{ value: 'v' }] }).success).toBe(true);
  });

  it('rejects an empty messages array', () => {
    expect(produceRequestSchema.safeParse({ topic: 'orders', messages: [] }).success).toBe(false);
  });

  it('rejects an invalid topic name', () => {
    expect(produceRequestSchema.safeParse({ topic: 'orders/v2', messages: [{ value: 'v' }] }).success).toBe(false);
  });

  it('accepts an explicit acks value', () => {
    expect(produceRequestSchema.safeParse({ topic: 'orders', messages: [{ value: 'v' }], acks: -1 }).success).toBe(
      true,
    );
  });

  it('rejects an out-of-range acks value', () => {
    expect(produceRequestSchema.safeParse({ topic: 'orders', messages: [{ value: 'v' }], acks: 2 }).success).toBe(
      false,
    );
  });
});

describe('burstRequestSchema', () => {
  it('accepts a template, count, and rate', () => {
    expect(
      burstRequestSchema.safeParse({ topic: 'orders', template: { value: '{{seq}}' }, count: 100, ratePerSecond: 10 })
        .success,
    ).toBe(true);
  });

  it('defaults ratePerSecond so a burst is always rate-limited, even when omitted', () => {
    const result = burstRequestSchema.safeParse({ topic: 'orders', template: { value: 'v' }, count: 1 });
    expect(result.success).toBe(true);
    expect(result.success && result.data.ratePerSecond).toBe(200);
  });

  it('rejects a non-positive count', () => {
    expect(burstRequestSchema.safeParse({ topic: 'orders', template: { value: 'v' }, count: 0 }).success).toBe(false);
  });

  it('rejects a count above the burst cap', () => {
    expect(burstRequestSchema.safeParse({ topic: 'orders', template: { value: 'v' }, count: 1_000_000 }).success).toBe(
      false,
    );
  });
});
