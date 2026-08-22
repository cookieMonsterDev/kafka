import { describe, expect, it } from 'vitest';
import { StickyPartitioner } from './producer/partitioners/index';
import { throughputPreset } from './throughput-preset';

describe('throughputPreset', () => {
  it('returns producer and consumer fragments without flipping constructor defaults', () => {
    const preset = throughputPreset();

    expect(preset).toEqual({
      producer: {
        lingerMs: 5,
        batchSize: 16_384,
        maxInFlightRequests: 5,
        createPartitioner: StickyPartitioner,
        bufferMemory: 32 * 1024 * 1024,
      },
      consumer: {
        partitionsConsumedConcurrently: 4,
      },
    });
  });

  it('returns a new object each call so callers can mutate a copy safely', () => {
    const first = throughputPreset();
    const second = throughputPreset();

    expect(first).not.toBe(second);
    expect(first.producer).not.toBe(second.producer);
    expect(first.consumer).not.toBe(second.consumer);
    expect(first.producer.createPartitioner).toBe(StickyPartitioner);
  });
});
