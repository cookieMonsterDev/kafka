import { describe, expect, it } from 'vitest';
import { createNodeLatencyTracker } from './node-latency-tracker';

describe('producer/node-latency-tracker', () => {
  it('has no reading for a node until it is recorded', () => {
    const tracker = createNodeLatencyTracker();
    expect(tracker.latencyFor(1)).toBeUndefined();
  });

  it('takes the raw sample as the first reading', () => {
    const tracker = createNodeLatencyTracker();
    tracker.record(1, 40);
    expect(tracker.latencyFor(1)).toBe(40);
  });

  it('moves the average toward new samples without jumping straight to them', () => {
    const tracker = createNodeLatencyTracker();
    tracker.record(1, 100);
    tracker.record(1, 0);

    const latency = tracker.latencyFor(1)!;
    expect(latency).toBeGreaterThan(0);
    expect(latency).toBeLessThan(100);
  });

  it('converges toward a node that is consistently slower or faster', () => {
    const tracker = createNodeLatencyTracker();
    for (let i = 0; i < 50; i++) tracker.record(1, 10);
    for (let i = 0; i < 50; i++) tracker.record(2, 200);

    expect(tracker.latencyFor(1)!).toBeCloseTo(10, 0);
    expect(tracker.latencyFor(2)!).toBeCloseTo(200, 0);
  });

  it('tracks nodes independently', () => {
    const tracker = createNodeLatencyTracker();
    tracker.record(1, 10);
    expect(tracker.latencyFor(2)).toBeUndefined();
  });
});
