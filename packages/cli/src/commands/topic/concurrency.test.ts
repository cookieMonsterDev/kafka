import { describe, expect, it } from 'vitest';
import { mapWithConcurrency } from './concurrency';

describe('mapWithConcurrency', () => {
  it('preserves input order in the result regardless of completion order', async () => {
    const delays = [30, 10, 20];
    const result = await mapWithConcurrency(delays, 8, (delay, index) => {
      return new Promise<number>((resolve) => setTimeout(() => resolve(index), delay));
    });
    expect(result).toEqual([0, 1, 2]);
  });

  it('never runs more than `limit` at once', async () => {
    let inFlight = 0;
    let maxInFlight = 0;
    const items = Array.from({ length: 20 }, (_, i) => i);

    await mapWithConcurrency(items, 3, async (item) => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await new Promise((resolve) => setTimeout(resolve, 1));
      inFlight -= 1;
      return item;
    });

    expect(maxInFlight).toBeLessThanOrEqual(3);
  });

  it('processes every item exactly once', async () => {
    const items = [1, 2, 3, 4, 5];
    const seen: number[] = [];
    await mapWithConcurrency(items, 2, async (item) => {
      seen.push(item);
      return item;
    });
    expect(seen.sort((a, b) => a - b)).toEqual(items);
  });

  it('handles an empty input', async () => {
    const result = await mapWithConcurrency([], 8, async (item: never) => item);
    expect(result).toEqual([]);
  });

  it('handles a limit larger than the item count', async () => {
    const result = await mapWithConcurrency([1, 2], 100, async (item) => item * 2);
    expect(result).toEqual([2, 4]);
  });
});
