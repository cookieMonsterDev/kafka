import { describe, expect, it } from 'vitest';
import { exponentialBackoffMs } from './defaults';

describe('network/exponentialBackoffMs', () => {
  it('returns 0 when there have been no failures', () => {
    expect(exponentialBackoffMs(0, 50, 1000)).toBe(0);
  });

  it('returns the initial delay on the first failure', () => {
    expect(exponentialBackoffMs(1, 50, 1000)).toBe(50);
  });

  it('doubles until the cap', () => {
    expect(exponentialBackoffMs(2, 50, 1000)).toBe(100);
    expect(exponentialBackoffMs(3, 50, 1000)).toBe(200);
    expect(exponentialBackoffMs(6, 50, 1000)).toBe(1000);
    expect(exponentialBackoffMs(10, 50, 1000)).toBe(1000);
  });
});
