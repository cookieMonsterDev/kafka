import { describe, expect, it } from 'vitest';
import { RETRY_DEFAULTS } from './defaults';
import { FAST_RETRY_DEFAULTS } from './test-defaults';

describe('retry/defaults', () => {
  it('keeps public retry defaults frozen and internally consistent', () => {
    expect(RETRY_DEFAULTS.retries).toBe(5);
    expect(RETRY_DEFAULTS.initialRetryTime).toBe(300);
    expect(RETRY_DEFAULTS.maxRetryTime).toBe(30_000);
    expect(RETRY_DEFAULTS.multiplier).toBe(2);
    expect(RETRY_DEFAULTS.factor).toBe(0.2);
    expect(Object.isFrozen(RETRY_DEFAULTS)).toBe(true);
  });

  it('uses a faster schedule for tests than production defaults', () => {
    expect(FAST_RETRY_DEFAULTS.initialRetryTime).toBeLessThan(RETRY_DEFAULTS.initialRetryTime);
    expect(FAST_RETRY_DEFAULTS.maxRetryTime).toBeLessThan(RETRY_DEFAULTS.maxRetryTime);
    expect(Object.isFrozen(FAST_RETRY_DEFAULTS)).toBe(true);
  });
});
