import { afterEach, describe, expect, it, vi } from 'vitest';
import { RANDOM_WORDS_PER_BATCH, randomFraction } from './random-fraction';

describe('utils/randomFraction', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('returns numbers in [0, 1)', () => {
    for (let i = 0; i < 1000; i++) {
      const value = randomFraction();
      expect(value).toBeGreaterThanOrEqual(0);
      expect(value).toBeLessThan(1);
    }
  });

  it('draws from Web Crypto instead of Math.random', () => {
    const mathRandom = vi.spyOn(Math, 'random');
    const getRandomValues = vi.spyOn(globalThis.crypto, 'getRandomValues');

    // More draws than one batch holds, so the batch is refilled at least once.
    for (let i = 0; i <= RANDOM_WORDS_PER_BATCH; i++) {
      randomFraction();
    }

    expect(mathRandom).not.toHaveBeenCalled();
    expect(getRandomValues).toHaveBeenCalled();
  });
});
