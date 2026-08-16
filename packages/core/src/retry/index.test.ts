import { describe, expect, it, vi } from 'vitest';
import { retrier } from './index';
import { FAST_RETRY_DEFAULTS } from './test-defaults';

describe('retry/retrier', () => {
  it('resolves with the value returned by fn', async () => {
    const retry = retrier(FAST_RETRY_DEFAULTS);
    await expect(retry(async () => 'success')).resolves.toBe('success');
  });

  it('retries a retriable failure until it succeeds', async () => {
    let attempts = 0;
    const retry = retrier(FAST_RETRY_DEFAULTS);

    const result = await retry(async () => {
      attempts += 1;
      if (attempts < 3) {
        const error = new Error('not yet') as Error & { retriable?: boolean };
        error.retriable = true;
        throw error;
      }
      return 'done';
    });

    expect(result).toBe('done');
    expect(attempts).toBe(3);
  });

  it('gives up after exhausting retries and wraps the error', async () => {
    const retry = retrier({ ...FAST_RETRY_DEFAULTS, retries: 2 });
    let attempts = 0;

    await expect(
      retry(async () => {
        attempts += 1;
        const error = new Error('always fails') as Error & { retriable?: boolean };
        error.retriable = true;
        throw error;
      }),
    ).rejects.toMatchObject({ name: 'KafkaNumberOfRetriesExceeded' });

    expect(attempts).toBe(3); // initial attempt + 2 retries
  });

  it('does not retry a non-retriable error', async () => {
    const retry = retrier(FAST_RETRY_DEFAULTS);
    const fn = vi.fn(async () => {
      const error = new Error('fatal') as Error & { retriable?: boolean };
      error.retriable = false;
      throw error;
    });

    await expect(retry(fn)).rejects.toMatchObject({ name: 'KafkaNonRetriableError' });
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('treats TypeError/RangeError/etc as unrecoverable regardless of the retriable flag', async () => {
    const retry = retrier(FAST_RETRY_DEFAULTS);
    const fn = vi.fn(async () => {
      throw new TypeError('boom');
    });

    await expect(retry(fn)).rejects.toMatchObject({ name: 'KafkaNonRetriableError' });
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('stops immediately when bail is called', async () => {
    const retry = retrier(FAST_RETRY_DEFAULTS);
    const fn = vi.fn(async (bail: (error: Error) => void) => {
      bail(new Error('give up'));
    });

    await expect(retry(fn)).rejects.toThrow('give up');
  });
});
