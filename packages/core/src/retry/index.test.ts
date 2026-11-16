import { describe, expect, it, vi } from 'vitest';
import { KafkaConnectionError } from '../errors';
import { Deadline } from '../utils/deadline';
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

  it('rejects with Aborted when bail is called without an error', async () => {
    const retry = retrier(FAST_RETRY_DEFAULTS);
    await expect(
      retry(async (bail) => {
        bail();
      }),
    ).rejects.toThrow('Aborted');
  });

  it('does not retry when retries is 0', async () => {
    const retry = retrier({ ...FAST_RETRY_DEFAULTS, retries: 0 });
    let attempts = 0;

    await expect(
      retry(async () => {
        attempts += 1;
        const error = new Error('fail') as Error & { retriable?: boolean };
        error.retriable = true;
        throw error;
      }),
    ).rejects.toMatchObject({ name: 'KafkaNumberOfRetriesExceeded', retryCount: 0 });

    expect(attempts).toBe(1);
  });

  it('retries when the retriable flag is omitted', async () => {
    let attempts = 0;
    const retry = retrier(FAST_RETRY_DEFAULTS);

    await expect(
      retry(async () => {
        attempts += 1;
        if (attempts < 2) {
          throw new Error('try again');
        }
        return 'ok';
      }),
    ).resolves.toBe('ok');

    expect(attempts).toBe(2);
  });

  it.each([RangeError, ReferenceError, SyntaxError])(
    'treats %s as unrecoverable regardless of the retriable flag',
    async (ErrorType) => {
      const retry = retrier(FAST_RETRY_DEFAULTS);
      const fn = vi.fn(async () => {
        throw new ErrorType('boom');
      });

      await expect(retry(fn)).rejects.toMatchObject({ name: 'KafkaNonRetriableError' });
      expect(fn).toHaveBeenCalledTimes(1);
    },
  );

  it('stops retrying once the deadline expires, rejecting with its reason without waiting out the backoff', async () => {
    vi.useFakeTimers();
    try {
      const retry = retrier({ retries: 10, initialRetryTime: 30_000, maxRetryTime: 30_000, factor: 0, multiplier: 1 });
      const deadline = new Deadline();
      const fn = vi.fn(async () => {
        throw new KafkaConnectionError('down');
      });

      const result = retry(fn, { deadline });
      // eslint-disable-next-line vitest/valid-expect -- attach the matcher before the deadline expires
      const assertion = expect(result).rejects.toThrow('deadline');
      await vi.advanceTimersByTimeAsync(100);
      deadline.expire(new Error('deadline'));
      await assertion;

      await vi.advanceTimersByTimeAsync(120_000);
      expect(fn).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('lets an in-flight attempt finish after the deadline expires, then rejects instead of retrying', async () => {
    const retry = retrier(FAST_RETRY_DEFAULTS);
    const deadline = new Deadline();
    const fn = vi.fn(async () => {
      deadline.expire(new Error('deadline'));
      await Promise.resolve();
      throw new KafkaConnectionError('down');
    });

    await expect(retry(fn, { deadline })).rejects.toThrow('deadline');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('does not start an attempt once the deadline has expired', async () => {
    const retry = retrier(FAST_RETRY_DEFAULTS);
    const deadline = new Deadline();
    deadline.expire(new Error('deadline'));
    const fn = vi.fn(async () => 'success');

    await expect(retry(fn, { deadline })).rejects.toThrow('deadline');
    expect(fn).not.toHaveBeenCalled();
  });

  it("reads the deadline's signal only to wait out a backoff", async () => {
    const retry = retrier(FAST_RETRY_DEFAULTS);
    const deadline = new Deadline();
    const signal = vi.spyOn(deadline, 'signal', 'get');
    let attempts = 0;

    await expect(retry(async () => 'success', { deadline })).resolves.toBe('success');
    expect(signal).not.toHaveBeenCalled();

    await expect(
      retry(
        async () => {
          attempts += 1;
          if (attempts === 1) throw new KafkaConnectionError('down');
          return 'success';
        },
        { deadline },
      ),
    ).resolves.toBe('success');
    expect(signal).toHaveBeenCalledOnce();
  });
});
