import { describe, expect, it, vi } from 'vitest';
import { sharedPromiseTo } from './shared-promise-to.js';

describe('utils/sharedPromiseTo', () => {
  const deferred = <T>() => {
    let resolve!: (value: T) => void;
    let reject!: (reason?: unknown) => void;
    const promise = new Promise<T>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    return { promise, resolve, reject };
  };

  it('returns the same pending promise for every invocation', () => {
    const { promise } = deferred<string>();
    const asyncFunction = vi.fn(() => promise);
    const shared = sharedPromiseTo(asyncFunction);

    const p1 = shared();
    const p2 = shared();
    const p3 = shared();

    expect(p1).toBe(p2);
    expect(p2).toBe(p3);
    expect(asyncFunction).toHaveBeenCalledTimes(1);
  });

  it('returns a new promise on the next invocation after resolving', async () => {
    const { promise, resolve } = deferred<string>();
    const asyncFunction = vi.fn(() => promise);
    const shared = sharedPromiseTo(asyncFunction);

    const p1 = shared();
    resolve('Resolved promise #1');
    await expect(p1).resolves.toBe('Resolved promise #1');

    const p2 = shared();
    expect(p2).not.toBe(p1);
    expect(asyncFunction).toHaveBeenCalledTimes(2);
  });

  it('returns a new promise on the next invocation after rejecting', async () => {
    const asyncFunction = vi
      .fn()
      .mockRejectedValueOnce(new Error('Rejected promise #1'))
      .mockResolvedValueOnce('second call');
    const shared = sharedPromiseTo(asyncFunction);

    const p1 = shared();
    await expect(p1).rejects.toThrow('Rejected promise #1');

    const p2 = shared();
    expect(p2).not.toBe(p1);
    await expect(p2).resolves.toBe('second call');
    expect(asyncFunction).toHaveBeenCalledTimes(2);
  });

  it('passes through arguments given at invocation time', async () => {
    const { promise, resolve } = deferred<void>();
    const asyncFunction = vi.fn((_a: string, _b: string) => promise);
    const shared = sharedPromiseTo(asyncFunction);

    const p1 = shared('arg1', 'arg2');
    resolve();
    await p1;

    expect(asyncFunction).toHaveBeenCalledWith('arg1', 'arg2');
  });
});
