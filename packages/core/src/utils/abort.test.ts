import { describe, expect, it } from 'vitest';
import { abortError, rejectOnAbort } from './abort';

describe('abort', () => {
  it('uses the signal reason when it is already an Error', () => {
    const reason = new Error('nope');
    expect(abortError(AbortSignal.abort(reason))).toBe(reason);
  });

  it('wraps a non-Error reason', () => {
    const error = abortError(AbortSignal.abort('stopped'));
    expect(error).toBeInstanceOf(Error);
    expect(error.message).toBe('Aborted');
    expect(error.cause).toBe('stopped');
  });

  it('returns the original promise when no signal is given', async () => {
    const promise = Promise.resolve(7);
    expect(await rejectOnAbort(promise, undefined)).toBe(7);
  });

  it('rejects immediately when the signal is already aborted', async () => {
    const pending = new Promise<number>(() => {});
    await expect(rejectOnAbort(pending, AbortSignal.abort('already'))).rejects.toMatchObject({
      message: 'Aborted',
      cause: 'already',
    });
  });

  it('rejects when the signal fires while the promise is still pending', async () => {
    const controller = new AbortController();
    const pending = new Promise<number>(() => {});
    const result = rejectOnAbort(pending, controller.signal);
    controller.abort(new Error('later'));
    await expect(result).rejects.toThrow('later');
  });

  it('resolves normally when the promise wins the race', async () => {
    const controller = new AbortController();
    await expect(rejectOnAbort(Promise.resolve('ok'), controller.signal)).resolves.toBe('ok');
  });
});
