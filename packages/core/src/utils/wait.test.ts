import { describe, expect, it, vi } from 'vitest';
import { sleep, waitFor } from './wait';

describe('utils/wait > sleep', () => {
  it('resolves after the given delay', async () => {
    const start = Date.now();
    await sleep(10);
    expect(Date.now() - start).toBeGreaterThanOrEqual(9);
  });

  it('rejects when aborted', async () => {
    const controller = new AbortController();
    const promise = sleep(1000, { signal: controller.signal });
    controller.abort();
    await expect(promise).rejects.toThrow();
  });
});

describe('utils/wait > waitFor', () => {
  it('waits for the condition', async () => {
    let conditionValid = false;
    setTimeout(() => {
      conditionValid = true;
    }, 6);

    await expect(waitFor(() => conditionValid, { delay: 5 })).resolves.toBe(true);
  });

  it('rejects the promise if the callback fails', async () => {
    await expect(
      waitFor(
        () => {
          throw new Error('callback failed!');
        },
        { delay: 1 },
      ),
    ).rejects.toThrow('callback failed!');
  });

  it('rejects the promise if the callback never succeeds', async () => {
    const condition = vi.fn().mockReturnValue(false);
    await expect(waitFor(condition, { delay: 1, maxWait: 20 })).rejects.toThrow('Timeout');
  });

  it('rejects the promise with a custom timeout message', async () => {
    await expect(waitFor(() => false, { delay: 1, maxWait: 20, timeoutMessage: 'foo bar' })).rejects.toThrow('foo bar');
  });

  it('rejects with the abort reason when the signal is aborted', async () => {
    const controller = new AbortController();
    const promise = waitFor(() => false, { delay: 1, signal: controller.signal });
    controller.abort(new Error('aborted by caller'));
    await expect(promise).rejects.toThrow('aborted by caller');
  });
});
