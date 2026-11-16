import { describe, expect, it, vi } from 'vitest';
import { Deadline } from './deadline';

describe('utils/Deadline', () => {
  it('has no reason until it expires, then keeps the first one', () => {
    const deadline = new Deadline();
    expect(deadline.reason).toBeUndefined();

    const reason = new Error('first');
    deadline.expire(reason);
    deadline.expire(new Error('second'));
    expect(deadline.reason).toBe(reason);
  });

  it('aborts its signal with the reason when it expires', () => {
    const deadline = new Deadline();
    const { signal } = deadline;
    const onAbort = vi.fn();
    signal.addEventListener('abort', onAbort);
    expect(signal.aborted).toBe(false);

    const reason = new Error('deadline');
    deadline.expire(reason);
    expect(onAbort).toHaveBeenCalledOnce();
    expect(signal.reason).toBe(reason);
  });

  it('creates its signal on first use, already aborted once expired, and reuses it', () => {
    const deadline = new Deadline();
    const reason = new Error('deadline');
    deadline.expire(reason);

    const { signal } = deadline;
    expect(signal.aborted).toBe(true);
    expect(signal.reason).toBe(reason);
    expect(deadline.signal).toBe(signal);
  });
});
