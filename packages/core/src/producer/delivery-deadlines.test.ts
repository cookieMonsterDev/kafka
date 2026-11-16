import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from 'vitest';
import { DeliveryDeadlines } from './delivery-deadlines';

describe('producer/DeliveryDeadlines', () => {
  let setTimeoutSpy: MockInstance<typeof setTimeout>;

  beforeEach(() => {
    vi.useFakeTimers();
    setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout');
  });

  afterEach(() => {
    // Restore the spy before the fake timers, or restoring it would reinstall a fake setTimeout.
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  const createDeadlines = () => new DeliveryDeadlines(() => new Error('delivery timeout'));
  const timer = (index = 0) => setTimeoutSpy.mock.results[index]?.value as NodeJS.Timeout;

  it('expires a deadline once it passes, passing its error to onExpire', async () => {
    const deadlines = createDeadlines();
    const onExpire = vi.fn();
    const deadline = deadlines.track(performance.now() + 500, onExpire);

    await vi.advanceTimersByTimeAsync(499);
    expect(deadline.reason).toBeUndefined();
    expect(onExpire).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    expect(deadline.reason).toEqual(new Error('delivery timeout'));
    expect(onExpire).toHaveBeenCalledWith(deadline.reason);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('arms one timer for deadlines that come in order, and none per settled send', async () => {
    const deadlines = createDeadlines();
    for (let index = 0; index < 3; index++) {
      deadlines.untrack(deadlines.track(performance.now() + 500));
      await vi.advanceTimersByTimeAsync(10);
    }
    const pending = [deadlines.track(performance.now() + 500), deadlines.track(performance.now() + 500)];

    expect(setTimeoutSpy).toHaveBeenCalledOnce();
    expect(pending.map(({ reason }) => reason)).toEqual([undefined, undefined]);
  });

  it('re-arms the timer for the next deadline when it fires for one that settled', async () => {
    const deadlines = createDeadlines();
    deadlines.untrack(deadlines.track(performance.now() + 500));
    await vi.advanceTimersByTimeAsync(100);
    const next = deadlines.track(performance.now() + 500);

    await vi.advanceTimersByTimeAsync(400);
    expect(next.reason).toBeUndefined();
    expect(setTimeoutSpy).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(99);
    expect(next.reason).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1);
    expect(next.reason).toBeInstanceOf(Error);
  });

  it('re-arms the timer for a deadline earlier than the one it waits for', async () => {
    const deadlines = createDeadlines();
    const later = deadlines.track(performance.now() + 1000);
    const earlier = deadlines.track(performance.now() + 200);

    await vi.advanceTimersByTimeAsync(200);
    expect(earlier.reason).toBeInstanceOf(Error);
    expect(later.reason).toBeUndefined();

    await vi.advanceTimersByTimeAsync(800);
    expect(later.reason).toBeInstanceOf(Error);
  });

  it('expires every deadline that passed in one firing, in the order they were tracked', async () => {
    const deadlines = createDeadlines();
    const expired: string[] = [];
    deadlines.track(performance.now() + 500, () => expired.push('first'));
    deadlines.track(performance.now() + 500, () => expired.push('second'));

    await vi.advanceTimersByTimeAsync(500);
    expect(expired).toEqual(['first', 'second']);
  });

  it('expires deadlines that come in order each at its own time', async () => {
    const deadlines = createDeadlines();
    const tracked = [];
    for (let index = 0; index < 3; index++) {
      tracked.push(deadlines.track(performance.now() + 500));
      await vi.advanceTimersByTimeAsync(100);
    }

    await vi.advanceTimersByTimeAsync(200);
    expect(tracked.map(({ reason }) => reason !== undefined)).toEqual([true, false, false]);
    await vi.advanceTimersByTimeAsync(100);
    expect(tracked.map(({ reason }) => reason !== undefined)).toEqual([true, true, false]);
    await vi.advanceTimersByTimeAsync(100);
    expect(tracked.map(({ reason }) => reason !== undefined)).toEqual([true, true, true]);
  });

  it('expires each deadline on time when they come out of order', async () => {
    const deadlines = createDeadlines();
    const [first, second, third] = [500, 1000, 700].map((delay) => deadlines.track(performance.now() + delay));

    await vi.advanceTimersByTimeAsync(500);
    expect([first, second, third].map((deadline) => deadline?.reason !== undefined)).toEqual([true, false, false]);
    await vi.advanceTimersByTimeAsync(199);
    expect(third?.reason).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1);
    expect(third?.reason).toBeInstanceOf(Error);
    expect(second?.reason).toBeUndefined();
    await vi.advanceTimersByTimeAsync(300);
    expect(second?.reason).toBeInstanceOf(Error);
  });

  it('is not moved by a wall-clock jump', async () => {
    const deadlines = createDeadlines();
    const deadline = deadlines.track(performance.now() + 500);

    vi.setSystemTime(Date.now() + 60_000);
    await vi.advanceTimersByTimeAsync(499);
    expect(deadline.reason).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1);
    expect(deadline.reason).toBeInstanceOf(Error);
  });

  it('waits out a deadline beyond the longest timer delay without firing every millisecond', async () => {
    const deadlines = createDeadlines();
    const deadline = deadlines.track(performance.now() + 2 ** 31 + 1000);

    await vi.advanceTimersByTimeAsync(100);
    expect(setTimeoutSpy).toHaveBeenCalledOnce();
    expect(deadline.reason).toBeUndefined();
  });

  it('ignores untracking a deadline that already expired', async () => {
    const deadlines = createDeadlines();
    const deadline = deadlines.track(performance.now() + 500);
    await vi.advanceTimersByTimeAsync(500);

    expect(() => deadlines.untrack(deadline)).not.toThrow();
  });

  it('unrefs the timer while nothing is tracked and refs it again for the next send', () => {
    const deadlines = createDeadlines();
    const first = deadlines.track(performance.now() + 500);
    expect(timer().hasRef()).toBe(true);

    deadlines.untrack(first);
    expect(timer().hasRef()).toBe(false);

    deadlines.track(performance.now() + 500);
    expect(timer().hasRef()).toBe(true);
    expect(setTimeoutSpy).toHaveBeenCalledOnce();
  });
});
