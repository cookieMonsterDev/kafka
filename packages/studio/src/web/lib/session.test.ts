import { afterEach, describe, expect, it, vi } from 'vitest';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetModules();
});

describe('session state', () => {
  it('requires recovery when a reloaded tab has no token', async () => {
    vi.resetModules();
    const { isSessionExpired } = await import('./auth');
    expect(isSessionExpired()).toBe(true);
  });

  it('notifies subscribers once when a valid session is rejected', async () => {
    vi.resetModules();
    const replaceState = vi.fn();
    vi.stubGlobal('window', {
      location: { hash: '#token=example', href: 'http://localhost:5757/#token=example' },
      history: { replaceState },
    });
    const { getSessionToken, isSessionExpired, rejectSession, subscribeToSession } = await import('./auth');
    expect(getSessionToken()).toBe('example');
    expect(isSessionExpired()).toBe(false);
    expect(replaceState).toHaveBeenCalledWith(null, '', 'http://localhost:5757/');
    const listener = vi.fn();
    const unsubscribe = subscribeToSession(listener);
    rejectSession();
    rejectSession();
    expect(isSessionExpired()).toBe(true);
    expect(listener).toHaveBeenCalledOnce();
    unsubscribe();
  });
});
