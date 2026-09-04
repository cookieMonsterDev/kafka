import { describe, expect, it, vi } from 'vitest';
import { Router } from './router';

describe('Router', () => {
  it('matches an exact path and method', () => {
    const handler = vi.fn();
    const router = new Router().get('/api/health', handler);

    const match = router.match('GET', '/api/health');
    expect(match?.handler).toBe(handler);
    expect(match?.params).toEqual({});
  });

  it('is case-insensitive on the method', () => {
    const handler = vi.fn();
    const router = new Router().get('/api/health', handler);

    expect(router.match('get', '/api/health')?.handler).toBe(handler);
  });

  it('does not match a different method on the same path', () => {
    const router = new Router().get('/api/topics', vi.fn());
    expect(router.match('POST', '/api/topics')).toBeUndefined();
  });

  it('extracts named params, url-decoded', () => {
    const handler = vi.fn();
    const router = new Router().get('/api/topics/:name', handler);

    const match = router.match('GET', '/api/topics/orders%2Fv2');
    expect(match?.params).toEqual({ name: 'orders/v2' });
  });

  it('does not match when segment counts differ', () => {
    const router = new Router().get('/api/topics/:name', vi.fn());
    expect(router.match('GET', '/api/topics')).toBeUndefined();
    expect(router.match('GET', '/api/topics/a/b')).toBeUndefined();
  });

  it('ignores leading/trailing slashes and repeated slashes', () => {
    const handler = vi.fn();
    const router = new Router().get('/api/health/', handler);
    expect(router.match('GET', 'api/health')?.handler).toBe(handler);
    expect(router.match('GET', '//api//health//')?.handler).toBe(handler);
  });

  it('returns undefined for an unregistered path', () => {
    const router = new Router().get('/api/health', vi.fn());
    expect(router.match('GET', '/api/nope')).toBeUndefined();
  });

  it('supports post/patch/delete', () => {
    const post = vi.fn();
    const patch = vi.fn();
    const del = vi.fn();
    const router = new Router().post('/api/topics', post).patch('/api/topics/:n', patch).delete('/api/topics/:n', del);

    expect(router.match('POST', '/api/topics')?.handler).toBe(post);
    expect(router.match('PATCH', '/api/topics/a')?.handler).toBe(patch);
    expect(router.match('DELETE', '/api/topics/a')?.handler).toBe(del);
  });
});
