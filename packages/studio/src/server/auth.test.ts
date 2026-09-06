import { describe, expect, it } from 'vitest';
import {
  checkRequestAuth,
  generateSessionToken,
  hostSecurityWarning,
  isAllowedHostHeader,
  isAllowedOriginHeader,
  isLoopbackHost,
  isValidToken,
  requiresAuth,
  withSessionTokenHash,
} from './auth';

const POLICY = { host: '127.0.0.1', port: 5757, token: 'a'.repeat(64) };

function req(headers: Record<string, string | undefined>): { headers: Record<string, string | undefined> } {
  return { headers };
}

describe('generateSessionToken', () => {
  it('generates a long, url-safe, unique token each call', () => {
    const a = generateSessionToken();
    const b = generateSessionToken();
    expect(a).not.toBe(b);
    expect(a).toMatch(/^[0-9a-f]{64}$/);
  });
});

describe('requiresAuth', () => {
  it('protects /api/* and the runtime bootstrap file', () => {
    expect(requiresAuth('/api/topics')).toBe(true);
    expect(requiresAuth('/__studio_runtime.json')).toBe(true);
  });

  it('leaves static assets unprotected — that is how the token-bearing page loads in the first place', () => {
    expect(requiresAuth('/')).toBe(false);
    expect(requiresAuth('/assets/main.js')).toBe(false);
  });
});

describe('isAllowedHostHeader', () => {
  it('accepts the exact bound host and port', () => {
    expect(isAllowedHostHeader('127.0.0.1:5757', POLICY)).toBe(true);
  });

  it('rejects a mismatched hostname — the DNS-rebinding shape this exists to catch', () => {
    expect(isAllowedHostHeader('evil.example:5757', POLICY)).toBe(false);
  });

  it('rejects a mismatched port', () => {
    expect(isAllowedHostHeader('127.0.0.1:9999', POLICY)).toBe(false);
  });

  it('treats loopback aliases as equivalent when bound to a loopback address', () => {
    expect(isAllowedHostHeader('localhost:5757', POLICY)).toBe(true);
  });

  it('rejects a missing Host header — it is mandatory on every real request', () => {
    expect(isAllowedHostHeader(undefined, POLICY)).toBe(false);
  });

  it('accepts any hostname when bound to all interfaces', () => {
    expect(isAllowedHostHeader('192.168.1.20:5757', { host: '0.0.0.0', port: 5757 })).toBe(true);
  });
});

describe('isAllowedOriginHeader', () => {
  it('tolerates an absent Origin — same-origin requests often omit it', () => {
    expect(isAllowedOriginHeader(undefined, POLICY)).toBe(true);
  });

  it('accepts a matching Origin', () => {
    expect(isAllowedOriginHeader('http://127.0.0.1:5757', POLICY)).toBe(true);
  });

  it('rejects a present but foreign Origin', () => {
    expect(isAllowedOriginHeader('http://evil.example', POLICY)).toBe(false);
  });

  it('rejects a malformed Origin', () => {
    expect(isAllowedOriginHeader('not a url', POLICY)).toBe(false);
  });
});

describe('isValidToken', () => {
  it('accepts the token from the header', () => {
    expect(
      isValidToken(req({ 'x-kafka-studio-token': POLICY.token }), new URL('http://x/api/health'), POLICY.token),
    ).toBe(true);
  });

  it('accepts the token from the query string — EventSource cannot set headers', () => {
    const url = new URL(`http://x/api/events?token=${POLICY.token}`);
    expect(isValidToken(req({}), url, POLICY.token)).toBe(true);
  });

  it('rejects a wrong token', () => {
    expect(isValidToken(req({ 'x-kafka-studio-token': 'wrong' }), new URL('http://x/api/health'), POLICY.token)).toBe(
      false,
    );
  });

  it('rejects a missing token', () => {
    expect(isValidToken(req({}), new URL('http://x/api/health'), POLICY.token)).toBe(false);
  });
});

describe('checkRequestAuth', () => {
  it('passes through routes that do not require auth', () => {
    expect(checkRequestAuth(req({}), new URL('http://x/'), POLICY)).toBeNull();
  });

  it('rejects a protected route with a forbidden host before even checking the token', () => {
    const result = checkRequestAuth(req({ host: 'evil.example:5757' }), new URL('http://x/api/health'), POLICY);
    expect(result).toMatchObject({ status: 403, code: 'forbidden_origin' });
  });

  it('rejects a protected route with a missing token', () => {
    const result = checkRequestAuth(req({ host: '127.0.0.1:5757' }), new URL('http://x/api/health'), POLICY);
    expect(result).toMatchObject({ status: 401, code: 'unauthorized' });
  });

  it('allows a protected route with the right host and token', () => {
    const result = checkRequestAuth(
      req({ host: '127.0.0.1:5757', 'x-kafka-studio-token': POLICY.token }),
      new URL('http://x/api/health'),
      POLICY,
    );
    expect(result).toBeNull();
  });
});

describe('withSessionTokenHash', () => {
  it('appends the token as a URL-encoded hash fragment', () => {
    expect(withSessionTokenHash('http://127.0.0.1:5757/', 'abc 123')).toBe('http://127.0.0.1:5757/#token=abc%20123');
  });
});

describe('hostSecurityWarning', () => {
  it('is silent for loopback hosts', () => {
    expect(hostSecurityWarning('127.0.0.1')).toBeNull();
    expect(hostSecurityWarning('localhost')).toBeNull();
  });

  it('warns for anything else', () => {
    expect(hostSecurityWarning('0.0.0.0')).toMatch(/WARNING/);
    expect(isLoopbackHost('0.0.0.0')).toBe(false);
  });
});
