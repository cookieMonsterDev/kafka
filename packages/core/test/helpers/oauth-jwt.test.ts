import { describe, expect, it } from 'vitest';
import { createUnsignedJwt } from './index';

describe('createUnsignedJwt', () => {
  it('emits an unsecured JWT with iat, exp, and the given claims', () => {
    const token = createUnsignedJwt({ sub: 'test' });
    const [header, payload, signature] = token.split('.');

    expect(header).toBeTruthy();
    expect(payload).toBeTruthy();
    expect(signature).toBe('');

    const claims = JSON.parse(Buffer.from(payload!, 'base64').toString()) as {
      sub: string;
      iat: number;
      exp: number;
    };
    expect(claims.sub).toBe('test');
    expect(claims.exp).toBeGreaterThan(claims.iat);
    expect(JSON.parse(Buffer.from(header!, 'base64').toString())).toEqual({ alg: 'none', typ: 'JWT' });
  });
});
