import { describe, expect, it } from 'vitest';
import { redactSecrets } from './redact';

describe('redactSecrets', () => {
  it('redacts a top-level secret field', () => {
    expect(redactSecrets({ password: 'hunter2' })).toEqual({ password: '[REDACTED]' });
  });

  it('redacts a secret field nested inside an array of objects', () => {
    const input = { results: [{ name: 'u1', saltedPassword: Buffer.from('x') }, { name: 'u2' }] };
    expect(redactSecrets(input)).toEqual({
      results: [{ name: 'u1', saltedPassword: '[REDACTED]' }, { name: 'u2' }],
    });
  });

  it('is case-insensitive on the field name', () => {
    expect(redactSecrets({ TokenHmac: 'abc' })).toEqual({ TokenHmac: '[REDACTED]' });
  });

  it('leaves non-secret fields untouched', () => {
    expect(redactSecrets({ name: 'orders', partitions: 3 })).toEqual({ name: 'orders', partitions: 3 });
  });

  it('does not redact a Buffer value directly (only object keys)', () => {
    const buffer = Buffer.from('hello');
    expect(redactSecrets(buffer)).toBe(buffer);
  });

  it('redacts every known credential field name', () => {
    const input = {
      password: 'a',
      saltedPassword: 'b',
      salt: 'c',
      hmac: 'd',
      tokenHmac: 'e',
      secretAccessKey: 'f',
      sessionToken: 'g',
      passphrase: 'h',
    };
    const result = redactSecrets(input) as Record<string, string>;
    for (const key of Object.keys(input)) {
      expect(result[key]).toBe('[REDACTED]');
    }
  });

  it('passes primitives through untouched', () => {
    expect(redactSecrets('hello')).toBe('hello');
    expect(redactSecrets(42)).toBe(42);
    expect(redactSecrets(null)).toBe(null);
  });
});
