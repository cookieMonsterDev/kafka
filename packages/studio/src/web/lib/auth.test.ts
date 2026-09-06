import { describe, expect, it } from 'vitest';
import { parseTokenFromHash } from './auth';

describe('parseTokenFromHash', () => {
  it('reads a token out of a hash fragment', () => {
    expect(parseTokenFromHash('#token=abc123')).toBe('abc123');
  });

  it('works with or without the leading #', () => {
    expect(parseTokenFromHash('token=abc123')).toBe('abc123');
  });

  it('url-decodes the value', () => {
    expect(parseTokenFromHash('#token=abc%20123')).toBe('abc 123');
  });

  it('returns null when there is no token', () => {
    expect(parseTokenFromHash('')).toBeNull();
    expect(parseTokenFromHash('#other=1')).toBeNull();
  });
});
