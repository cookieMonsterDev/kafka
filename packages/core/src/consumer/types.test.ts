import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../errors';
import { parseOffset } from './types';

describe('consumer/types parseOffset', () => {
  it('returns a bigint as-is', () => {
    expect(parseOffset(42n)).toBe(42n);
    expect(parseOffset(0n)).toBe(0n);
    expect(parseOffset(-1n)).toBe(-1n);
    expect(parseOffset(-2n)).toBe(-2n);
  });

  it('accepts integer numbers including zero and negatives', () => {
    expect(parseOffset(0)).toBe(0n);
    expect(parseOffset(10)).toBe(10n);
    expect(parseOffset(-1)).toBe(-1n);
    expect(parseOffset(Number.MAX_SAFE_INTEGER)).toBe(BigInt(Number.MAX_SAFE_INTEGER));
  });

  it('accepts numeric strings including hex, whitespace, negatives, and leading zeros', () => {
    expect(parseOffset('0')).toBe(0n);
    expect(parseOffset('123')).toBe(123n);
    expect(parseOffset('-2')).toBe(-2n);
    expect(parseOffset('0007')).toBe(7n);
    expect(parseOffset(' 1')).toBe(1n);
    expect(parseOffset('0x10')).toBe(16n);
    expect(parseOffset(`${Number.MAX_SAFE_INTEGER}0`)).toBe(90071992547409910n);
  });

  it.each([
    1.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    '',
    '1.0',
    '1n',
    'abc',
    true,
    false,
    null,
    undefined,
    {},
    [],
    [1],
  ])('rejects %j', (value) => {
    expect(() => parseOffset(value)).toThrow(KafkaNonRetriableError);
    expect(() => parseOffset(value)).toThrow('Invalid offset, expected a long');
  });
});
