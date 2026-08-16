import { describe, expect, it } from 'vitest';
import { isInvalidOffset } from './is-invalid-offset';

describe('consumer/offset-manager/is-invalid-offset', () => {
  it('returns true for negative offsets', () => {
    expect(isInvalidOffset(-1)).toEqual(true);
    expect(isInvalidOffset(-1n)).toEqual(true);
    expect(isInvalidOffset('-1')).toEqual(true);
    expect(isInvalidOffset(-2)).toEqual(true);
    expect(isInvalidOffset('-3')).toEqual(true);
  });

  it('returns true for blank values', () => {
    expect(isInvalidOffset(null)).toEqual(true);
    expect(isInvalidOffset(undefined)).toEqual(true);
    expect(isInvalidOffset('')).toEqual(true);
  });

  it('returns false for positive offsets, including zero', () => {
    expect(isInvalidOffset(0)).toEqual(false);
    expect(isInvalidOffset(0n)).toEqual(false);
    expect(isInvalidOffset(1)).toEqual(false);
    expect(isInvalidOffset(1n)).toEqual(false);
    expect(isInvalidOffset('2')).toEqual(false);
    expect(isInvalidOffset('3')).toEqual(false);
  });
});
