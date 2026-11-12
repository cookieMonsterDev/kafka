import { describe, expect, it } from 'vitest';
import { isPromiseLike } from './is-promise-like';

describe('utils/isPromiseLike', () => {
  it('accepts native promises and custom thenables', () => {
    const thenableFunction = Object.assign(() => undefined, { then: () => undefined });

    expect(isPromiseLike(Promise.resolve(1))).toBe(true);
    expect(isPromiseLike({ then: () => undefined })).toBe(true);
    expect(isPromiseLike(thenableFunction)).toBe(true);
  });

  it('rejects values without a callable then', () => {
    for (const value of [undefined, null, 0, 'then', {}, { then: 1 }, () => undefined]) {
      expect(isPromiseLike(value)).toBe(false);
    }
  });
});
