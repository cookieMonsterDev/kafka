import { describe, expect, it } from 'vitest';
import { swapObject } from './swap-object';

describe('utils/swapObject', () => {
  it('swaps keys with values', () => {
    expect(swapObject({ a1: 'a2', b1: 'b2', c1: 'c2' })).toEqual({
      a2: 'a1',
      b2: 'b1',
      c2: 'c1',
    });
  });
});
