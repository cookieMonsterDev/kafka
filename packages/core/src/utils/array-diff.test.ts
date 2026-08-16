import { describe, expect, it } from 'vitest';
import { arrayDiff } from './array-diff';

describe('utils/arrayDiff', () => {
  it('returns the elements in A that are not in B', () => {
    expect(arrayDiff([1, 2, 3, 4], [2, 3, 4])).toEqual([1]);
  });

  it('takes null and undefined into consideration', () => {
    expect(arrayDiff([1, 2, 3, 4, null, undefined], [2, 3, 4, 5])).toEqual([1, null, undefined]);
  });

  it('returns empty if A is empty', () => {
    expect(arrayDiff([], [2, 3, 4, 5])).toEqual([]);
  });

  it('only takes A into consideration', () => {
    expect(arrayDiff([1, 2, 3], [1, 2, 3, 4, 5, 6, 7, 8, 9])).toEqual([]);
  });
});
