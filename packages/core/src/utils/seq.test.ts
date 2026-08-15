import { describe, expect, it } from 'vitest';
import { seq } from './seq.js';

describe('utils/seq', () => {
  it('defaults to the index', () => {
    expect(seq(4)).toEqual([0, 1, 2, 3]);
  });

  it('maps each index through the callback', () => {
    expect(seq(3, (index) => index * 2)).toEqual([0, 2, 4]);
  });

  it('returns an empty array for count 0', () => {
    expect(seq(0)).toEqual([]);
  });
});
