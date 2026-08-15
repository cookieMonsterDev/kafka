import { describe, expect, it } from 'vitest';
import { mapValues } from './map-values.js';

describe('utils/mapValues', () => {
  it('maps each value through the mapper, keeping the keys', () => {
    expect(mapValues({ a: 1, b: 2 }, (value) => value * 2)).toEqual({ a: 2, b: 4 });
  });

  it('passes the key to the mapper', () => {
    expect(mapValues({ a: 1, b: 2 }, (value, key) => `${key}:${value}`)).toEqual({
      a: 'a:1',
      b: 'b:2',
    });
  });
});
