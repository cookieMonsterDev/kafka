import { describe, expect, it } from 'vitest';
import * as studio from './index';

describe('index', () => {
  it('exposes no public API yet', () => {
    expect(Object.keys(studio)).toEqual([]);
  });
});
