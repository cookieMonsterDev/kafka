import { describe, expect, it } from 'vitest';
import { OffsetFetch } from './index';

describe('protocol/requests/offset-fetch', () => {
  it('implements versions 1 through 4', () => {
    expect(OffsetFetch.versions).toEqual([1, 2, 3, 4]);
  });
});
