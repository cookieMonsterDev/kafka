import { describe, expect, it } from 'vitest';
import { ListOffsets } from './index';

describe('protocol/requests/list-offsets', () => {
  it('implements versions 0 through 3', () => {
    expect(ListOffsets.versions).toEqual([0, 1, 2, 3]);
  });
});
