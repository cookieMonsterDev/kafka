import { describe, expect, it } from 'vitest';
import { OffsetCommit } from './index';

describe('protocol/requests/offset-commit', () => {
  it('implements versions 0 through 5', () => {
    expect(OffsetCommit.versions).toEqual([0, 1, 2, 3, 4, 5]);
  });
});
