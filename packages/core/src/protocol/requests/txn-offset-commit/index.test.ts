import { describe, expect, it } from 'vitest';
import { TxnOffsetCommit } from './index';

describe('protocol/requests/txn-offset-commit', () => {
  it('implements versions 0 through 3', () => {
    expect(TxnOffsetCommit.versions).toEqual([0, 1, 2, 3]);
  });
});
