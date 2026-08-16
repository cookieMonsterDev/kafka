import { describe, expect, it } from 'vitest';
import { EndTxn } from './index';

describe('protocol/requests/end-txn', () => {
  it('implements versions 0 through 3', () => {
    expect(EndTxn.versions).toEqual([0, 1, 2, 3]);
  });
});
