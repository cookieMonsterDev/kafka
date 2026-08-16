import { describe, expect, it } from 'vitest';
import { EndTxn } from './index';

describe('protocol/requests/end-txn', () => {
  it('implements versions 0 through 1', () => {
    expect(EndTxn.versions).toEqual([0, 1]);
  });
});
