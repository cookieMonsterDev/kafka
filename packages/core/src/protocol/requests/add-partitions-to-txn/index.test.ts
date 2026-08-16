import { describe, expect, it } from 'vitest';
import { AddPartitionsToTxn } from './index';

describe('protocol/requests/add-partitions-to-txn', () => {
  it('implements versions 0 through 3', () => {
    expect(AddPartitionsToTxn.versions).toEqual([0, 1, 2, 3]);
  });
});
