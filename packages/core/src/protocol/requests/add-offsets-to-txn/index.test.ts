import { describe, expect, it } from 'vitest';
import { AddOffsetsToTxn } from './index';

describe('protocol/requests/add-offsets-to-txn', () => {
  it('implements versions 0 through 4', () => {
    expect(AddOffsetsToTxn.versions).toEqual([0, 1, 2, 3, 4]);
  });
});
