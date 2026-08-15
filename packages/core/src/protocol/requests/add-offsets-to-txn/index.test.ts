import { describe, expect, it } from 'vitest';
import { AddOffsetsToTxn } from './index.js';

describe('protocol/requests/add-offsets-to-txn', () => {
  it('implements versions 0 through 1', () => {
    expect(AddOffsetsToTxn.versions).toEqual([0, 1]);
  });
});
