import { describe, expect, it } from 'vitest';
import { Produce } from './index';

describe('protocol/requests/produce', () => {
  it('implements versions 0 through 7 (MessageSet v0-v2, RecordBatch v3-v7)', () => {
    expect(Produce.versions).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
  });
});
