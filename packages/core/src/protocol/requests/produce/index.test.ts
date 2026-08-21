import { describe, expect, it } from 'vitest';
import { Produce } from './index';

describe('protocol/requests/produce', () => {
  it('implements versions 0 through 13 (MessageSet v0-v2, RecordBatch v3-v13)', () => {
    expect(Produce.versions).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]);
  });
});
