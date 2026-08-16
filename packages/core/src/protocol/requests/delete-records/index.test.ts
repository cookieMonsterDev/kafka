import { describe, expect, it } from 'vitest';
import { DeleteRecords } from './index';

describe('protocol/requests/delete-records', () => {
  it('implements versions 0 through 1', () => {
    expect(DeleteRecords.versions).toEqual([0, 1]);
  });
});
