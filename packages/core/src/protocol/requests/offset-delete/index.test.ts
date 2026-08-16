import { describe, expect, it } from 'vitest';
import { OffsetDelete } from './index';

describe('protocol/requests/offset-delete', () => {
  it('implements versions 0 through 1', () => {
    expect(OffsetDelete.versions).toEqual([0, 1]);
  });

  it('builds a request for the requested version', () => {
    const options = { groupId: 'g', topics: [{ topic: 'orders', partitions: [0] }] };
    expect(OffsetDelete.protocol({ version: 0 })(options).request.apiVersion).toBe(0);
    expect(OffsetDelete.protocol({ version: 1 })(options).request.apiVersion).toBe(1);
  });
});
