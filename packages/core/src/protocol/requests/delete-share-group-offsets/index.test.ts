import { describe, expect, it } from 'vitest';
import { DeleteShareGroupOffsets } from './index';

describe('protocol/requests/delete-share-group-offsets', () => {
  it('implements version 0', () => {
    expect(DeleteShareGroupOffsets.versions).toEqual([0]);
  });

  it('builds a version 0 request', () => {
    const { request } = DeleteShareGroupOffsets.protocol({ version: 0 })({
      groupId: 'g',
      topics: ['events'],
    });
    expect(request).toMatchObject({ apiKey: 92, apiVersion: 0, apiName: 'DeleteShareGroupOffsets' });
  });
});
