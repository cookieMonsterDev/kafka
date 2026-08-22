import { describe, expect, it } from 'vitest';
import { AlterShareGroupOffsets } from './index';

describe('protocol/requests/alter-share-group-offsets', () => {
  it('implements version 0', () => {
    expect(AlterShareGroupOffsets.versions).toEqual([0]);
  });

  it('builds a version 0 request', () => {
    const { request } = AlterShareGroupOffsets.protocol({ version: 0 })({
      groupId: 'g',
      topics: [{ topicName: 'events', partitions: [{ partitionIndex: 0, startOffset: 10n }] }],
    });
    expect(request).toMatchObject({ apiKey: 91, apiVersion: 0, apiName: 'AlterShareGroupOffsets' });
  });
});
