import { describe, expect, it } from 'vitest';
import { ShareFetch } from './index';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/share-fetch', () => {
  it('implements version 1', () => {
    expect(ShareFetch.versions).toEqual([1]);
  });

  it('builds a version 1 request', () => {
    const { request } = ShareFetch.protocol({ version: 1 })({
      groupId: 'g',
      memberId: 'm',
      shareSessionEpoch: 0,
      maxWaitMs: 500,
      minBytes: 1,
      maxRecords: 100,
      batchSize: 10,
      topics: [{ topicId, partitions: [{ partitionIndex: 0 }] }],
    });
    expect(request).toMatchObject({ apiKey: 78, apiVersion: 1, apiName: 'ShareFetch' });
  });
});
