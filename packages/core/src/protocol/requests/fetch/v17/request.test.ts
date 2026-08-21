import { describe, expect, it } from 'vitest';
import { fetchRequestV15 } from '../v15/request';
import { fetchRequestV17 } from './request';

const topicId = Buffer.from('0123456789abcdef');
const options = {
  replicaId: -1,
  maxWaitTime: 100,
  minBytes: 1,
  maxBytes: 1,
  topics: [{ topic: 'orders', topicId, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 100 }] }],
};

describe('protocol/requests/fetch/v17/request', () => {
  it('uses the same consumer wire shape as v15 with apiVersion 17', async () => {
    expect(fetchRequestV17(options).apiVersion).toBe(17);
    const v15 = await fetchRequestV15(options).encode();
    const v17 = await fetchRequestV17(options).encode();
    expect(v17.buffer).toEqual(v15.buffer);
  });
});
