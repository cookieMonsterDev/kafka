import { describe, expect, it } from 'vitest';
import { fetchRequestV15 } from '../v15/request';
import { fetchRequestV18 } from './request';

const topicId = Buffer.from('0123456789abcdef');
const options = {
  replicaId: -1,
  maxWaitTime: 100,
  minBytes: 1,
  maxBytes: 1,
  topics: [{ topic: 'orders', topicId, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 100 }] }],
};

describe('protocol/requests/fetch/v18/request', () => {
  it('uses the same consumer wire shape as v15 with apiVersion 18', async () => {
    expect(fetchRequestV18(options).apiVersion).toBe(18);
    const v15 = await fetchRequestV15(options).encode();
    const v18 = await fetchRequestV18(options).encode();
    expect(v18.buffer).toEqual(v15.buffer);
  });
});
