import { describe, expect, it } from 'vitest';
import { fetchRequestV13 } from '../v13/request';
import { fetchRequestV14 } from './request';

const topicId = Buffer.from('0123456789abcdef');
const options = {
  replicaId: -1,
  maxWaitTime: 100,
  minBytes: 1,
  maxBytes: 1,
  topics: [{ topic: 'orders', topicId, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 100 }] }],
};

describe('protocol/requests/fetch/v14/request', () => {
  it('uses the same wire shape as v13 with apiVersion 14', async () => {
    expect(fetchRequestV14(options).apiVersion).toBe(14);
    const v13 = await fetchRequestV13(options).encode();
    const v14 = await fetchRequestV14(options).encode();
    expect(v14.buffer).toEqual(v13.buffer);
  });
});
