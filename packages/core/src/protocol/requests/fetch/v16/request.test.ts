import { describe, expect, it } from 'vitest';
import { fetchRequestV15 } from '../v15/request';
import { fetchRequestV16 } from './request';

const topicId = Buffer.from('0123456789abcdef');
const options = {
  replicaId: -1,
  maxWaitTime: 100,
  minBytes: 1,
  maxBytes: 1,
  topics: [{ topic: 'orders', topicId, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 100 }] }],
};

describe('protocol/requests/fetch/v16/request', () => {
  it('uses the same wire shape as v15 with apiVersion 16', async () => {
    expect(fetchRequestV16(options).apiVersion).toBe(16);
    const v15 = await fetchRequestV15(options).encode();
    const v16 = await fetchRequestV16(options).encode();
    expect(v16.buffer).toEqual(v15.buffer);
  });
});
