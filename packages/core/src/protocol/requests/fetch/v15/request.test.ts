import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { fetchRequestV13 } from '../v13/request';
import { requestSchemaV15 } from '../shared';
import { fetchRequestV15 } from './request';

const topicId = Buffer.from('0123456789abcdef');
const options = {
  replicaId: -1,
  maxWaitTime: 100,
  minBytes: 1,
  maxBytes: 10_485_760,
  topics: [{ topic: 'orders', topicId, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }] }],
};

describe('protocol/requests/fetch/v15/request', () => {
  it('omits replicaId from the body (ReplicaState is tagged and defaulted)', async () => {
    expect(fetchRequestV15(options).apiVersion).toBe(15);
    const v13 = await fetchRequestV13(options).encode();
    const v15 = await fetchRequestV15(options).encode();
    expect(v15.buffer).toEqual(v13.buffer.subarray(4));

    const decoded = requestSchemaV15.read(new Decoder(v15.buffer));
    expect(decoded.maxWaitTime).toBe(100);
    expect(decoded.topics[0]?.topicId).toEqual(topicId);
    expect('replicaId' in decoded).toBe(false);
  });
});
