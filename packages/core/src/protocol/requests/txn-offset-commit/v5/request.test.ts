import { describe, expect, it } from 'vitest';
import { txnOffsetCommitRequestV3 } from '../v3/request';
import { txnOffsetCommitRequestV5 } from './request';

const payload = {
  transactionalId: 'txn-1',
  groupId: 'g1',
  producerId: 20000n,
  producerEpoch: 0,
  generationId: -1,
  memberId: '',
  groupInstanceId: null,
  topics: [{ topic: 'orders', partitions: [{ partition: 1, offset: 0n, leaderEpoch: -1, metadata: null }] }],
};

describe('protocol/requests/txn-offset-commit/v5/request', () => {
  it('encodes the same body as v3 with apiVersion 5', async () => {
    const definition = txnOffsetCommitRequestV5(payload);
    expect(definition.apiVersion).toBe(5);
    const v5 = await definition.encode();
    const v3 = await txnOffsetCommitRequestV3(payload).encode();
    expect(v5.buffer).toEqual(v3.buffer);
  });
});
