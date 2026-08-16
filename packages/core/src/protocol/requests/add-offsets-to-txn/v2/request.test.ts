import { describe, expect, it } from 'vitest';
import { addOffsetsToTxnRequestV1 } from '../v1/request';
import { addOffsetsToTxnRequestV2 } from './request';

const payload = {
  transactionalId: 'txn-1',
  producerId: 1001n,
  producerEpoch: 0,
  groupId: 'g1',
};

describe('protocol/requests/add-offsets-to-txn/v2/request', () => {
  it('round-trips the same wire as v1 with apiVersion 2', async () => {
    const definition = addOffsetsToTxnRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const v1 = await addOffsetsToTxnRequestV1(payload).encode();
    expect(encoder.buffer).toEqual(v1.buffer);
  });
});
