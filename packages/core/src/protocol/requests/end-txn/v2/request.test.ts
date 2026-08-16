import { describe, expect, it } from 'vitest';
import { endTxnRequestV1 } from '../v1/request';
import { endTxnRequestV2 } from './request';

const payload = {
  transactionalId: 'txn-1',
  producerId: 1001n,
  producerEpoch: 0,
  transactionResult: true,
};

describe('protocol/requests/end-txn/v2/request', () => {
  it('round-trips the same wire as v1 with apiVersion 2', async () => {
    const definition = endTxnRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const v1 = await endTxnRequestV1(payload).encode();
    expect(encoder.buffer).toEqual(v1.buffer);
  });
});
