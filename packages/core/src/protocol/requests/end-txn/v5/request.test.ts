import { describe, expect, it } from 'vitest';
import { endTxnRequestV3 } from '../v3/request';
import { endTxnRequestV5 } from './request';

const payload = { transactionalId: 'txn-1', producerId: 1001n, producerEpoch: 0, transactionResult: false };

describe('protocol/requests/end-txn/v5/request', () => {
  it('encodes the same body as v3 with apiVersion 5', async () => {
    const definition = endTxnRequestV5(payload);
    expect(definition.apiVersion).toBe(5);
    const v5 = await definition.encode();
    const v3 = await endTxnRequestV3(payload).encode();
    expect(v5.buffer).toEqual(v3.buffer);
  });
});
