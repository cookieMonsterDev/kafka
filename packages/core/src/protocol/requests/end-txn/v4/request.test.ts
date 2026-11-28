import { describe, expect, it } from 'vitest';
import { endTxnRequestV3 } from '../v3/request';
import { endTxnRequestV4 } from './request';

const payload = { transactionalId: 'txn-1', producerId: 1001n, producerEpoch: 0, transactionResult: true };

describe('protocol/requests/end-txn/v4/request', () => {
  it('encodes the same body as v3 with apiVersion 4', async () => {
    const definition = endTxnRequestV4(payload);
    expect(definition.apiVersion).toBe(4);
    const v4 = await definition.encode();
    const v3 = await endTxnRequestV3(payload).encode();
    expect(v4.buffer).toEqual(v3.buffer);
  });
});
