import { describe, expect, it } from 'vitest';
import { addOffsetsToTxnRequestV3 } from '../v3/request';
import { addOffsetsToTxnRequestV4 } from './request';

const payload = { transactionalId: 'txn-1', producerId: 1001n, producerEpoch: 0, groupId: 'g1' };

describe('protocol/requests/add-offsets-to-txn/v4/request', () => {
  it('encodes the same body as v3 with apiVersion 4', async () => {
    const definition = addOffsetsToTxnRequestV4(payload);
    expect(definition.apiVersion).toBe(4);
    const v4 = await definition.encode();
    const v3 = await addOffsetsToTxnRequestV3(payload).encode();
    expect(v4.buffer).toEqual(v3.buffer);
  });
});
