import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { endTxnRequestV0 } from './request.js';

describe('protocol/requests/end-txn/v0/request', () => {
  it('encodes to match the real fixture', async () => {
    const encoder = await endTxnRequestV0({
      transactionalId: 'test-transactional-id',
      producerId: 1001n,
      producerEpoch: 0,
      transactionResult: true,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
