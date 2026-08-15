import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { addPartitionsToTxnRequestV0 } from './request.js';

describe('protocol/requests/add-partitions-to-txn/v0/request', () => {
  it('encodes to match the real fixture', async () => {
    const encoder = await addPartitionsToTxnRequestV0({
      transactionalId: 'test-transactional-id',
      producerId: 1001n,
      producerEpoch: 0,
      topics: [{ topic: 'test-topic', partitions: [0, 1, 2, 3] }],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
