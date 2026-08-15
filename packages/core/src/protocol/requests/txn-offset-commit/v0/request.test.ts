import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { txnOffsetCommitRequestV0 } from './request.js';

describe('protocol/requests/txn-offset-commit/v0/request', () => {
  it('encodes to match the real fixture', async () => {
    const encoder = await txnOffsetCommitRequestV0({
      transactionalId: 'test-transactional-id',
      groupId: 'test-group-id',
      producerId: 20000n,
      producerEpoch: 0,
      topics: [
        {
          topic: 'test-topic',
          partitions: [
            { partition: 1, offset: 0n, metadata: null },
            { partition: 2, offset: 0n, metadata: null },
          ],
        },
      ],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
