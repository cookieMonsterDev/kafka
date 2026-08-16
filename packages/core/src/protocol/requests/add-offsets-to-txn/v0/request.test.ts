import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { addOffsetsToTxnRequestV0 } from './request';

describe('protocol/requests/add-offsets-to-txn/v0/request', () => {
  it('encodes to match the real fixture', async () => {
    const encoder = await addOffsetsToTxnRequestV0({
      transactionalId: 'test-transactional-id',
      producerId: 1001n,
      producerEpoch: 0,
      groupId: 'foobar',
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
