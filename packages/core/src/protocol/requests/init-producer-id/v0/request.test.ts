import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { initProducerIdRequestV0 } from './request.js';

describe('protocol/requests/init-producer-id/v0/request', () => {
  it('encodes to match the captured fixture', async () => {
    const encoder = await initProducerIdRequestV0({
      transactionalId: 'initproduceridtransaction',
      transactionTimeout: 30000,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
