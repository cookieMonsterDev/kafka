import { describe, expect, it } from 'vitest';
import { initProducerIdRequestV4 } from '../v4/request';
import { initProducerIdRequestV5 } from './request';

const payload = { transactionalId: 'txn-id', transactionTimeout: 30_000, producerId: 1006n, producerEpoch: 3 };

describe('protocol/requests/init-producer-id/v5/request', () => {
  it('encodes the same body as v4 with apiVersion 5', async () => {
    const definition = initProducerIdRequestV5(payload);
    expect(definition.apiVersion).toBe(5);
    const v5 = await definition.encode();
    const v4 = await initProducerIdRequestV4(payload).encode();
    expect(v5.buffer).toEqual(v4.buffer);
  });
});
