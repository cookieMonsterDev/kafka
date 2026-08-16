import { describe, expect, it } from 'vitest';
import { InitProducerId } from './index';

describe('protocol/requests/init-producer-id', () => {
  it('skips v2 during negotiation because Kafka 2.4 and 2.5+ disagree on that layout', () => {
    expect(InitProducerId.versions).toEqual([0, 1, 3, 4]);
  });

  it('still encodes the 2.5+ v2 layout when protocol() is called directly', () => {
    const { request } = InitProducerId.protocol({ version: 2 })({
      transactionalId: null,
      transactionTimeout: 30_000,
    });
    expect(request.apiVersion).toBe(2);
  });
});
