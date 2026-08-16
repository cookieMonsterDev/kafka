import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeClientQuotasRequestV0, requestSchema } from './request';

describe('protocol/requests/describe-client-quotas/v0/request', () => {
  it('round-trips a v0 request', async () => {
    const value = {
      components: [
        { entityType: 'client-id', matchType: 0, match: 'orders-producer' },
        { entityType: 'user', matchType: 1, match: null },
      ],
      strict: true,
    };

    const encoder = await describeClientQuotasRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
