import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeClientQuotasRequestV1, requestSchema } from './request';

describe('protocol/requests/describe-client-quotas/v1/request', () => {
  it('round-trips a flexible v1 request', async () => {
    const value = {
      components: [
        { entityType: 'client-id', matchType: 0, match: 'orders-producer' },
        { entityType: 'user', matchType: 1, match: null },
      ],
      strict: true,
    };

    const encoder = await describeClientQuotasRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
