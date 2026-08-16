import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { alterClientQuotasRequestV1, requestSchema } from './request';

describe('protocol/requests/alter-client-quotas/v1/request', () => {
  it('round-trips a flexible v1 request', async () => {
    const value = {
      entries: [
        {
          entity: [{ entityType: 'client-id', entityName: 'orders-producer' }],
          ops: [
            { key: 'producer_byte_rate', value: 1048576, remove: false },
            { key: 'request_percentage', value: 0, remove: true },
          ],
        },
      ],
      validateOnly: true,
    };

    const encoder = await alterClientQuotasRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
