import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeClientQuotasResponseV0 } from './response';

function encodeV0Response(options: { throttleTime: number; errorCode: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(options.errorCode)
    .writeString(null)
    .writeInt32(1)
    .writeInt32(1)
    .writeString('client-id')
    .writeString('orders-producer')
    .writeInt32(1)
    .writeString('producer_byte_rate')
    .writeDouble(1048576).buffer;
}

describe('protocol/requests/describe-client-quotas/v0/response', () => {
  it('decodes entries and remaps throttleTime to clientSideThrottleTime', async () => {
    const data = await describeClientQuotasResponseV0.decode(encodeV0Response({ throttleTime: 8, errorCode: 0 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      errorCode: 0,
      errorMessage: null,
      entries: [
        {
          entity: [{ entityType: 'client-id', entityName: 'orders-producer' }],
          values: [{ key: 'producer_byte_rate', value: 1048576 }],
        },
      ],
    });

    await expect(describeClientQuotasResponseV0.parse(data)).resolves.toBe(data);
  });

  it('throws on a broker failure error code', async () => {
    const data = await describeClientQuotasResponseV0.decode(encodeV0Response({ throttleTime: 0, errorCode: 31 }));
    await expect(describeClientQuotasResponseV0.parse(data)).rejects.toMatchObject({
      type: 'CLUSTER_AUTHORIZATION_FAILED',
    });
  });
});
