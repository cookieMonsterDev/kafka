import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeClientQuotasResponseV1 } from './response';

function encodeV1Response(options: { throttleTime: number; errorCode: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(options.errorCode)
    .writeUVarIntString(null)
    .writeUVarInt(2)
    .writeUVarInt(2)
    .writeUVarIntString('client-id')
    .writeUVarIntString('orders-producer')
    .writeUVarInt(0)
    .writeUVarInt(2)
    .writeUVarIntString('producer_byte_rate')
    .writeDouble(1048576)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/describe-client-quotas/v1/response', () => {
  it('decodes a flexible body, remapping throttleTime', async () => {
    const data = await describeClientQuotasResponseV1.decode(encodeV1Response({ throttleTime: 8, errorCode: 0 }));

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

    await expect(describeClientQuotasResponseV1.parse(data)).resolves.toBe(data);
  });

  it('throws on a broker failure error code', async () => {
    const data = await describeClientQuotasResponseV1.decode(encodeV1Response({ throttleTime: 0, errorCode: 31 }));
    await expect(describeClientQuotasResponseV1.parse(data)).rejects.toMatchObject({
      type: 'CLUSTER_AUTHORIZATION_FAILED',
    });
  });
});
