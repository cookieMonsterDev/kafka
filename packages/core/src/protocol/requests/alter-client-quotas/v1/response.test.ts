import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { alterClientQuotasResponseV1 } from './response';

function encodeV1Response(options: { throttleTime: number; errorCode: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeInt16(options.errorCode)
    .writeUVarIntString(options.errorCode === 0 ? null : 'Invalid quota')
    .writeUVarInt(2)
    .writeUVarIntString('client-id')
    .writeUVarIntString('orders-producer')
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/alter-client-quotas/v1/response', () => {
  it('decodes a flexible body, remapping throttleTime', async () => {
    const data = await alterClientQuotasResponseV1.decode(encodeV1Response({ throttleTime: 8, errorCode: 0 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      entries: [
        {
          errorCode: 0,
          errorMessage: null,
          entity: [{ entityType: 'client-id', entityName: 'orders-producer' }],
        },
      ],
    });

    await expect(alterClientQuotasResponseV1.parse(data)).resolves.toBe(data);
  });

  it('throws the first entry error from parse', async () => {
    const data = await alterClientQuotasResponseV1.decode(encodeV1Response({ throttleTime: 0, errorCode: 31 }));
    await expect(alterClientQuotasResponseV1.parse(data)).rejects.toMatchObject({
      type: 'CLUSTER_AUTHORIZATION_FAILED',
    });
  });
});
