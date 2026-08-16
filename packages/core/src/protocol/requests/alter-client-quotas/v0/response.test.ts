import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { alterClientQuotasResponseV0 } from './response';

function encodeV0Response(options: { throttleTime: number; errorCode: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt32(1)
    .writeInt16(options.errorCode)
    .writeString(options.errorCode === 0 ? null : 'Invalid quota')
    .writeInt32(1)
    .writeString('client-id')
    .writeString('orders-producer').buffer;
}

describe('protocol/requests/alter-client-quotas/v0/response', () => {
  it('decodes entries and remaps throttleTime to clientSideThrottleTime', async () => {
    const data = await alterClientQuotasResponseV0.decode(encodeV0Response({ throttleTime: 8, errorCode: 0 }));

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

    await expect(alterClientQuotasResponseV0.parse(data)).resolves.toBe(data);
  });

  it('throws the first entry error from parse', async () => {
    const data = await alterClientQuotasResponseV0.decode(encodeV0Response({ throttleTime: 0, errorCode: 31 }));
    await expect(alterClientQuotasResponseV0.parse(data)).rejects.toMatchObject({
      type: 'CLUSTER_AUTHORIZATION_FAILED',
    });
  });
});
