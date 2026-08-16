import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeAclsResponseV2 } from './response';

function encodeV2Response(options: { throttleTime: number; errorCode: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(options.errorCode)
    .writeUVarIntString(null)
    .writeUVarInt(2)
    .writeInt8(2)
    .writeUVarIntString('orders')
    .writeInt8(3)
    .writeUVarInt(2)
    .writeUVarIntString('User:alice')
    .writeUVarIntString('*')
    .writeInt8(2)
    .writeInt8(3)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/describe-acls/v2/response', () => {
  it('decodes a flexible body, remapping throttleTime', async () => {
    const data = await describeAclsResponseV2.decode(encodeV2Response({ throttleTime: 8, errorCode: 0 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      errorCode: 0,
      errorMessage: null,
      resources: [
        {
          resourceType: 2,
          resourceName: 'orders',
          resourcePatternType: 3,
          acls: [{ principal: 'User:alice', host: '*', operation: 2, permissionType: 3 }],
        },
      ],
    });

    await expect(describeAclsResponseV2.parse(data)).resolves.toBe(data);
  });

  it('throws on a broker failure error code', async () => {
    const data = await describeAclsResponseV2.decode(encodeV2Response({ throttleTime: 0, errorCode: 31 }));
    await expect(describeAclsResponseV2.parse(data)).rejects.toMatchObject({ type: 'CLUSTER_AUTHORIZATION_FAILED' });
  });
});
