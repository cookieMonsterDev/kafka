import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { deleteAclsResponseV2 } from './response';

function encodeV2Response(options: { throttleTime: number; filterErrorCode: number; aclErrorCode: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeInt16(options.filterErrorCode)
    .writeUVarIntString(null)
    .writeUVarInt(2)
    .writeInt16(options.aclErrorCode)
    .writeUVarIntString(null)
    .writeInt8(2)
    .writeUVarIntString('orders')
    .writeInt8(3)
    .writeUVarIntString('User:alice')
    .writeUVarIntString('*')
    .writeInt8(2)
    .writeInt8(3)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/delete-acls/v2/response', () => {
  it('decodes a flexible body, remapping throttleTime', async () => {
    const data = await deleteAclsResponseV2.decode(
      encodeV2Response({ throttleTime: 8, filterErrorCode: 0, aclErrorCode: 0 }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      filterResponses: [
        {
          errorCode: 0,
          errorMessage: null,
          matchingAcls: [
            {
              errorCode: 0,
              errorMessage: null,
              resourceType: 2,
              resourceName: 'orders',
              resourcePatternType: 3,
              principal: 'User:alice',
              host: '*',
              operation: 2,
              permissionType: 3,
            },
          ],
        },
      ],
    });

    await expect(deleteAclsResponseV2.parse(data)).resolves.toBe(data);
  });

  it('throws on a filter-level broker failure error code', async () => {
    const data = await deleteAclsResponseV2.decode(
      encodeV2Response({ throttleTime: 0, filterErrorCode: 31, aclErrorCode: 0 }),
    );
    await expect(deleteAclsResponseV2.parse(data)).rejects.toMatchObject({ type: 'CLUSTER_AUTHORIZATION_FAILED' });
  });

  it('throws on a matching-ACL broker failure error code', async () => {
    const data = await deleteAclsResponseV2.decode(
      encodeV2Response({ throttleTime: 0, filterErrorCode: 0, aclErrorCode: 31 }),
    );
    await expect(deleteAclsResponseV2.parse(data)).rejects.toMatchObject({ type: 'CLUSTER_AUTHORIZATION_FAILED' });
  });
});
