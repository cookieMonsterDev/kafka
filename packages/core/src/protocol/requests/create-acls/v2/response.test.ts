import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { createAclsResponseV2 } from './response';

function encodeV2Response(options: { throttleTime: number; errorCode: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeInt16(options.errorCode)
    .writeUVarIntString(null)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/create-acls/v2/response', () => {
  it('decodes a flexible body, remapping throttleTime', async () => {
    const data = await createAclsResponseV2.decode(encodeV2Response({ throttleTime: 8, errorCode: 0 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      creationResponses: [{ errorCode: 0, errorMessage: null }],
    });

    await expect(createAclsResponseV2.parse(data)).resolves.toBe(data);
  });

  it('throws on a broker failure error code', async () => {
    const data = await createAclsResponseV2.decode(encodeV2Response({ throttleTime: 0, errorCode: 31 }));
    await expect(createAclsResponseV2.parse(data)).rejects.toMatchObject({ type: 'CLUSTER_AUTHORIZATION_FAILED' });
  });
});
