import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listGroupsResponseV3 } from './response';

function encodeV3Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(0)
    .writeUVarInt(2)
    .writeUVarIntString('g1')
    .writeUVarIntString('consumer')
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/list-groups/v3/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await listGroupsResponseV3.decode(encodeV3Response({ throttleTime: 5 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 5,
      errorCode: 0,
      groups: [{ groupId: 'g1', protocolType: 'consumer' }],
    });
    await expect(listGroupsResponseV3.parse(data)).resolves.toBe(data);
  });
});
