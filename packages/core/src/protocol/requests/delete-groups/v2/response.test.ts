import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { deleteGroupsResponseV2 } from './response';

function encodeV2Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeUVarIntString('g1')
    .writeInt16(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/delete-groups/v2/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await deleteGroupsResponseV2.decode(encodeV2Response({ throttleTime: 7 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 7,
      results: [{ groupId: 'g1', errorCode: 0 }],
    });
    await expect(deleteGroupsResponseV2.parse(data)).resolves.toBe(data);
  });
});
