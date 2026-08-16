import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { leaveGroupResponseV4 } from './response';

function encodeV4Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(0)
    .writeUVarInt(2)
    .writeUVarIntString('m')
    .writeUVarIntString(null)
    .writeInt16(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/leave-group/v4/response', () => {
  it('decodes a flexible member batch and remaps throttleTime', async () => {
    const data = await leaveGroupResponseV4.decode(encodeV4Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      errorCode: 0,
      members: [{ memberId: 'm', groupInstanceId: null, errorCode: 0 }],
    });
    await expect(leaveGroupResponseV4.parse(data)).resolves.toBe(data);
  });
});
