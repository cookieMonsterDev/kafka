import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { leaveGroupResponseV5 } from './response';

function encodeV4Response(): Buffer {
  return new Encoder()
    .writeInt32(0)
    .writeInt16(0)
    .writeUVarInt(2)
    .writeUVarIntString('m')
    .writeUVarIntString(null)
    .writeInt16(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/leave-group/v5/response', () => {
  it('decodes the same flexible body as v4', async () => {
    const data = await leaveGroupResponseV5.decode(encodeV4Response());
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      members: [{ memberId: 'm', groupInstanceId: null, errorCode: 0 }],
    });
    await expect(leaveGroupResponseV5.parse(data)).resolves.toBe(data);
  });
});
