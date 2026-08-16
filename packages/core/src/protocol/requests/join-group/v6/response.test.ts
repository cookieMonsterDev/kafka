import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { joinGroupResponseV6 } from './response';

const memberMetadata = Buffer.from('meta');

function encodeV6Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(0)
    .writeInt32(1)
    .writeUVarIntString('AssignerName')
    .writeUVarIntString('leader-1')
    .writeUVarIntString('member-1')
    .writeUVarInt(2)
    .writeUVarIntString('member-1')
    .writeUVarIntString('instance-1')
    .writeUVarIntBytes(memberMetadata)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/join-group/v6/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await joinGroupResponseV6.decode(encodeV6Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      errorCode: 0,
      generationId: 1,
      groupProtocol: 'AssignerName',
      leaderId: 'leader-1',
      memberId: 'member-1',
      members: [{ memberId: 'member-1', groupInstanceId: 'instance-1', memberMetadata }],
    });
    await expect(joinGroupResponseV6.parse(data)).resolves.toBe(data);
  });
});
