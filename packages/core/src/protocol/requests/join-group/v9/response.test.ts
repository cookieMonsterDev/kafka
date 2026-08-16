import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { joinGroupResponseV9 } from './response';

const memberMetadata = Buffer.from('meta');

function encodeV9Response(options: { throttleTime: number; skipAssignment: boolean }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(0)
    .writeInt32(1)
    .writeUVarIntString('consumer')
    .writeUVarIntString('AssignerName')
    .writeUVarIntString('leader-1')
    .writeBoolean(options.skipAssignment)
    .writeUVarIntString('member-1')
    .writeUVarInt(2)
    .writeUVarIntString('member-1')
    .writeUVarIntString('instance-1')
    .writeUVarIntBytes(memberMetadata)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/join-group/v9/response', () => {
  it('decodes skipAssignment after leaderId and keeps groupProtocol', async () => {
    const data = await joinGroupResponseV9.decode(encodeV9Response({ throttleTime: 8, skipAssignment: true }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      errorCode: 0,
      generationId: 1,
      protocolType: 'consumer',
      protocolName: 'AssignerName',
      groupProtocol: 'AssignerName',
      leaderId: 'leader-1',
      skipAssignment: true,
      memberId: 'member-1',
      members: [{ memberId: 'member-1', groupInstanceId: 'instance-1', memberMetadata }],
    });
    await expect(joinGroupResponseV9.parse(data)).resolves.toBe(data);
  });
});
