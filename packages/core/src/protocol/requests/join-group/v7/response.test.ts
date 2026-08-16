import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { joinGroupResponseV7 } from './response';

const memberMetadata = Buffer.from('meta');

function encodeV7Response(options: { throttleTime: number; protocolName: string | null }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(0)
    .writeInt32(1)
    .writeUVarIntString('consumer')
    .writeUVarIntString(options.protocolName)
    .writeUVarIntString('leader-1')
    .writeUVarIntString('member-1')
    .writeUVarInt(2)
    .writeUVarIntString('member-1')
    .writeUVarIntString('instance-1')
    .writeUVarIntBytes(memberMetadata)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/join-group/v7/response', () => {
  it('decodes protocolType/protocolName and keeps groupProtocol', async () => {
    const data = await joinGroupResponseV7.decode(encodeV7Response({ throttleTime: 8, protocolName: 'AssignerName' }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      errorCode: 0,
      generationId: 1,
      protocolType: 'consumer',
      protocolName: 'AssignerName',
      groupProtocol: 'AssignerName',
      leaderId: 'leader-1',
      memberId: 'member-1',
      members: [{ memberId: 'member-1', groupInstanceId: 'instance-1', memberMetadata }],
    });
    await expect(joinGroupResponseV7.parse(data)).resolves.toBe(data);
  });

  it('maps a null protocolName onto an empty groupProtocol', async () => {
    const data = await joinGroupResponseV7.decode(encodeV7Response({ throttleTime: 0, protocolName: null }));
    expect(data.protocolName).toBeNull();
    expect(data.groupProtocol).toBe('');
  });
});
