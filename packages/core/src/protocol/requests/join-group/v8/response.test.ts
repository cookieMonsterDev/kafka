import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { joinGroupResponseV8 } from './response';

const memberMetadata = Buffer.from('meta');

function encodeV7Response(): Buffer {
  return new Encoder()
    .writeInt32(0)
    .writeInt16(0)
    .writeInt32(1)
    .writeUVarIntString('consumer')
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

describe('protocol/requests/join-group/v8/response', () => {
  it('decodes the same flexible body as v7', async () => {
    const data = await joinGroupResponseV8.decode(encodeV7Response());
    expect(data.groupProtocol).toBe('AssignerName');
    expect(data.protocolType).toBe('consumer');
    expect(data.protocolName).toBe('AssignerName');
    await expect(joinGroupResponseV8.parse(data)).resolves.toBe(data);
  });
});
