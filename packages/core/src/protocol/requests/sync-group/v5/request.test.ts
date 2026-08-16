import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { syncGroupRequestV4 } from '../v4/request';
import { syncGroupRequestV5, requestSchema } from './request';

const assignment = Buffer.from('assign');
const payload = {
  groupId: 'g',
  generationId: 1,
  memberId: 'm',
  groupInstanceId: 'instance-1' as string | null,
  protocolType: 'consumer' as string | null,
  protocolName: 'AssignerName' as string | null,
  groupAssignment: [{ memberId: 'm', memberAssignment: assignment }],
};

describe('protocol/requests/sync-group/v5/request', () => {
  it('encodes protocolType and protocolName after groupInstanceId', async () => {
    const definition = syncGroupRequestV5(payload);
    expect(definition.apiVersion).toBe(5);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeInt32(1)
      .writeUVarIntString('m')
      .writeUVarIntString('instance-1')
      .writeUVarIntString('consumer')
      .writeUVarIntString('AssignerName')
      .writeUVarInt(2)
      .writeUVarIntString('m')
      .writeUVarIntBytes(assignment)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes null protocol type/name', async () => {
    const encoder = await syncGroupRequestV5({ ...payload, protocolType: null, protocolName: null }).encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeInt32(1)
      .writeUVarIntString('m')
      .writeUVarIntString('instance-1')
      .writeUVarIntString(null)
      .writeUVarIntString(null)
      .writeUVarInt(2)
      .writeUVarIntString('m')
      .writeUVarIntBytes(assignment)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('is not the v4 encoding', async () => {
    const v5 = await syncGroupRequestV5(payload).encode();
    const v4 = await syncGroupRequestV4({
      groupId: payload.groupId,
      generationId: payload.generationId,
      memberId: payload.memberId,
      groupInstanceId: payload.groupInstanceId,
      groupAssignment: payload.groupAssignment,
    }).encode();
    expect(v5.buffer).not.toEqual(v4.buffer);
  });
});
