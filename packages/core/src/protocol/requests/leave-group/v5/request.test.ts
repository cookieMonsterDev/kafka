import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { leaveGroupRequestV4 } from '../v4/request';
import { leaveGroupRequestV5, requestSchema } from './request';

const payload = {
  groupId: 'g',
  members: [{ memberId: 'm', groupInstanceId: null as string | null, reason: null as string | null }],
};

describe('protocol/requests/leave-group/v5/request', () => {
  it('encodes compact strings/arrays, member reason, and a TAG_BUFFER on every struct', async () => {
    const definition = leaveGroupRequestV5(payload);
    expect(definition.apiVersion).toBe(5);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeUVarInt(2)
      .writeUVarIntString('m')
      .writeUVarIntString(null)
      .writeUVarIntString(null)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes a non-null member reason', async () => {
    const encoder = await leaveGroupRequestV5({
      groupId: 'g',
      members: [{ memberId: 'm', groupInstanceId: null, reason: 'shutdown' }],
    }).encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeUVarInt(2)
      .writeUVarIntString('m')
      .writeUVarIntString(null)
      .writeUVarIntString('shutdown')
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('is not the v4 encoding', async () => {
    const v5 = await leaveGroupRequestV5(payload).encode();
    const v4 = await leaveGroupRequestV4({
      groupId: payload.groupId,
      members: payload.members.map(({ memberId, groupInstanceId }) => ({ memberId, groupInstanceId })),
    }).encode();
    expect(v5.buffer).not.toEqual(v4.buffer);
  });
});
