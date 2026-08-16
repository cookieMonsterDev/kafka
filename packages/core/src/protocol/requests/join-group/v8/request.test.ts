import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { joinGroupRequestV6 } from '../v6/request';
import { withDefaultMetadata } from '../v0/request';
import { joinGroupRequestV8, requestSchema } from './request';

const metadata = Buffer.from('meta');
const payload = {
  groupId: 'g',
  sessionTimeout: 30_000,
  rebalanceTimeout: 60_000,
  memberId: '',
  groupInstanceId: 'instance-1',
  protocolType: 'consumer',
  groupProtocols: withDefaultMetadata([{ name: 'AssignerName', metadata }]),
  reason: null as string | null,
};

describe('protocol/requests/join-group/v8/request', () => {
  it('encodes compact strings/arrays, reason, and a TAG_BUFFER on every struct', async () => {
    const definition = joinGroupRequestV8(payload);
    expect(definition.apiVersion).toBe(8);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeInt32(30_000)
      .writeInt32(60_000)
      .writeUVarIntString('')
      .writeUVarIntString('instance-1')
      .writeUVarIntString('consumer')
      .writeUVarInt(2)
      .writeUVarIntString('AssignerName')
      .writeUVarIntBytes(metadata)
      .writeUVarInt(0)
      .writeUVarIntString(null)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes a non-null reason', async () => {
    const encoder = await joinGroupRequestV8({ ...payload, reason: 'rejoin' }).encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeInt32(30_000)
      .writeInt32(60_000)
      .writeUVarIntString('')
      .writeUVarIntString('instance-1')
      .writeUVarIntString('consumer')
      .writeUVarInt(2)
      .writeUVarIntString('AssignerName')
      .writeUVarIntBytes(metadata)
      .writeUVarInt(0)
      .writeUVarIntString('rejoin')
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('is not the v6 encoding', async () => {
    const v8 = await joinGroupRequestV8(payload).encode();
    const v6 = await joinGroupRequestV6({
      groupId: payload.groupId,
      sessionTimeout: payload.sessionTimeout,
      rebalanceTimeout: payload.rebalanceTimeout,
      memberId: payload.memberId,
      groupInstanceId: payload.groupInstanceId,
      protocolType: payload.protocolType,
      groupProtocols: payload.groupProtocols,
    }).encode();
    expect(v8.buffer).not.toEqual(v6.buffer);
  });
});
