import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { joinGroupRequestV5 } from '../v5/request';
import { withDefaultMetadata } from '../v0/request';
import { joinGroupRequestV6, requestSchema } from './request';

const metadata = Buffer.from('meta');
const payload = {
  groupId: 'g',
  sessionTimeout: 30_000,
  rebalanceTimeout: 60_000,
  memberId: '',
  groupInstanceId: 'instance-1',
  protocolType: 'consumer',
  groupProtocols: withDefaultMetadata([{ name: 'AssignerName', metadata }]),
};

describe('protocol/requests/join-group/v6/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = joinGroupRequestV6(payload);
    expect(definition.apiVersion).toBe(6);

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
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v5 encoding', async () => {
    const v6 = await joinGroupRequestV6(payload).encode();
    const v5 = await joinGroupRequestV5(payload).encode();
    expect(v6.buffer).not.toEqual(v5.buffer);
  });
});
