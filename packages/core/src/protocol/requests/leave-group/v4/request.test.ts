import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { leaveGroupRequestV3 } from '../v3/request';
import { leaveGroupRequestV4, requestSchema } from './request';

const payload = {
  groupId: 'g',
  members: [{ memberId: 'm', groupInstanceId: null as string | null }],
};

describe('protocol/requests/leave-group/v4/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = leaveGroupRequestV4(payload);
    expect(definition.apiVersion).toBe(4);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeUVarInt(2)
      .writeUVarIntString('m')
      .writeUVarIntString(null)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v3 encoding', async () => {
    const v4 = await leaveGroupRequestV4(payload).encode();
    const v3 = await leaveGroupRequestV3(payload).encode();
    expect(v4.buffer).not.toEqual(v3.buffer);
  });
});
