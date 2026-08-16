import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { syncGroupRequestV3 } from '../v3/request';
import { syncGroupRequestV4, requestSchema } from './request';

const assignment = Buffer.from('assign');
const payload = {
  groupId: 'g',
  generationId: 1,
  memberId: 'm',
  groupInstanceId: 'instance-1' as string | null,
  groupAssignment: [{ memberId: 'm', memberAssignment: assignment }],
};

describe('protocol/requests/sync-group/v4/request', () => {
  it('encodes compact strings/arrays/bytes and a TAG_BUFFER on every struct', async () => {
    const definition = syncGroupRequestV4(payload);
    expect(definition.apiVersion).toBe(4);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeInt32(1)
      .writeUVarIntString('m')
      .writeUVarIntString('instance-1')
      .writeUVarInt(2)
      .writeUVarIntString('m')
      .writeUVarIntBytes(assignment)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v3 encoding', async () => {
    const v4 = await syncGroupRequestV4(payload).encode();
    const v3 = await syncGroupRequestV3(payload).encode();
    expect(v4.buffer).not.toEqual(v3.buffer);
  });
});
