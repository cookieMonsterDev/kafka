import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { deleteGroupsRequestV1 } from '../v1/request';
import { deleteGroupsRequestV2, requestSchema } from './request';

const payload = { groupIds: ['g1', 'g2'] };

describe('protocol/requests/delete-groups/v2/request', () => {
  it('encodes a compact array of group ids and a trailing TAG_BUFFER', async () => {
    const definition = deleteGroupsRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const expected = new Encoder().writeUVarInt(3).writeUVarIntString('g1').writeUVarIntString('g2').writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const v2 = await deleteGroupsRequestV2(payload).encode();
    const v1 = await deleteGroupsRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
