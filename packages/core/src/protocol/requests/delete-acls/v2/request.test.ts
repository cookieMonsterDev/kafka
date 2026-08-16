import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { deleteAclsRequestV1 } from '../v1/request';
import { deleteAclsRequestV2, requestSchema } from './request';

const payload = {
  filters: [
    {
      resourceType: 2,
      resourceName: 'orders',
      resourcePatternType: 3,
      principal: 'User:alice',
      host: '*',
      operation: 2,
      permissionType: 3,
    },
  ],
};

describe('protocol/requests/delete-acls/v2/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = deleteAclsRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeInt8(2)
      .writeUVarIntString('orders')
      .writeInt8(3)
      .writeUVarIntString('User:alice')
      .writeUVarIntString('*')
      .writeInt8(2)
      .writeInt8(3)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const v2 = await deleteAclsRequestV2(payload).encode();
    const v1 = await deleteAclsRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
