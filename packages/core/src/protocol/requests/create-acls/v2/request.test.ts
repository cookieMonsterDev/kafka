import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { createAclsRequestV1 } from '../v1/request';
import { createAclsRequestV2, requestSchema } from './request';

const payload = {
  creations: [
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

describe('protocol/requests/create-acls/v2/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = createAclsRequestV2(payload);
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
    const v2 = await createAclsRequestV2(payload).encode();
    const v1 = await createAclsRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
