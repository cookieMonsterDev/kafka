import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { describeAclsRequestV1 } from '../v1/request';
import { describeAclsRequestV2, requestSchema } from './request';

const payload = {
  resourceType: 2,
  resourceName: 'orders',
  resourcePatternType: 3,
  principal: null,
  host: '*',
  operation: 2,
  permissionType: 3,
};

describe('protocol/requests/describe-acls/v2/request', () => {
  it('encodes compact strings and a TAG_BUFFER', async () => {
    const definition = describeAclsRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeInt8(2)
      .writeUVarIntString('orders')
      .writeInt8(3)
      .writeUVarIntString(null)
      .writeUVarIntString('*')
      .writeInt8(2)
      .writeInt8(3)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const v2 = await describeAclsRequestV2(payload).encode();
    const v1 = await describeAclsRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
