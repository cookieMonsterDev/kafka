import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { listGroupsRequestV2 } from '../v2/request';
import { listGroupsRequestV3, requestSchema } from './request';

describe('protocol/requests/list-groups/v3/request', () => {
  it('encodes an empty flexible body (TAG_BUFFER only)', async () => {
    const definition = listGroupsRequestV3({});
    expect(definition.apiVersion).toBe(3);

    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(new Encoder().writeUVarInt(0).buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({});
  });

  it('is not the non-flexible v2 encoding', async () => {
    const v3 = await listGroupsRequestV3({}).encode();
    const v2 = await listGroupsRequestV2({}).encode();
    expect(v3.buffer).not.toEqual(v2.buffer);
  });
});
