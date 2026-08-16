import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { describeGroupsRequestV4 } from '../v4/request';
import { describeGroupsRequestV5, requestSchema } from './request';

const payload = { groupIds: ['g1', 'g2'], includeAuthorizedOperations: true };

describe('protocol/requests/describe-groups/v5/request', () => {
  it('encodes compact group ids and a trailing TAG_BUFFER', async () => {
    const definition = describeGroupsRequestV5(payload);
    expect(definition.apiVersion).toBe(5);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeUVarIntString('g1')
      .writeUVarIntString('g2')
      .writeBoolean(true)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v4 encoding', async () => {
    const v5 = await describeGroupsRequestV5(payload).encode();
    const v4 = await describeGroupsRequestV4(payload).encode();
    expect(v5.buffer).not.toEqual(v4.buffer);
  });
});
