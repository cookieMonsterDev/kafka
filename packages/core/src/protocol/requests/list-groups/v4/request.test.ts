import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { listGroupsRequestV4, requestSchema } from './request';

describe('protocol/requests/list-groups/v4/request', () => {
  it('round-trips a compact statesFilter array', async () => {
    const payload = { states: ['Stable', 'Empty'] };
    const definition = listGroupsRequestV4(payload);
    expect(definition.apiVersion).toBe(4);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeUVarIntString('Stable')
      .writeUVarIntString('Empty')
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes an empty statesFilter as a compact empty array', async () => {
    const encoder = await listGroupsRequestV4({ states: [] }).encode();
    expect(encoder.buffer).toEqual(new Encoder().writeUVarInt(1).writeUVarInt(0).buffer);
  });
});
