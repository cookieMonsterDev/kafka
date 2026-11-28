import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { listGroupsRequestV5, requestSchema } from './request';

describe('protocol/requests/list-groups/v5/request', () => {
  it('round-trips compact statesFilter and typesFilter arrays', async () => {
    const payload = { states: ['Stable'], types: ['classic', 'consumer'] };
    const definition = listGroupsRequestV5(payload);
    expect(definition.apiVersion).toBe(5);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('Stable')
      .writeUVarInt(3)
      .writeUVarIntString('classic')
      .writeUVarIntString('consumer')
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes empty filters as compact empty arrays', async () => {
    const encoder = await listGroupsRequestV5({ states: [], types: [] }).encode();
    expect(encoder.buffer).toEqual(new Encoder().writeUVarInt(1).writeUVarInt(1).writeUVarInt(0).buffer);
  });
});
