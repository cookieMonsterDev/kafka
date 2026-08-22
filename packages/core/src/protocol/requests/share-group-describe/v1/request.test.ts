import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { shareGroupDescribeRequestV1, requestSchema } from './request';

describe('protocol/requests/share-group-describe/v1/request', () => {
  it('encodes group ids and the authorized-operations flag', async () => {
    const payload = { groupIds: ['g1', 'g2'], includeAuthorizedOperations: true };
    const encoder = await shareGroupDescribeRequestV1(payload).encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeUVarIntString('g1')
      .writeUVarIntString('g2')
      .writeBoolean(true)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
