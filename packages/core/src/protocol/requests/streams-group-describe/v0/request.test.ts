import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { requestSchema, streamsGroupDescribeRequestV0 } from './request';

describe('protocol/requests/streams-group-describe/v0/request', () => {
  it('encodes group ids and the authorized-operations flag', async () => {
    const payload = { groupIds: ['app-1', 'app-2'], includeAuthorizedOperations: true };
    const encoder = await streamsGroupDescribeRequestV0(payload).encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeUVarIntString('app-1')
      .writeUVarIntString('app-2')
      .writeBoolean(true)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
