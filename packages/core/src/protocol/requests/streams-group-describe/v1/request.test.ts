import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { requestSchema, streamsGroupDescribeRequestV1 } from './request';

describe('protocol/requests/streams-group-describe/v1/request', () => {
  it('encodes the topology-description flag after the authorized-operations flag', async () => {
    const payload = { groupIds: ['app'], includeAuthorizedOperations: false, includeTopologyDescription: true };
    const encoder = await streamsGroupDescribeRequestV1(payload).encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('app')
      .writeBoolean(false)
      .writeBoolean(true)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
