import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { offsetFetchRequestV10, requestSchema } from './request';

const topicId = Buffer.alloc(16, 7);
const payload = {
  groups: [
    {
      groupId: 'g1',
      memberId: 'm1',
      memberEpoch: 7,
      topics: [{ topic: 'orders', topicId, partitions: [{ partition: 2 }] }],
    },
  ],
  requireStable: false,
};

describe('protocol/requests/offset-fetch/v10/request', () => {
  it('encodes each topic by its id', async () => {
    const definition = offsetFetchRequestV10(payload);
    expect(definition.apiVersion).toBe(10);

    const encoder = definition.encode() as Encoder;
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('g1')
      .writeUVarIntString('m1')
      .writeInt32(7)
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(2)
      .writeInt32(2)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({
      groups: [
        { groupId: 'g1', memberId: 'm1', memberEpoch: 7, topics: [{ topicId, partitions: [{ partition: 2 }] }] },
      ],
      requireStable: false,
    });
  });
});
