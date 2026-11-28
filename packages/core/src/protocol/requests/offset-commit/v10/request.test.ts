import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { offsetCommitRequestV10, requestSchema } from './request';

const topicId = Buffer.alloc(16, 7);
const payload = {
  groupId: 'g1',
  groupGenerationId: 4,
  memberId: 'm1',
  groupInstanceId: null as string | null,
  topics: [{ topic: 'orders', topicId, partitions: [{ partition: 0, offset: 42n, leaderEpoch: 3 }] }],
};

describe('protocol/requests/offset-commit/v10/request', () => {
  it('encodes each topic by its id', async () => {
    const definition = offsetCommitRequestV10(payload);
    expect(definition.apiVersion).toBe(10);

    const encoder = definition.encode() as Encoder;
    const expected = new Encoder()
      .writeUVarIntString('g1')
      .writeInt32(4)
      .writeUVarIntString('m1')
      .writeUVarIntString(null)
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt64(42n)
      .writeInt32(3)
      .writeUVarIntString(null)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({
      ...payload,
      topics: [{ topicId, partitions: [{ partition: 0, offset: 42n, leaderEpoch: 3, metadata: null }] }],
    });
  });
});
