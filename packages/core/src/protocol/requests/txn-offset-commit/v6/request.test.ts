import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { requestSchema, txnOffsetCommitRequestV6 } from './request';

const topicId = Buffer.alloc(16, 7);
const payload = {
  transactionalId: 'txn-1',
  groupId: 'g1',
  producerId: 20000n,
  producerEpoch: 0,
  generationId: 4,
  memberId: 'm1',
  groupInstanceId: null as string | null,
  topics: [{ topic: 'orders', topicId, partitions: [{ partition: 1, offset: 42n, leaderEpoch: 3, metadata: null }] }],
};

describe('protocol/requests/txn-offset-commit/v6/request', () => {
  it('encodes each topic by its id', () => {
    const definition = txnOffsetCommitRequestV6(payload);
    expect(definition.apiVersion).toBe(6);

    const encoder = definition.encode() as Encoder;
    const expected = new Encoder()
      .writeUVarIntString('txn-1')
      .writeUVarIntString('g1')
      .writeInt64(20000n)
      .writeInt16(0)
      .writeInt32(4)
      .writeUVarIntString('m1')
      .writeUVarIntString(null)
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(2)
      .writeInt32(1)
      .writeInt64(42n)
      .writeInt32(3)
      .writeUVarIntString(null)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({
      ...payload,
      topics: [{ topicId, partitions: payload.topics[0]!.partitions }],
    });
  });

  it('refuses to encode a topic without a usable id', () => {
    const definition = txnOffsetCommitRequestV6({
      ...payload,
      topics: [{ ...payload.topics[0]!, topicId: Buffer.alloc(16) }],
    });
    expect(() => definition.encode()).toThrow('TxnOffsetCommit v6 requires a 16-byte topicId for topic orders');
  });
});
