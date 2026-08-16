import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { txnOffsetCommitRequestV2 } from '../v2/request';
import { requestSchema, txnOffsetCommitRequestV3 } from './request';

const payload = {
  transactionalId: 'txn-1',
  groupId: 'g1',
  producerId: 20000n,
  producerEpoch: 0,
  generationId: -1,
  memberId: '',
  groupInstanceId: null as string | null,
  topics: [{ topic: 'orders', partitions: [{ partition: 1, offset: 0n, leaderEpoch: -1, metadata: null }] }],
};

describe('protocol/requests/txn-offset-commit/v3/request', () => {
  it('encodes compact fields including generation/member defaults', async () => {
    const definition = txnOffsetCommitRequestV3(payload);
    expect(definition.apiVersion).toBe(3);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('txn-1')
      .writeUVarIntString('g1')
      .writeInt64(20000n)
      .writeInt16(0)
      .writeInt32(-1)
      .writeUVarIntString('')
      .writeUVarIntString(null)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(1)
      .writeInt64(0n)
      .writeInt32(-1)
      .writeUVarIntString(null)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v2 encoding', async () => {
    const v3 = await txnOffsetCommitRequestV3(payload).encode();
    const v2 = await txnOffsetCommitRequestV2({
      transactionalId: payload.transactionalId,
      groupId: payload.groupId,
      producerId: payload.producerId,
      producerEpoch: payload.producerEpoch,
      topics: payload.topics,
    }).encode();
    expect(v3.buffer).not.toEqual(v2.buffer);
  });
});
