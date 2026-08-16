import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { txnOffsetCommitRequestV1 } from '../v1/request';
import { requestSchema, txnOffsetCommitRequestV2 } from './request';

const payload = {
  transactionalId: 'txn-1',
  groupId: 'g1',
  producerId: 20000n,
  producerEpoch: 0,
  topics: [{ topic: 'orders', partitions: [{ partition: 1, offset: 0n, leaderEpoch: -1, metadata: null }] }],
};

describe('protocol/requests/txn-offset-commit/v2/request', () => {
  it('round-trips partitions with leaderEpoch', async () => {
    const definition = txnOffsetCommitRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeString('txn-1')
      .writeString('g1')
      .writeInt64(20000n)
      .writeInt16(0)
      .writeInt32(1)
      .writeString('orders')
      .writeInt32(1)
      .writeInt32(1)
      .writeInt64(0n)
      .writeInt32(-1)
      .writeString(null);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the v1 encoding (no leaderEpoch)', async () => {
    const v2 = await txnOffsetCommitRequestV2(payload).encode();
    const v1 = await txnOffsetCommitRequestV1({
      transactionalId: payload.transactionalId,
      groupId: payload.groupId,
      producerId: payload.producerId,
      producerEpoch: payload.producerEpoch,
      topics: [{ topic: 'orders', partitions: [{ partition: 1, offset: 0n, metadata: null }] }],
    }).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
