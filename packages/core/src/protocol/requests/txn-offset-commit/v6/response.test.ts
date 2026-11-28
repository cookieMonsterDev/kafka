import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { txnOffsetCommitResponseV6 } from './response';

const ordersId = Buffer.alloc(16, 1);

function encodeV6Response(errorCode: number): Buffer {
  return new Encoder()
    .writeInt32(8)
    .writeUVarInt(2)
    .writeBuffer(ordersId)
    .writeUVarInt(2)
    .writeInt32(1)
    .writeInt16(errorCode)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/txn-offset-commit/v6/response', () => {
  const response = txnOffsetCommitResponseV6([{ topic: 'orders', topicId: ordersId }]);

  it("restores each topic's name from the request and remaps throttleTime", async () => {
    const data = await response.decode(encodeV6Response(0));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      topics: [{ topic: 'orders', topicId: ordersId, partitions: [{ partition: 1, errorCode: 0 }] }],
    });
    await expect(response.parse(data)).resolves.toBe(data);
  });

  it.each([
    ['GROUP_ID_NOT_FOUND', 69, false],
    ['UNKNOWN_TOPIC_ID', 100, true],
    ['STALE_MEMBER_EPOCH', 113, true],
  ])('throws %s from a partition', async (type, errorCode, retriable) => {
    const data = await response.decode(encodeV6Response(errorCode));
    await expect(response.parse(data)).rejects.toMatchObject({ type, retriable });
  });
});
