import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { offsetFetchResponseV10 } from './response';

const ordersId = Buffer.alloc(16, 1);
const paymentsId = Buffer.alloc(16, 2);

function encodeV10Response(topicIds: Buffer[], errorCode = 0): Buffer {
  const encoder = new Encoder()
    .writeInt32(4)
    .writeUVarInt(2)
    .writeUVarIntString('g1')
    .writeUVarInt(topicIds.length + 1);
  for (const topicId of topicIds) {
    encoder
      .writeBuffer(topicId)
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt64(3n)
      .writeInt32(-1)
      .writeUVarIntString(null)
      .writeInt16(errorCode)
      .writeUVarInt(0)
      .writeUVarInt(0);
  }
  return encoder.writeInt16(0).writeUVarInt(0).writeUVarInt(0).buffer;
}

describe('protocol/requests/offset-fetch/v10/response', () => {
  const requestedGroups = [
    {
      groupId: 'g1',
      topics: [
        { topic: 'orders', topicId: ordersId, partitions: [{ partition: 0 }] },
        { topic: 'payments', topicId: paymentsId, partitions: [{ partition: 0 }] },
      ],
    },
  ];

  it("restores each topic's name from the requested group", async () => {
    const response = offsetFetchResponseV10(requestedGroups);
    const data = await response.decode(encodeV10Response([paymentsId, ordersId]));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 4,
      groups: [
        {
          groupId: 'g1',
          errorCode: 0,
          topics: [
            {
              topic: 'payments',
              topicId: paymentsId,
              partitions: [{ partition: 0, offset: 3n, leaderEpoch: -1, metadata: null, errorCode: 0 }],
            },
            {
              topic: 'orders',
              topicId: ordersId,
              partitions: [{ partition: 0, offset: 3n, leaderEpoch: -1, metadata: null, errorCode: 0 }],
            },
          ],
        },
      ],
    });
    await expect(response.parse(data)).resolves.toBe(data);
  });

  it('throws UNKNOWN_TOPIC_ID from a partition', async () => {
    const response = offsetFetchResponseV10(requestedGroups);
    const data = await response.decode(encodeV10Response([ordersId], 100));
    await expect(response.parse(data)).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_ID', retriable: true });
  });
});
