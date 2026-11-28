import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { offsetCommitResponseV10 } from './response';

const ordersId = Buffer.alloc(16, 1);

function encodeV10Response(errorCode: number): Buffer {
  return new Encoder()
    .writeInt32(8)
    .writeUVarInt(2)
    .writeBuffer(ordersId)
    .writeUVarInt(2)
    .writeInt32(0)
    .writeInt16(errorCode)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/offset-commit/v10/response', () => {
  const response = offsetCommitResponseV10([{ topic: 'orders', topicId: ordersId, partitions: [] }]);

  it("restores each topic's name from the request and remaps throttleTime", async () => {
    const data = await response.decode(encodeV10Response(0));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      responses: [{ topic: 'orders', topicId: ordersId, partitions: [{ partition: 0, errorCode: 0 }] }],
    });
    await expect(response.parse(data)).resolves.toBe(data);
  });

  it('throws UNKNOWN_TOPIC_ID from a partition', async () => {
    const data = await response.decode(encodeV10Response(100));
    await expect(response.parse(data)).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_ID' });
  });
});
