import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { deleteRecordsResponseV2 } from './response';

function encodeV2Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeUVarIntString('orders')
    .writeUVarInt(2)
    .writeInt32(0)
    .writeInt64(7n)
    .writeInt16(0)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/delete-records/v2/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const response = deleteRecordsResponseV2({
      topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 7n }] }],
    });
    const data = await response.decode(encodeV2Response({ throttleTime: 9 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 9,
      topics: [{ topic: 'orders', partitions: [{ partition: 0, lowWatermark: 7n, errorCode: 0 }] }],
    });
    await expect(response.parse(data)).resolves.toBe(data);
  });
});
