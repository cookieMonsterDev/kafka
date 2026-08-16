import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { createPartitionsResponseV2 } from './response';

function encodeV2Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeUVarIntString('orders')
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/create-partitions/v2/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await createPartitionsResponseV2.decode(encodeV2Response({ throttleTime: 12 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 12,
      topicErrors: [{ topic: 'orders', errorCode: 0, errorMessage: null }],
    });
    await expect(createPartitionsResponseV2.parse(data)).resolves.toBe(data);
  });
});
