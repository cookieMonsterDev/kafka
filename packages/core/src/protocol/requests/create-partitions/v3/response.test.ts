import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { createPartitionsResponseV3 } from './response';

function encodeV3Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeUVarIntString('orders')
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/create-partitions/v3/response', () => {
  it('decodes the same flexible body as v2', async () => {
    const data = await createPartitionsResponseV3.decode(encodeV3Response({ throttleTime: 12 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 12,
      topicErrors: [{ topic: 'orders', errorCode: 0, errorMessage: null }],
    });
    await expect(createPartitionsResponseV3.parse(data)).resolves.toBe(data);
  });
});
