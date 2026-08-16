import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { deleteTopicsResponseV4 } from './response';

function encodeV4Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(3)
    .writeUVarIntString('payments')
    .writeInt16(0)
    .writeUVarInt(0)
    .writeUVarIntString('orders')
    .writeInt16(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/delete-topics/v4/response', () => {
  it('decodes a flexible body, sorts by topic name, and remaps throttleTime', async () => {
    const data = await deleteTopicsResponseV4.decode(encodeV4Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      topicErrors: [
        { topic: 'orders', errorCode: 0 },
        { topic: 'payments', errorCode: 0 },
      ],
    });
    await expect(deleteTopicsResponseV4.parse(data)).resolves.toBe(data);
  });
});
