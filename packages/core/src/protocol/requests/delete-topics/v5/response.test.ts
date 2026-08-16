import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { deleteTopicsResponseV5 } from './response';

function encodeV5Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(3)
    .writeUVarIntString('payments')
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarInt(0)
    .writeUVarIntString('orders')
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/delete-topics/v5/response', () => {
  it('decodes errorMessage, sorts by topic name, and remaps throttleTime', async () => {
    const data = await deleteTopicsResponseV5.decode(encodeV5Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      topicErrors: [
        { topic: 'orders', errorCode: 0, errorMessage: null },
        { topic: 'payments', errorCode: 0, errorMessage: null },
      ],
    });
    await expect(deleteTopicsResponseV5.parse(data)).resolves.toBe(data);
  });
});
