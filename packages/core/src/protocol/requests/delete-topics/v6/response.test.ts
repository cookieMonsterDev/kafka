import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { deleteTopicsResponseV6 } from './response';

const topicId = Buffer.from('0123456789abcdef');

function encodeV6Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(3)
    .writeUVarIntString('payments')
    .writeBuffer(topicId)
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarInt(0)
    .writeUVarIntString('orders')
    .writeBuffer(topicId)
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/delete-topics/v6/response', () => {
  it('decodes topicId, sorts by topic name, and remaps throttleTime', async () => {
    const data = await deleteTopicsResponseV6.decode(encodeV6Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      topicErrors: [
        { topic: 'orders', topicId, errorCode: 0, errorMessage: null },
        { topic: 'payments', topicId, errorCode: 0, errorMessage: null },
      ],
    });
    await expect(deleteTopicsResponseV6.parse(data)).resolves.toBe(data);
  });
});
