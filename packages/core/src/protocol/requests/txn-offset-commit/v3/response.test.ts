import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { txnOffsetCommitResponseV3 } from './response';

function encodeV3Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeUVarIntString('orders')
    .writeUVarInt(2)
    .writeInt32(1)
    .writeInt16(0)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/txn-offset-commit/v3/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await txnOffsetCommitResponseV3.decode(encodeV3Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      topics: [{ topic: 'orders', partitions: [{ partition: 1, errorCode: 0 }] }],
    });
    await expect(txnOffsetCommitResponseV3.parse(data)).resolves.toBe(data);
  });
});
