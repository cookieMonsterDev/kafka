import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { addPartitionsToTxnResponseV3 } from './response';

function encodeV3Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeUVarIntString('orders')
    .writeUVarInt(2)
    .writeInt32(0)
    .writeInt16(0)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/add-partitions-to-txn/v3/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await addPartitionsToTxnResponseV3.decode(encodeV3Response({ throttleTime: 6 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 6,
      errors: [{ topic: 'orders', partitionErrors: [{ partition: 0, errorCode: 0 }] }],
    });
    await expect(addPartitionsToTxnResponseV3.parse(data)).resolves.toBe(data);
  });
});
