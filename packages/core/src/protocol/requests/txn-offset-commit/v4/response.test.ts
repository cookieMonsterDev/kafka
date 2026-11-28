import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { txnOffsetCommitResponseV4 } from './response';

describe('protocol/requests/txn-offset-commit/v4/response', () => {
  it('rejects a partition that failed with TRANSACTION_ABORTABLE as a non-retriable error', async () => {
    const data = await txnOffsetCommitResponseV4.decode(
      new Encoder()
        .writeInt32(0)
        .writeUVarInt(2)
        .writeUVarIntString('orders')
        .writeUVarInt(2)
        .writeInt32(1)
        .writeInt16(120)
        .writeUVarInt(0)
        .writeUVarInt(0)
        .writeUVarInt(0).buffer,
    );

    expect(data.topics).toEqual([{ topic: 'orders', partitions: [{ partition: 1, errorCode: 120 }] }]);
    await expect(txnOffsetCommitResponseV4.parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      retriable: false,
    });
  });
});
