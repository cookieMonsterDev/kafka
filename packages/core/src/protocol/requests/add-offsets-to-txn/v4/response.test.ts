import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { addOffsetsToTxnResponseV4 } from './response';

describe('protocol/requests/add-offsets-to-txn/v4/response', () => {
  it('rejects TRANSACTION_ABORTABLE as a non-retriable error', async () => {
    const data = await addOffsetsToTxnResponseV4.decode(
      new Encoder().writeInt32(0).writeInt16(120).writeUVarInt(0).buffer,
    );

    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 0, errorCode: 120 });
    await expect(addOffsetsToTxnResponseV4.parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      retriable: false,
    });
  });
});
