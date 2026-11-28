import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { endTxnResponseV4 } from './response';

describe('protocol/requests/end-txn/v4/response', () => {
  it('rejects TRANSACTION_ABORTABLE as a non-retriable error', async () => {
    const data = await endTxnResponseV4.decode(new Encoder().writeInt32(0).writeInt16(120).writeUVarInt(0).buffer);

    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 0, errorCode: 120 });
    await expect(endTxnResponseV4.parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      retriable: false,
    });
  });
});
