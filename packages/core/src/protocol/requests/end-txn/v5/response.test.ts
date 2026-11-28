import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { endTxnResponseV5 } from './response';

function encodeBody({
  errorCode,
  producerId,
  producerEpoch,
}: {
  errorCode: number;
  producerId: bigint;
  producerEpoch: number;
}) {
  return new Encoder()
    .writeInt32(4)
    .writeInt16(errorCode)
    .writeInt64(producerId)
    .writeInt16(producerEpoch)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/end-txn/v5/response', () => {
  it('decodes the bumped producer id and epoch and remaps throttleTime', async () => {
    const data = await endTxnResponseV5.decode(encodeBody({ errorCode: 0, producerId: 1001n, producerEpoch: 3 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 4,
      errorCode: 0,
      producerId: 1001n,
      producerEpoch: 3,
    });
    await expect(endTxnResponseV5.parse(data)).resolves.toBe(data);
  });

  it('rejects TRANSACTION_ABORTABLE as a non-retriable error', async () => {
    const data = await endTxnResponseV5.decode(encodeBody({ errorCode: 120, producerId: -1n, producerEpoch: -1 }));

    await expect(endTxnResponseV5.parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      retriable: false,
    });
  });
});
