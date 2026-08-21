import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeTransactionsResponseV0 } from './response';

function responseFixture(errorCode = 0): Buffer {
  return new Encoder()
    .writeInt32(12)
    .writeUVarInt(2)
    .writeInt16(errorCode)
    .writeUVarIntString('tx-a')
    .writeUVarIntString('Ongoing')
    .writeInt32(60_000)
    .writeInt64(1_700_000_000_000n)
    .writeInt64(42n)
    .writeInt16(3)
    .writeUVarInt(2)
    .writeUVarIntString('orders')
    .writeUVarInt(3)
    .writeInt32(0)
    .writeInt32(2)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/describe-transactions/v0/response', () => {
  it('decodes transaction state, bigint IDs, and topic partitions', async () => {
    await expect(describeTransactionsResponseV0.decode(responseFixture())).resolves.toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 12,
      transactionStates: [
        {
          errorCode: 0,
          transactionalId: 'tx-a',
          transactionState: 'Ongoing',
          transactionTimeoutMs: 60_000,
          transactionStartTimeMs: 1_700_000_000_000n,
          producerId: 42n,
          producerEpoch: 3,
          topics: [{ topic: 'orders', partitions: [0, 2] }],
        },
      ],
    });
  });

  it('rejects a transaction-level protocol error', async () => {
    const data = await describeTransactionsResponseV0.decode(responseFixture(53));
    await expect(describeTransactionsResponseV0.parse(data)).rejects.toMatchObject({
      type: 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED',
    });
  });
});
