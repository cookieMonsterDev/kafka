import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listTransactionsResponseV0 } from './response';

function responseFixture(errorCode = 0): Buffer {
  return new Encoder()
    .writeInt32(12)
    .writeInt16(errorCode)
    .writeUVarInt(2)
    .writeUVarIntString('UnknownState')
    .writeUVarInt(2)
    .writeUVarIntString('tx-a')
    .writeInt64(42n)
    .writeUVarIntString('Ongoing')
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/list-transactions/v0/response', () => {
  it('decodes listings, bigint producer IDs, and unknown state filters', async () => {
    await expect(listTransactionsResponseV0.decode(responseFixture())).resolves.toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 12,
      errorCode: 0,
      unknownStateFilters: ['UnknownState'],
      transactionStates: [{ transactionalId: 'tx-a', producerId: 42n, transactionState: 'Ongoing' }],
    });
  });

  it('rejects a top-level protocol error', async () => {
    const data = await listTransactionsResponseV0.decode(responseFixture(53));
    await expect(listTransactionsResponseV0.parse(data)).rejects.toMatchObject({
      type: 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED',
    });
  });
});
