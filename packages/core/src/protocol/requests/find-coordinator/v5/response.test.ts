import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { findCoordinatorResponseV5 } from './response';

describe('protocol/requests/find-coordinator/v5/response', () => {
  it('rejects a key that failed with TRANSACTION_ABORTABLE as a non-retriable error', async () => {
    const data = await findCoordinatorResponseV5.decode(
      new Encoder()
        .writeInt32(0)
        .writeUVarInt(2)
        .writeUVarIntString('tx-a')
        .writeInt32(-1)
        .writeUVarIntString('')
        .writeInt32(-1)
        .writeInt16(120)
        .writeUVarIntString(null)
        .writeUVarInt(0)
        .writeUVarInt(0).buffer,
    );

    await expect(findCoordinatorResponseV5.parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      retriable: false,
    });
  });
});
