import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { initProducerIdResponseV5 } from './response';

function encodeBody(errorCode: number): Buffer {
  return new Encoder().writeInt32(0).writeInt16(errorCode).writeInt64(1006n).writeInt16(2).writeUVarInt(0).buffer;
}

describe('protocol/requests/init-producer-id/v5/response', () => {
  it('decodes the v4 body', async () => {
    const data = await initProducerIdResponseV5.decode(encodeBody(0));
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      producerId: 1006n,
      producerEpoch: 2,
    });
  });

  it('rejects TRANSACTION_ABORTABLE as a non-retriable error', async () => {
    const data = await initProducerIdResponseV5.decode(encodeBody(120));
    await expect(initProducerIdResponseV5.parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      retriable: false,
    });
  });
});
