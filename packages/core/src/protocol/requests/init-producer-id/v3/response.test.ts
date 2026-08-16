import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { initProducerIdResponseV3 } from './response';

function encodeBody({
  throttleTime,
  errorCode,
  producerId,
  producerEpoch,
}: {
  throttleTime: number;
  errorCode: number;
  producerId: bigint;
  producerEpoch: number;
}): Buffer {
  return new Encoder()
    .writeInt32(throttleTime)
    .writeInt16(errorCode)
    .writeInt64(producerId)
    .writeInt16(producerEpoch)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/init-producer-id/v3/response', () => {
  it('decodes a flexible body and remaps throttleTime to clientSideThrottleTime', async () => {
    const data = await initProducerIdResponseV3.decode(
      encodeBody({ throttleTime: 100, errorCode: 0, producerId: 1006n, producerEpoch: 2 }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 100,
      errorCode: 0,
      producerId: 1006n,
      producerEpoch: 2,
    });
    await expect(initProducerIdResponseV3.parse(data)).resolves.toBeTruthy();
  });

  it('throws if the version is not supported', async () => {
    const data = await initProducerIdResponseV3.decode(
      encodeBody({ throttleTime: 0, errorCode: 35, producerId: 0n, producerEpoch: 0 }),
    );
    await expect(initProducerIdResponseV3.parse(data)).rejects.toThrow(/The version of API is not supported/);
  });

  it('throws on a broker failure error code', async () => {
    const data = await initProducerIdResponseV3.decode(
      encodeBody({ throttleTime: 0, errorCode: 59, producerId: 1006n, producerEpoch: 0 }),
    );
    await expect(initProducerIdResponseV3.parse(data)).rejects.toMatchObject({ type: 'UNKNOWN_PRODUCER_ID' });
  });
});
