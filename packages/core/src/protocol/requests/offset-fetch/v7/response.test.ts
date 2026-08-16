import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { offsetFetchResponseV7 } from './response';

function encodeV6Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeUVarIntString('orders')
    .writeUVarInt(2)
    .writeInt32(1)
    .writeInt64(10n)
    .writeInt32(6)
    .writeUVarIntString(null)
    .writeInt16(0)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeInt16(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/offset-fetch/v7/response', () => {
  it('decodes the v6 wire format, remapping throttleTime', async () => {
    const data = await offsetFetchResponseV7.decode(encodeV6Response({ throttleTime: 3 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 3,
      errorCode: 0,
      responses: [
        {
          topic: 'orders',
          partitions: [{ partition: 1, offset: 10n, leaderEpoch: 6, metadata: null, errorCode: 0 }],
        },
      ],
    });
    await expect(offsetFetchResponseV7.parse(data)).resolves.toBe(data);
  });
});
