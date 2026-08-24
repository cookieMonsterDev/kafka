import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listOffsetsResponseV6 } from '../v6/response';
import { listOffsetsResponseV7 } from './response';

describe('protocol/requests/list-offsets/v7/response', () => {
  it('re-exports the v6 decoder', () => {
    expect(listOffsetsResponseV7).toBe(listOffsetsResponseV6);
  });

  it('decodes a flexible body through the v7 alias', async () => {
    const encoded = new Encoder()
      .writeInt32(4)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt16(0)
      .writeInt64(-1n)
      .writeInt64(42n)
      .writeInt32(3)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    const data = await listOffsetsResponseV7.decode(encoded);
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 4,
      responses: [
        {
          topic: 'orders',
          partitions: [{ partition: 0, errorCode: 0, timestamp: -1n, offset: 42n, leaderEpoch: 3 }],
        },
      ],
    });
    await expect(listOffsetsResponseV7.parse(data)).resolves.toBe(data);
  });
});
