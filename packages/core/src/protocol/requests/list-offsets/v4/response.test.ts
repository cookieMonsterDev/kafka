import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listOffsetsResponseV4 } from './response';

describe('protocol/requests/list-offsets/v4/response', () => {
  it('decodes leaderEpoch (KIP-320) and remaps throttleTime to clientSideThrottleTime', async () => {
    const raw = new Encoder()
      .writeInt32(7)
      .writeInt32(1)
      .writeString('orders')
      .writeInt32(2)
      .writeInt32(0)
      .writeInt16(0)
      .writeInt64(-1n)
      .writeInt64(10n)
      .writeInt32(3)
      .writeInt32(1)
      .writeInt16(0)
      .writeInt64(-1n)
      .writeInt64(20n)
      .writeInt32(4).buffer;

    const data = await listOffsetsResponseV4.decode(raw);
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 7,
      responses: [
        {
          topic: 'orders',
          partitions: [
            { partition: 0, errorCode: 0, timestamp: -1n, offset: 10n, leaderEpoch: 3 },
            { partition: 1, errorCode: 0, timestamp: -1n, offset: 20n, leaderEpoch: 4 },
          ],
        },
      ],
    });
    await expect(listOffsetsResponseV4.parse(data)).resolves.toBe(data);
  });
});
