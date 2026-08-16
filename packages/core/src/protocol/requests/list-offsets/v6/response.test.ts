import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listOffsetsResponseV6 } from './response';

function encodeV6Response(options: {
  throttleTime: number;
  topic: string;
  partition: number;
  errorCode: number;
  timestamp: bigint;
  offset: bigint;
  leaderEpoch: number;
}): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeUVarIntString(options.topic)
    .writeUVarInt(2)
    .writeInt32(options.partition)
    .writeInt16(options.errorCode)
    .writeInt64(options.timestamp)
    .writeInt64(options.offset)
    .writeInt32(options.leaderEpoch)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/list-offsets/v6/response', () => {
  it('decodes a flexible body with leaderEpoch and remaps throttleTime', async () => {
    const data = await listOffsetsResponseV6.decode(
      encodeV6Response({
        throttleTime: 8,
        topic: 'orders',
        partition: 1,
        errorCode: 0,
        timestamp: -1n,
        offset: 10n,
        leaderEpoch: 6,
      }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      responses: [
        {
          topic: 'orders',
          partitions: [{ partition: 1, errorCode: 0, timestamp: -1n, offset: 10n, leaderEpoch: 6 }],
        },
      ],
    });
    await expect(listOffsetsResponseV6.parse(data)).resolves.toBe(data);
  });
});
