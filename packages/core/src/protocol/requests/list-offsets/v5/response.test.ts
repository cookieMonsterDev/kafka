import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listOffsetsResponseV5 } from './response';

function encodeV5Response(options: {
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
    .writeInt32(1)
    .writeString(options.topic)
    .writeInt32(1)
    .writeInt32(options.partition)
    .writeInt16(options.errorCode)
    .writeInt64(options.timestamp)
    .writeInt64(options.offset)
    .writeInt32(options.leaderEpoch).buffer;
}

describe('protocol/requests/list-offsets/v5/response', () => {
  it('round-trips leaderEpoch and remaps throttleTime to clientSideThrottleTime', async () => {
    const raw = encodeV5Response({
      throttleTime: 12,
      topic: 'orders',
      partition: 2,
      errorCode: 0,
      timestamp: -1n,
      offset: 99n,
      leaderEpoch: 5,
    });

    const data = await listOffsetsResponseV5.decode(raw);
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 12,
      responses: [
        {
          topic: 'orders',
          partitions: [{ partition: 2, errorCode: 0, timestamp: -1n, offset: 99n, leaderEpoch: 5 }],
        },
      ],
    });
    await expect(listOffsetsResponseV5.parse(data)).resolves.toBe(data);
  });
});
