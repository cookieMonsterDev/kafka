import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { offsetFetchResponseV5 } from './response';

function encodeV5Response(options: {
  throttleTime: number;
  topic: string;
  partition: number;
  offset: bigint;
  leaderEpoch: number;
  metadata: string | null;
  errorCode: number;
  topLevelErrorCode: number;
}): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt32(1)
    .writeString(options.topic)
    .writeInt32(1)
    .writeInt32(options.partition)
    .writeInt64(options.offset)
    .writeInt32(options.leaderEpoch)
    .writeString(options.metadata)
    .writeInt16(options.errorCode)
    .writeInt16(options.topLevelErrorCode).buffer;
}

describe('protocol/requests/offset-fetch/v5/response', () => {
  it('decodes leaderEpoch and remaps throttleTime to clientSideThrottleTime', async () => {
    const data = await offsetFetchResponseV5.decode(
      encodeV5Response({
        throttleTime: 12,
        topic: 'orders',
        partition: 2,
        offset: 99n,
        leaderEpoch: 5,
        metadata: null,
        errorCode: 0,
        topLevelErrorCode: 0,
      }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 12,
      errorCode: 0,
      responses: [
        {
          topic: 'orders',
          partitions: [{ partition: 2, offset: 99n, leaderEpoch: 5, metadata: null, errorCode: 0 }],
        },
      ],
    });
    await expect(offsetFetchResponseV5.parse(data)).resolves.toBe(data);
  });
});
