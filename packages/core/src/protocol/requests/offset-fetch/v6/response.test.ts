import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { offsetFetchResponseV6 } from './response';

function encodeV6Response(options: {
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
    .writeUVarInt(2)
    .writeUVarIntString(options.topic)
    .writeUVarInt(2)
    .writeInt32(options.partition)
    .writeInt64(options.offset)
    .writeInt32(options.leaderEpoch)
    .writeUVarIntString(options.metadata)
    .writeInt16(options.errorCode)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeInt16(options.topLevelErrorCode)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/offset-fetch/v6/response', () => {
  it('decodes a flexible body with leaderEpoch and remaps throttleTime', async () => {
    const data = await offsetFetchResponseV6.decode(
      encodeV6Response({
        throttleTime: 8,
        topic: 'orders',
        partition: 1,
        offset: 10n,
        leaderEpoch: 6,
        metadata: null,
        errorCode: 0,
        topLevelErrorCode: 0,
      }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      errorCode: 0,
      responses: [
        {
          topic: 'orders',
          partitions: [{ partition: 1, offset: 10n, leaderEpoch: 6, metadata: null, errorCode: 0 }],
        },
      ],
    });
    await expect(offsetFetchResponseV6.parse(data)).resolves.toBe(data);
  });
});
