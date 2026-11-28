import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { offsetFetchResponseV8 } from './response';

function encodeV8Response(options: { groupErrorCode: number; partitionErrorCode: number }): Buffer {
  return new Encoder()
    .writeInt32(5)
    .writeUVarInt(2)
    .writeUVarIntString('g1')
    .writeUVarInt(2)
    .writeUVarIntString('orders')
    .writeUVarInt(2)
    .writeInt32(1)
    .writeInt64(10n)
    .writeInt32(6)
    .writeUVarIntString('meta')
    .writeInt16(options.partitionErrorCode)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeInt16(options.groupErrorCode)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/offset-fetch/v8/response', () => {
  it('decodes one entry per group and remaps throttleTime', async () => {
    const data = await offsetFetchResponseV8.decode(encodeV8Response({ groupErrorCode: 0, partitionErrorCode: 0 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 5,
      groups: [
        {
          groupId: 'g1',
          topics: [
            {
              topic: 'orders',
              partitions: [{ partition: 1, offset: 10n, leaderEpoch: 6, metadata: 'meta', errorCode: 0 }],
            },
          ],
          errorCode: 0,
        },
      ],
    });
    await expect(offsetFetchResponseV8.parse(data)).resolves.toBe(data);
  });

  it('throws a group-level error, then a partition-level one', async () => {
    const groupError = await offsetFetchResponseV8.decode(
      encodeV8Response({ groupErrorCode: 16, partitionErrorCode: 0 }),
    );
    await expect(offsetFetchResponseV8.parse(groupError)).rejects.toMatchObject({ type: 'NOT_COORDINATOR_FOR_GROUP' });

    const partitionError = await offsetFetchResponseV8.decode(
      encodeV8Response({ groupErrorCode: 0, partitionErrorCode: 100 }),
    );
    await expect(offsetFetchResponseV8.parse(partitionError)).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_ID' });
  });
});
