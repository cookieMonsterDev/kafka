import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { fetchResponseV13 } from './response';

const topicId = Buffer.from('0123456789abcdef');

function encodeV13Response(): Buffer {
  return new Encoder()
    .writeInt32(5)
    .writeInt16(0)
    .writeInt32(42)
    .writeUVarInt(2)
    .writeBuffer(topicId)
    .writeUVarInt(2)
    .writeInt32(0)
    .writeInt16(0)
    .writeInt64(10n)
    .writeInt64(10n)
    .writeInt64(0n)
    .writeUVarInt(1)
    .writeInt32(-1)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/fetch/v13/response', () => {
  it('decodes topicId and restores topicName from the request', async () => {
    const decoded = await fetchResponseV13({
      topics: [{ topic: 'orders', topicId, partitions: [] }],
    }).decode(encodeV13Response());

    expect(decoded).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 5,
      errorCode: 0,
      sessionId: 42,
      responses: [
        {
          topicName: 'orders',
          topicId,
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: 10n,
              lastStableOffset: 10n,
              logStartOffset: 0n,
              abortedTransactions: [],
              preferredReadReplica: -1,
              messages: [],
            },
          ],
        },
      ],
    });
  });

  it('still throws from the partition error_code with the restored topic name', async () => {
    const encoded = new Encoder()
      .writeInt32(0)
      .writeInt16(0)
      .writeInt32(0)
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(2)
      .writeInt32(1)
      .writeInt16(1)
      .writeInt64(0n)
      .writeInt64(0n)
      .writeInt64(0n)
      .writeUVarInt(1)
      .writeInt32(-1)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    const response = fetchResponseV13({
      topics: [{ topic: 'orders', topicId, partitions: [] }],
    });
    const data = await response.decode(encoded);
    await expect(response.parse(data)).rejects.toMatchObject({
      type: 'OFFSET_OUT_OF_RANGE',
      topic: 'orders',
      partition: 1,
    });
  });
});
