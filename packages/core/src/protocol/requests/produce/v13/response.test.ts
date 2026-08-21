import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { produceResponseV13 } from './response';

const topicId = Buffer.from('0123456789abcdef');

function encodeV13Response(errorCode = 0, errorMessage: string | null = null): Buffer {
  return new Encoder()
    .writeUVarIntArray([
      new Encoder()
        .writeBuffer(topicId)
        .writeUVarIntArray([
          new Encoder()
            .writeInt32(1)
            .writeInt16(errorCode)
            .writeInt64(0n)
            .writeInt64(-1n)
            .writeInt64(0n)
            .writeUVarIntArray([])
            .writeUVarIntString(errorMessage)
            .writeUVarInt(0),
        ])
        .writeUVarInt(0),
    ])
    .writeInt32(20)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/produce/v13/response', () => {
  it('decodes topicId and restores topicName from the request', async () => {
    const data = await produceResponseV13({
      topicData: [{ topic: 'test-topic', topicId, partitions: [] }],
    }).decode(encodeV13Response());

    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic',
          topicId,
          partitions: [
            {
              partition: 1,
              errorCode: 0,
              baseOffset: 0n,
              logAppendTime: -1n,
              logStartOffset: 0n,
              recordErrors: [],
              errorMessage: null,
            },
          ],
        },
      ],
      throttleTime: 0,
      clientSideThrottleTime: 20,
    });
  });

  it('still throws from the partition error_code with the restored topic name', async () => {
    const response = produceResponseV13({
      topicData: [{ topic: 'test-topic', topicId, partitions: [] }],
    });
    const data = await response.decode(encodeV13Response(3, 'unknown topic'));
    await expect(response.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
      topic: 'test-topic',
      partition: 1,
    });
  });
});
