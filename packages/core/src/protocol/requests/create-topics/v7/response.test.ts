import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { createTopicsResponseV7 } from './response';

const topicId = Buffer.from('0123456789abcdef');

function encodeV7Response(options: {
  throttleTime: number;
  topic: string;
  topicId: Buffer;
  errorCode: number;
  numPartitions: number;
  replicationFactor: number;
}): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeUVarIntString(options.topic)
    .writeBuffer(options.topicId)
    .writeInt16(options.errorCode)
    .writeUVarIntString(null)
    .writeInt32(options.numPartitions)
    .writeInt16(options.replicationFactor)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/create-topics/v7/response', () => {
  it('decodes topicId as a 16-byte Buffer and remaps throttleTime', async () => {
    const data = await createTopicsResponseV7.decode(
      encodeV7Response({
        throttleTime: 4,
        topic: 'orders',
        topicId,
        errorCode: 0,
        numPartitions: 3,
        replicationFactor: 1,
      }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 4,
      topicErrors: [
        {
          topic: 'orders',
          topicId,
          errorCode: 0,
          errorMessage: null,
          numPartitions: 3,
          replicationFactor: 1,
          configs: null,
        },
      ],
    });
    await expect(createTopicsResponseV7.parse(data)).resolves.toBe(data);
  });
});
