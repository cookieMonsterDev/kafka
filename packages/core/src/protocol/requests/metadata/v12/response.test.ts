import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { AUTHORIZED_OPERATIONS_OMITTED, ZERO_TOPIC_ID } from '../shared';
import { metadataResponseV12 } from './response';

const topicId = Buffer.from('0123456789abcdef');

function writeCompactInt32Array(encoder: Encoder, values: number[]): Encoder {
  encoder.writeUVarInt(values.length + 1);
  for (const value of values) encoder.writeInt32(value);
  return encoder;
}

function encodeV12Response({
  throttleTime = 0,
  topicErrorCode = 0,
  topicName = 'orders',
  topicId: id = topicId,
}: {
  throttleTime?: number;
  topicErrorCode?: number;
  topicName?: string | null;
  topicId?: Buffer;
} = {}): Buffer {
  const partition = new Encoder().writeInt16(0).writeInt32(0).writeInt32(1).writeInt32(5);
  writeCompactInt32Array(partition, [1]);
  writeCompactInt32Array(partition, [1]);
  writeCompactInt32Array(partition, []);
  partition.writeUVarInt(0);

  const topic = new Encoder()
    .writeInt16(topicErrorCode)
    .writeUVarIntString(topicName)
    .writeBuffer(id)
    .writeBoolean(false)
    .writeUVarInt(2)
    .writeEncoder(partition)
    .writeInt32(AUTHORIZED_OPERATIONS_OMITTED)
    .writeUVarInt(0);

  const broker = new Encoder()
    .writeInt32(1)
    .writeUVarIntString('localhost')
    .writeInt32(9092)
    .writeUVarIntString(null)
    .writeUVarInt(0);

  return new Encoder()
    .writeInt32(throttleTime)
    .writeUVarInt(2)
    .writeEncoder(broker)
    .writeUVarIntString('cluster-1')
    .writeInt32(1)
    .writeUVarInt(2)
    .writeEncoder(topic)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/metadata/v12/response', () => {
  it('decodes a named topic with topicId', async () => {
    const data = await metadataResponseV12.decode(encodeV12Response({ throttleTime: 4 }));

    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(4);
    expect(data.topicMetadata[0]?.topic).toBe('orders');
    expect(data.topicMetadata[0]?.topicId).toEqual(topicId);
    expect(data).not.toHaveProperty('clusterAuthorizedOperations');
    await expect(metadataResponseV12.parse(data)).resolves.toBeTruthy();
  });

  it('decodes a null topic name when queried by id and missing', async () => {
    const data = await metadataResponseV12.decode(
      encodeV12Response({ topicErrorCode: 3, topicName: null, topicId: ZERO_TOPIC_ID }),
    );

    expect(data.topicMetadata[0]?.topic).toBeNull();
    expect(data.topicMetadata[0]?.topicId).toEqual(ZERO_TOPIC_ID);
    await expect(metadataResponseV12.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    });
  });
});
