import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { AUTHORIZED_OPERATIONS_OMITTED } from '../shared';
import { metadataResponseV11 } from './response';

const topicId = Buffer.from('0123456789abcdef');

function writeCompactInt32Array(encoder: Encoder, values: number[]): Encoder {
  encoder.writeUVarInt(values.length + 1);
  for (const value of values) encoder.writeInt32(value);
  return encoder;
}

function encodeV11Response({ throttleTime = 0, topicErrorCode = 0, offlineReplicas = [] as number[] } = {}): Buffer {
  const partition = new Encoder().writeInt16(0).writeInt32(0).writeInt32(1).writeInt32(5);
  writeCompactInt32Array(partition, [1]);
  writeCompactInt32Array(partition, [1]);
  writeCompactInt32Array(partition, offlineReplicas);
  partition.writeUVarInt(0);

  const topic = new Encoder()
    .writeInt16(topicErrorCode)
    .writeUVarIntString('orders')
    .writeBuffer(topicId)
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

describe('protocol/requests/metadata/v11/response', () => {
  it('decodes topicId and has no clusterAuthorizedOperations', async () => {
    const data = await metadataResponseV11.decode(encodeV11Response({ throttleTime: 4, offlineReplicas: [9] }));

    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(4);
    expect(data.clusterId).toBe('cluster-1');
    expect(data).not.toHaveProperty('clusterAuthorizedOperations');
    expect(data.topicMetadata[0]?.topicId).toEqual(topicId);
    expect(data.topicMetadata[0]?.partitionMetadata[0]?.offlineReplicas).toEqual([9]);
    await expect(metadataResponseV11.parse(data)).resolves.toBeTruthy();
  });

  it('throws on a topic-level error', async () => {
    const data = await metadataResponseV11.decode(encodeV11Response({ topicErrorCode: 3 }));
    await expect(metadataResponseV11.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
      topic: 'orders',
    });
  });
});
