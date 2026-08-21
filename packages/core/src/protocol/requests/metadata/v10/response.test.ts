import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { AUTHORIZED_OPERATIONS_OMITTED } from '../shared';
import { metadataResponseV10 } from './response';

const topicId = Buffer.from('0123456789abcdef');

function writeCompactInt32Array(encoder: Encoder, values: number[]): Encoder {
  encoder.writeUVarInt(values.length + 1);
  for (const value of values) encoder.writeInt32(value);
  return encoder;
}

function encodeV10Response({
  throttleTime = 0,
  topicErrorCode = 0,
  offlineReplicas = [] as number[],
  clusterAuthorizedOperations = AUTHORIZED_OPERATIONS_OMITTED,
} = {}): Buffer {
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
    .writeInt32(clusterAuthorizedOperations)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/metadata/v10/response', () => {
  it('decodes topicId and remaps throttleTime', async () => {
    const data = await metadataResponseV10.decode(
      encodeV10Response({ throttleTime: 4, offlineReplicas: [9], clusterAuthorizedOperations: 16 }),
    );

    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(4);
    expect(data.clusterId).toBe('cluster-1');
    expect(data.clusterAuthorizedOperations).toBe(16);
    expect(data.topicMetadata[0]?.topic).toBe('orders');
    expect(data.topicMetadata[0]?.topicId).toEqual(topicId);
    expect(data.topicMetadata[0]?.partitionMetadata[0]).toEqual({
      partitionErrorCode: 0,
      partitionId: 0,
      leader: 1,
      leaderEpoch: 5,
      replicas: [1],
      isr: [1],
      offlineReplicas: [9],
    });
    await expect(metadataResponseV10.parse(data)).resolves.toBeTruthy();
  });

  it('throws on a topic-level error', async () => {
    const data = await metadataResponseV10.decode(encodeV10Response({ topicErrorCode: 3 }));
    await expect(metadataResponseV10.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
      topic: 'orders',
    });
  });
});
