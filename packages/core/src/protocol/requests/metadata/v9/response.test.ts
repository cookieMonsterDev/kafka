import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { AUTHORIZED_OPERATIONS_OMITTED } from '../shared';
import { metadataResponseV9 } from './response';

function writeCompactInt32Array(encoder: Encoder, values: number[]): Encoder {
  encoder.writeUVarInt(values.length + 1);
  for (const value of values) encoder.writeInt32(value);
  return encoder;
}

function encodeV9Response({
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

describe('protocol/requests/metadata/v9/response', () => {
  it('decodes the flexible v8 body, including offlineReplicas', async () => {
    const data = await metadataResponseV9.decode(
      encodeV9Response({ throttleTime: 4, offlineReplicas: [9], clusterAuthorizedOperations: 16 }),
    );

    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(4);
    expect(data.clusterId).toBe('cluster-1');
    expect(data.clusterAuthorizedOperations).toBe(16);
    expect(data.topicMetadata[0]?.partitionMetadata[0]).toEqual({
      partitionErrorCode: 0,
      partitionId: 0,
      leader: 1,
      leaderEpoch: 5,
      replicas: [1],
      isr: [1],
      offlineReplicas: [9],
    });
    await expect(metadataResponseV9.parse(data)).resolves.toBeTruthy();
  });

  it('throws on a topic-level error', async () => {
    const data = await metadataResponseV9.decode(encodeV9Response({ topicErrorCode: 3 }));
    await expect(metadataResponseV9.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
      topic: 'orders',
    });
  });
});
