import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { metadataResponseV7 } from './response';

function encodeV7Response({
  throttleTime = 0,
  topicErrorCode = 0,
  partitionErrorCode = 0,
  offlineReplicas = [] as number[],
  leaderEpoch = 3,
} = {}): Buffer {
  return new Encoder()
    .writeInt32(throttleTime)
    .writeInt32(1)
    .writeInt32(1)
    .writeString('localhost')
    .writeInt32(9092)
    .writeString(null)
    .writeString('cluster-1')
    .writeInt32(1)
    .writeInt32(1)
    .writeInt16(topicErrorCode)
    .writeString('orders')
    .writeBoolean(false)
    .writeInt32(1)
    .writeInt16(partitionErrorCode)
    .writeInt32(0)
    .writeInt32(1)
    .writeInt32(leaderEpoch)
    .writeArray([1], 'int32')
    .writeArray([1], 'int32')
    .writeArray(offlineReplicas, 'int32').buffer;
}

describe('protocol/requests/metadata/v7/response', () => {
  it('decodes leaderEpoch and offlineReplicas, remapping throttleTime', async () => {
    const data = await metadataResponseV7.decode(
      encodeV7Response({ throttleTime: 12, offlineReplicas: [2], leaderEpoch: 7 }),
    );

    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(12);
    expect(data.clusterId).toBe('cluster-1');
    expect(data.topicMetadata[0]?.partitionMetadata[0]).toEqual({
      partitionErrorCode: 0,
      partitionId: 0,
      leader: 1,
      leaderEpoch: 7,
      replicas: [1],
      isr: [1],
      offlineReplicas: [2],
    });
    await expect(metadataResponseV7.parse(data)).resolves.toBe(data);
  });

  it('throws on a topic-level error', async () => {
    const data = await metadataResponseV7.decode(encodeV7Response({ topicErrorCode: 3 }));
    await expect(metadataResponseV7.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
      topic: 'orders',
    });
  });
});
