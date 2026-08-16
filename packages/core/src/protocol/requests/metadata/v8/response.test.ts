import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { AUTHORIZED_OPERATIONS_OMITTED } from '../shared';
import { metadataResponseV8 } from './response';

function encodeV8Response({
  throttleTime = 0,
  topicErrorCode = 0,
  clusterAuthorizedOperations = AUTHORIZED_OPERATIONS_OMITTED,
  topicAuthorizedOperations = AUTHORIZED_OPERATIONS_OMITTED,
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
    .writeInt16(0)
    .writeInt32(0)
    .writeInt32(1)
    .writeInt32(4)
    .writeArray([1], 'int32')
    .writeArray([1], 'int32')
    .writeArray([], 'int32')
    .writeInt32(topicAuthorizedOperations)
    .writeInt32(clusterAuthorizedOperations).buffer;
}

describe('protocol/requests/metadata/v8/response', () => {
  it('decodes clusterAuthorizedOperations after topics', async () => {
    const data = await metadataResponseV8.decode(
      encodeV8Response({ throttleTime: 9, clusterAuthorizedOperations: 32 }),
    );

    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(9);
    expect(data.clusterAuthorizedOperations).toBe(32);
    expect(data.topicMetadata[0]?.topicAuthorizedOperations).toBe(AUTHORIZED_OPERATIONS_OMITTED);
    expect(data.topicMetadata[0]?.partitionMetadata[0]?.offlineReplicas).toEqual([]);
    await expect(metadataResponseV8.parse(data)).resolves.toBe(data);
  });

  it('throws on a topic-level error', async () => {
    const data = await metadataResponseV8.decode(encodeV8Response({ topicErrorCode: 3 }));
    await expect(metadataResponseV8.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
      topic: 'orders',
    });
  });
});
