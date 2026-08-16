import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { metadataResponseV0 } from './response';

function buildResponse({ topicErrorCode = 0, partitionErrorCode = 0 } = {}): Buffer {
  return new Encoder()
    .writeArray([]) // brokers
    .writeInt32(1) // topicMetadata length
    .writeInt16(topicErrorCode)
    .writeString('my-topic')
    .writeInt32(1) // partitionMetadata length
    .writeInt16(partitionErrorCode)
    .writeInt32(0) // partitionId
    .writeInt32(1) // leader
    .writeArray([1], 'int32') // replicas
    .writeArray(
      [1],
      'int32',
    ) // isr
  .buffer;
}

describe('protocol/requests/metadata/v0/response', () => {
  it('decodes brokers and nested topic/partition metadata', async () => {
    const data = await metadataResponseV0.decode(buildResponse());

    expect(data).toEqual({
      brokers: [],
      topicMetadata: [
        {
          topicErrorCode: 0,
          topic: 'my-topic',
          partitionMetadata: [{ partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1] }],
        },
      ],
    });
    await expect(metadataResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('throws on a topic-level error', async () => {
    const data = await metadataResponseV0.decode(buildResponse({ topicErrorCode: 3 }));
    await expect(metadataResponseV0.parse(data)).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
      topic: 'my-topic',
      message: expect.stringContaining('topic: my-topic'),
    });
  });

  it('throws on a partition-level error', async () => {
    const data = await metadataResponseV0.decode(buildResponse({ partitionErrorCode: 5 }));
    await expect(metadataResponseV0.parse(data)).rejects.toThrow();
  });
});
