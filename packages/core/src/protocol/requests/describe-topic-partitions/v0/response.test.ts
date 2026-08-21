import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { KafkaAggregateError } from '../../../../errors';
import { AUTHORIZED_OPERATIONS_OMITTED, ZERO_TOPIC_ID } from '../../metadata/shared';
import { describeTopicPartitionsResponseV0, responseSchema } from './response';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/describe-topic-partitions/v0/response', () => {
  it('decodes topics with topicId, ELR fields, and a next cursor', async () => {
    const value = {
      throttleTime: 7,
      topics: [
        {
          errorCode: 0,
          topic: 'orders',
          topicId,
          isInternal: false,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leader: 1,
              leaderEpoch: 4,
              replicas: [1, 2],
              isr: [1],
              eligibleLeaderReplicas: [1, 2],
              lastKnownElr: null,
              offlineReplicas: [],
            },
          ],
          topicAuthorizedOperations: AUTHORIZED_OPERATIONS_OMITTED,
        },
      ],
      nextCursor: { topic: 'orders', partitionIndex: 1 },
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeTopicPartitionsResponseV0.decode(encoder.buffer);

    expect(data).toEqual({
      ...value,
      throttleTime: 0,
      clientSideThrottleTime: 7,
    });
    await expect(describeTopicPartitionsResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('decodes a name-only topic with a zero topicId and a null next cursor', async () => {
    const value = {
      throttleTime: 0,
      topics: [
        {
          errorCode: 0,
          topic: 'orders',
          topicId: ZERO_TOPIC_ID,
          isInternal: false,
          partitions: [],
          topicAuthorizedOperations: AUTHORIZED_OPERATIONS_OMITTED,
        },
      ],
      nextCursor: null,
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeTopicPartitionsResponseV0.decode(encoder.buffer);

    expect(data.topics[0]?.topicId).toEqual(ZERO_TOPIC_ID);
    expect(data.nextCursor).toBeNull();
    await expect(describeTopicPartitionsResponseV0.parse(data)).resolves.toEqual({
      ...value,
      throttleTime: 0,
      clientSideThrottleTime: 0,
    });
  });

  it('rejects topic and partition errors', async () => {
    await expect(
      describeTopicPartitionsResponseV0.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        topics: [
          {
            errorCode: 3,
            topic: 'missing',
            topicId: ZERO_TOPIC_ID,
            isInternal: false,
            partitions: [],
            topicAuthorizedOperations: AUTHORIZED_OPERATIONS_OMITTED,
          },
        ],
        nextCursor: null,
      }),
    ).rejects.toBeInstanceOf(KafkaAggregateError);

    await expect(
      describeTopicPartitionsResponseV0.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        topics: [
          {
            errorCode: 0,
            topic: 'orders',
            topicId,
            isInternal: false,
            partitions: [
              {
                errorCode: 3,
                partitionIndex: 2,
                leader: -1,
                leaderEpoch: -1,
                replicas: [],
                isr: [],
                eligibleLeaderReplicas: null,
                lastKnownElr: null,
                offlineReplicas: [],
              },
            ],
            topicAuthorizedOperations: AUTHORIZED_OPERATIONS_OMITTED,
          },
        ],
        nextCursor: null,
      }),
    ).rejects.toBeInstanceOf(KafkaAggregateError);
  });
});
