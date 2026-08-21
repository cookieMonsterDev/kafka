import { KafkaAggregateError } from '../../../../errors';
import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  boolean,
  compactArray,
  compactNullableArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  nullableFlexibleObject,
  uuid,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DescribeTopicPartitionsResponseV0Cursor {
  topic: string;
  partitionIndex: number;
}

export interface DescribeTopicPartitionsResponseV0Partition {
  errorCode: number;
  partitionIndex: number;
  leader: number;
  leaderEpoch: number;
  replicas: number[];
  isr: number[];
  eligibleLeaderReplicas: number[] | null;
  lastKnownElr: number[] | null;
  offlineReplicas: number[];
}

export interface DescribeTopicPartitionsResponseV0Topic {
  errorCode: number;
  topic: string | null;
  topicId: Buffer;
  isInternal: boolean;
  partitions: DescribeTopicPartitionsResponseV0Partition[];
  topicAuthorizedOperations: number;
}

export interface DescribeTopicPartitionsResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  topics: DescribeTopicPartitionsResponseV0Topic[];
  nextCursor: DescribeTopicPartitionsResponseV0Cursor | null;
}

/**
 * DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor TAG_BUFFER
 *   topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER
 *     name => COMPACT_NULLABLE_STRING
 *     topic_id => UUID
 *     partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes]
 *                   [eligible_leader_replicas] [last_known_elr] [offline_replicas] TAG_BUFFER
 *
 * Flexible from v0. `nextCursor` is a nullable compact struct. Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([
  field('errorCode', int16),
  field('partitionIndex', int32),
  field('leader', int32),
  field('leaderEpoch', int32),
  field('replicas', compactArray(int32)),
  field('isr', compactArray(int32)),
  field('eligibleLeaderReplicas', compactNullableArray(int32)),
  field('lastKnownElr', compactNullableArray(int32)),
  field('offlineReplicas', compactArray(int32)),
]);

const topicSchema = flexibleObject([
  field('errorCode', int16),
  field('topic', compactNullableString),
  field('topicId', uuid),
  field('isInternal', boolean),
  field('partitions', compactArray(partitionSchema)),
  field('topicAuthorizedOperations', int32),
]);

const cursorSchema = nullableFlexibleObject([field('topic', compactString), field('partitionIndex', int32)]);

export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('topics', compactArray(topicSchema)),
  field('nextCursor', cursorSchema),
]);

export const describeTopicPartitionsResponseV0: ResponseDefinition<DescribeTopicPartitionsResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    const errors = [
      ...data.topics
        .filter(({ errorCode }) => failure(errorCode))
        .map(({ errorCode, topic }) => createErrorFromCode(errorCode, { topic: topic ?? undefined })),
      ...data.topics.flatMap(({ topic, partitions }) =>
        partitions
          .filter(({ errorCode }) => failure(errorCode))
          .map(({ errorCode, partitionIndex }) =>
            createErrorFromCode(errorCode, { topic: topic ?? undefined, partition: partitionIndex }),
          ),
      ),
    ];
    if (errors.length > 0) {
      throw new KafkaAggregateError('Errors describing topic partitions', errors);
    }
    return data;
  },
};
