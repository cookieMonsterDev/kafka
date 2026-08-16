import { KafkaDeleteTopicRecordsError } from '../../../../errors';
import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { DeleteRecordsTopic } from '../v0/request';

export interface DeleteRecordsResponseV2Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  topics: {
    topic: string;
    partitions: { partition: number; lowWatermark: bigint; errorCode: number }[];
  }[];
}

/**
 * DeleteRecords Response (Version: 2) => throttle_time_ms [topics] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index low_watermark error_code TAG_BUFFER
 *       partition_index => INT32
 *       low_watermark => INT64
 *       error_code => INT16
 *
 * First flexible version (KIP-482). Same fields as v0/v1. Factory still takes `{ topics }` so
 * parse can attach request offsets to {@link KafkaDeleteTopicRecordsError}. Quota timing follows
 * v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([
  field('partition', int32),
  field('lowWatermark', int64),
  field('errorCode', int16),
]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('topics', compactArray(topicSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

export function deleteRecordsResponseV2(requestOptions: {
  topics: readonly DeleteRecordsTopic[];
}): ResponseDefinition<DeleteRecordsResponseV2Body> {
  return {
    decode: async (rawData) => {
      const decoded = bodySchema.read(new Decoder(rawData));
      return {
        throttleTime: 0,
        clientSideThrottleTime: decoded.throttleTime,
        topics: [...decoded.topics].sort(topicNameComparator),
      };
    },
    parse: async (data) => {
      const topicsWithErrors = data.topics
        .map(({ partitions }) => ({ partitionsWithErrors: partitions.filter(({ errorCode }) => failure(errorCode)) }))
        .filter(({ partitionsWithErrors }) => partitionsWithErrors.length > 0);

      if (topicsWithErrors.length > 0) {
        const responseTopic = data.topics[0];
        const requestTopic = requestOptions.topics[0];
        const partitionsWithErrors = topicsWithErrors[0]?.partitionsWithErrors;
        if (responseTopic && requestTopic && partitionsWithErrors) {
          throw new KafkaDeleteTopicRecordsError({
            partitions: partitionsWithErrors.map(({ partition, errorCode }) => ({
              partition,
              error: createErrorFromCode(errorCode),
              offset: requestTopic.partitions.find((p) => p.partition === partition)?.offset,
            })),
          });
        }
      }

      return data;
    },
  };
}
