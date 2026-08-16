import { KafkaDeleteTopicRecordsError } from '../../../../errors';
import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import type { ResponseDefinition } from '../../../schema';
import { array, field, int16, int32, int64, object, string } from '../../../schema';
import type { DeleteRecordsTopic } from './request';

export interface DeleteRecordsResponseV0Body {
  throttleTime: number;
  topics: {
    topic: string;
    partitions: { partition: number; lowWatermark: bigint; errorCode: number }[];
  }[];
}

/**
 * DeleteRecords Response (Version: 0) => throttle_time_ms [topics]
 *  throttle_time_ms => INT32
 *  topics => name [partitions]
 *    name => STRING
 *    partitions => partition low_watermark error_code
 *      partition => INT32
 *      low_watermark => INT64
 *      error_code => INT16
 */
const partitionSchema = object([field('partition', int32), field('lowWatermark', int64), field('errorCode', int16)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const bodySchema = object([field('throttleTime', int32), field('topics', array(topicSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

/**
 * Factory over the request's per-partition offsets so parse can attach them to
 * {@link KafkaDeleteTopicRecordsError}.
 */
export function deleteRecordsResponseV0(requestOptions: {
  topics: readonly DeleteRecordsTopic[];
}): ResponseDefinition<DeleteRecordsResponseV0Body> {
  return {
    decode: async (rawData) => {
      const decoded = bodySchema.read(new Decoder(rawData));
      return { ...decoded, topics: [...decoded.topics].sort(topicNameComparator) };
    },
    parse: async (data) => {
      const topicsWithErrors = data.topics
        .map(({ partitions }) => ({ partitionsWithErrors: partitions.filter(({ errorCode }) => failure(errorCode)) }))
        .filter(({ partitionsWithErrors }) => partitionsWithErrors.length > 0);

      if (topicsWithErrors.length > 0) {
        // Only ever one topic is requested at a time, so the first entry is the only entry.
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
