import { Decoder } from '../../../decoder';
import { array, field, int16, int32, int64, object, string, type ResponseDefinition } from '../../../schema';
import { parseProduceResponse } from '../shared';

export interface ProduceResponseV2Body {
  topics: {
    topicName: string;
    partitions: {
      partition: number;
      errorCode: number;
      baseOffset: bigint;
      logAppendTime: bigint;
      logStartOffset: bigint;
    }[];
  }[];
  throttleTime: number;
}

/**
 * Produce Response (Version: 2) => [responses] throttle_time_ms
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code offset timestamp
 *       partition => INT32
 *       error_code => INT16
 *       offset => INT64
 *       timestamp => INT64
 *   throttle_time_ms => INT32
 *
 * Wire `offset`/`timestamp` are exposed as `baseOffset`/`logAppendTime`.
 */
const bodySchema = object([
  field(
    'topics',
    array(
      object([
        field('topicName', string),
        field(
          'partitions',
          array(
            object([
              field('partition', int32),
              field('errorCode', int16),
              field('baseOffset', int64),
              field('logAppendTime', int64),
            ]),
          ),
        ),
      ]),
    ),
  ),
  field('throttleTime', int32),
]);

export const produceResponseV2: ResponseDefinition<ProduceResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return {
      topics: decoded.topics.map((topic) => ({
        topicName: topic.topicName,
        partitions: topic.partitions.map((partition) => ({
          ...partition,
          logStartOffset: -1n,
        })),
      })),
      throttleTime: decoded.throttleTime,
    };
  },
  parse: parseProduceResponse,
};
