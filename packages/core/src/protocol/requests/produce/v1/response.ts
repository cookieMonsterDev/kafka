import { Decoder } from '../../../decoder';
import { array, field, int16, int32, int64, object, string, type ResponseDefinition } from '../../../schema';
import { parseProduceResponse } from '../shared';
import type { ProduceResponseV0Body } from '../v0/response';

export type ProduceResponseV1Body = ProduceResponseV0Body;

/**
 * Produce Response (Version: 1) => [responses] throttle_time_ms
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code offset
 *       partition => INT32
 *       error_code => INT16
 *       offset => INT64
 *   throttle_time_ms => INT32
 */
const bodySchema = object([
  field(
    'topics',
    array(
      object([
        field('topicName', string),
        field(
          'partitions',
          array(object([field('partition', int32), field('errorCode', int16), field('baseOffset', int64)])),
        ),
      ]),
    ),
  ),
  field('throttleTime', int32),
]);

export const produceResponseV1: ResponseDefinition<ProduceResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return {
      topics: decoded.topics.map((topic) => ({
        topicName: topic.topicName,
        partitions: topic.partitions.map((partition) => ({
          ...partition,
          logAppendTime: -1n,
          logStartOffset: -1n,
        })),
      })),
      throttleTime: decoded.throttleTime,
    };
  },
  parse: parseProduceResponse,
};
