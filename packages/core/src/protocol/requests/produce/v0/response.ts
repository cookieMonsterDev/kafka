import { Decoder } from '../../../decoder';
import { array, field, int16, int32, int64, object, string, type ResponseDefinition } from '../../../schema';
import { parseProduceResponse } from '../shared';

export interface ProduceResponseV0Body {
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
 * Produce Response (Version: 0) => [responses]
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code offset
 *       partition => INT32
 *       error_code => INT16
 *       offset => INT64
 *
 * The wire field is `offset`; it is exposed as `baseOffset` so the producer stack stays
 * consistent with v3+. Missing later fields default to -1 / 0.
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
]);

export const produceResponseV0: ResponseDefinition<ProduceResponseV0Body> = {
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
      throttleTime: 0,
    };
  },
  parse: parseProduceResponse,
};
