import {
  compactArray,
  compactNullableString,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
  uuid,
  type ResponseDefinition,
} from '../../../schema';
import { parseProduceResponse, resolveProduceTopicName, type ProduceRequestOptions } from '../shared';
import type { ProduceResponseV9Body } from '../v9/response';

export interface ProduceResponseV13Body extends ProduceResponseV9Body {
  topics: (ProduceResponseV9Body['topics'][number] & { topicId: Buffer })[];
}

/**
 * Produce Response (Version: 13) => [responses] throttle_time_ms TAG_BUFFER
 *   responses => topic_id [partition_responses] TAG_BUFFER
 *     topic_id => UUID
 *     partition_responses => partition error_code base_offset log_append_time log_start_offset
 *                            [record_errors] error_message TAG_BUFFER
 *
 * Topic names are replaced with topic IDs (KIP-516). `decode` restores `topicName` from the
 * request's `topicData` so `RecordMetadata` stays name-based.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const bodySchema = flexibleObject([
  field(
    'topics',
    compactArray(
      flexibleObject([
        field('topicId', uuid),
        field(
          'partitions',
          compactArray(
            flexibleObject([
              field('partition', int32),
              field('errorCode', int16),
              field('baseOffset', int64),
              field('logAppendTime', int64),
              field('logStartOffset', int64),
              field(
                'recordErrors',
                compactArray(
                  flexibleObject([field('batchIndex', int32), field('batchIndexErrorMessage', compactNullableString)]),
                ),
              ),
              field('errorMessage', compactNullableString),
            ]),
          ),
        ),
      ]),
    ),
  ),
  field('throttleTime', int32),
]);

const raw = defineResponse({
  schema: bodySchema,
  parse: parseProduceResponse,
});

export function produceResponseV13(
  options: Pick<ProduceRequestOptions, 'topicData'> = { topicData: [] },
): ResponseDefinition<ProduceResponseV13Body> {
  return {
    decode: async (rawData) => {
      const decoded = await raw.decode(rawData);
      return {
        ...decoded,
        topics: decoded.topics.map((topic, index) => ({
          ...topic,
          topicName: resolveProduceTopicName(topic.topicId, index, options.topicData),
        })),
        throttleTime: 0,
        clientSideThrottleTime: decoded.throttleTime,
      };
    },
    parse: parseProduceResponse,
  };
}
