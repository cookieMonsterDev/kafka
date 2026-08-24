import { Decoder } from '../../../decoder';
import { compactArray, field, flexibleObject, int32, object, uuid, type ResponseDefinition } from '../../../schema';
import {
  parseProduceResponse,
  readProduceResponseNodeEndpoints,
  resolveProduceTopicName,
  type ProduceRequestOptions,
} from '../shared';
import { producePartitionSchemaV9, type ProduceResponseV9Body } from '../v9/response';

export interface ProduceResponseV13Body extends ProduceResponseV9Body {
  topics: (ProduceResponseV9Body['topics'][number] & { topicId: Buffer })[];
}

const bodySchema = object([
  field(
    'topics',
    compactArray(flexibleObject([field('topicId', uuid), field('partitions', compactArray(producePartitionSchemaV9))])),
  ),
  field('throttleTime', int32),
]);

/**
 * Produce Response (Version: 13) => [responses] throttle_time_ms TAG_BUFFER
 *   responses => topic_id [partition_responses] TAG_BUFFER
 *     topic_id => UUID
 *     partition_responses => partition error_code base_offset log_append_time log_start_offset
 *                            [record_errors] error_message TAG_BUFFER
 *
 * Topic names are replaced with topic IDs (KIP-516). `decode` restores `topicName` from the
 * request's `topicData` so `RecordMetadata` stays name-based. CurrentLeader (partition tag 0)
 * and NodeEndpoints (response tag 0) are decoded (KIP-951), same as v9+.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function produceResponseV13(
  options: Pick<ProduceRequestOptions, 'topicData'> = { topicData: [] },
): ResponseDefinition<ProduceResponseV13Body> {
  return {
    decode: async (rawData) => {
      const decoder = new Decoder(rawData);
      const decoded = bodySchema.read(decoder);
      const nodeEndpoints = readProduceResponseNodeEndpoints(decoder);
      return {
        ...decoded,
        topics: decoded.topics.map((topic, index) => ({
          ...topic,
          topicName: resolveProduceTopicName(topic.topicId, index, options.topicData),
        })),
        throttleTime: 0,
        clientSideThrottleTime: decoded.throttleTime,
        nodeEndpoints,
      };
    },
    parse: parseProduceResponse,
  };
}
