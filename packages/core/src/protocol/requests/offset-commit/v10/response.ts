import { Decoder } from '../../../decoder';
import { compactArray, field, flexibleObject, int32, uuid } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { requestedTopicName } from '../../topic-ids';
import { checkOffsetCommitErrors, type OffsetCommitTopicOptions } from '../shared';
import { partitionSchema, type OffsetCommitResponseV8Body } from '../v8/response';

export interface OffsetCommitResponseV10Body extends OffsetCommitResponseV8Body {
  responses: (OffsetCommitResponseV8Body['responses'][number] & { topicId: Buffer })[];
}

const topicSchema = flexibleObject([field('topicId', uuid), field('partitions', compactArray(partitionSchema))]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('responses', compactArray(topicSchema))]);

/**
 * OffsetCommit Response (Version: 10) => throttle_time_ms [topics] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   topics => topic_id [partitions] TAG_BUFFER
 *     topic_id => UUID
 *     partitions => partition_index error_code TAG_BUFFER
 *       partition_index => INT32
 *       error_code => INT16
 *
 * v8 with topic ids in place of topic names (KIP-848, KIP-516). `decode` restores each topic's
 * name from the request's topics, so callers stay name-based.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export function offsetCommitResponseV10(
  requestedTopics: readonly OffsetCommitTopicOptions[] = [],
): ResponseDefinition<OffsetCommitResponseV10Body> {
  return {
    decode: async (rawData) => {
      const { throttleTime, responses } = bodySchema.read(new Decoder(rawData));
      return {
        throttleTime: 0,
        clientSideThrottleTime: throttleTime,
        responses: responses.map((topic, index) => ({
          topic: requestedTopicName(topic.topicId, index, requestedTopics),
          ...topic,
        })),
      };
    },
    parse: async (data) => {
      checkOffsetCommitErrors(data);
      return data;
    },
  };
}
