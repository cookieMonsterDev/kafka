import { Decoder } from '../../../decoder';
import { compactArray, field, flexibleObject, int32, uuid } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { requestedTopicName } from '../../topic-ids';
import { partitionSchema, txnOffsetCommitResponseV3, type TxnOffsetCommitResponseV3Body } from '../v3/response';

export interface TxnOffsetCommitResponseV6Body extends TxnOffsetCommitResponseV3Body {
  topics: (TxnOffsetCommitResponseV3Body['topics'][number] & { topicId: Buffer })[];
}

const topicSchema = flexibleObject([field('topicId', uuid), field('partitions', compactArray(partitionSchema))]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('topics', compactArray(topicSchema))]);

/**
 * TxnOffsetCommit Response (Version: 6) => throttle_time_ms [topics] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   topics => topic_id [partitions] TAG_BUFFER
 *     topic_id => UUID
 *     partitions => partition_index error_code TAG_BUFFER
 *       partition_index => INT32
 *       error_code => INT16
 *
 * v5 with topic ids in place of topic names (KIP-1319). May also return GROUP_ID_NOT_FOUND,
 * STALE_MEMBER_EPOCH and UNKNOWN_TOPIC_ID, where older versions answer ILLEGAL_GENERATION for the
 * first two. `decode` restores each topic's name from the request's topics, so callers stay
 * name-based.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export function txnOffsetCommitResponseV6(
  requestedTopics: readonly { topic: string; topicId?: Buffer | undefined }[] = [],
): ResponseDefinition<TxnOffsetCommitResponseV6Body> {
  return {
    decode: async (rawData) => {
      const { throttleTime, topics } = bodySchema.read(new Decoder(rawData));
      return {
        throttleTime: 0,
        clientSideThrottleTime: throttleTime,
        topics: topics.map((topic, index) => ({
          topic: requestedTopicName(topic.topicId, index, requestedTopics),
          ...topic,
        })),
      };
    },
    parse: async (data) => {
      await txnOffsetCommitResponseV3.parse(data);
      return data;
    },
  };
}
