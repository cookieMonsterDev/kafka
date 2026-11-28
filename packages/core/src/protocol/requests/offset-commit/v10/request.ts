import { Encoder } from '../../../encoder';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int32,
  uuid,
  type RequestDefinition,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requireTopicId } from '../../topic-ids';
import { withDefaultMetadata, type OffsetCommitTopicOptions } from '../shared';
import { partitionSchema } from '../v8/request';

const topicSchema = flexibleObject([field('topicId', uuid), field('partitions', compactArray(partitionSchema))]);

/**
 * OffsetCommit Request (Version: 10) => group_id generation_id_or_member_epoch member_id group_instance_id [topics] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   generation_id_or_member_epoch => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *   topics => topic_id [partitions] TAG_BUFFER
 *     topic_id => UUID
 *     partitions => partition_index committed_offset committed_leader_epoch committed_metadata TAG_BUFFER
 *       partition_index => INT32
 *       committed_offset => INT64
 *       committed_leader_epoch => INT32
 *       committed_metadata => COMPACT_NULLABLE_STRING
 *
 * v9 with topic ids in place of topic names (KIP-848, KIP-516). May return UNKNOWN_TOPIC_ID.
 * Takes each topic by name and id, and fails to encode a topic without a usable id.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('groupGenerationId', int32),
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('topics', compactArray(topicSchema)),
]);

export function offsetCommitRequestV10({
  topics,
  ...member
}: {
  groupId: string;
  groupGenerationId: number;
  memberId: string;
  groupInstanceId: string | null;
  topics: readonly OffsetCommitTopicOptions[];
}): RequestDefinition {
  return {
    apiKey: API_KEYS.OffsetCommit,
    apiVersion: 10,
    apiName: 'OffsetCommit',
    encode: () => {
      const encoder = new Encoder();
      requestSchema.write(encoder, {
        ...member,
        topics: withDefaultMetadata(topics).map((topic) => ({
          topicId: requireTopicId('OffsetCommit', 10, topic),
          partitions: topic.partitions,
        })),
      });
      return encoder;
    },
  };
}
