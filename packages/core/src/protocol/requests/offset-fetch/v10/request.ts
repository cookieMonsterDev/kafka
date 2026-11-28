import { Encoder } from '../../../encoder';
import {
  boolean,
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
import { emptyAsNullCompactArray, type OffsetFetchTopicOptions } from '../shared';
import { partitionIndexesSchema } from '../v6/request';

const topicSchema = flexibleObject([field('topicId', uuid), field('partitions', partitionIndexesSchema)]);
const groupSchema = flexibleObject([
  field('groupId', compactString),
  field('memberId', compactNullableString),
  field('memberEpoch', int32),
  field('topics', emptyAsNullCompactArray(topicSchema)),
]);

/**
 * OffsetFetch Request (Version: 10) => [groups] require_stable TAG_BUFFER
 *   groups => group_id member_id member_epoch [topics] TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     member_id => COMPACT_NULLABLE_STRING
 *     member_epoch => INT32
 *     topics => topic_id [partition_indexes] TAG_BUFFER
 *       topic_id => UUID
 *       partition_indexes => INT32
 *   require_stable => BOOLEAN
 *
 * v9 with topic ids in place of topic names (KIP-848, KIP-516). May return UNKNOWN_TOPIC_ID.
 * Takes each topic by name and id, and fails to encode a topic without a usable id.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('groups', compactArray(groupSchema)),
  field('requireStable', boolean),
]);

export function offsetFetchRequestV10({
  groups,
  requireStable,
}: {
  groups: readonly {
    groupId: string;
    memberId: string | null;
    memberEpoch: number;
    topics: OffsetFetchTopicOptions[];
  }[];
  requireStable: boolean;
}): RequestDefinition {
  return {
    apiKey: API_KEYS.OffsetFetch,
    apiVersion: 10,
    apiName: 'OffsetFetch',
    encode: () => {
      const encoder = new Encoder();
      requestSchema.write(encoder, {
        groups: groups.map(({ topics, ...group }) => ({
          ...group,
          topics: topics.map((topic) => ({
            topicId: requireTopicId('OffsetFetch', 10, topic),
            partitions: topic.partitions,
          })),
        })),
        requireStable,
      });
      return encoder;
    },
  };
}
