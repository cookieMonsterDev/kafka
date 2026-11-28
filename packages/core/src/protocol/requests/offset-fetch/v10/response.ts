import { Decoder } from '../../../decoder';
import { compactArray, compactString, field, flexibleObject, int16, int32, uuid } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { requestedTopicName } from '../../topic-ids';
import { checkOffsetFetchGroupErrors, type OffsetFetchGroupOptions } from '../shared';
import { partitionSchema, type OffsetFetchResponseV8Body } from '../v8/response';

export interface OffsetFetchResponseV10Body extends OffsetFetchResponseV8Body {
  groups: (OffsetFetchResponseV8Body['groups'][number] & {
    topics: (OffsetFetchResponseV8Body['groups'][number]['topics'][number] & { topicId: Buffer })[];
  })[];
}

const topicSchema = flexibleObject([field('topicId', uuid), field('partitions', compactArray(partitionSchema))]);
const groupSchema = flexibleObject([
  field('groupId', compactString),
  field('topics', compactArray(topicSchema)),
  field('errorCode', int16),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('groups', compactArray(groupSchema))]);

/**
 * OffsetFetch Response (Version: 10) => throttle_time_ms [groups] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   groups => group_id [topics] error_code TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     topics => topic_id [partitions] TAG_BUFFER
 *       topic_id => UUID
 *       partitions => partition_index committed_offset committed_leader_epoch metadata error_code TAG_BUFFER
 *     error_code => INT16
 *
 * v8 with topic ids in place of topic names (KIP-848, KIP-516). `decode` restores each topic's
 * name from the request's groups, so callers stay name-based.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export function offsetFetchResponseV10(
  requestedGroups: readonly OffsetFetchGroupOptions[] = [],
): ResponseDefinition<OffsetFetchResponseV10Body> {
  return {
    decode: async (rawData) => {
      const { throttleTime, groups } = bodySchema.read(new Decoder(rawData));
      return {
        throttleTime: 0,
        clientSideThrottleTime: throttleTime,
        groups: groups.map((group) => {
          const requestedTopics = requestedGroups.find(({ groupId }) => groupId === group.groupId)?.topics ?? [];
          return {
            ...group,
            topics: group.topics.map((topic, index) => ({
              topic: requestedTopicName(topic.topicId, index, requestedTopics),
              ...topic,
            })),
          };
        }),
      };
    },
    parse: async (data) => {
      checkOffsetFetchGroupErrors(data);
      return data;
    },
  };
}
