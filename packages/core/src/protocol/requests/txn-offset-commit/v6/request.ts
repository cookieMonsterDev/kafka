import { Encoder } from '../../../encoder';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
  uuid,
  type RequestDefinition,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requireTopicId } from '../../topic-ids';
import { partitionSchema } from '../v3/request';

const topicSchema = flexibleObject([field('topicId', uuid), field('partitions', compactArray(partitionSchema))]);

/**
 * TxnOffsetCommit Request (Version: 6) => transactional_id group_id producer_id producer_epoch
 *                                         generation_id_or_member_epoch member_id group_instance_id [topics] TAG_BUFFER
 *   transactional_id => COMPACT_STRING
 *   group_id => COMPACT_STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
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
 * v5 with topic ids in place of topic names (KIP-1319). `generationId` carries the generation of a
 * classic group or the member epoch of a consumer group. Takes each topic by name and id, and fails
 * to encode a topic without a usable id.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('transactionalId', compactString),
  field('groupId', compactString),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('generationId', int32),
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('topics', compactArray(topicSchema)),
]);

export function txnOffsetCommitRequestV6({
  topics,
  ...body
}: {
  transactionalId: string;
  groupId: string;
  producerId: bigint;
  producerEpoch: number;
  generationId: number;
  memberId: string;
  groupInstanceId: string | null;
  topics: readonly {
    topic: string;
    topicId?: Buffer | undefined;
    partitions: { partition: number; offset: bigint; leaderEpoch: number; metadata: string | null }[];
  }[];
}): RequestDefinition {
  return {
    apiKey: API_KEYS.TxnOffsetCommit,
    apiVersion: 6,
    apiName: 'TxnOffsetCommit',
    encode: () => {
      const encoder = new Encoder();
      requestSchema.write(encoder, {
        ...body,
        topics: topics.map((topic) => ({
          topicId: requireTopicId('TxnOffsetCommit', 6, topic),
          partitions: topic.partitions,
        })),
      });
      return encoder;
    },
  };
}
