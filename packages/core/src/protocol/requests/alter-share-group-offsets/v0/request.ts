import { compactArray, compactString, defineRequest, field, flexibleObject, int32, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface AlterShareGroupOffsetsRequestPartition {
  partitionIndex: number;
  startOffset: bigint;
}

export interface AlterShareGroupOffsetsRequestTopic {
  topicName: string;
  partitions: AlterShareGroupOffsetsRequestPartition[];
}

export interface AlterShareGroupOffsetsRequestV0Fields {
  groupId: string;
  topics: AlterShareGroupOffsetsRequestTopic[];
}

/**
 * AlterShareGroupOffsets Request (Version: 0) => group_id [topics] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   topics => topic_name [partitions] TAG_BUFFER
 *     topic_name => COMPACT_STRING
 *     partitions => partition_index start_offset TAG_BUFFER
 *       partition_index => INT32
 *       start_offset => INT64
 *
 * Flexible from v0 (KIP-932).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([field('partitionIndex', int32), field('startOffset', int64)]);
const topicSchema = flexibleObject([
  field('topicName', compactString),
  field('partitions', compactArray(partitionSchema)),
]);
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('topics', compactArray(topicSchema)),
]);

export const alterShareGroupOffsetsRequestV0 = defineRequest({
  apiKey: API_KEYS.AlterShareGroupOffsets,
  apiVersion: 0,
  apiName: 'AlterShareGroupOffsets',
  schema: requestSchema,
});
