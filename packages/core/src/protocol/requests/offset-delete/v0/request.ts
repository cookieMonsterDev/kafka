import { array, defineRequest, field, int32, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface OffsetDeletePartition {
  partitionIndex: number;
}

export interface OffsetDeleteTopic {
  name: string;
  partitions: OffsetDeletePartition[];
}

export interface OffsetDeleteRequestV0Fields {
  groupId: string;
  topics: OffsetDeleteTopic[];
}

/**
 * OffsetDelete Request (Version: 0) => group_id [topics]
 *   group_id => STRING
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index
 *       partition_index => INT32
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = object([field('partitionIndex', int32)]);
const topicSchema = object([field('name', string), field('partitions', array(partitionSchema))]);
export const requestSchema = object([field('groupId', string), field('topics', array(topicSchema))]);

export const offsetDeleteRequestV0 = defineRequest({
  apiKey: API_KEYS.OffsetDelete,
  apiVersion: 0,
  apiName: 'OffsetDelete',
  schema: requestSchema,
});
