import { compactArray, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeQuorumPartitionInput {
  partitionIndex: number;
}

export interface DescribeQuorumTopicInput {
  topicName: string;
  partitions: DescribeQuorumPartitionInput[];
}

export interface DescribeQuorumRequestV0Fields {
  topics: DescribeQuorumTopicInput[];
}

/**
 * DescribeQuorum Request (Version: 0) => [topics] TAG_BUFFER
 *   topics => topic_name [partitions] TAG_BUFFER
 *     topic_name => COMPACT_STRING
 *     partitions => partition_index TAG_BUFFER
 *       partition_index => INT32
 *
 * Flexible from v0. An empty `topics` array requests metadata quorum state (Java
 * `Admin.describeMetadataQuorum`).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([field('partitionIndex', int32)]);
const topicSchema = flexibleObject([
  field('topicName', compactString),
  field('partitions', compactArray(partitionSchema)),
]);
export const requestSchema = flexibleObject([field('topics', compactArray(topicSchema))]);

export const describeQuorumRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeQuorum,
  apiVersion: 0,
  apiName: 'DescribeQuorum',
  schema: requestSchema,
});
