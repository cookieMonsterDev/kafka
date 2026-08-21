import {
  compactArray,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int32,
  nullableFlexibleObject,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

export const DEFAULT_RESPONSE_PARTITION_LIMIT = 2000;

export interface DescribeTopicPartitionsRequestV0Cursor {
  topic: string;
  partitionIndex: number;
}

export interface DescribeTopicPartitionsRequestV0Topic {
  topic: string;
}

export interface DescribeTopicPartitionsRequestV0Options {
  topics: DescribeTopicPartitionsRequestV0Topic[];
  responsePartitionLimit?: number;
  cursor?: DescribeTopicPartitionsRequestV0Cursor | null;
}

/**
 * DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor TAG_BUFFER
 *   topics => name TAG_BUFFER
 *     name => COMPACT_STRING
 *   response_partition_limit => INT32
 *   cursor => INT8 topic_name partition_index TAG_BUFFER
 *     topic_name => COMPACT_STRING
 *     partition_index => INT32
 *
 * API key 75 (KIP-966). Flexible from v0. Topics are identified by name; a null cursor
 * starts from the beginning. `responsePartitionLimit` defaults to 2000.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topic', compactString)]);
const cursorSchema = nullableFlexibleObject([field('topic', compactString), field('partitionIndex', int32)]);

export const requestSchema = flexibleObject([
  field('topics', compactArray(topicSchema)),
  field('responsePartitionLimit', int32),
  field('cursor', cursorSchema),
]);

const create = defineRequest({
  apiKey: API_KEYS.DescribeTopicPartitions,
  apiVersion: 0,
  apiName: 'DescribeTopicPartitions',
  schema: requestSchema,
});

export const describeTopicPartitionsRequestV0 = ({
  topics,
  responsePartitionLimit = DEFAULT_RESPONSE_PARTITION_LIMIT,
  cursor = null,
}: DescribeTopicPartitionsRequestV0Options) => create({ topics, responsePartitionLimit, cursor });
