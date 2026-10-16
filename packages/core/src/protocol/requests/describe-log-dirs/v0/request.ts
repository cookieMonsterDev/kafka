import { array, defineRequest, field, int32, nullPreservingArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeLogDirsTopic {
  topic: string;
  partitions: number[];
}

export interface DescribeLogDirsRequestV0Fields {
  topics: DescribeLogDirsTopic[] | null;
}

/**
 * DescribeLogDirs Request (Version: 0) => [topics]
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => INT32
 *
 * `topics` is nullable; null describes every replica, while an empty array describes none.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = object([field('topic', string), field('partitions', array(int32))]);
export const requestSchema = object([field('topics', nullPreservingArray(topicSchema))]);

export const describeLogDirsRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeLogDirs,
  apiVersion: 0,
  apiName: 'DescribeLogDirs',
  schema: requestSchema,
});
