import {
  compactArray,
  compactNullableArray,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int32,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { DescribeLogDirsTopic } from '../v0/request';

export type { DescribeLogDirsTopic };

export interface DescribeLogDirsRequestV2Fields {
  topics: DescribeLogDirsTopic[] | null;
}

/**
 * DescribeLogDirs Request (Version: 2) => [topics] TAG_BUFFER
 *   topics => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => INT32
 *
 * Flexible-version API. Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 * `topics: null` describes every log dir.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(int32))]);
export const requestSchema = flexibleObject([field('topics', compactNullableArray(topicSchema))]);

export const describeLogDirsRequestV2 = defineRequest({
  apiKey: API_KEYS.DescribeLogDirs,
  apiVersion: 2,
  apiName: 'DescribeLogDirs',
  schema: requestSchema,
});
