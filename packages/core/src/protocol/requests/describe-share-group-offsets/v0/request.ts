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

export interface DescribeShareGroupOffsetsRequestTopic {
  topicName: string;
  partitions: number[];
}

export interface DescribeShareGroupOffsetsRequestGroup {
  groupId: string;
  topics: DescribeShareGroupOffsetsRequestTopic[] | null;
}

export interface DescribeShareGroupOffsetsRequestV0Fields {
  groups: DescribeShareGroupOffsetsRequestGroup[];
}

/**
 * DescribeShareGroupOffsets Request (Version: 0) => [groups] TAG_BUFFER
 *   groups => group_id [topics] TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     topics => topic_name [partitions] TAG_BUFFER
 *       topic_name => COMPACT_STRING
 *       partitions => INT32
 *
 * Flexible from v0 (KIP-932). Version 1 of the request is the same as version 0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topicName', compactString), field('partitions', compactArray(int32))]);
const groupSchema = flexibleObject([
  field('groupId', compactString),
  field('topics', compactNullableArray(topicSchema)),
]);
export const requestSchema = flexibleObject([field('groups', compactArray(groupSchema))]);

export const describeShareGroupOffsetsRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeShareGroupOffsets,
  apiVersion: 0,
  apiName: 'DescribeShareGroupOffsets',
  schema: requestSchema,
});
