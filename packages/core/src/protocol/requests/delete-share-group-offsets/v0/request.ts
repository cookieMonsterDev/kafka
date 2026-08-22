import { compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DeleteShareGroupOffsetsRequestTopic {
  topicName: string;
}

export interface DeleteShareGroupOffsetsRequestV0Fields {
  groupId: string;
  topics: DeleteShareGroupOffsetsRequestTopic[];
}

/**
 * DeleteShareGroupOffsets Request (Version: 0) => group_id [topics] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   topics => topic_name TAG_BUFFER
 *     topic_name => COMPACT_STRING
 *
 * Flexible from v0 (KIP-932).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topicName', compactString)]);
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('topics', compactArray(topicSchema)),
]);

export const deleteShareGroupOffsetsRequestV0 = defineRequest({
  apiKey: API_KEYS.DeleteShareGroupOffsets,
  apiVersion: 0,
  apiName: 'DeleteShareGroupOffsets',
  schema: requestSchema,
});
