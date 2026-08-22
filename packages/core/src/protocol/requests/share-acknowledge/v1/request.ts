import {
  compactArray,
  compactNullableString,
  defineRequest,
  field,
  flexibleObject,
  int32,
  uuid,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { acknowledgementBatchSchema } from '../shared';

export interface ShareAcknowledgeRequestPartition {
  partitionIndex: number;
  acknowledgementBatches: ShareAcknowledgeRequestAcknowledgementBatch[];
}

export interface ShareAcknowledgeRequestAcknowledgementBatch {
  firstOffset: bigint;
  lastOffset: bigint;
  acknowledgeTypes: number[];
}

export interface ShareAcknowledgeRequestTopic {
  topicId: Buffer;
  partitions: ShareAcknowledgeRequestPartition[];
}

export interface ShareAcknowledgeRequestV1Fields {
  groupId: string | null;
  memberId: string | null;
  shareSessionEpoch: number;
  topics: ShareAcknowledgeRequestTopic[];
}

/**
 * ShareAcknowledge Request (Version: 1) => group_id member_id share_session_epoch [topics] TAG_BUFFER
 *   group_id => COMPACT_NULLABLE_STRING
 *   member_id => COMPACT_NULLABLE_STRING
 *   share_session_epoch => INT32
 *   topics => topic_id [partitions] TAG_BUFFER
 *     topic_id => UUID
 *     partitions => partition_index [acknowledgement_batches] TAG_BUFFER
 *       partition_index => INT32
 *       acknowledgement_batches => first_offset last_offset [acknowledge_types] TAG_BUFFER
 *
 * Flexible from v0 (KIP-932). Renew acknowledgements are v2+ and omitted.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([
  field('partitionIndex', int32),
  field('acknowledgementBatches', compactArray(acknowledgementBatchSchema)),
]);
const topicSchema = flexibleObject([field('topicId', uuid), field('partitions', compactArray(partitionSchema))]);
export const requestSchema = flexibleObject([
  field('groupId', compactNullableString),
  field('memberId', compactNullableString),
  field('shareSessionEpoch', int32),
  field('topics', compactArray(topicSchema)),
]);

export const shareAcknowledgeRequestV1 = defineRequest({
  apiKey: API_KEYS.ShareAcknowledge,
  apiVersion: 1,
  apiName: 'ShareAcknowledge',
  schema: requestSchema,
});

export type { ShareAcknowledgeRequestAcknowledgementBatch as ShareAcknowledgeBatch };
