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

export interface ShareFetchRequestPartition {
  partitionIndex: number;
  acknowledgementBatches: ShareFetchRequestAcknowledgementBatch[];
}

export interface ShareFetchRequestAcknowledgementBatch {
  firstOffset: bigint;
  lastOffset: bigint;
  acknowledgeTypes: number[];
}

export interface ShareFetchRequestTopic {
  topicId: Buffer;
  partitions: ShareFetchRequestPartition[];
}

export interface ShareFetchForgottenTopic {
  topicId: Buffer;
  partitions: number[];
}

export interface ShareFetchRequestV1Fields {
  groupId: string | null;
  memberId: string | null;
  shareSessionEpoch: number;
  maxWaitMs: number;
  minBytes: number;
  maxBytes: number;
  maxRecords: number;
  batchSize: number;
  topics: ShareFetchRequestTopic[];
  forgottenTopics: ShareFetchForgottenTopic[];
}

/**
 * ShareFetch Request (Version: 1) => group_id member_id share_session_epoch max_wait_ms min_bytes
 *                                    max_bytes max_records batch_size [topics]
 *                                    [forgotten_topics_data] TAG_BUFFER
 *   group_id => COMPACT_NULLABLE_STRING
 *   member_id => COMPACT_NULLABLE_STRING
 *   share_session_epoch => INT32
 *   max_wait_ms => INT32
 *   min_bytes => INT32
 *   max_bytes => INT32
 *   max_records => INT32
 *   batch_size => INT32
 *   topics => topic_id [partitions] TAG_BUFFER
 *     topic_id => UUID
 *     partitions => partition_index [acknowledgement_batches] TAG_BUFFER
 *       partition_index => INT32
 *       acknowledgement_batches => first_offset last_offset [acknowledge_types] TAG_BUFFER
 *         first_offset => INT64
 *         last_offset => INT64
 *         acknowledge_types => INT8
 *
 * Flexible from v0 (KIP-932). ShareAcquireMode and Renew acknowledgements are v2+ and omitted.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([
  field('partitionIndex', int32),
  field('acknowledgementBatches', compactArray(acknowledgementBatchSchema)),
]);
const topicSchema = flexibleObject([field('topicId', uuid), field('partitions', compactArray(partitionSchema))]);
const forgottenTopicSchema = flexibleObject([field('topicId', uuid), field('partitions', compactArray(int32))]);
export const requestSchema = flexibleObject([
  field('groupId', compactNullableString),
  field('memberId', compactNullableString),
  field('shareSessionEpoch', int32),
  field('maxWaitMs', int32),
  field('minBytes', int32),
  field('maxBytes', int32),
  field('maxRecords', int32),
  field('batchSize', int32),
  field('topics', compactArray(topicSchema)),
  field('forgottenTopics', compactArray(forgottenTopicSchema)),
]);

export const shareFetchRequestV1 = defineRequest({
  apiKey: API_KEYS.ShareFetch,
  apiVersion: 1,
  apiName: 'ShareFetch',
  schema: requestSchema,
});

export type { ShareFetchRequestAcknowledgementBatch as ShareFetchAcknowledgementBatch };
