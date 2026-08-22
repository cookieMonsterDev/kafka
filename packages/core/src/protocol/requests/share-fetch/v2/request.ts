import {
  boolean,
  compactArray,
  compactNullableString,
  defineRequest,
  field,
  flexibleObject,
  int32,
  int8,
  uuid,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { acknowledgementBatchSchema } from '../shared';

export interface ShareFetchRequestV2Partition {
  partitionIndex: number;
  acknowledgementBatches: { firstOffset: bigint; lastOffset: bigint; acknowledgeTypes: number[] }[];
}

export interface ShareFetchRequestV2Topic {
  topicId: Buffer;
  partitions: ShareFetchRequestV2Partition[];
}

export interface ShareFetchForgottenTopicV2 {
  topicId: Buffer;
  partitions: number[];
}

export interface ShareFetchRequestV2Fields {
  groupId: string | null;
  memberId: string | null;
  shareSessionEpoch: number;
  maxWaitMs: number;
  minBytes: number;
  maxBytes: number;
  maxRecords: number;
  batchSize: number;
  shareAcquireMode: number;
  isRenewAck: boolean;
  topics: ShareFetchRequestV2Topic[];
  forgottenTopics: ShareFetchForgottenTopicV2[];
}

/**
 * ShareFetch Request (Version: 2) adds ShareAcquireMode (KIP-1206) and IsRenewAck (KIP-1222)
 * after BatchSize. Version 1 remains the stable Kafka 4.1 layout.
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
  field('shareAcquireMode', int8),
  field('isRenewAck', boolean),
  field('topics', compactArray(topicSchema)),
  field('forgottenTopics', compactArray(forgottenTopicSchema)),
]);

export const shareFetchRequestV2 = defineRequest({
  apiKey: API_KEYS.ShareFetch,
  apiVersion: 2,
  apiName: 'ShareFetch',
  schema: requestSchema,
});
