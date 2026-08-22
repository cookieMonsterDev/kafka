import {
  boolean,
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

export interface ShareAcknowledgeRequestV2Partition {
  partitionIndex: number;
  acknowledgementBatches: { firstOffset: bigint; lastOffset: bigint; acknowledgeTypes: number[] }[];
}

export interface ShareAcknowledgeRequestV2Topic {
  topicId: Buffer;
  partitions: ShareAcknowledgeRequestV2Partition[];
}

export interface ShareAcknowledgeRequestV2Fields {
  groupId: string | null;
  memberId: string | null;
  shareSessionEpoch: number;
  isRenewAck: boolean;
  topics: ShareAcknowledgeRequestV2Topic[];
}

/**
 * ShareAcknowledge Request (Version: 2) adds IsRenewAck after ShareSessionEpoch (KIP-1222).
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
  field('isRenewAck', boolean),
  field('topics', compactArray(topicSchema)),
]);

export const shareAcknowledgeRequestV2 = defineRequest({
  apiKey: API_KEYS.ShareAcknowledge,
  apiVersion: 2,
  apiName: 'ShareAcknowledge',
  schema: requestSchema,
});
