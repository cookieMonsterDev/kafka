import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  uuid,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface ShareGroupDescribeTopicPartitions {
  topicId: Buffer;
  topicName: string;
  partitions: number[];
}

export interface ShareGroupDescribeAssignment {
  topicPartitions: ShareGroupDescribeTopicPartitions[];
}

export interface ShareGroupDescribeMemberV1 {
  memberId: string;
  rackId: string | null;
  memberEpoch: number;
  clientId: string;
  clientHost: string;
  subscribedTopicNames: string[];
  assignment: ShareGroupDescribeAssignment;
}

export interface ShareGroupDescribeGroupV1 {
  errorCode: number;
  errorMessage: string | null;
  groupId: string;
  groupState: string;
  groupEpoch: number;
  assignmentEpoch: number;
  assignorName: string;
  members: ShareGroupDescribeMemberV1[];
  authorizedOperations: number;
}

export interface ShareGroupDescribeResponseV1Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  groups: ShareGroupDescribeGroupV1[];
}

/**
 * ShareGroupDescribe Response (Version: 1) => throttle_time_ms [groups] TAG_BUFFER
 *
 * Members omit instanceId, subscribedTopicRegex, and targetAssignment compared to
 * ConsumerGroupDescribe. Assignment structs include topic id and topic name. Quota timing
 * follows KIP-219.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicPartitionsSchema = flexibleObject([
  field('topicId', uuid),
  field('topicName', compactString),
  field('partitions', compactArray(int32)),
]);
const assignmentSchema = flexibleObject([field('topicPartitions', compactArray(topicPartitionsSchema))]);
export const memberSchemaV1 = flexibleObject([
  field('memberId', compactString),
  field('rackId', compactNullableString),
  field('memberEpoch', int32),
  field('clientId', compactString),
  field('clientHost', compactString),
  field('subscribedTopicNames', compactArray(compactString)),
  field('assignment', assignmentSchema),
]);
const groupSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('groupId', compactString),
  field('groupState', compactString),
  field('groupEpoch', int32),
  field('assignmentEpoch', int32),
  field('assignorName', compactString),
  field('members', compactArray(memberSchemaV1)),
  field('authorizedOperations', int32),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('groups', compactArray(groupSchema)),
]);

export const shareGroupDescribeResponseV1: ResponseDefinition<ShareGroupDescribeResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    const failed = data.groups.find(({ errorCode }) => failure(errorCode));
    if (failed) throw createErrorFromCode(failed.errorCode);
    return data;
  },
};
