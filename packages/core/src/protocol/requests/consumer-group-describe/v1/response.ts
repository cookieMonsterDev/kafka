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
  int8,
  uuid,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type {
  ConsumerGroupDescribeAssignment,
  ConsumerGroupDescribeMemberV0,
  ConsumerGroupDescribeResponseV0Body,
} from '../v0/response';

export interface ConsumerGroupDescribeMemberV1 extends ConsumerGroupDescribeMemberV0 {
  memberType: number;
}

export interface ConsumerGroupDescribeGroupV1 {
  errorCode: number;
  errorMessage: string | null;
  groupId: string;
  groupState: string;
  groupEpoch: number;
  assignmentEpoch: number;
  assignorName: string;
  members: ConsumerGroupDescribeMemberV1[];
  authorizedOperations: number;
}

export interface ConsumerGroupDescribeResponseV1Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  groups: ConsumerGroupDescribeGroupV1[];
}

export type { ConsumerGroupDescribeAssignment };

/**
 * ConsumerGroupDescribe Response (Version: 1) adds `memberType` after target assignment
 * (KIP-1099): -1 unknown, 0 classic, 1 consumer.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicPartitionsSchema = flexibleObject([
  field('topicId', uuid),
  field('topicName', compactString),
  field('partitions', compactArray(int32)),
]);
const assignmentSchema = flexibleObject([field('topicPartitions', compactArray(topicPartitionsSchema))]);
const memberSchemaV1 = flexibleObject([
  field('memberId', compactString),
  field('instanceId', compactNullableString),
  field('rackId', compactNullableString),
  field('memberEpoch', int32),
  field('clientId', compactString),
  field('clientHost', compactString),
  field('subscribedTopicNames', compactArray(compactString)),
  field('subscribedTopicRegex', compactNullableString),
  field('assignment', assignmentSchema),
  field('targetAssignment', assignmentSchema),
  field('memberType', int8),
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

export const consumerGroupDescribeResponseV1: ResponseDefinition<ConsumerGroupDescribeResponseV1Body> = {
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

export type { ConsumerGroupDescribeResponseV0Body };
