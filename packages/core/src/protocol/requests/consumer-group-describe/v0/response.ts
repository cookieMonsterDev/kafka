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

export interface ConsumerGroupDescribeTopicPartitions {
  topicId: Buffer;
  topicName: string;
  partitions: number[];
}

export interface ConsumerGroupDescribeAssignment {
  topicPartitions: ConsumerGroupDescribeTopicPartitions[];
}

export interface ConsumerGroupDescribeMemberV0 {
  memberId: string;
  instanceId: string | null;
  rackId: string | null;
  memberEpoch: number;
  clientId: string;
  clientHost: string;
  subscribedTopicNames: string[];
  subscribedTopicRegex: string | null;
  assignment: ConsumerGroupDescribeAssignment;
  targetAssignment: ConsumerGroupDescribeAssignment;
}

export interface ConsumerGroupDescribeGroupV0 {
  errorCode: number;
  errorMessage: string | null;
  groupId: string;
  groupState: string;
  groupEpoch: number;
  assignmentEpoch: number;
  assignorName: string;
  members: ConsumerGroupDescribeMemberV0[];
  authorizedOperations: number;
}

export interface ConsumerGroupDescribeResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  groups: ConsumerGroupDescribeGroupV0[];
}

/**
 * ConsumerGroupDescribe Response (Version: 0) => throttle_time_ms [groups] TAG_BUFFER
 *
 * Assignment structs include topic id and topic name. Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicPartitionsSchema = flexibleObject([
  field('topicId', uuid),
  field('topicName', compactString),
  field('partitions', compactArray(int32)),
]);
const assignmentSchema = flexibleObject([field('topicPartitions', compactArray(topicPartitionsSchema))]);
export const memberSchemaV0 = flexibleObject([
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
]);
const groupSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('groupId', compactString),
  field('groupState', compactString),
  field('groupEpoch', int32),
  field('assignmentEpoch', int32),
  field('assignorName', compactString),
  field('members', compactArray(memberSchemaV0)),
  field('authorizedOperations', int32),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('groups', compactArray(groupSchema)),
]);

export const consumerGroupDescribeResponseV0: ResponseDefinition<ConsumerGroupDescribeResponseV0Body> = {
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
