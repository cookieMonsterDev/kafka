import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  field,
  flexibleObject,
  int16,
  int32,
  nullableStruct,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { heartbeatTopicPartitionsSchema, type ShareGroupHeartbeatTopicPartitions } from '../shared';

export interface ShareGroupHeartbeatAssignment {
  topicPartitions: ShareGroupHeartbeatTopicPartitions[];
}

export interface ShareGroupHeartbeatResponseV1Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
  memberId: string | null;
  memberEpoch: number;
  heartbeatIntervalMs: number;
  assignment: ShareGroupHeartbeatAssignment | null;
}

/**
 * ShareGroupHeartbeat Response (Version: 1) => throttle_time_ms error_code error_message
 *                                                 member_id member_epoch heartbeat_interval_ms
 *                                                 assignment TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   member_id => COMPACT_NULLABLE_STRING
 *   member_epoch => INT32
 *   heartbeat_interval_ms => INT32
 *   assignment => NULLABLE_STRUCT [topic_partitions] TAG_BUFFER
 *     topic_partitions => topic_id [partitions] TAG_BUFFER
 *       topic_id => UUID
 *       partitions => INT32
 *
 * Flexible from v0. Assignment is a nullable struct (INT8 marker -1/1), not a compact array.
 * Quota timing follows KIP-219: the decoded throttle is exposed as `clientSideThrottleTime`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const assignmentSchema = flexibleObject([field('topicPartitions', compactArray(heartbeatTopicPartitionsSchema))]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('memberId', compactNullableString),
  field('memberEpoch', int32),
  field('heartbeatIntervalMs', int32),
  field('assignment', nullableStruct(assignmentSchema)),
]);

export const shareGroupHeartbeatResponseV1: ResponseDefinition<ShareGroupHeartbeatResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
