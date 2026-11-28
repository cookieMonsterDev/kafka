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
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { DeleteGroupsResponseV2Body } from '../v2/response';

export type DeleteGroupsResponseV3Body = DeleteGroupsResponseV2Body;

/**
 * DeleteGroups Response (Version: 3) => throttle_time_ms [results] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   results => group_id error_code error_message TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *
 * Adds `error_message` on each result, which replaces the generic message of that result's
 * `.error` when the broker sends one, such as the cause of GROUP_DELETION_FAILED. `parse` never
 * throws, matching v0. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
const resultSchema = flexibleObject([
  field('groupId', compactString),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('results', compactArray(resultSchema))]);

export const deleteGroupsResponseV3: ResponseDefinition<DeleteGroupsResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    const results = decoded.results.map(({ groupId, errorCode, errorMessage }) =>
      failure(errorCode)
        ? { groupId, errorCode, error: createErrorFromCode(errorCode, { message: errorMessage }) }
        : { groupId, errorCode },
    );
    return { throttleTime: 0, clientSideThrottleTime: decoded.throttleTime, results };
  },
  parse: async (data) => data,
};
